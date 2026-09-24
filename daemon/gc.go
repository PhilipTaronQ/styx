package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/pb"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const gcrecPunch = 0

type (
	gcCtx struct {
		context.Context
		GcReq
		*GcResp
		tx *bbolt.Tx

		ib, cb, mb *bbolt.Bucket

		keepImage map[string]struct{}    // sph string
		keepSphps map[SphPrefix]struct{} // sph prefix
		keepDig   map[cdig.CDig]struct{}

		held      map[string]struct{}    // sph string with an operation in progress
		heldSphps map[SphPrefix]struct{} // sph prefixes of those and their manifests
	}

	// gcGuard tracks what in-progress operations need gc to leave alone.
	gcGuard struct {
		mu       sync.Mutex
		holds    map[string]int         // sph string -> number of operations using it
		reserved map[slabRange]struct{} // slab space allocated without slab keys yet
	}

	// blocks [start, end) of a slab
	slabRange struct {
		slabId     uint16
		start, end uint32
	}

	rewriteChunk struct {
		d cdig.CDig
		v []byte
	}
	locWithEnd struct {
		erofs.SlabLoc
		end uint32
		ok  bool
	}
)

func (s *Server) handleGcReq(ctx context.Context, r *GcReq) (*GcResp, error) {
	if s.p() == nil {
		return nil, mwErr(http.StatusPreconditionFailed, "styx is not initialized, call 'styx init --params=...'")
	}

	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	resp := &GcResp{
		DeleteImagesByState: make(map[pb.MountState]int),
		RemainImagesByState: make(map[pb.MountState]int),
	}
	g := &gcCtx{
		Context:   ctx,
		GcReq:     *r,
		GcResp:    resp,
		tx:        tx,
		ib:        tx.Bucket(imageBucket),
		cb:        tx.Bucket(chunkBucket),
		mb:        tx.Bucket(manifestBucket),
		keepImage: make(map[string]struct{}, 1000),
		keepSphps: make(map[SphPrefix]struct{}, 1000),
		keepDig:   make(map[cdig.CDig]struct{}, 100000),
		held:      make(map[string]struct{}),
		heldSphps: make(map[SphPrefix]struct{}),
	}

	// A mount, materialize or vaporize in progress keeps its image whatever the image's state,
	// and any chunk allocated for it so far, even one no manifest refers to yet. Read the
	// holds inside the write transaction: an operation that takes one later does its first
	// update after we commit.
	for _, sphStr := range s.gcHolds() {
		sph, _, err := ParseSph(sphStr)
		if err != nil {
			continue
		}
		manifestSph := makeManifestSph(sph)
		g.held[sphStr] = struct{}{}
		for _, sphp := range []SphPrefix{SphPrefixFromBytes(sph[:]), SphPrefixFromBytes(manifestSph[:])} {
			g.heldSphps[sphp] = struct{}{}
			g.keepSphps[sphp] = struct{}{}
		}
	}
	reserved := s.gcReserved()

	// use image bucket as roots
	var candidates []string
	candidateImgs := make(map[string]*pb.DbImage)
	ibcur := g.ib.Cursor()
	for k, v := ibcur.First(); k != nil; k, v = ibcur.Next() {
		img := &pb.DbImage{}
		if proto.Unmarshal(v, img) != nil {
			continue
		}
		sphStr := string(k)
		if _, held := g.held[sphStr]; g.GcByState[img.MountState] && !held {
			candidates = append(candidates, sphStr)
			candidateImgs[sphStr] = img
		} else if err := s.gcTraceImage(g, sphStr, img); err != nil {
			return nil, err
		}
	}
	// umount detaches lazily, so an Unmounted image can still be in use through open files.
	// Freeing its chunks would give those readers EIO, or SIGBUS for a running binary.
	busy := s.imagesInUse(candidates)
	for _, sphStr := range candidates {
		img := candidateImgs[sphStr]
		if !busy[sphStr] {
			g.DeleteImagesByState[img.MountState]++
			continue
		}
		log.Printf("gc: keeping %s, its image is still in use", img.StorePath)
		if err := s.gcTraceImage(g, sphStr, img); err != nil {
			return nil, err
		}
	}

	// find images to delete
	var delImages, delCatalogF, delCatalogR [][]byte
	for k, v := ibcur.First(); k != nil; k, v = ibcur.Next() {
		if _, ok := g.keepImage[string(k)]; ok {
			continue
		}
		delImages = append(delImages, bytes.Clone(k))
		var img pb.DbImage
		var sph Sph
		var err error
		var spName string
		if proto.Unmarshal(v, &img) != nil {
			continue
		} else if sph, spName, err = ParseSph(img.StorePath); err != nil || spName == "" {
			continue
		}
		fkey := bytes.Join([][]byte{[]byte(spName), []byte{0}, sph[:]}, nil)
		rkey := bytes.Clone(sph[:])
		manifestSph := makeManifestSph(sph)
		mfkey := bytes.Join([][]byte{[]byte(isManifestPrefix), []byte(spName), []byte{0}, manifestSph[:]}, nil)
		mrkey := bytes.Clone(manifestSph[:])
		delCatalogF = append(delCatalogF, fkey, mfkey)
		delCatalogR = append(delCatalogR, rkey, mrkey)
	}

	// find manifests to delete
	var delManifests [][]byte
	mbcur := g.mb.Cursor()
	for k, _ := mbcur.First(); k != nil; k, _ = mbcur.Next() {
		_, keep := g.keepImage[string(k)]
		if _, held := g.held[string(k)]; !keep && !held {
			delManifests = append(delManifests, bytes.Clone(k))
		}
	}

	// find all chunks to delete
	var delChunks []cdig.CDig
	var delLocs []erofs.SlabLoc
	var rewriteChunks []rewriteChunk
	cbcur := g.cb.Cursor()
	for k, v := cbcur.First(); k != nil; k, v = cbcur.Next() {
		d := cdig.FromBytes(k)
		sphps := sphpsFromLoc(v)
		if _, ok := g.keepDig[d]; !ok && !g.anyHeld(sphps) {
			delChunks = append(delChunks, d)
			delLocs = append(delLocs, loadLoc(v))
			continue
		}
		g.RemainHaveChunks++
		if g.keepAllSphps(sphps) {
			continue
		}
		newv := make([]byte, 6, len(v))
		copy(newv, v)
		for _, sphp := range sphps {
			if _, ok := g.keepSphps[sphp]; ok {
				newv = append(newv, sphp[:]...)
			}
		}
		rewriteChunks = append(rewriteChunks, rewriteChunk{d: d, v: newv})
	}

	g.DeleteImages = len(delImages)
	g.DeleteManifests = len(delManifests)
	g.DeleteChunks = len(delChunks)
	g.RemainImages = len(g.keepImage)
	g.RemainRefChunks = len(g.keepDig)
	g.RewriteChunks = len(rewriteChunks)

	log.Printf("gc: will delete:")
	log.Printf("gc:   %d images / %d manifests", len(delImages), len(delManifests))
	log.Printf("gc:   %d chunks", len(delChunks))
	log.Printf("gc: remaining:")
	log.Printf("gc:   %d images", len(g.keepImage))
	log.Printf("gc:   %d chunks (%d)", len(g.keepDig), g.RemainHaveChunks)
	log.Printf("gc: rewrite %d chunks", len(rewriteChunks))

	// dry run fast: just read
	if r.DryRunFast {
		return resp, tx.Rollback()
	}

	// figure out what to punch (can still roll back db)

	// sort for locality and coalescing gaps
	slices.SortFunc(delLocs, locCmp)

	sb := tx.Bucket(slabBucket)
	var lastBucket *bbolt.Bucket
	var lastKey uint16 = math.MaxUint16
	for _, l := range delLocs {
		lsb := lastBucket
		if l.SlabId != lastKey {
			lsb = sb.Bucket(slabKey(l.SlabId))
			lastBucket, lastKey = lsb, l.SlabId
		}
		if lsb == nil {
			return nil, fmt.Errorf("inconsistency: slab bucket %d not found", l.SlabId)
		}
		lsb.Delete(addrKey(l.Addr))
		lsb.Delete(addrKey(l.Addr | presentMask))
	}

	// after all locs have been deleted, find ranges to punch out
	gcb, err := tx.CreateBucketIfNotExists(gcstateBucket)
	if err != nil {
		return nil, err
	}
	var lastEnd uint32
	for _, l := range delLocs {
		lsb := lastBucket
		if l.SlabId != lastKey {
			lsb = sb.Bucket(slabKey(l.SlabId))
			lastBucket, lastKey, lastEnd = lsb, l.SlabId, 0
		}
		if lsb == nil {
			return nil, fmt.Errorf("inconsistency: slab bucket %d not found", l.SlabId)
		}
		var end uint32
		if k, _ := lsb.Cursor().Seek(addrKey(l.Addr)); k == nil {
			end = common.TruncU32(lsb.Sequence()) // end of slab
		} else if end = addrFromKey(k); end&presentMask != 0 {
			end = common.TruncU32(lsb.Sequence()) // also end of slab
		}
		end = clampToReserved(reserved, l, end)
		// we're looking at locs in order, so if we're deleting two consecutive chunks,
		// the first one should find the largest range to punch. if we found the same end we
		// can ignore it.
		if end == lastEnd {
			continue
		} else if end < lastEnd {
			return nil, errors.New("this shouldn't happen")
		}
		lastEnd = end
		gcb.Put(punchKey(l.SlabId, l.Addr, end), nil)
	}

	// read back from gc state bucket (pick up anything unfinished from previous gc)
	var punchLocs []locWithEnd
	gcbcur := gcb.Cursor()
	for k, _ := gcbcur.First(); k != nil; k, _ = gcbcur.Next() {
		switch k[0] {
		case gcrecPunch:
			rec := recFromPunchKey(k)
			punchLocs = append(punchLocs, rec)
			g.PunchBytes += int64(rec.end-rec.Addr) << s.blockShift
		}
	}

	g.PunchLocs = len(punchLocs)

	log.Printf("gc: will free:")
	log.Printf("gc:   %d ranges", len(punchLocs))
	log.Printf("gc:   %d bytes", g.PunchBytes)

	// dry run slow: roll back here
	if r.DryRunSlow {
		return resp, tx.Rollback()
	}

	// images
	for _, k := range delImages {
		g.ib.Delete(k)
	}
	// manifests
	for _, k := range delManifests {
		g.mb.Delete(k)
	}

	// chunks delete
	for _, d := range delChunks {
		g.cb.Delete(d[:])
	}

	// chunks rewrite
	for _, rew := range rewriteChunks {
		g.cb.Put(rew.d[:], rew.v)
	}

	// catalog
	cfb := tx.Bucket(catalogFBucket)
	for _, dcf := range delCatalogF {
		cfb.Delete(dcf)
	}
	crb := tx.Bucket(catalogRBucket)
	for _, dcr := range delCatalogR {
		crb.Delete(dcr)
	}

	// end first transaction here
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// the images' backing files point at the chunks we're about to punch
	delImageSphs := make([]string, len(delImages))
	for i, k := range delImages {
		delImageSphs[i] = string(k)
	}
	s.cullImageFiles(delImageSphs)

	if len(punchLocs) > 0 {
		// actually punch holes
		s.stateLock.Lock()
		readFds := make(map[uint16]int)
		for id, fds := range s.readfdBySlab {
			if fds.cacheFd > 0 {
				if dfd, err := unix.Dup(fds.cacheFd); err == nil {
					readFds[id] = dfd
				}
			}
		}
		s.stateLock.Unlock()

		defer func() {
			for _, fd := range readFds {
				unix.Close(fd)
			}
		}()

		for i, le := range punchLocs {
			if cfd, ok := readFds[le.SlabId]; ok {
				err := unix.Fallocate(
					cfd,
					unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
					int64(le.Addr)<<s.blockShift,
					int64(le.end-le.Addr)<<s.blockShift,
				)
				if err != nil {
					log.Printf("fallocate punch error (slab %d as fd %d, %d-%d): %s",
						le.SlabId, cfd, le.Addr, le.end, err,
					)
				}
				punchLocs[i].ok = err == nil
			}
		}

		// record success in second transaction
		_ = s.db.Update(func(tx *bbolt.Tx) error {
			gcb := tx.Bucket(gcstateBucket)
			for _, le := range punchLocs {
				if le.ok {
					gcb.Delete(punchKey(le.SlabId, le.Addr, le.end))
				}
			}
			return nil
		})
	}

	return resp, nil
}

func (s *Server) gcTraceImage(g *gcCtx, sphStr string, img *pb.DbImage) error {
	sph, _, err := ParseSph(sphStr)
	if err != nil {
		return err
	}
	sphPrefix := SphPrefixFromBytes(sph[:])
	manifestSph := makeManifestSph(sph)
	manifestSphPrefix := SphPrefixFromBytes(manifestSph[:])

	g.keepImage[sphStr] = struct{}{}
	g.keepSphps[sphPrefix] = struct{}{}
	g.keepSphps[manifestSphPrefix] = struct{}{}
	g.RemainImagesByState[img.MountState]++

	m, mdigs, err := s.getManifestLocal(g.tx, sphStr)
	if err != nil {
		if gcNeedsManifest(img.MountState) {
			return err
		}
		// A mount or materialize that failed, or is still running, before it had the whole
		// manifest. It has no image chunks yet, so there is nothing more to trace, but keep
		// any manifest chunks it got so a retry can use them.
		log.Printf("gc: keeping %s (%s) without its manifest: %v", sphStr, img.MountState, err)
		if v := g.mb.Get([]byte(sphStr)); v != nil {
			var sm pb.SignedMessage
			if proto.Unmarshal(v, &sm) == nil {
				for _, mdig := range cdig.FromSliceAlias(sm.Msg.GetDigests()) {
					g.keepDig[mdig] = struct{}{}
				}
			}
		}
		return nil
	}

	for _, mdig := range mdigs {
		g.keepDig[mdig] = struct{}{}
	}
	for _, e := range m.Entries {
		for _, d := range cdig.FromSliceAlias(e.Digests) {
			g.keepDig[d] = struct{}{}
		}
	}

	return nil
}

// Images in these states were mounted or materialized, so they have chunks, and gc must read
// their manifests to know which. An image in another state may have no manifest yet.
func gcNeedsManifest(st pb.MountState) bool {
	switch st {
	case pb.MountState_Mounted, pb.MountState_UnmountRequested, pb.MountState_Unmounted, pb.MountState_Materialized:
		return true
	default:
		return false
	}
}

var errNoDevnode = errors.New("cachefiles device not open")

// cachefilesFileCmds runs the cachefiles command cmd ("inuse" or "cull") on the backing file
// of each fsid. The kernel looks the name up in the calling thread's working directory, so
// this runs on a thread with a working directory of its own, which exits afterwards.
func (s *Server) cachefilesFileCmds(cmd string, fsids []string) []error {
	errs := make([]error, len(fsids))
	devfd := int(s.devnode.Load())
	if devfd == 0 || len(fsids) == 0 {
		for i := range errs {
			errs[i] = errNoDevnode
		}
		return errs
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Never unlocked, so the thread exits with this goroutine rather than run others
		// in our working directory.
		runtime.LockOSThread()
		if err := unix.Unshare(unix.CLONE_FS); err != nil {
			for i := range errs {
				errs[i] = fmt.Errorf("unshare: %w", err)
			}
			return
		}
		for i, fsid := range fsids {
			p := filepath.Join(s.cfg.CachePath, fscachePath(s.cfg.CacheDomain, fsid))
			if errs[i] = unix.Chdir(filepath.Dir(p)); errs[i] == nil {
				_, errs[i] = unix.Write(devfd, []byte(cmd+" "+filepath.Base(p)))
			}
		}
	}()
	<-done
	return errs
}

// imagesInUse returns which of the images cachefiles has open, from the kernel's own record
// (so it holds across daemon restarts). A file stays open until the last user of a lazily
// detached mount goes away, and for a moment after a plain unmount, since the kernel
// releases it asynchronously; so wait up to a second for busy ones to become free.
func (s *Server) imagesInUse(sphs []string) map[string]bool {
	busy := make(map[string]bool)
	deadline := time.Now().Add(time.Second)
	for len(sphs) > 0 {
		var again []string
		for i, err := range s.cachefilesFileCmds("inuse", sphs) {
			switch {
			case err == nil, errors.Is(err, unix.ENOENT), errors.Is(err, errNoDevnode):
				delete(busy, sphs[i])
			case errors.Is(err, unix.EBUSY):
				busy[sphs[i]] = true
				again = append(again, sphs[i])
			default:
				log.Printf("gc: can't tell if image %s is in use, keeping it: %v", sphs[i], err)
				busy[sphs[i]] = true
			}
		}
		if len(again) == 0 || time.Now().After(deadline) {
			break
		}
		sphs = again
		time.Sleep(20 * time.Millisecond)
	}
	return busy
}

// cullImageFiles removes the cachefiles backing files of images that aren't in use.
// cachefiles checks only an object's size before reusing its file, so a new image of the
// same size for the same store path would otherwise be served from the old image.
func (s *Server) cullImageFiles(sphs []string) {
	for i, err := range s.cachefilesFileCmds("cull", sphs) {
		if err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, errNoDevnode) {
			log.Printf("cull image file for %s: %v", sphs[i], err)
		}
	}
}

// holdForGc makes gc keep sphStr's image, manifest and chunks until the returned function is
// called. Take it before the operation's first db update.
func (s *Server) holdForGc(sphStr string) func() {
	g := &s.gcGuard
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.holds == nil {
		g.holds = make(map[string]int)
	}
	g.holds[sphStr]++
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.holds[sphStr]--
		if g.holds[sphStr] <= 0 {
			delete(g.holds, sphStr)
		}
	}
}

func (s *Server) gcHolds() []string {
	g := &s.gcGuard
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Collect(maps.Keys(g.holds))
}

// reserveForGc keeps gc from punching the given slab ranges until the returned function is
// called. Call it inside the transaction that allocates them.
func (s *Server) reserveForGc(ranges []slabRange) func() {
	g := &s.gcGuard
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reserved == nil {
		g.reserved = make(map[slabRange]struct{})
	}
	ranges = slices.DeleteFunc(ranges, func(r slabRange) bool { return r.start >= r.end })
	for _, r := range ranges {
		g.reserved[r] = struct{}{}
	}
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		for _, r := range ranges {
			delete(g.reserved, r)
		}
	}
}

func (s *Server) gcReserved() []slabRange {
	g := &s.gcGuard
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Collect(maps.Keys(g.reserved))
}

// A punch runs from a deleted chunk to the next slab key, or the end of the slab. Reserved
// space has no keys yet, so stop at it.
func clampToReserved(reserved []slabRange, l erofs.SlabLoc, end uint32) uint32 {
	for _, r := range reserved {
		if r.slabId == l.SlabId && r.start > l.Addr && r.start < end {
			end = r.start
		}
	}
	return end
}

func (g *gcCtx) anyHeld(sphps []SphPrefix) bool {
	for _, sphp := range sphps {
		if _, ok := g.heldSphps[sphp]; ok {
			return true
		}
	}
	return false
}

func (g *gcCtx) keepAllSphps(sphps []SphPrefix) bool {
	for _, sphp := range sphps {
		if _, ok := g.keepSphps[sphp]; !ok {
			return false
		}
	}
	return true
}

func locCmp(a, b erofs.SlabLoc) int {
	if a.SlabId < b.SlabId {
		return -1
	} else if a.SlabId > b.SlabId {
		return 1
	} else if a.Addr < b.Addr {
		return -1
	} else if a.Addr > b.Addr {
		return 1
	} else {
		return 0
	}
}

func punchKey(slab uint16, addr, end uint32) []byte {
	b := make([]byte, 11)
	b[0] = gcrecPunch
	binary.BigEndian.PutUint16(b[1:], slab)
	binary.BigEndian.PutUint32(b[3:], addr)
	binary.BigEndian.PutUint32(b[7:], end)
	return b
}

func recFromPunchKey(b []byte) (le locWithEnd) {
	le.SlabId = binary.BigEndian.Uint16(b[1:])
	le.Addr = binary.BigEndian.Uint32(b[3:])
	le.end = binary.BigEndian.Uint32(b[7:])
	return
}
