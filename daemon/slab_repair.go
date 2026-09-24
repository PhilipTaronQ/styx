package daemon

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/pb"
)

// Until AppendBlocksList was fixed, the last chunk of a file whose size is an exact multiple of
// its chunk size was allocated 0 blocks, so the next chunk allocated in that slab got the same
// address. The later chunk took over the slab's address key, the earlier chunk's record kept
// pointing at it, and whichever was written first is what both read back. A write of the
// earlier chunk's full data also covers the chunks allocated right after it.
//
// repairSlabOverlaps fixes a database written by those versions. It runs once, at startup,
// before anything reads or allocates:
//
//   - A slab whose last chunk got 0 blocks gets its sequence moved past room for a full chunk,
//     so the next allocation doesn't land on it.
//   - At each address shared by several chunk records, one record keeps the address: the chunk
//     whose data is there, or else the one the address key names. Each other record moves to
//     new, punched-out space at the end of the slab, with nothing present, so it's fetched
//     again. Every image using a moved chunk has the old address baked in, so it's marked for
//     rebuilding.
//   - Every chunk marked present within a maximum chunk size after a shared address is read
//     back and checked against its digest. Chunks that don't match lose their present mark and
//     their data is punched out of the slab, so the kernel asks for them again.
//
// Data is only dropped when it's proven wrong, since vaporized chunks can't be fetched again.
// The repair runs in one transaction and punches holes before it commits, so if anything
// fails, nothing is recorded and the next start does it all again.
var metaSlabOverlapRepaired = []byte("slab-overlap-repaired")

type slabRepairStats struct {
	tails, moved, dropped int
	images                []string
}

func (s *Server) repairSlabOverlaps() error {
	var st slabRepairStats
	var dropImageFiles []string
	err := s.db.Update(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		if mb.Get(metaSlabOverlapRepaired) != nil {
			return nil
		}
		files := make(map[uint16]*os.File)
		defer func() {
			for _, f := range files {
				if f != nil {
					f.Close()
				}
			}
		}()
		var err error
		if dropImageFiles, err = s.repairSlabOverlapsTx(tx, files, &st); err != nil {
			return err
		}
		return mb.Put(metaSlabOverlapRepaired, []byte{1})
	})
	if err != nil {
		return fmt.Errorf("repairing overlapping slab allocations: %w", err)
	}
	for _, sphStr := range dropImageFiles {
		s.removeImageCacheFile(sphStr)
	}
	if st.tails+st.moved+st.dropped > 0 {
		log.Printf("slab repair: %d slab ends extended, %d chunks moved, %d chunks dropped, %d images to rebuild",
			st.tails, st.moved, st.dropped, len(st.images))
	}
	return nil
}

func (s *Server) repairSlabOverlapsTx(tx *bbolt.Tx, files map[uint16]*os.File, st *slabRepairStats) ([]string, error) {
	cb, slabroot, ib := tx.Bucket(chunkBucket), tx.Bucket(slabBucket), tx.Bucket(imageBucket)
	span := uint32(shift.MaxChunkShift.Size() >> s.blockShift)

	// 1: a slab whose last chunk got 0 blocks has its sequence at that chunk's address.
	var slabIds []uint16
	scur := slabroot.Cursor()
	for k, v := scur.First(); k != nil; k, v = scur.Next() {
		if v == nil && len(k) == 2 {
			slabIds = append(slabIds, binary.BigEndian.Uint16(k))
		}
	}
	for _, id := range slabIds {
		sb := slabroot.Bucket(slabKey(id))
		c := sb.Cursor()
		var last []byte
		if k, _ := c.Seek(addrKey(presentMask)); k != nil {
			last, _ = c.Prev()
		} else {
			last, _ = c.Last()
		}
		if last == nil {
			continue
		} else if addr := addrFromKey(last); addr&presentMask == 0 && uint64(addr) >= sb.Sequence() {
			log.Printf("slab repair: slab %d ends with a 0-block chunk at %d", id, addr)
			if err := sb.SetSequence(uint64(addr) + uint64(span)); err != nil {
				return nil, err
			}
			st.tails++
		}
	}

	// 2: find chunk records whose address key names another chunk
	type rec struct {
		d cdig.CDig
		v []byte
	}
	shared := make(map[erofs.SlabLoc][]rec)
	ccur := cb.Cursor()
	for k, v := ccur.First(); k != nil; k, v = ccur.Next() {
		if len(k) != cdig.Bytes || len(v) < 6 {
			continue
		}
		loc := loadLoc(v)
		sb := slabroot.Bucket(slabKey(loc.SlabId))
		if sb == nil {
			continue
		}
		if !bytes.Equal(sb.Get(addrKey(loc.Addr)), k) {
			shared[loc] = append(shared[loc], rec{cdig.FromBytes(k), bytes.Clone(v)})
		}
	}
	if len(shared) == 0 {
		return nil, nil
	}
	locs := slices.SortedFunc(maps.Keys(shared), locCmp)

	affected := make(map[SphPrefix]struct{})
	type punch struct{ addr, end uint32 }
	punches := make(map[uint16][]punch)
	checked := make(map[erofs.SlabLoc]bool)

	// drop marks a chunk not present and schedules its data to be punched out
	drop := func(sb *bbolt.Bucket, loc erofs.SlabLoc, end uint32, d cdig.CDig) error {
		st.dropped++
		punches[loc.SlabId] = append(punches[loc.SlabId], punch{loc.Addr, end})
		if loc.SlabId >= manifestSlabOffset {
			// the manifest of an image using it has to be fetched again. rebuilding the image
			// does that.
			if v := cb.Get(d[:]); v != nil {
				for _, sphp := range sphpsFromLoc(v) {
					affected[sphp] = struct{}{}
				}
			}
		}
		return sb.Delete(addrKey(loc.Addr | presentMask))
	}

	for _, loc := range locs {
		sb := slabroot.Bucket(slabKey(loc.SlabId))
		checked[loc] = true
		recs := shared[loc]
		if w := sb.Get(addrKey(loc.Addr)); len(w) == cdig.Bytes {
			if v := cb.Get(w); v != nil && loadLoc(v) == loc {
				// the address key's own chunk goes first, it was allocated the right size
				recs = append([]rec{{cdig.FromBytes(w), bytes.Clone(v)}}, recs...)
			}
		}

		end := slabChunkEnd(sb, loc.Addr)
		keep := -1
		if sb.Get(addrKey(loc.Addr|presentMask)) != nil {
			data, err := s.readSlabData(files, loc, end, span)
			if err != nil {
				return nil, err
			}
			for i, r := range recs {
				if r.d.MatchesPadded(data) {
					keep = i
					break
				}
			}
			if keep == -1 {
				log.Printf("slab repair: data at %d/%d matches none of the %d chunks there", loc.SlabId, loc.Addr, len(recs))
				if err := drop(sb, loc, end, recs[0].d); err != nil {
					return nil, err
				}
			}
		}
		keep = max(keep, 0)
		if err := sb.Put(addrKey(loc.Addr), recs[keep].d[:]); err != nil {
			return nil, err
		}

		// move the rest to new space. that space may hold data written past the end of a 0-block
		// chunk, so punch it out too.
		a, err := s.newSlabAllocator(slabroot, loc.SlabId)
		if err != nil {
			return nil, err
		}
		for i, r := range recs {
			if i == keep {
				continue
			}
			nloc, nsb, err := a.alloc(common.TruncU16(span))
			if err != nil {
				return nil, err
			}
			punches[nloc.SlabId] = append(punches[nloc.SlabId], punch{nloc.Addr, nloc.Addr + span})
			log.Printf("slab repair: moving chunk %s from %d/%d to %d/%d", r.d, loc.SlabId, loc.Addr, nloc.SlabId, nloc.Addr)
			nv := bytes.Clone(r.v)
			binary.LittleEndian.PutUint16(nv, nloc.SlabId)
			binary.LittleEndian.PutUint32(nv[2:], nloc.Addr)
			if err := cb.Put(r.d[:], nv); err != nil {
				return nil, err
			} else if err := nsb.Put(addrKey(nloc.Addr), r.d[:]); err != nil {
				return nil, err
			}
			for _, sphp := range sphpsFromLoc(r.v) {
				affected[sphp] = struct{}{}
			}
			st.moved++
		}
		if err := a.finish(); err != nil {
			return nil, err
		}
	}

	// don't allocate anything else where a full chunk written at a shared address could have
	// reached, until it's punched out
	for _, loc := range locs {
		sb := slabroot.Bucket(slabKey(loc.SlabId))
		if seq, end := sb.Sequence(), uint64(loc.Addr)+uint64(span); seq < end {
			punches[loc.SlabId] = append(punches[loc.SlabId], punch{common.TruncU32(seq), common.TruncU32(end)})
			if err := sb.SetSequence(end); err != nil {
				return nil, err
			}
		}
	}

	// 3: check what's present after each shared address. a full chunk written there may have
	// run over the chunks allocated after it.
	for _, loc := range locs {
		sb := slabroot.Bucket(slabKey(loc.SlabId))
		var next []uint32
		c := sb.Cursor()
		for k, _ := c.Seek(addrKey(loc.Addr)); k != nil; k, _ = c.Next() {
			addr := addrFromKey(k)
			if addr&presentMask != 0 || addr >= loc.Addr+span {
				break
			}
			next = append(next, addr)
		}
		for _, addr := range next {
			nloc := erofs.SlabLoc{SlabId: loc.SlabId, Addr: addr}
			if checked[nloc] || sb.Get(addrKey(addr|presentMask)) == nil {
				continue
			}
			checked[nloc] = true
			v := sb.Get(addrKey(addr))
			if len(v) != cdig.Bytes {
				continue
			}
			end := slabChunkEnd(sb, addr)
			data, err := s.readSlabData(files, nloc, end, span)
			if err != nil {
				return nil, err
			}
			if d := cdig.FromBytes(v); !d.MatchesPadded(data) {
				log.Printf("slab repair: chunk %s at %d/%d doesn't match its data", d, loc.SlabId, addr)
				if err := drop(sb, nloc, end, d); err != nil {
					return nil, err
				}
			}
		}
	}

	// punch out dropped data so the kernel asks for it again. do this before committing: if we
	// crash in between, the data is gone but still marked present, and it gets fetched again.
	for id, ps := range punches {
		f, err := s.openSlabData(files, id)
		if err != nil {
			return nil, err
		} else if f == nil {
			continue
		}
		for _, p := range ps {
			if p.end <= p.addr {
				continue
			}
			err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
				int64(p.addr)<<s.blockShift, int64(p.end-p.addr)<<s.blockShift)
			if err != nil {
				return nil, fmt.Errorf("punching slab %d at %d-%d: %w", id, p.addr, p.end, err)
			}
		}
	}

	// 4: images that use a moved chunk get rebuilt the next time they're mounted
	var dropImageFiles []string
	type imgUpdate struct{ k, v []byte }
	var updates []imgUpdate
	icur := ib.Cursor()
	for k, v := icur.First(); k != nil; k, v = icur.Next() {
		sph, _, err := ParseSph(string(k))
		if err != nil {
			continue
		}
		msph := makeManifestSph(sph)
		_, a1 := affected[SphPrefixFromBytes(sph[:])]
		_, a2 := affected[SphPrefixFromBytes(msph[:])]
		if !a1 && !a2 {
			continue
		}
		var img pb.DbImage
		if err := proto.Unmarshal(v, &img); err != nil {
			continue
		}
		st.images = append(st.images, img.StorePath)
		if src, ok := strings.CutPrefix(img.Upstream, "vaporize://"); ok {
			log.Printf("slab repair: %s may have lost vaporized data, vaporize %s again", img.StorePath, src)
		}
		switch img.MountState {
		case pb.MountState_Mounted, pb.MountState_UnmountRequested:
			// can't replace the image under a mount. it's rebuilt when restored after a reboot.
			log.Printf("slab repair: %s is mounted on %s and may read wrong data until it's mounted again", img.StorePath, img.MountPoint)
		default:
			dropImageFiles = append(dropImageFiles, string(k))
		}
		if img.ImageSize == 0 {
			continue
		}
		img.ImageSize = 0
		buf, err := proto.Marshal(&img)
		if err != nil {
			return nil, err
		}
		updates = append(updates, imgUpdate{bytes.Clone(k), buf})
	}
	for _, u := range updates {
		if err := ib.Put(u.k, u.v); err != nil {
			return nil, err
		}
	}
	return dropImageFiles, nil
}

// slabChunkEnd returns the address after the chunk at addr: the next chunk's, or the end of
// allocated space.
func slabChunkEnd(sb *bbolt.Bucket, addr uint32) uint32 {
	c := sb.Cursor()
	if k, _ := c.Seek(addrKey(addr + 1)); k != nil && addrFromKey(k)&presentMask == 0 {
		return addrFromKey(k)
	}
	return common.TruncU32(sb.Sequence())
}

func (s *Server) slabDataPath(id uint16) string {
	if id >= manifestSlabOffset {
		return filepath.Join(s.cfg.CachePath, manifestSlabPrefix+strconv.Itoa(int(id)))
	}
	tag, _ := s.SlabInfo(id)
	return filepath.Join(s.cfg.CachePath, fscachePath(s.cfg.CacheDomain, tag))
}

// openSlabData opens the file holding a slab's data. It returns nil if there's no such file,
// and so no data.
func (s *Server) openSlabData(files map[uint16]*os.File, id uint16) (*os.File, error) {
	if f, ok := files[id]; ok {
		return f, nil
	}
	f, err := os.OpenFile(s.slabDataPath(id), os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		f, err = nil, nil
	} else if err != nil {
		return nil, err
	}
	files[id] = f
	return f, nil
}

// readSlabData reads a chunk's space, up to span blocks of it. Missing data reads as zeros.
func (s *Server) readSlabData(files map[uint16]*os.File, loc erofs.SlabLoc, end, span uint32) ([]byte, error) {
	var blocks uint32
	if end > loc.Addr {
		blocks = min(end-loc.Addr, span)
	}
	buf := make([]byte, int64(blocks)<<s.blockShift)
	f, err := s.openSlabData(files, loc.SlabId)
	if err != nil || f == nil {
		return buf, err
	}
	if _, err := f.ReadAt(buf, int64(loc.Addr)<<s.blockShift); err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

// removeImageCacheFile deletes cachefiles' copy of an image that isn't mounted, so a rebuilt
// image of the same size isn't passed over for the old one.
func (s *Server) removeImageCacheFile(sphStr string) {
	p := filepath.Join(s.cfg.CachePath, fscachePath(s.cfg.CacheDomain, sphStr))
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("removing cached image %s: %v", p, err)
	}
}
