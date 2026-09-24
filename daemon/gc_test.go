package daemon

// Tests of local gc and the image lifecycle that need no root and no kernel support. The
// devnode is one end of a SOCK_SEQPACKET socketpair, so on-demand features look enabled.

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

const gcTestDomain = "styxgctest"

var gcDefault = map[pb.MountState]bool{pb.MountState_Unmounted: true}

type gcTestFdStore struct{}

func (gcTestFdStore) Ready() {}

func (gcTestFdStore) GetFd(string) (int, error) { return 0, os.ErrNotExist }

func (gcTestFdStore) SaveFd(string, int) {}

func (gcTestFdStore) RemoveFd(string) {}

func gcTestSph(c byte) string { return strings.Repeat(string(c), 32) }

func gcTestStorePath(c byte, name string) string { return gcTestSph(c) + "-" + name }

func newGcTestServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(Config{
		DevPath:         "/nonexistent",
		CachePath:       t.TempDir(),
		CacheTag:        gcTestDomain,
		CacheDomain:     gcTestDomain,
		ErofsBlockShift: 12,
		Workers:         2,
		IsTesting:       true,
		FdStore:         gcTestFdStore{},
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() })
	return s
}

func initGcTestServer(t *testing.T, s *Server, url string) {
	t.Helper()
	require.NoError(t, s.postInit(&pb.DaemonParams{
		Params:           &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		ManifesterUrl:    url,
		ManifestCacheUrl: url,
		ChunkReadUrl:     url,
		ChunkDiffUrl:     url,
	}, nil))
}

// Gives s a fake devnode so mounts are accepted. Nothing reads the other end.
func gcTestDevnode(t *testing.T, s *Server) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetNonblock(fds[0], true))
	t.Cleanup(func() { unix.Close(fds[0]); unix.Close(fds[1]) })
	s.devnode.Store(int32(fds[0]))
}

func gcTestDigest(b byte) cdig.CDig {
	var d cdig.CDig
	d[0], d[1] = b, 0xcd
	return d
}

// Records an image in state st whose inline manifest has one file made of digs, and
// allocates its chunks and catalog entries, as getManifestAndBuildImage does.
func gcTestImage(t *testing.T, s *Server, c byte, name string, st pb.MountState, digs ...cdig.CDig) []erofs.SlabLoc {
	t.Helper()
	sp := gcTestStorePath(c, name)
	sph, sphStr, spName, err := ParseSphAndName(sp)
	require.NoError(t, err)
	mdata, err := proto.Marshal(&pb.Manifest{Entries: []*pb.Entry{{
		Path:    "/",
		Type:    pb.EntryType_REGULAR,
		Size:    int64(len(digs)) << 16,
		Digests: cdig.ToSliceAlias(digs),
	}}})
	require.NoError(t, err)
	env, err := proto.Marshal(&pb.SignedMessage{Msg: &pb.Entry{InlineData: mdata}})
	require.NoError(t, err)
	img, err := proto.Marshal(&pb.DbImage{StorePath: sp, MountState: st})
	require.NoError(t, err)

	blocks := make([]uint16, len(digs))
	for i := range blocks {
		blocks[i] = 16
	}
	locs, err := s.AllocateBatch(withAllocateCtx(context.Background(), sph, false), blocks, digs)
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		return errors.Join(
			tx.Bucket(manifestBucket).Put([]byte(sphStr), env),
			tx.Bucket(imageBucket).Put([]byte(sphStr), img),
			tx.Bucket(catalogFBucket).Put(bytes.Join([][]byte{[]byte(spName), {0}, sph[:]}, nil), []byte{}),
			tx.Bucket(catalogRBucket).Put(sph[:], []byte(spName)),
		)
	}))
	return locs
}

func gcTestHasChunk(t *testing.T, s *Server, d cdig.CDig) bool {
	t.Helper()
	var have bool
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		have = tx.Bucket(chunkBucket).Get(d[:]) != nil
		return nil
	}))
	return have
}

func gcTestGetImage(t *testing.T, s *Server, sphStr string) *pb.DbImage {
	t.Helper()
	var img *pb.DbImage
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(imageBucket).Get([]byte(sphStr)); v != nil {
			img = &pb.DbImage{}
			return proto.Unmarshal(v, img)
		}
		return nil
	}))
	return img
}

// A mount whose manifest can't be fetched used to leave its image Requested with no
// manifest. gc keeps Requested images by default and gave up on the one it couldn't
// trace, so one failed mount made every default "styx gc" fail.
func TestGcAfterFailedMount(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler()) // manifest cache and manifester both 404
	t.Cleanup(srv.Close)

	s := newGcTestServer(t)
	initGcTestServer(t, s, srv.URL)
	gcTestDevnode(t, s)

	_, err := s.handleMountReq(context.Background(), &MountReq{
		Upstream:   srv.URL,
		StorePath:  gcTestStorePath('0', "missing-1.0"),
		MountPoint: filepath.Join(t.TempDir(), "mp"),
	})
	require.Error(t, err, "mount of a path the manifester can't find must fail")

	img := gcTestGetImage(t, s, gcTestSph('0'))
	require.NotNil(t, img)
	require.Equal(t, pb.MountState_MountError, img.MountState)
	require.NotEmpty(t, img.LastMountError)

	// the request "styx gc" sends with default flags
	_, err = s.handleGcReq(context.Background(), &GcReq{DryRunFast: true, GcByState: gcDefault})
	require.NoError(t, err, "default gc must not be wedged by an earlier failed mount")
}

// Serves 404 for everything, but holds each request until release is called, so a mount
// can be caught while it fetches its manifest.
func gcTestBlockingServer(t *testing.T) (url string, entered <-chan struct{}, release func()) {
	t.Helper()
	ent := make(chan struct{}, 16)
	rel := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(rel) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ent <- struct{}{}
		select {
		case <-rel:
		case <-r.Context().Done():
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(release)
	return srv.URL, ent, release
}

func gcTestWaitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("mount never reached the manifester")
	}
}

// handleMountReq used to write the request into the image record before checking for a
// mount in progress, so a rejected second request replaced the first one's mount point,
// and a later umount would detach the wrong path.
func TestRejectedConcurrentMountKeepsRecord(t *testing.T) {
	url, entered, release := gcTestBlockingServer(t)
	s := newGcTestServer(t)
	initGcTestServer(t, s, url)
	gcTestDevnode(t, s)

	sp := gcTestStorePath('1', "pkg-1.0")
	mp1 := filepath.Join(t.TempDir(), "first")
	mp2 := filepath.Join(t.TempDir(), "second")

	firstDone := make(chan error, 1)
	go func() {
		_, err := s.handleMountReq(context.Background(), &MountReq{Upstream: url, StorePath: sp, MountPoint: mp1})
		firstDone <- err
	}()
	gcTestWaitEntered(t, entered)

	_, err := s.handleMountReq(context.Background(), &MountReq{Upstream: url, StorePath: sp, MountPoint: mp2})
	require.ErrorContains(t, err, "another mount is in progress")

	img := gcTestGetImage(t, s, gcTestSph('1'))
	release()
	<-firstDone

	require.NotNil(t, img)
	require.Equal(t, mp1, img.MountPoint, "a rejected concurrent mount replaced the mount point of the one in progress")
	require.Equal(t, pb.MountState_Requested, img.MountState)
}

// `styx gc --error_states` puts Requested in GcByState, and gc used to delete the record
// of a mount that was still fetching its manifest.
func TestGcErrorStatesKeepsMountInProgress(t *testing.T) {
	url, entered, release := gcTestBlockingServer(t)
	s := newGcTestServer(t)
	initGcTestServer(t, s, url)
	gcTestDevnode(t, s)

	done := make(chan error, 1)
	go func() {
		_, err := s.handleMountReq(context.Background(), &MountReq{
			Upstream:   url,
			StorePath:  gcTestStorePath('2', "pkg-1.0"),
			MountPoint: filepath.Join(t.TempDir(), "mp"),
		})
		done <- err
	}()
	gcTestWaitEntered(t, entered)

	allStates := make(map[pb.MountState]bool)
	for st := range pb.MountState_name {
		allStates[pb.MountState(st)] = true
	}
	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: allStates})
	require.NoError(t, err)
	require.Zero(t, res.DeleteImages, "gc deleted the image of a mount in progress")
	require.NotNil(t, gcTestGetImage(t, s, gcTestSph('2')))

	release()
	<-done
}

// materialize leaves an Unmounted or Materialized image in its state while it copies the
// image's chunks, so gc must keep an image that is held.
func TestGcKeepsHeldImage(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	d := gcTestDigest(3)
	gcTestImage(t, s, '3', "pkg-1.0", pb.MountState_Unmounted, d)

	release := s.holdForGc(gcTestSph('3'))
	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Zero(t, res.DeleteImages, "gc deleted a held image")
	require.True(t, gcTestHasChunk(t, s, d), "gc deleted a chunk of a held image")

	release()
	res, err = s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteImages)
	require.False(t, gcTestHasChunk(t, s, d))
}

// gotNewChunk records a chunk present in a later batch. When gc deleted the chunk in
// between, that batch used to add a present key for an address no chunk uses.
func TestPresentNotRecordedAfterGc(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	dLive, dGone := gcTestDigest(10), gcTestDigest(11)
	liveLoc := gcTestImage(t, s, 'a', "pkg-1.0", pb.MountState_Mounted, dLive)[0]
	goneLoc := gcTestImage(t, s, 'b', "pkg-2.0", pb.MountState_Unmounted, dGone)[0]

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteChunks)

	// the batched present writes for both land after gc
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		return errors.Join(s.recordPresent(tx, liveLoc, dLive), s.recordPresent(tx, goneLoc, dGone))
	}))
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		require.True(t, s.locPresent(tx, liveLoc))
		require.False(t, s.locPresent(tx, goneLoc), "present key for a chunk gc deleted")
		return nil
	}))
}

// openDb's compaction renamed the old db aside, then ignored a failure to rename the
// compacted one into place, so bbolt created an empty db.
func TestCompactionKeepsDb(t *testing.T) {
	cfg := Config{CachePath: t.TempDir(), ErofsBlockShift: 12, FdStore: gcTestFdStore{}}
	sphStr := gcTestSph('c')
	s := NewServer(cfg)
	require.NoError(t, s.openDb())
	require.NoError(t, s.imageTx(sphStr, func(img *pb.DbImage) error {
		img.StorePath = gcTestStorePath('c', "pkg-1.0")
		return nil
	}))
	require.NoError(t, s.db.Close())

	reopen := func() {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(cfg.CachePath, compactFile), nil, 0o644))
		s = NewServer(cfg)
		require.NoError(t, s.openDb())
		require.NotNil(t, gcTestGetImage(t, s, sphStr), "compaction lost the db")
		require.NoError(t, s.db.Close())
	}

	calls := 0
	renameFile = func(from, to string) error {
		if calls++; calls == 2 {
			return errors.New("injected rename failure")
		}
		return os.Rename(from, to)
	}
	t.Cleanup(func() { renameFile = os.Rename })
	reopen()
	require.Equal(t, 3, calls, "rename aside, rename into place (fails), rename back")

	renameFile = os.Rename
	reopen()
}

func TestSyncSlab(t *testing.T) {
	s := newGcTestServer(t)
	fd, err := unix.Open(filepath.Join(t.TempDir(), "slab"), unix.O_RDWR|unix.O_CREAT, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { unix.Close(fd) })
	s.readfdBySlab[0] = slabFds{fd, fd}

	errs := make(chan error, 8)
	for range cap(errs) {
		go func() { errs <- s.syncSlab(0) }()
	}
	for range cap(errs) {
		require.NoError(t, <-errs)
	}
	require.Error(t, s.syncSlab(1), "slab 1 has no fd")
}

func gcTestCatalogF(t *testing.T, s *Server) []string {
	t.Helper()
	var names []string
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		cur := tx.Bucket(catalogFBucket).Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			name, _, _ := bytes.Cut(k, []byte{0})
			names = append(names, string(name))
		}
		return nil
	}))
	return names
}

func gcTestPutStaleCatalogF(t *testing.T, s *Server, c byte, name string) {
	t.Helper()
	sph, _, err := ParseSph(gcTestSph(c))
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(catalogFBucket).Put(bytes.Join([][]byte{[]byte(name), {0}, sph[:]}, nil), []byte{})
	}))
}

// gc built the catalogf keys to delete from ParseSph's second result, which is the hash, not
// the name, so it never deleted any, and the stale entries were then picked as diff bases.
func TestGcPrunesCatalog(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	gcTestImage(t, s, '6', "pkg-1.0", pb.MountState_Unmounted, gcTestDigest(6))
	gcTestImage(t, s, '7', "pkg-1.1", pb.MountState_Mounted, gcTestDigest(7))
	gcTestPutStaleCatalogF(t, s, '8', "pkg-0.9") // left behind by an older gc
	require.Equal(t, []string{"pkg-0.9", "pkg-1.0", "pkg-1.1"}, gcTestCatalogF(t, s))

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteImages)
	require.Equal(t, []string{"pkg-1.1"}, gcTestCatalogF(t, s))
}

// Stale catalogf entries in existing databases must not be picked as diff bases.
func TestCatalogSkipsStaleBase(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	gcTestImage(t, s, '7', "pkg-1.0", pb.MountState_Mounted, gcTestDigest(7))
	// sorts after the live entry, and the last best match wins
	gcTestPutStaleCatalogF(t, s, '8', "pkg-1.1")

	reqSph, _, err := ParseSph(gcTestSph('9'))
	require.NoError(t, err)
	var res catalogResult
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		res, err = s.catalogFindBaseFromHashAndName(tx, reqSph, "pkg-1.2")
		return err
	}))
	require.Equal(t, "pkg-1.0", res.baseName)
}

// vaporize reserves slab space in one transaction and links chunks to it in a later one,
// and writes its image and manifest only at the end. gc used to punch the reservation as
// part of the garbage chunk before it, and delete the chunks vaporize had committed.
func TestGcDuringVaporizeKeepsItsSpace(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")

	// garbage at blocks 4-20: no image refers to it
	garbageSph, _, err := ParseSph(gcTestSph('4'))
	require.NoError(t, err)
	dGarbage := gcTestDigest(4)
	_, err = s.AllocateBatch(withAllocateCtx(context.Background(), garbageSph, false), []uint16{16}, []cdig.CDig{dGarbage})
	require.NoError(t, err)

	// a vaporize in progress: space reserved at 20-28 for one file, and a chunk of another
	// file already committed after it, at 28-30
	vapSph, vapSphStr, err := ParseSph(gcTestSph('5'))
	require.NoError(t, err)
	vapCtx := withAllocateCtx(context.Background(), vapSph, false)
	unhold := s.holdForGc(vapSphStr)
	defer unhold()
	locs, _, unreserve, err := s.preallocateBatch(vapCtx, []uint16{8}, []cdig.CDig{gcTestDigest(50)})
	require.NoError(t, err)
	defer unreserve()
	require.Equal(t, erofs.SlabLoc{SlabId: 0, Addr: 20}, locs[0])
	dCommitted := gcTestDigest(51)
	_, err = s.AllocateBatch(vapCtx, []uint16{2}, []cdig.CDig{dCommitted})
	require.NoError(t, err)

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteChunks)
	require.False(t, gcTestHasChunk(t, s, dGarbage))
	require.True(t, gcTestHasChunk(t, s, dCommitted), "gc deleted a chunk vaporize had committed")

	// there's no slab fd here, so the punch is still pending in gcstate
	var punches []locWithEnd
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		cur := tx.Bucket(gcstateBucket).Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			punches = append(punches, recFromPunchKey(k))
		}
		return nil
	}))
	require.Equal(t, []locWithEnd{{SlabLoc: erofs.SlabLoc{SlabId: 0, Addr: 4}, end: 20}}, punches,
		"gc must punch the garbage chunk only, not the space reserved after it")
}

// Sets up slab 0 as a plain file, as the daemon does without cachefiles, and returns its fd.
func gcTestFakeSlab(t *testing.T, s *Server) int {
	t.Helper()
	require.NoError(t, s.setupFakeSlabImage(0))
	fd := s.readfdBySlab[0].readFd
	t.Cleanup(func() { unix.Close(fd) })
	return fd
}

// 64 KiB of data, the size of one chunk of the files gcTestImage records.
func gcTestChunkData(seed byte) []byte {
	b := make([]byte, 1<<16)
	for i := range b {
		b[i] = byte(i*7) ^ seed
	}
	return b
}

// Serves chunks from m by digest at the chunk read path, 404 for anything else, and records
// which chunks it served.
func gcTestChunkServer(t *testing.T, m map[cdig.CDig][]byte) (url string, served func() []cdig.CDig) {
	t.Helper()
	var mu sync.Mutex
	var got []cdig.CDig
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if key, ok := strings.CutPrefix(r.URL.Path, manifester.ChunkReadPath); ok {
			if d, err := cdig.FromBase64(key); err == nil && m[d] != nil {
				mu.Lock()
				got = append(got, d)
				mu.Unlock()
				w.Write(m[d])
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() []cdig.CDig {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// Whether the db records loc present (ignoring presentMap).
func gcTestPresentInDb(s *Server, loc erofs.SlabLoc) bool {
	var present bool
	_ = s.db.View(func(tx *bbolt.Tx) error {
		sb := tx.Bucket(slabBucket).Bucket(slabKey(loc.SlabId))
		present = sb != nil && sb.Get(addrKey(loc.Addr|presentMask)) != nil
		return nil
	})
	return present
}

// Writes a chunk as a fetch does, and waits for its present record.
func gcTestWriteChunk(t *testing.T, s *Server, loc erofs.SlabLoc, data []byte) {
	t.Helper()
	require.NoError(t, s.gotNewChunk(loc, cdig.Sum(data), slices.Clone(data)))
	require.Eventually(t, func() bool { return gcTestPresentInDb(s, loc) }, 10*time.Second, 5*time.Millisecond)
}

// Punches a hole as gc does. Safe to call from any goroutine.
func gcTestPunch(t *testing.T, fd int, off, n int64) {
	if err := unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, n); err != nil {
		t.Errorf("punch %d+%d: %v", off, n, err)
	}
}

// materialize copies chunks straight from the slab's backing file, where a hole reads as
// zeros. A chunk whose data went missing after materialize looked it up (punched by gc, or
// lost in a crash) was copied as zeros, and the request succeeded.
func TestMaterializeChunkMissingAtCopy(t *testing.T) {
	for _, tc := range []struct {
		name             string
		clone, fetchable bool
	}{
		{"clone", true, true},
		{"clone-unfetchable", true, false},
		{"plain", false, true},
		{"plain-unfetchable", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := [][]byte{gcTestChunkData(1), gcTestChunkData(2), gcTestChunkData(3)}
			digs := make([]cdig.CDig, len(data))
			serve := make(map[cdig.CDig][]byte)
			for i, b := range data {
				digs[i] = cdig.Sum(b)
				if tc.fetchable {
					serve[digs[i]] = b
				}
			}
			url, served := gcTestChunkServer(t, serve)
			s := newGcTestServer(t)
			initGcTestServer(t, s, url)
			fd := gcTestFakeSlab(t, s)
			if !tc.clone {
				s.readfdBySlab[0] = slabFds{readFd: fd} // no backing fd to clone from
			}
			sp := gcTestStorePath('m', "pkg-1.0")
			locs := gcTestImage(t, s, 'm', "pkg-1.0", pb.MountState_Unmounted, digs...)
			for i, loc := range locs {
				gcTestWriteChunk(t, s, loc, data[i])
			}

			victim := locs[1]
			var once sync.Once
			testHookMaterializeChunk = func(loc erofs.SlabLoc) {
				if loc == victim {
					once.Do(func() { gcTestPunch(t, fd, int64(loc.Addr)<<s.blockShift, int64(len(data[1]))) })
				}
			}
			t.Cleanup(func() { testHookMaterializeChunk = nil })

			dest := filepath.Join(t.TempDir(), "out")
			_, err := s.handleMaterializeReq(context.Background(), &MaterializeReq{
				Upstream:  url,
				StorePath: sp,
				DestPath:  dest,
			})
			if !tc.fetchable {
				require.Error(t, err, "materialize succeeded although a chunk's data was gone and couldn't be fetched")
				return
			}
			require.NoError(t, err)
			got, err := os.ReadFile(dest)
			require.NoError(t, err)
			require.True(t, bytes.Equal(bytes.Join(data, nil), got), "materialized file doesn't match its chunks")
			require.Contains(t, served(), digs[1], "the missing chunk wasn't fetched again")
		})
	}
}

// A manifest chunk whose data was lost while its present record survived (after a power cut
// it reads back as zeros) made getManifestLocal fail for good: nothing fetched the chunk
// again, and gc, which has to read the manifest of every mounted image, failed every time.
func TestGcRecoversDamagedManifestChunk(t *testing.T) {
	dataDigs := []cdig.CDig{gcTestDigest(20), gcTestDigest(21)}
	mdata, err := proto.Marshal(&pb.Manifest{Entries: []*pb.Entry{{
		Path:    "/",
		Type:    pb.EntryType_REGULAR,
		Size:    int64(len(dataDigs)) << 16,
		Digests: cdig.ToSliceAlias(dataDigs),
	}}})
	require.NoError(t, err)
	mdig := cdig.Sum(mdata)
	url, served := gcTestChunkServer(t, map[cdig.CDig][]byte{mdig: mdata})

	s := newGcTestServer(t)
	initGcTestServer(t, s, url)
	require.NoError(t, s.setupManifestSlab())
	mfd := s.readfdBySlab[manifestSlabOffset].readFd
	t.Cleanup(func() { unix.Close(mfd) })

	// a mounted image with a chunked manifest, as getManifestAndBuildImage records it
	sp := gcTestStorePath('r', "pkg-1.0")
	sph, sphStr, _, err := ParseSphAndName(sp)
	require.NoError(t, err)
	_, err = s.AllocateBatch(withAllocateCtx(context.Background(), sph, false), []uint16{16, 16}, dataDigs)
	require.NoError(t, err)
	mblocks := common.AppendBlocksList(nil, int64(len(mdata)), s.blockShift, shift.DefaultChunkShift)
	mlocs, err := s.AllocateBatch(withAllocateCtx(context.Background(), makeManifestSph(sph), true), mblocks, []cdig.CDig{mdig})
	require.NoError(t, err)
	env, err := proto.Marshal(&pb.SignedMessage{Msg: &pb.Entry{
		Size:    int64(len(mdata)),
		Digests: cdig.ToSliceAlias([]cdig.CDig{mdig}),
	}})
	require.NoError(t, err)
	img, err := proto.Marshal(&pb.DbImage{StorePath: sp, MountState: pb.MountState_Mounted})
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		return errors.Join(
			tx.Bucket(manifestBucket).Put([]byte(sphStr), env),
			tx.Bucket(imageBucket).Put([]byte(sphStr), img),
		)
	}))
	gcTestWriteChunk(t, s, mlocs[0], mdata)

	// lose the data, keep the present record
	_, err = unix.Pwrite(mfd, make([]byte, s.blockShift.Roundup(int64(len(mdata)))), int64(mlocs[0].Addr)<<s.blockShift)
	require.NoError(t, err)

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err, "gc failed on an image with a damaged manifest chunk")
	require.Zero(t, res.DeleteImages)
	for _, d := range dataDigs {
		require.True(t, gcTestHasChunk(t, s, d), "gc deleted a chunk of an image whose manifest it couldn't read")
	}

	// the chunk was fetched again, so the manifest reads back
	require.Contains(t, served(), mdig, "the damaged manifest chunk wasn't fetched again")
	var m *pb.Manifest
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		m, _, err = s.getManifestLocal(tx, sphStr)
		return err
	}))
	require.Equal(t, dataDigs, cdig.FromSliceAlias(m.Entries[0].Digests))
}

// Whether [off, off+n) of fd is all hole.
func gcTestIsHole(fd int, off, n int64) bool {
	data, err := unix.Seek(fd, off, unix.SEEK_DATA)
	return err == unix.ENXIO || err == nil && data >= off+n
}

// A chunk write that landed after gc deleted the chunk and punched its space left the data
// there: gc never punches a range twice, so the space leaked.
func TestChunkWriteAfterGcPunchIsFreed(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	fd := gcTestFakeSlab(t, s)
	data := gcTestChunkData(30)
	d := cdig.Sum(data)
	loc := gcTestImage(t, s, 'g', "pkg-1.0", pb.MountState_Unmounted, d)[0]
	gcTestImage(t, s, 'h', "pkg-2.0", pb.MountState_Mounted, gcTestDigest(31))

	// a fetch of the chunk that writes it after gc
	entered, release := make(chan struct{}), make(chan struct{})
	testHookBeforeChunkWrite = func(l erofs.SlabLoc) {
		if l == loc {
			close(entered)
			<-release
		}
	}
	t.Cleanup(func() { testHookBeforeChunkWrite = nil })
	wrote := make(chan error, 1)
	go func() { wrote <- s.gotNewChunk(loc, d, slices.Clone(data)) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("write never started")
	}

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteChunks)
	require.Equal(t, 1, res.PunchLocs)
	off := int64(loc.Addr) << s.blockShift
	require.True(t, gcTestIsHole(fd, off, int64(len(data))), "gc didn't punch the chunk")

	close(release)
	require.NoError(t, <-wrote)
	require.Eventually(t, func() bool { return gcTestIsHole(fd, off, int64(len(data))) }, 10*time.Second, 10*time.Millisecond,
		"data written after gc punched the chunk's space is still there")
	require.False(t, gcTestPresentInDb(s, loc))
}

// Makes cachefiles report the backing file of sphStr in use while busy returns true, and
// signals asked the first time gc asks.
func gcTestInUse(t *testing.T, sphStr string, busy func() bool) (asked <-chan struct{}) {
	ch := make(chan struct{})
	var once sync.Once
	orig := cachefilesFileCmd
	t.Cleanup(func() { cachefilesFileCmd = orig })
	cachefilesFileCmd = func(_ int, cmd, _, name string) error {
		if cmd == "inuse" && name == "D"+sphStr {
			once.Do(func() { close(ch) })
			if busy() {
				return unix.EBUSY
			}
		}
		return nil
	}
	return ch
}

// gc waited up to a second for a busy image to become free inside its write transaction, so
// every db writer (a mount recording its image, a fetch recording chunks present) stalled.
func TestGcWaitsForBusyImageUnlocked(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	gcTestDevnode(t, s)
	gcTestImage(t, s, 'k', "pkg-1.0", pb.MountState_Unmounted, gcTestDigest(40))
	asked := gcTestInUse(t, gcTestSph('k'), func() bool { return true })

	type result struct {
		res *GcResp
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
		done <- result{res, err}
	}()
	select {
	case <-asked:
	case <-time.After(10 * time.Second):
		t.Fatal("gc never asked whether the image is in use")
	}

	start := time.Now()
	require.NoError(t, s.imageTx(gcTestSph('l'), func(img *pb.DbImage) error {
		img.StorePath = gcTestStorePath('l', "pkg-2.0")
		img.MountState = pb.MountState_Requested
		return nil
	}))
	elapsed := time.Since(start)

	r := <-done
	require.NoError(t, r.err)
	require.Zero(t, r.res.DeleteImages, "gc deleted an image in use")
	require.Less(t, elapsed, 500*time.Millisecond, "a db write waited for gc to wait for a busy image")
}

// gc waits for busy images before its write transaction, so it checks again inside it: an
// image found free then may be in use by the time gc would delete it.
func TestGcKeepsImageBusyAfterWait(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	gcTestDevnode(t, s)
	gcTestImage(t, s, 'n', "pkg-1.0", pb.MountState_Unmounted, gcTestDigest(41))
	var asks atomic.Int32
	gcTestInUse(t, gcTestSph('n'), func() bool { return asks.Add(1) > 1 })

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Zero(t, res.DeleteImages, "gc deleted an image in use")
	require.True(t, gcTestHasChunk(t, s, gcTestDigest(41)))
}

// The kernel releases an unmounted image's file asynchronously, so gc waits a moment for it.
func TestGcWaitsForImageRelease(t *testing.T) {
	s := newGcTestServer(t)
	initGcTestServer(t, s, "http://localhost:1")
	gcTestDevnode(t, s)
	gcTestImage(t, s, 'p', "pkg-1.0", pb.MountState_Unmounted, gcTestDigest(42))
	var asks atomic.Int32
	gcTestInUse(t, gcTestSph('p'), func() bool { return asks.Add(1) <= 3 })

	res, err := s.handleGcReq(context.Background(), &GcReq{GcByState: gcDefault})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeleteImages)
	require.False(t, gcTestHasChunk(t, s, gcTestDigest(42)))
}
