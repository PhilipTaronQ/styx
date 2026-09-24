package daemon

// Tests that demonstrate review findings in the daemon core (mount state, the
// cachefiles protocol loop and shutdown). They need no root and no kernel
// support: the devnode is a SOCK_SEQPACKET socketpair and cachefiles object fds
// are plain temp files.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lunixbochs/struc"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

const reviewDomain = "styxreview"

type reviewFdStore struct{}

func (reviewFdStore) Ready() {}

func (reviewFdStore) GetFd(string) (int, error) { return 0, os.ErrNotExist }

func (reviewFdStore) SaveFd(string, int) {}

func (reviewFdStore) RemoveFd(string) {}

func reviewSph(c byte) string { return strings.Repeat(string(c), 32) }

func reviewStorePath(c byte, name string) string { return reviewSph(c) + "-" + name }

func newReviewServer(t *testing.T, workers int, isTesting bool) *Server {
	t.Helper()
	s := NewServer(Config{
		DevPath:         "/nonexistent",
		CachePath:       t.TempDir(),
		CacheTag:        reviewDomain,
		CacheDomain:     reviewDomain,
		ErofsBlockShift: 12,
		Workers:         workers,
		IsTesting:       isTesting,
		FdStore:         reviewFdStore{},
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() }) // closing twice is a no-op in bbolt
	return s
}

func initReviewServer(t *testing.T, s *Server, url string) {
	t.Helper()
	require.NoError(t, s.postInit(&pb.DaemonParams{
		Params:           &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		ManifesterUrl:    url,
		ManifestCacheUrl: url,
		ChunkReadUrl:     url,
		ChunkDiffUrl:     url,
	}, nil))
}

// Returns (daemon end, test end) of a fake devnode. The daemon end is non-blocking so
// that, like /dev/cachefiles, reading it with nothing queued returns instead of blocking.
func reviewDevnode(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetNonblock(fds[0], true))
	return fds[0], fds[1]
}

func reviewTempFd(t *testing.T) int {
	t.Helper()
	fd, err := unix.Open(filepath.Join(t.TempDir(), "obj"), unix.O_RDWR|unix.O_CREAT, 0o600)
	require.NoError(t, err)
	return fd
}

func reviewImage(t *testing.T, s *Server, sphStr string) *pb.DbImage {
	t.Helper()
	var img pb.DbImage
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(imageBucket).Get([]byte(sphStr))
		require.NotNil(t, v, "image record for %s", sphStr)
		return proto.Unmarshal(v, &img)
	}))
	return &img
}

func packOpenMsg(t *testing.T, msgId, objectId uint32, fd int, fsid string) []byte {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, struc.Pack(&body, &cachefiles_open{
		Fd:        uint32(fd),
		VolumeKey: []byte("erofs," + reviewDomain + "\x00"),
		CookieKey: []byte(fsid),
	}))
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_OPEN, Len: uint32(16 + body.Len()), ObjectId: objectId,
	}))
	out.Write(body.Bytes())
	return out.Bytes()
}

func packCloseMsg(t *testing.T, msgId, objectId uint32) []byte {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_CLOSE, Len: 16, ObjectId: objectId,
	}))
	return out.Bytes()
}

func packReadMsg(t *testing.T, msgId, objectId uint32, off, ln uint64) []byte {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_READ, Len: 32, ObjectId: objectId,
	}))
	require.NoError(t, struc.Pack(&out, &cachefiles_read{Off: off, Len: ln}))
	return out.Bytes()
}

// Reads one reply that the daemon wrote to the fake devnode, or "" on timeout.
func readDevnodeReply(t *testing.T, fd int, timeout time.Duration) string {
	t.Helper()
	tv := unix.NsecToTimeval(timeout.Nanoseconds())
	require.NoError(t, unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv))
	buf := make([]byte, 256)
	n, err := unix.Read(fd, buf)
	if err == unix.EAGAIN {
		return ""
	}
	require.NoError(t, err)
	return string(buf[:n])
}

// A mount whose manifest fetch fails leaves its image record in MountState_Requested
// with no manifest (tryMount returns before its final imageTx). GC keeps Requested images
// by default and aborts the whole collection when a kept image has no manifest, so one
// failed mount wedges "styx gc" until someone passes --error_states.
func TestReviewGcAfterFailedMount(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler()) // manifest cache and manifester both 404
	t.Cleanup(srv.Close)

	s := newReviewServer(t, 2, true)
	initReviewServer(t, s, srv.URL)
	devA, devB := reviewDevnode(t)
	t.Cleanup(func() { unix.Close(devA); unix.Close(devB) })
	s.devnode.Store(int32(devA)) // on-demand enabled; no kernel messages are involved here

	sp := reviewStorePath('0', "missing-1.0")
	_, err := s.handleMountReq(context.Background(), &MountReq{
		Upstream:   srv.URL,
		StorePath:  sp,
		MountPoint: filepath.Join(t.TempDir(), "mp"),
	})
	require.Error(t, err, "mount of a path the manifester can't find must fail")

	img := reviewImage(t, s, reviewSph('0'))
	t.Logf("record left by failed mount: state=%v lastErr=%q", img.MountState, img.LastMountError)

	// the same request "styx gc" sends with default flags
	_, err = s.handleGcReq(context.Background(), &GcReq{
		DryRunFast: true,
		GcByState:  map[pb.MountState]bool{pb.MountState_Unmounted: true},
	})
	require.NoError(t, err, "default gc must not be wedged by an earlier failed mount")
}

// handleMountReq writes the new MountPoint/Upstream into the image record before
// tryMount checks mountCtxMap for a mount in progress. A second request for the same
// store path is rejected with "another mount is in progress", but it has already
// overwritten the in-progress mount's record, so the DB describes the rejected request.
func TestReviewRejectedConcurrentMountOverwritesRecord(t *testing.T) {
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { once.Do(func() { close(release) }) })

	s := newReviewServer(t, 2, true)
	initReviewServer(t, s, srv.URL)
	devA, devB := reviewDevnode(t)
	t.Cleanup(func() { unix.Close(devA); unix.Close(devB) })
	s.devnode.Store(int32(devA))

	sp := reviewStorePath('1', "pkg-1.0")
	mp1 := filepath.Join(t.TempDir(), "first")
	mp2 := filepath.Join(t.TempDir(), "second")

	firstDone := make(chan error, 1)
	go func() {
		_, err := s.handleMountReq(context.Background(), &MountReq{Upstream: srv.URL, StorePath: sp, MountPoint: mp1})
		firstDone <- err
	}()
	select {
	case <-entered: // first mount is now fetching its manifest
	case <-time.After(10 * time.Second):
		t.Fatal("first mount never reached the manifester")
	}

	_, err := s.handleMountReq(context.Background(), &MountReq{Upstream: srv.URL, StorePath: sp, MountPoint: mp2})
	require.ErrorContains(t, err, "another mount is in progress")

	img := reviewImage(t, s, reviewSph('1'))
	once.Do(func() { close(release) })
	<-firstDone

	require.Equal(t, mp1, img.MountPoint,
		"a rejected concurrent mount must not replace the in-progress mount's mount point in the db")
}

// handleClose deletes stateBySlab/readfdBySlab by slab id without checking that the
// entries belong to the object being closed. If the kernel's CLOSE for an old slab
// object is handled after the OPEN for its replacement (the reader hands CLOSE and OPEN
// to different workers, so their order is not preserved), the new object's write fd is
// forgotten and the slab image fds that mountSlabImage just opened are closed.
func TestReviewCloseOfOldSlabObjectKeepsNewState(t *testing.T) {
	s := newReviewServer(t, 2, true)

	oldFd, newFd := reviewTempFd(t), reviewTempFd(t)
	_, err := s.handleOpenSlab(1, 100, uint32(oldFd), 0, 0)
	require.NoError(t, err)
	_, err = s.handleOpenSlab(2, 101, uint32(newFd), 0, 0) // replacement object for slab 0
	require.NoError(t, err)

	readFd, cacheFd := reviewTempFd(t), reviewTempFd(t)
	s.stateLock.Lock()
	s.readfdBySlab[0] = slabFds{readFd, cacheFd} // as set by mountSlabImage
	s.stateLock.Unlock()

	require.NoError(t, s.handleClose(3, 100)) // late CLOSE for the old object

	s.stateLock.Lock()
	st := s.stateBySlab[0]
	fds := s.readfdBySlab[0]
	s.stateLock.Unlock()
	_, fcntlErr := unix.FcntlInt(uintptr(readFd), unix.F_GETFD, 0)
	t.Logf("after CLOSE(old): stateBySlab[0]=%v readfdBySlab[0]=%v; readFd fcntl err=%v", st, fds, fcntlErr)

	require.NotNil(t, st, "CLOSE of the old slab object dropped the new object's state")
	require.Equal(t, uint32(newFd), st.writeFd)
	require.Equal(t, slabFds{readFd, cacheFd}, fds)
	require.NoError(t, fcntlErr, "CLOSE of the old slab object closed the current slab image read fd")
}

// tryMount leaves the image bytes in mountContext after the first (private) mount has
// written them. The real mount's OPEN copies them into the new object's state, and since
// the kernel finds the data already in the backing file it never sends a READ, so the
// daemon holds a full copy of every mounted image until that image is unmounted.
func TestReviewImageDataNotRetainedAfterWrite(t *testing.T) {
	s := newReviewServer(t, 2, true)
	devA, devB := reviewDevnode(t)
	t.Cleanup(func() { unix.Close(devA); unix.Close(devB) })
	s.devnode.Store(int32(devA))

	sphStr := reviewSph('2')
	image := bytes.Repeat([]byte{0xab}, 64<<10)
	mctx := &mountContext{imageSize: int64(len(image)), imageData: image}
	s.mountCtxMap.Put(sphStr, withMountContext(context.Background(), mctx))
	volume := []byte("erofs," + reviewDomain + "\x00")

	// first mount at CachePath/initial/<sph>: OPEN, READ (whole image written), CLOSE
	fdA := reviewTempFd(t)
	require.NoError(t, s.handleOpen(1, 10, uint32(fdA), 0, volume, []byte(sphStr)))
	require.Equal(t, "copen 1,65536", readDevnodeReply(t, devB, 5*time.Second))
	_ = s.handleRead(2, 10, 4096, 0) // READ_COMPLETE ioctl fails on a temp file; the pwrite has happened
	got := make([]byte, len(image))
	_, err := unix.Pread(fdA, got, 0)
	require.NoError(t, err)
	require.Equal(t, image, got, "first mount wrote the image")
	require.NoError(t, s.handleClose(3, 10))

	// real mount: OPEN; no READ follows because the backing file already has the data
	fdB := reviewTempFd(t)
	require.NoError(t, s.handleOpen(4, 11, uint32(fdB), 0, volume, []byte(sphStr)))
	require.Equal(t, "copen 4,65536", readDevnodeReply(t, devB, 5*time.Second))
	s.mountCtxMap.Delete(sphStr) // tryMount returns

	s.stateLock.Lock()
	st := s.cacheState[11]
	s.stateLock.Unlock()
	require.NotNil(t, st)
	require.Nil(t, st.imageData,
		"mounted image object still holds %d bytes of image data that was already written", len(st.imageData))
}

// main() calls Stop(false) on SIGTERM. Stop waits for cachefilesServer, which only checks
// for shutdown between polls, and outside tests the poll timeout is an hour. With no
// cachefiles traffic, SIGTERM hangs until systemd's TimeoutStopSec SIGKILLs the daemon
// (and signal.NotifyContext swallows any further SIGTERM).
func TestReviewStopReturnsPromptlyWhenIdle(t *testing.T) {
	s := newReviewServer(t, 2, false) // IsTesting=false: production poll timeout
	devA, devB := reviewDevnode(t)
	t.Cleanup(func() { unix.Close(devA) })
	s.devnode.Store(int32(devA))

	go s.cachefilesServer()
	// One message (a CLOSE for an unknown object) so a worker has run and logged before
	// Stop; the shared log mutex orders cachefilesServer's shutdownWait.Add before Stop's
	// Wait for the race detector. After it, the loop is back in poll with nothing queued.
	time.Sleep(200 * time.Millisecond)
	_, err := unix.Write(devB, packCloseMsg(t, 1, 99))
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		s.Stop(false)
		close(done)
	}()
	select {
	case <-done:
		unix.Close(devB)
	case <-time.After(5 * time.Second):
		unix.Close(devB) // POLLHUP wakes the poll so the goroutines can exit
		<-done
		t.Fatal("Stop(false) was still blocked after 5s on an idle devnode")
	}
}

// Every cachefiles message goes through one unbuffered channel to cfg.Workers workers,
// and a slab READ holds its worker until the chunk arrives (context.Background, retries
// until success, no deadline). Once cfg.Workers READs are stalled on the network, OPEN
// and CLOSE for unrelated objects are not handled either, so mount(2) and umount(2) of
// fully cached images hang too. Workers=1 here; production uses 16.
func TestReviewStalledReadDoesNotBlockOpen(t *testing.T) {
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	s := newReviewServer(t, 1, true)
	initReviewServer(t, s, srv.URL)

	var sph Sph
	sph[0] = 1
	var dig cdig.CDig
	dig[0] = 7
	locs, err := s.AllocateBatch(withAllocateCtx(context.Background(), sph, false), []uint16{1}, []cdig.CDig{dig})
	require.NoError(t, err)

	devA, devB := reviewDevnode(t)
	s.devnode.Store(int32(devA))
	go s.cachefilesServer()
	t.Cleanup(func() {
		rel()
		unix.Close(devB)
		s.Stop(false)
		unix.Close(devA)
	})

	send := func(b []byte) {
		_, err := unix.Write(devB, b)
		require.NoError(t, err)
	}

	send(packOpenMsg(t, 1, 1, reviewTempFd(t), slabPrefix+"0"))
	require.Equal(t, "copen 1,1099511627776", readDevnodeReply(t, devB, 5*time.Second))

	send(packReadMsg(t, 2, 1, uint64(locs[0].Addr)<<12, 4096))
	select {
	case <-entered: // the READ is now waiting on the chunk store
	case <-time.After(10 * time.Second):
		t.Fatal("slab READ never reached the chunk store")
	}

	send(packOpenMsg(t, 3, 2, reviewTempFd(t), slabImagePrefix+"0"))
	require.Equal(t, "copen 3,4096", readDevnodeReply(t, devB, 5*time.Second),
		"OPEN for an unrelated object was not answered while one slab READ waited on the network")
}
