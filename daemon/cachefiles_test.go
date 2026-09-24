package daemon

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/cdig"
)

// main() calls Stop(false) on SIGTERM. Outside tests the cachefiles poll timeout is an
// hour, so Stop must wake the poll rather than wait for it.
func TestStopReturnsPromptlyWhenIdle(t *testing.T) {
	s := newTestServer(t, 2, false) // IsTesting=false: production poll timeout
	devA, devB := testDevnode(t)
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

// A slab READ waiting on the chunk store must not hold up OPEN (or CLOSE) for other
// objects, even with every READ worker busy. Workers=1 here; production uses 16.
func TestStalledReadDoesNotBlockOpen(t *testing.T) {
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

	s := newTestServer(t, 1, true)
	initTestServer(t, s, srv.URL)

	var sph Sph
	sph[0] = 1
	var dig cdig.CDig
	dig[0] = 7
	locs, err := s.AllocateBatch(withAllocateCtx(context.Background(), sph, false), []uint16{1}, []cdig.CDig{dig})
	require.NoError(t, err)

	devA, devB := testDevnode(t)
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

	send(packOpenMsg(t, 1, 1, testTempFd(t), slabPrefix+"0"))
	require.Equal(t, "copen 1,1099511627776", readDevnodeReply(t, devB, 5*time.Second))

	send(packReadMsg(t, 2, 1, uint64(locs[0].Addr)<<12, 4096))
	select {
	case <-entered: // the READ is now waiting on the chunk store
	case <-time.After(10 * time.Second):
		t.Fatal("slab READ never reached the chunk store")
	}

	send(packOpenMsg(t, 3, 2, testTempFd(t), slabImagePrefix+"0"))
	require.Equal(t, "copen 3,4096", readDevnodeReply(t, devB, 5*time.Second),
		"OPEN for an unrelated object was not answered while one slab READ waited on the network")
}

// The late CLOSE for a slab object that the kernel has already replaced (as happens when
// mountSlabImage remounts) must not drop the new object's state or close the slab image
// fds.
func TestCloseOfOldSlabObjectKeepsNewState(t *testing.T) {
	s := newTestServer(t, 2, true)

	oldFd, newFd := testTempFd(t), testTempFd(t)
	_, err := s.handleOpenSlab(1, 100, uint32(oldFd), 0, 0)
	require.NoError(t, err)
	_, err = s.handleOpenSlab(2, 101, uint32(newFd), 0, 0) // replacement object for slab 0
	require.NoError(t, err)

	readFd, cacheFd := testTempFd(t), testTempFd(t)
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

// Once the first (private) mount has written the image, the real mount's object must not
// hold another copy of it: the kernel finds the data in the backing file and never sends
// the READ that would drop it.
func TestImageDataNotRetainedAfterWrite(t *testing.T) {
	s := newTestServer(t, 2, true)
	devA, devB := testDevnode(t)
	t.Cleanup(func() { unix.Close(devA); unix.Close(devB) })
	s.devnode.Store(int32(devA))

	sphStr := testSph('2')
	image := bytes.Repeat([]byte{0xab}, 64<<10)
	mctx := &mountContext{imageSize: int64(len(image)), imageData: image}
	s.mountCtxMap.Put(sphStr, withMountContext(context.Background(), mctx))
	volume := []byte("erofs," + testDomain + "\x00")

	// first mount at CachePath/initial/<sph>: OPEN, READ (whole image written), CLOSE
	fdA := testTempFd(t)
	require.NoError(t, s.handleOpen(1, 10, uint32(fdA), 0, volume, []byte(sphStr)))
	require.Equal(t, "copen 1,65536", readDevnodeReply(t, devB, 5*time.Second))
	_ = s.handleRead(2, 10, 4096, 0) // READ_COMPLETE ioctl fails on a temp file; the pwrite has happened
	got := make([]byte, len(image))
	_, err := unix.Pread(fdA, got, 0)
	require.NoError(t, err)
	require.Equal(t, image, got, "first mount wrote the image")
	require.NoError(t, s.handleClose(3, 10))

	// real mount: OPEN; no READ follows because the backing file already has the data
	fdB := testTempFd(t)
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

type restoreFdStore struct {
	fd      int
	removed bool
}

func (*restoreFdStore) Ready() {}

func (f *restoreFdStore) GetFd(string) (int, error) { return f.fd, nil }

func (*restoreFdStore) SaveFd(string, int) {}

func (f *restoreFdStore) RemoveFd(string) { f.removed = true }

// If the saved devnode can't be restored, setupDevNode drops it and closes it, expecting
// systemd to restart the daemon. Start must fail so that happens, instead of carrying on
// without on-demand.
func TestStartFailsWhenDevnodeRestoreFails(t *testing.T) {
	var p [2]int
	require.NoError(t, unix.Pipe2(p[:], unix.O_CLOEXEC))
	t.Cleanup(func() { unix.Close(p[1]) })
	fdStore := &restoreFdStore{fd: p[0]} // writing "restore" to a pipe's read end fails

	s := NewServer(Config{
		DevPath:         "/nonexistent",
		CachePath:       t.TempDir(),
		CacheTag:        testDomain,
		CacheDomain:     testDomain,
		ErofsBlockShift: 12,
		Workers:         2,
		IsTesting:       true,
		FdStore:         fdStore,
	})
	err := s.Start()
	t.Cleanup(func() {
		if err == nil {
			s.Stop(false)
		} else if s.db != nil {
			s.db.Close()
		}
	})
	require.ErrorIs(t, err, errRestoreFailed)
	require.True(t, fdStore.removed, "saved devnode was not removed")
}
