package daemon

import (
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
