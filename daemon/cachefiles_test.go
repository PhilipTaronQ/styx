package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
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
