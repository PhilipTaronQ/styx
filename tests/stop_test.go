package tests

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PhilipTaronQ/styx/manifester"
)

// On SIGTERM the daemon calls Stop(false). Stop waits for the cachefiles
// workers, so it must end a kernel read that is waiting on a stalled chunk
// store instead of waiting for the fetch.
func TestStopWithReadInFlight(t *testing.T) {
	tb := newTestBase(t)
	tb.startManifester()

	// proxy in front of the manifester that can hold chunk and diff requests
	target, err := url.Parse(tb.manifesterAddr)
	require.NoError(t, err)
	rp := httputil.NewSingleHostReverseProxy(target)
	var hold atomic.Bool
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hold.Load() && (r.URL.Path == manifester.ChunkDiffPath || strings.HasPrefix(r.URL.Path, manifester.ChunkReadPath)) {
			select {
			case arrived <- struct{}{}:
			default:
			}
			<-release
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(doRelease)

	tb.manifesterAddr = proxy.URL + "/"
	tb.startDaemon()
	tb.initDaemon()

	mp := tb.mount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")

	// a file big enough that its data is in the slab, not inline
	var big string
	_ = filepath.WalkDir(mp, func(p string, d fs.DirEntry, err error) error {
		if err != nil || big != "" || !d.Type().IsRegular() {
			return err
		}
		if info, err := d.Info(); err == nil && info.Size() > 4096 {
			big = p
		}
		return nil
	})
	require.NotEmpty(t, big)

	hold.Store(true)
	readDone := make(chan error, 1)
	go func() {
		_, err := os.ReadFile(big)
		readDone <- err
	}()
	select {
	case <-arrived:
	case <-time.After(30 * time.Second):
		doRelease()
		t.Fatal("read of", big, "never reached the chunk store")
	}

	d := tb.daemon
	tb.daemon = nil // stopped here, not in cleanup
	start := time.Now()
	stopDone := make(chan struct{})
	go func() {
		d.Stop(true)
		close(stopDone)
	}()

	stopped := false
	select {
	case <-stopDone:
		stopped = true
	case <-time.After(5 * time.Second):
	}
	stopWait := time.Since(start)

	// let the fetch through either way so the read and Stop can finish
	doRelease()
	if !stopped {
		select {
		case <-stopDone:
		case <-time.After(60 * time.Second):
			t.Fatal("Stop did not return even after the chunk store answered")
		}
	}
	t.Logf("Stop returned %v after it was called", time.Since(start))
	select {
	case err := <-readDone:
		t.Log("read result:", err)
	case <-time.After(30 * time.Second):
		t.Log("read still blocked")
	}
	require.True(t, stopped, "Stop blocked for %v on a kernel read waiting for the chunk store", stopWait)
}
