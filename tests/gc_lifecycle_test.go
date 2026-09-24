package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
)

const (
	lcOpusfile     = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	lcOpusfileHash = "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm"
	// valid nixbase32, not in the test data
	lcFakeSph = "1b9p07z77phvv2hf6gm9f28syp39f1ag"
)

// lcCall makes a request and returns the status and raw body, without asserting success.
func (tb *testBase) lcCall(path string, req any) (int, string) {
	var raw json.RawMessage
	c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
	code, err := c.Call(path, req, &raw)
	if err != nil && code == 0 {
		tb.t.Fatalf("call %s: %v", path, err)
	}
	return code, string(raw)
}

// A mount that fails before its manifest is stored used to leave its image Requested with
// no manifest, and gc, which keeps Requested images by default, failed tracing it.
func TestGcAfterFailedMount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))

	// the upstream has no narinfo for this, so the manifester fails and so does the mount
	code, body := tb.lcCall(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  lcFakeSph + "-not-in-upstream",
		MountPoint: t.TempDir(),
	})
	require.NotEqual(t, http.StatusOK, code, "mount of a path the upstream doesn't have should fail: %s", body)

	// what `styx gc` sends with no flags
	code, body = tb.lcCall(daemon.GcPath, daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	require.Equal(t, http.StatusOK, code, "default gc failed after an unrelated failed mount: %s", body)
}

// umount detaches lazily and records Unmounted right away, so gc used to free the image
// while a process still had one of its files open.
func TestGcWhileDetachedMountInUse(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	var big string
	var bigSize int64
	require.NoError(t, filepath.WalkDir(mp, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			if fi, err := d.Info(); err == nil && fi.Size() > bigSize {
				big, bigSize = p, fi.Size()
			}
		}
		return nil
	}))
	require.Greater(t, bigSize, int64(4096), "want a chunked file")

	f, err := os.Open(big)
	require.NoError(t, err)
	defer f.Close()
	want, err := io.ReadAll(f)
	require.NoError(t, err)

	tb.umount(lcOpusfile) // MNT_DETACH: f keeps the filesystem alive
	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc with a file open:", gc)
	require.Zero(t, gc.DeleteImages, "gc deleted an image that is still in use")

	unix.Sync()
	tb.dropCaches()

	got := make([]byte, len(want))
	n, err := f.ReadAt(got, 0)
	require.NoError(t, err, "reading still-open %s after umount and gc", big)
	require.Equal(t, len(want), n)
	require.True(t, bytes.Equal(want, got), "contents of still-open %s changed after umount and gc", big)

	// once the last user is gone, the image can go
	require.NoError(t, f.Close())
	gc = tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc after close:", gc)
	require.Equal(t, 1, gc.DeleteImages)
}

// vaporize reserves slab space in one transaction (preallocateBatch) and links chunks to it
// in a later one (commitPreallocated). gc derives punch ranges from the remaining slab keys
// and the slab Sequence, so it used to punch a reservation along with the garbage chunks
// just before it.
func TestGcDuringVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// make the most recently allocated chunks garbage
	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))
	tb.umount(lcOpusfile)

	name := lcFakeSph + "-gcvaporize"
	src := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.Mkdir(src, 0o755))
	data := make([]byte, 256<<20+12345)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(data)
	require.NoError(t, os.WriteFile(filepath.Join(src, "big"), data, 0o644))

	slab0 := func() daemon.DebugSizeStats {
		for _, si := range tb.debug(daemon.DebugReq{IncludeSlabs: true}).Slabs {
			if si.Index == 0 {
				return si.Stats
			}
		}
		t.Fatal("no slab 0")
		return daemon.DebugSizeStats{}
	}
	before := slab0()

	type result struct {
		code int
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var raw json.RawMessage
		c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
		code, err := c.Call(daemon.VaporizePath, daemon.VaporizeReq{Path: src}, &raw)
		done <- result{code, string(raw), err}
	}()

	// Wait until vaporize has reserved space for "big" (Sequence moved, so the last chunk's
	// computed size grew) but not yet linked any chunk to it.
	for {
		select {
		case res := <-done:
			t.Skipf("inconclusive: vaporize finished (%d %s) before gc could run inside its reservation window", res.code, res.body)
		default:
		}
		st := slab0()
		if st.TotalChunks == before.TotalChunks && st.TotalBlocks > before.TotalBlocks {
			gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
			t.Logf("gc during vaporize (reserved %d blocks): %+v", st.TotalBlocks-before.TotalBlocks, gc)
			break
		}
		time.Sleep(time.Millisecond)
	}

	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, http.StatusOK, res.code, "vaporize with a gc in the middle: %s", res.body)

	dst := tb.materialize(name)
	got, err := os.ReadFile(filepath.Join(dst, "big"))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "materialized file differs from the vaporized source")
}
