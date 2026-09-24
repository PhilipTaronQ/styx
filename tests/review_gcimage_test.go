package tests

// Tests written during review to demonstrate suspected bugs in gc, vaporize,
// materialize and the erofs builder / slab allocator. Each one asserts the
// correct behavior, so it fails while the bug is present.

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
)

const (
	reviewOpusfile     = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	reviewOpusfileHash = "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm"
	// valid nixbase32, not in the test data
	reviewFakeHash = "1b9p07z77phvv2hf6gm9f28syp39f1ag"
)

func (tb *testBase) reviewClient() *client.StyxClient {
	return client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
}

// reviewCall makes a request and returns the status and raw body, without
// asserting success.
func (tb *testBase) reviewCall(path string, req any) (int, string) {
	var raw json.RawMessage
	code, err := tb.reviewClient().Call(path, req, &raw)
	if err != nil && code == 0 {
		tb.t.Fatalf("call %s: %v", path, err)
	}
	return code, string(raw)
}

// reviewServeTarball serves a tar of files at <upstream>/review/<name>, next to
// the normal test data. Call it before tb.startAll: the harness's own test data
// server then fails to bind the same address, and its goroutine exits quietly.
func (tb *testBase) reviewServeTarball(name string, files map[string][]byte) string {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, n := range slices.Sorted(maps.Keys(files)) {
		require.NoError(tb.t, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     n,
			Mode:     0o644,
			Size:     int64(len(files[n])),
		}))
		_, err := tw.Write(files[n])
		require.NoError(tb.t, err)
	}
	require.NoError(tb.t, tw.Close())
	data := buf.Bytes()

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(TestdataDir)))
	mux.HandleFunc("/review/"+name, func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
	})
	l, err := net.Listen("tcp", tb.upstreamHost)
	require.NoError(tb.t, err)
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	tb.t.Cleanup(func() { srv.Close() })
	return tb.upstreamUrl + "review/" + name
}

func reviewPrefix(b []byte) []byte { return b[:min(8, len(b))] }

// common.AppendBlocksList gives the last chunk of a file whose size is an exact
// multiple of the chunk size zero blocks, so AllocateBatch puts the next chunk
// at the same slab address. Reading the first file then returns the second
// file's data.
func TestReviewExactChunkMultipleMount(t *testing.T) {
	tb := newTestBase(t)
	a := bytes.Repeat([]byte{'a'}, 64<<10)      // exactly one 64 KiB chunk
	b := bytes.Repeat([]byte{'b'}, 64<<10+4096) // a full chunk and a 4 KiB chunk
	url := tb.reviewServeTarball("exactmult.tar", map[string][]byte{"a": a, "b": b})
	tb.startAll()

	tr := tb.tarball(url)
	sp := tr.StorePathHash + "-" + tr.StorePathName
	mp := t.TempDir()
	t.Cleanup(func() { _ = unix.Unmount(mp, 0) })
	code, body := tb.reviewCall(daemon.MountPath, daemon.MountReq{
		Upstream:   "http://localhost:7444", // daemon.fakeCacheBind
		StorePath:  sp,
		MountPoint: mp,
	})
	require.Equal(t, http.StatusOK, code, "mount %s: %s", sp, body)

	gotA, errA := os.ReadFile(filepath.Join(mp, "a"))
	gotB, errB := os.ReadFile(filepath.Join(mp, "b"))
	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Truef(t, bytes.Equal(a, gotA), "a: read %d bytes starting %q, want %d bytes of 'a'", len(gotA), reviewPrefix(gotA), len(a))
	require.Truef(t, bytes.Equal(b, gotB), "b: read %d bytes starting %q, want %d bytes of 'b'", len(gotB), reviewPrefix(gotB), len(b))
}

// Same allocator bug through vaporize: the second file's chunk is written over
// the first file's.
func TestReviewExactChunkMultipleVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	name := reviewFakeHash + "-exactmult"
	src := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.Mkdir(src, 0o755))
	a := bytes.Repeat([]byte{'a'}, 64<<10)
	b := bytes.Repeat([]byte{'b'}, 64<<10)
	require.NoError(t, os.WriteFile(filepath.Join(src, "a"), a, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b"), b, 0o644))

	code, body := tb.reviewCall(daemon.VaporizePath, daemon.VaporizeReq{Path: src})
	require.Equal(t, http.StatusOK, code, "vaporize: %s", body)

	dst := tb.materialize(name)
	gotA, err := os.ReadFile(filepath.Join(dst, "a"))
	require.NoError(t, err)
	gotB, err := os.ReadFile(filepath.Join(dst, "b"))
	require.NoError(t, err)
	require.Truef(t, bytes.Equal(a, gotA), "a: materialized %d bytes starting %q", len(gotA), reviewPrefix(gotA))
	require.Truef(t, bytes.Equal(b, gotB), "b: materialized %d bytes starting %q", len(gotB), reviewPrefix(gotB))
}

// A mount that fails before its manifest is stored leaves the image record in
// Requested. gc keeps Requested images by default, so it traces this one, can't
// find its manifest, and fails.
func TestReviewGcAfterFailedMount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	code, body := tb.reviewCall(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  reviewFakeHash + "-not-in-upstream",
		MountPoint: t.TempDir(),
	})
	require.NotEqual(t, http.StatusOK, code, "mount of a path the upstream doesn't have should fail: %s", body)

	code, body = tb.reviewCall(daemon.GcPath, daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	require.Equal(t, http.StatusOK, code, "default gc failed after an unrelated failed mount: %s", body)
}

// gc deletes an unmounted image's record, chunks and slab data, but not the
// image's cachefiles object. Mounting the same store path again builds a new
// image of the same size; cachefiles' coherency check only compares the size,
// so the kernel keeps the old image, whose chunk addresses were just freed.
func TestReviewGcThenRemount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp1 := tb.mount(reviewOpusfile)
	require.Equal(t, reviewOpusfileHash, tb.nixHash(mp1))
	tb.umount(reviewOpusfile)

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc:", gc)
	require.Equal(t, 1, gc.DeleteImages)
	require.Greater(t, gc.PunchBytes, int64(0))

	unix.Sync()
	tb.dropCaches()

	mp2 := tb.mount(reviewOpusfile)
	out, err := exec.Command("nix-hash", "--type", "sha256", "--base32", mp2).CombinedOutput()
	require.NoError(t, err, "nix-hash after gc and remount: %s", out)
	require.Equal(t, reviewOpusfileHash, strings.TrimSpace(string(out)))
}

// umount detaches lazily and records Unmounted right away, so gc frees the
// image while a process still has one of its files open.
func TestReviewGcWhileDetachedMountInUse(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(reviewOpusfile)
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

	tb.umount(reviewOpusfile) // MNT_DETACH: f keeps the filesystem alive
	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc:", gc)
	require.Equal(t, 1, gc.DeleteImages)

	unix.Sync()
	tb.dropCaches()

	got := make([]byte, len(want))
	n, err := f.ReadAt(got, 0)
	require.NoError(t, err, "reading still-open %s after umount and gc", big)
	require.Equal(t, len(want), n)
	require.True(t, bytes.Equal(want, got), "contents of still-open %s changed after umount and gc", big)
}

// vaporize reserves slab space in one transaction (preallocateBatch) and links
// chunks to it in a later one (commitPreallocated). gc derives punch ranges
// from the remaining slab keys and the slab Sequence, so a reservation with no
// keys yet is inside the range punched for the garbage chunks just before it.
func TestReviewGcDuringVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// make the most recently allocated chunks garbage
	mp := tb.mount(reviewOpusfile)
	require.Equal(t, reviewOpusfileHash, tb.nixHash(mp))
	tb.umount(reviewOpusfile)

	name := reviewFakeHash + "-gcvaporize"
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
		code, err := tb.reviewClient().Call(daemon.VaporizePath, daemon.VaporizeReq{Path: src}, &raw)
		done <- result{code, string(raw), err}
	}()

	// Wait until vaporize has reserved space for "big" (Sequence moved, so the
	// last chunk's computed size grew) but not yet linked any chunk to it.
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
