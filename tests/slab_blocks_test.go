package tests

import (
	"archive/tar"
	"bytes"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/daemon"
)

// valid nixbase32, not in the test data
const unusedStorePathHash = "1b9p07z77phvv2hf6gm9f28syp39f1ag"

// serveExtraTarball serves a tar of files and symlinks at <upstream>/extra/<name>, next to the
// normal test data. Call it before tb.startAll: the harness's own test data server then fails
// to bind the same address, and its goroutine exits quietly.
func (tb *testBase) serveExtraTarball(name string, files map[string][]byte, links map[string]string) string {
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
	for _, n := range slices.Sorted(maps.Keys(links)) {
		require.NoError(tb.t, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     n,
			Linkname: links[n],
			Mode:     0o777,
		}))
	}
	require.NoError(tb.t, tw.Close())
	data := buf.Bytes()

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(TestdataDir)))
	mux.HandleFunc("/extra/"+name, func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
	})
	l, err := net.Listen("tcp", tb.upstreamHost)
	require.NoError(tb.t, err)
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	tb.t.Cleanup(func() { srv.Close() })
	return tb.upstreamUrl + "extra/" + name
}

// mountExtraTarball runs "styx tarball" on url and mounts the result.
func (tb *testBase) mountExtraTarball(url string) (storePath, mp string) {
	tr := tb.tarball(url)
	storePath = tr.StorePathHash + "-" + tr.StorePathName
	mp = tb.t.TempDir()
	tb.t.Cleanup(func() {
		// like tb.mount's cleanup (collect doesn't hash these, they have no narinfo)
		tb.collect()
		_ = unix.Unmount(mp, 0)
	})
	var res daemon.Status
	tb.call(daemon.MountPath, daemon.MountReq{
		Upstream:   "http://localhost:7444", // daemon.fakeCacheBind
		StorePath:  storePath,
		MountPoint: mp,
	}, &res)
	return storePath, mp
}

func requireFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Truef(t, bytes.Equal(want, got), "%s: read %d bytes starting %q, want %d bytes starting %q",
		path, len(got), got[:min(8, len(got))], len(want), want[:min(8, len(want))])
}

// A file whose size is an exact multiple of the chunk size used to get 0 slab blocks for its
// last chunk, so the next chunk was allocated at the same address and reading the first file
// returned the second file's data.
func TestExactChunkMultipleMount(t *testing.T) {
	tb := newTestBase(t)
	a := bytes.Repeat([]byte{'a'}, 64<<10)      // exactly one 64 KiB chunk
	b := bytes.Repeat([]byte{'b'}, 64<<10+4096) // a full chunk and a 4 KiB chunk
	url := tb.serveExtraTarball("exactmult.tar", map[string][]byte{"a": a, "b": b}, nil)
	tb.startAll()

	_, mp := tb.mountExtraTarball(url)
	requireFileBytes(t, filepath.Join(mp, "a"), a)
	requireFileBytes(t, filepath.Join(mp, "b"), b)
}

// Same through vaporize: the second file's chunk was written over the first file's.
func TestExactChunkMultipleVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	name := unusedStorePathHash + "-exactmult"
	src := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.Mkdir(src, 0o755))
	a := bytes.Repeat([]byte{'a'}, 64<<10)
	b := bytes.Repeat([]byte{'b'}, 64<<10)
	require.NoError(t, os.WriteFile(filepath.Join(src, "a"), a, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "b"), b, 0o644))
	tb.vaporize(src)

	dst := tb.materialize(name)
	requireFileBytes(t, filepath.Join(dst, "a"), a)
	requireFileBytes(t, filepath.Join(dst, "b"), b)
}

// Symlink targets of 4065 to 4095 bytes used to panic the image builder.
func TestLongSymlink(t *testing.T) {
	tb := newTestBase(t)
	links := make(map[string]string)
	for _, n := range []int{100, 4064, 4065, 4095} {
		links["link"+strconv.Itoa(n)] = strings.Repeat("t", n-1) + "!"
	}
	url := tb.serveExtraTarball("longlink.tar", map[string][]byte{"f": []byte("hello")}, links)
	tb.startAll()

	_, mp := tb.mountExtraTarball(url)
	for name, target := range links {
		got, err := os.Readlink(filepath.Join(mp, name))
		require.NoError(t, err)
		require.Equal(t, target, got, "target of %s (%d bytes)", name, len(target))
	}
	requireFileBytes(t, filepath.Join(mp, "f"), []byte("hello"))
}
