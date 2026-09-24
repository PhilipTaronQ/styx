package tests

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/client"
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

// rawCall makes a request and returns the status and raw body, without asserting success.
func (tb *testBase) rawCall(path string, req any) (int, string) {
	var raw json.RawMessage
	code, err := client.NewClient(filepath.Join(tb.cachedir, "styx.sock")).Call(path, req, &raw)
	if err != nil && code == 0 {
		tb.t.Fatalf("call %s: %v", path, err)
	}
	return code, string(raw)
}

// mountExtraTarball runs "styx tarball" on url and mounts the result.
func (tb *testBase) mountExtraTarball(url string) (storePath, mp string) {
	tr := tb.tarball(url)
	storePath = tr.StorePathHash + "-" + tr.StorePathName
	mp = tb.t.TempDir()
	tb.t.Cleanup(func() { _ = unix.Unmount(mp, 0) })
	code, body := tb.rawCall(daemon.MountPath, daemon.MountReq{
		Upstream:   "http://localhost:7444", // daemon.fakeCacheBind
		StorePath:  storePath,
		MountPoint: mp,
	})
	require.Equal(tb.t, http.StatusOK, code, "mount %s: %s", storePath, body)
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

// A database written with the old allocation, where two chunks of an image share an address,
// is repaired when the daemon starts, and the image is rebuilt when it's restored after a
// reboot.
func TestRepairSlabOverlapOnReboot(t *testing.T) {
	tb := newTestBase(t)
	a := bytes.Repeat([]byte{'a'}, 64<<10)
	b := bytes.Repeat([]byte{'b'}, 64<<10)
	url := tb.serveExtraTarball("overlap.tar", map[string][]byte{"a": a, "b": b}, nil)
	tb.startAll()

	_, mp := tb.mountExtraTarball(url)
	requireFileBytes(t, filepath.Join(mp, "a"), a)
	requireFileBytes(t, filepath.Join(mp, "b"), b)

	// reboot
	tb.daemon.Stop(true)
	tb.daemon = nil
	require.NoError(t, unix.Unmount(mp, 0))
	tb.dropCaches()

	// make the database look like the old allocation did it: b's chunk was allocated at a's
	// address and took over its address key. the image still has b's chunk at its own
	// address, so if it isn't rebuilt, a and b can't both read back right.
	da, db := cdig.Sum(a), cdig.Sum(b)
	var addrA, addrB uint32
	bdb, err := bbolt.Open(filepath.Join(tb.cachedir, "styx.bolt"), 0o644, nil)
	require.NoError(t, err)
	require.NoError(t, bdb.Update(func(tx *bbolt.Tx) error {
		cb := tx.Bucket([]byte("chunk"))
		sb := tx.Bucket([]byte("slab")).Bucket([]byte{0, 0})
		va, vb := bytes.Clone(cb.Get(da[:])), bytes.Clone(cb.Get(db[:]))
		require.NotNil(t, va)
		require.NotNil(t, vb)
		addrA, addrB = binary.LittleEndian.Uint32(va[2:]), binary.LittleEndian.Uint32(vb[2:])
		require.NotEqual(t, addrA, addrB)
		key := func(addr uint32) []byte { return binary.BigEndian.AppendUint32(nil, addr) }
		require.NoError(t, sb.Delete(key(addrB)))
		require.NoError(t, sb.Delete(key(addrB|1<<31)))
		require.NoError(t, sb.Put(key(addrA), db[:]))
		binary.LittleEndian.PutUint32(vb[2:], addrA)
		require.NoError(t, cb.Put(db[:], vb))
		// written before the repair existed
		return tx.Bucket([]byte("meta")).Delete([]byte("slab-overlap-repaired"))
	}))
	require.NoError(t, bdb.Close())
	t.Logf("a's chunk at %d, b's chunk moved from %d to %d", addrA, addrB, addrA)

	tb.startDaemon()

	requireFileBytes(t, filepath.Join(mp, "a"), a)
	requireFileBytes(t, filepath.Join(mp, "b"), b)

	chunks := tb.debug(daemon.DebugReq{IncludeChunks: []string{da.String(), db.String()}}).Chunks
	t.Logf("after repair: a's chunk at %d, b's at %d", chunks[da.String()].Addr, chunks[db.String()].Addr)
	require.NotEqual(t, chunks[da.String()].Addr, chunks[db.String()].Addr)
}
