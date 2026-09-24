package daemon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

const (
	testSpX = "53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12"
	testSpB = "kcyrz2y8si9ry5p8qkmj0gp41n01sa1y-opusfile-0.11"
)

// fetchEnv is a Server with a real bbolt db, a plain file standing in for slab 0, and an
// httptest server playing chunk store, chunk differ, manifest cache and manifester.
type fetchEnv struct {
	t   *testing.T
	s   *Server
	srv *httptest.Server

	mu     sync.Mutex
	chunks map[cdig.CDig][]byte

	diffStatus atomic.Int32 // if nonzero, chunk differ fails with this status
	diffCalls  atomic.Int32
	diffBomb   atomic.Bool // if set, chunk differ sends 64 MiB of zeros instead of the data

	manifestCacheHit []byte        // zstd envelope to serve from the manifest cache (nil: 404)
	manifesterHang   bool          // first manifester request blocks until the client goes away
	manifesterGate   chan struct{} // if set, first manifester request succeeds once it's closed
	manifestStarted  chan struct{} // closed when the first manifester request arrives
	manifestPosts    atomic.Int32
	manifestReqs     []manifester.ManifestReq // requests the manifester got (under mu)
	quit             chan struct{}            // closed at cleanup so hanging handlers return
}

func newFetchEnv(t *testing.T) *fetchEnv {
	e := &fetchEnv{
		t:               t,
		chunks:          make(map[cdig.CDig][]byte),
		manifestStarted: make(chan struct{}),
		quit:            make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc(manifester.ChunkReadPath, e.handleChunk)
	mux.HandleFunc(manifester.ChunkDiffPath, e.handleChunkDiff)
	mux.HandleFunc(manifester.ManifestCachePath, e.handleManifestCache)
	mux.HandleFunc(manifester.ManifestPath, e.handleManifester)
	e.srv = httptest.NewServer(mux)
	t.Cleanup(e.srv.Close)
	t.Cleanup(func() { close(e.quit) }) // runs before srv.Close

	s := newTestServer(t, 4, false)
	initTestServer(t, s, e.srv.URL)

	// plain file standing in for the slab 0 cachefiles object (like setupFakeSlabImage)
	f, err := os.OpenFile(filepath.Join(s.cfg.CachePath, "slab0"), os.O_RDWR|os.O_CREATE, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	fd := int(f.Fd())
	s.stateLock.Lock()
	s.stateBySlab[0] = &openFileState{writeFd: uint32(fd), tp: typeSlab, slabId: 0}
	s.readfdBySlab[0] = slabFds{readFd: fd, cacheFd: fd}
	s.stateLock.Unlock()

	e.s = s
	return e
}

func (e *fetchEnv) chunk(d cdig.CDig) ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.chunks[d]
	return b, ok
}

func (e *fetchEnv) handleChunk(w http.ResponseWriter, r *http.Request) {
	d, err := cdig.FromBase64(filepath.Base(r.URL.Path))
	if err != nil {
		http.Error(w, "bad digest", http.StatusBadRequest)
		return
	}
	b, ok := e.chunk(d)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Write(b)
}

// Minimal chunk differ: only batches (no bases, no expand), which is what the daemon asks
// for when there is no diff base.
func (e *fetchEnv) handleChunkDiff(w http.ResponseWriter, r *http.Request) {
	e.diffCalls.Add(1)
	if st := e.diffStatus.Load(); st != 0 {
		http.Error(w, "injected chunk differ failure", int(st))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req pb.ManifesterChunkDiffReq
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var lens pb.Lengths
	var out []byte
	for _, rr := range req.Req {
		if len(rr.Bases) > 0 || rr.ExpandBeforeDiff != "" {
			http.Error(w, "test differ only does batches", http.StatusBadRequest)
			return
		}
		n := 0
		for _, d := range cdig.FromSliceAlias(rr.Reqs) {
			b, ok := e.chunk(d)
			if !ok {
				http.NotFound(w, r)
				return
			}
			out = append(out, b...)
			n += len(b)
		}
		lens.Length = append(lens.Length, int64(n))
	}
	if e.diffBomb.Load() {
		out = make([]byte, 64<<20)
	}
	out = append(out, "{}"...)
	comp, err := zstd.Compress(nil, out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lb, err := proto.Marshal(&lens)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(manifester.LengthsHeader, base64.RawURLEncoding.EncodeToString(lb))
	w.Write(comp)
}

func (e *fetchEnv) handleManifestCache(w http.ResponseWriter, r *http.Request) {
	if e.manifestCacheHit == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(e.manifestCacheHit)
}

func (e *fetchEnv) handleManifester(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var mreq manifester.ManifestReq
	if err := json.NewDecoder(r.Body).Decode(&mreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	e.mu.Lock()
	e.manifestReqs = append(e.manifestReqs, mreq)
	e.mu.Unlock()
	if n := e.manifestPosts.Add(1); n == 1 && e.manifesterHang {
		close(e.manifestStarted)
		select {
		case <-r.Context().Done(): // the client gave up
		case <-e.quit:
		}
		return
	} else if n == 1 && e.manifesterGate != nil {
		close(e.manifestStarted)
		select {
		case <-e.manifesterGate:
		case <-r.Context().Done():
			return
		case <-e.quit:
			return
		}
	}
	comp, err := zstd.Compress(nil, []byte("rebuilt envelope"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(comp)
}

func testChunks(n int, seed uint64) [][]byte {
	r := rand.New(rand.NewPCG(seed, seed))
	out := make([][]byte, n)
	for i := range out {
		b := make([]byte, 1<<16) // default chunk shift
		for j := range b {
			b[j] = byte(r.Uint32())
		}
		out[i] = b
	}
	return out
}

func (e *fetchEnv) serveChunks(chunks [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, c := range chunks {
		e.chunks[cdig.Sum(c)] = c
	}
}

// addImage leaves the db the way getManifestAndBuildImage does for an image with one
// regular file made of the given chunks: inline manifest envelope, catalog entries, and
// chunk/slab allocations.
func (e *fetchEnv) addImage(storePath string, chunks [][]byte) ([]cdig.CDig, []erofs.SlabLoc) {
	return e.addImageFile(storePath, "/file", chunks)
}

// addImageFile is addImage with the file at path.
func (e *fetchEnv) addImageFile(storePath, path string, chunks [][]byte) ([]cdig.CDig, []erofs.SlabLoc) {
	t, s := e.t, e.s
	sph, sphStr, spName, err := ParseSphAndName(storePath)
	require.NoError(t, err)

	digests := make([]cdig.CDig, len(chunks))
	blocks := make([]uint16, len(chunks))
	var size int64
	for i, c := range chunks {
		digests[i] = cdig.Sum(c)
		blocks[i] = uint16(s.blockShift.Blocks(int64(len(c))))
		size += int64(len(c))
	}
	m := &pb.Manifest{Entries: []*pb.Entry{
		{Path: "/", Type: pb.EntryType_DIRECTORY},
		{Path: path, Type: pb.EntryType_REGULAR, Size: size, Digests: cdig.ToSliceAlias(digests)},
	}}
	mb, err := proto.Marshal(m)
	require.NoError(t, err)
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:       common.ManifestContext + "/" + storePath,
			Type:       pb.EntryType_REGULAR,
			Size:       int64(len(mb)),
			InlineData: mb,
		},
	})
	require.NoError(t, err)
	e.putManifest(sph, sphStr, spName, envelope)

	locs, err := s.AllocateBatch(withAllocateCtx(context.Background(), sph, false), blocks, digests)
	require.NoError(t, err)
	return digests, locs
}

func (e *fetchEnv) putManifest(sph Sph, sphStr, spName string, envelope []byte) {
	require.NoError(e.t, e.s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(manifestBucket).Put([]byte(sphStr), envelope); err != nil {
			return err
		}
		fkey := append(append([]byte(spName), 0), sph[:]...)
		if err := tx.Bucket(catalogFBucket).Put(fkey, []byte{}); err != nil {
			return err
		}
		return tx.Bucket(catalogRBucket).Put(sph[:], []byte(spName))
	}))
}

func (e *fetchEnv) sphps(d cdig.CDig) (out []SphPrefix) {
	_ = e.s.db.View(func(tx *bbolt.Tx) error {
		out = sphpsFromLoc(tx.Bucket(chunkBucket).Get(d[:]))
		return nil
	})
	return
}

func (e *fetchEnv) diffMapLen() int {
	e.s.diffLock.Lock()
	defer e.s.diffLock.Unlock()
	return len(e.s.diffMap)
}

func requestChunkWithin(t *testing.T, s *Server, loc erofs.SlabLoc, d cdig.CDig, sphps []SphPrefix, within time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.requestChunk(context.Background(), loc, d, sphps) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(within):
		t.Fatalf("requestChunk(%v) did not return within %v", loc, within)
	}
}
