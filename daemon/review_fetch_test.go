package daemon

// Review tests for the daemon's data-fetch path (diff.go, manifest_client.go).
// Each test demonstrates one finding; they are expected to FAIL on the current code.

import (
	"context"
	"encoding/base64"
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
	"github.com/stretchr/testify/assert"
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
	reviewSpX = "53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12"
	reviewSpB = "kcyrz2y8si9ry5p8qkmj0gp41n01sa1y-opusfile-0.11"
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

	manifestCacheHit []byte        // zstd envelope to serve from the manifest cache (nil: 404)
	manifesterHang   bool          // first manifester request blocks until the client goes away
	manifestStarted  chan struct{} // closed when the first manifester request arrives
	manifestPosts    atomic.Int32
	quit             chan struct{} // closed at cleanup so hanging handlers return
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

	dir := t.TempDir()
	s := NewServer(Config{
		CachePath:       dir,
		CacheDomain:     "reviewtest",
		ErofsBlockShift: 12,
		Workers:         4,
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() })
	require.NoError(t, s.postInit(&pb.DaemonParams{
		Params:           &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		ManifesterUrl:    e.srv.URL,
		ManifestCacheUrl: e.srv.URL,
		ChunkReadUrl:     e.srv.URL,
		ChunkDiffUrl:     e.srv.URL,
	}, nil))

	// plain file standing in for the slab 0 cachefiles object (like setupFakeSlabImage)
	f, err := os.OpenFile(filepath.Join(dir, "slab0"), os.O_RDWR|os.O_CREATE, 0o600)
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
	io.Copy(io.Discard, r.Body)
	if n := e.manifestPosts.Add(1); n == 1 && e.manifesterHang {
		close(e.manifestStarted)
		select {
		case <-r.Context().Done(): // the client gave up
		case <-e.quit:
		}
		return
	}
	comp, err := zstd.Compress(nil, []byte("rebuilt envelope"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(comp)
}

func reviewChunks(n int, seed uint64) [][]byte {
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
		{Path: "/file", Type: pb.EntryType_REGULAR, Size: size, Digests: cdig.ToSliceAlias(digests)},
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

// requestChunk for a chunk that is already present (a second kernel read request for
// another page of the chunk, issued before the first request's op wrote it and handled
// after that op finished; or a present bit that survived a crash without its data):
// buildDiff skips the present target but adds its missing neighbours to set.ops and to
// diffMap, then requestChunk hits "buildDiff did not include requested chunk" and never
// starts those ops. Their done channels are never closed, so any later read of those
// neighbours blocks forever.
func TestReviewRequestChunkForPresentChunkLeaksUnstartedOps(t *testing.T) {
	e := newFetchEnv(t)
	chunks := reviewChunks(12, 1)
	e.serveChunks(chunks)
	digests, locs := e.addImage(reviewSpX, chunks)
	sphps := e.sphps(digests[0])

	// first kernel read of chunk 0: a batch op fetches chunks 0..7 (InitOpSize)
	requestChunkWithin(t, e.s, locs[0], digests[0], sphps, 10*time.Second)
	require.EqualValues(t, 1, e.diffCalls.Load())
	require.Zero(t, e.diffMapLen())

	// second read request for chunk 0, handled after the first op finished
	requestChunkWithin(t, e.s, locs[0], digests[0], sphps, 10*time.Second)

	assert.Zero(t, e.diffMapLen(), "requestChunk left diffMap entries for ops it never started")

	// a later read of chunk 8 finds the leaked op in diffMap and waits on it forever
	requestChunkWithin(t, e.s, locs[8], digests[8], e.sphps(digests[8]), 5*time.Second)
}

// requestChunk recovers from a failed diff op by reading the chunk directly.
// requestPrefetch (styx prefetch, and materialize) has no fallback, so the same chunk
// differ failure fails the whole request even though every chunk can be read directly.
// ("recompress mismatch", which doDiffOp says should "fall back to single", is the
// deterministic version of this.)
func TestReviewPrefetchDoesNotFallBackWhenDiffFails(t *testing.T) {
	e := newFetchEnv(t)
	chunks := reviewChunks(4, 2)
	e.serveChunks(chunks)
	digests, locs := e.addImage(reviewSpX, chunks)
	e.diffStatus.Store(http.StatusInternalServerError)

	requestChunkWithin(t, e.s, locs[0], digests[0], e.sphps(digests[0]), 10*time.Second)
	require.EqualValues(t, 1, e.diffCalls.Load(), "requestChunk should have tried a diff first")

	err := e.s.requestPrefetch(context.Background(), digests[1:])
	require.EqualValues(t, 2, e.diffCalls.Load(), "requestPrefetch should have tried a diff")
	require.NoError(t, err, "prefetch failed although every chunk is readable directly")
}

// doRemanifestReqs dedups through remanifestCache. The first caller runs the remanifest on
// its own context and caches the result, including a context.Canceled caused by that
// caller going away. Every other caller for the next remanifestCacheExpiry (1 minute)
// gets that failure without the manifester being asked again. For a kernel read that hit
// NotFound, that means EIO.
func TestReviewRemanifestCacheSharesFirstCallersCancellation(t *testing.T) {
	e := newFetchEnv(t)
	e.manifesterHang = true
	req := MountReq{StorePath: reviewSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	ctx1, cancel1 := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- e.s.doRemanifestReqs(ctx1, []MountReq{req}) }()
	select {
	case <-e.manifestStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("manifester never got the first request")
	}
	cancel1() // e.g. `styx materialize` interrupted, or nix killed during a mount
	require.Error(t, <-errc)

	// another caller with a live context; the manifester would succeed now
	err := e.s.doRemanifestReqs(context.Background(), []MountReq{req})
	assert.NoError(t, err, "second caller got the first caller's cancellation")
	assert.EqualValues(t, 2, e.manifestPosts.Load(), "second caller never reached the manifester")
}

// Remanifesting exists to make the manifester rebuild a manifest and re-upload its chunks
// after a chunk read hit NotFound. doRemanifestReqs goes through getManifestFromManifester,
// which returns the manifest cache entry if there is one, so when the chunks are gone but
// the cached manifest is not (sharded build where a later shard failed, S3 GC race), the
// manifester is never asked and nothing is repaired.
func TestReviewRemanifestIsServedFromManifestCache(t *testing.T) {
	e := newFetchEnv(t)
	comp, err := zstd.Compress(nil, []byte("cached envelope"))
	require.NoError(t, err)
	e.manifestCacheHit = comp
	req := MountReq{StorePath: reviewSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	require.NoError(t, e.s.doRemanifestReqs(context.Background(), []MountReq{req}))
	require.EqualValues(t, 1, e.manifestPosts.Load(), "remanifest did not ask the manifester to rebuild")
}

// requestChunk takes diffLock without defer and runs buildDiff (manifest reads, proto
// decoding, iterator arithmetic on db contents) under it. handleMessage recovers panics, so
// a panic there leaves diffLock held and every later chunk request blocks forever.
// One data-driven trigger: an envelope's chunk_shift is not covered by the signature
// (entryFingerprint), and a negative value panics in Shift.Size() when a diff base's
// chunked manifest is read.
func TestReviewPanicInBuildDiffLeavesDiffLockHeld(t *testing.T) {
	e := newFetchEnv(t)
	chunks := reviewChunks(2, 3)
	e.serveChunks(chunks)
	digests, locs := e.addImage(reviewSpX, chunks)

	// diff base candidate (same pname) with a chunked manifest envelope, chunk_shift = -1
	sphB, sphStrB, nameB, err := ParseSphAndName(reviewSpB)
	require.NoError(t, err)
	mdata := []byte("manifest chunk")
	mdig := cdig.Sum(mdata)
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:       common.ManifestContext + "/" + reviewSpB,
			Type:       pb.EntryType_REGULAR,
			Size:       int64(len(mdata)),
			Digests:    mdig[:],
			ChunkShift: -1,
		},
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
	})
	require.NoError(t, err)
	e.putManifest(sphB, sphStrB, nameB, envelope)
	mlocs, err := e.s.AllocateBatch(withAllocateCtx(context.Background(), makeManifestSph(sphB), true), []uint16{1}, []cdig.CDig{mdig})
	require.NoError(t, err)
	e.s.presentMap.Put(mlocs[0], struct{}{})

	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		_ = e.s.requestChunk(context.Background(), locs[0], digests[0], e.sphps(digests[0]))
		return false
	}()
	require.True(t, panicked, "precondition: expected buildDiff to panic on chunk_shift=-1")

	if !e.s.diffLock.TryLock() {
		t.Fatal("diffLock is still held after a panic in requestChunk; every later chunk request would block forever")
	}
	e.s.diffLock.Unlock()
}
