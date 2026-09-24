package manifester

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/zstd"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PhilipTaronQ/styx/common"
	"github.com/PhilipTaronQ/styx/common/cdig"
	"github.com/PhilipTaronQ/styx/common/shift"
	"github.com/PhilipTaronQ/styx/pb"
)

// Upstream 404 for the narinfo: RetryHttpRequest turns it into an error, which was wrapped
// with ErrReq, so the StatusNotFound branch in BuildFromNar was unreachable and writeError
// answered 417.
func TestNarinfo404IsReportedAs417(t *testing.T) {
	_, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0, up.host())
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	body, err := json.Marshal(ManifestReq{
		Upstream:      up.url(),
		StorePathHash: sphOf("absent"),
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}

func putChunks(t *testing.T, cs *mockChunkStore, chunks ...[]byte) []byte {
	var digests []byte
	for _, c := range chunks {
		d := cdig.Sum(c)
		_, err := cs.PutIfNotExists(context.Background(), ChunkReadPath, d.String(), c)
		require.NoError(t, err)
		digests = append(digests, d[:]...)
	}
	return digests
}

func doChunkDiff(t *testing.T, ctx context.Context, srv *server, reqs ...*pb.ManifesterChunkDiffReq_Req) *httptest.ResponseRecorder {
	body, err := proto.Marshal(&pb.ManifesterChunkDiffReq{
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		Req:    reqs,
	})
	require.NoError(t, err)
	hr := httptest.NewRequest(http.MethodPost, ChunkDiffPath, bytes.NewReader(body)).WithContext(ctx)
	hr.Header.Set(common.CTHdr, common.CTProto)
	rec := httptest.NewRecorder()
	srv.handleChunkDiff(rec, hr)
	return rec
}

// The chunkdiff protocol documents a max of 256 digests per side, but the server didn't
// enforce it (nor ChunkDiffMaxBytes, nor the number of Req entries, nor repeats).
func TestChunkDiffDigestLimitNotEnforced(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 8, ChunkDiffZstdLevel: 3}, mb)
	require.NoError(t, err)

	var chunks [][]byte
	for i := range ChunkDiffMaxDigests + 44 {
		chunks = append(chunks, []byte(fmt.Sprintf("test chunk %06d payload", i)))
	}
	reqs := putChunks(t, cs, chunks...)
	rec := doChunkDiff(t, context.Background(), srv, &pb.ManifesterChunkDiffReq_Req{Bases: reqs[:cdig.Bytes], Reqs: reqs})
	assert.GreaterOrEqual(t, rec.Code, 400,
		"request with %d req digests (documented max %d) was served", len(reqs)/cdig.Bytes, ChunkDiffMaxDigests)

	// the limit is for the whole request
	half := reqs[:ChunkDiffMaxDigests/2*cdig.Bytes]
	rec = doChunkDiff(t, context.Background(), srv,
		&pb.ManifesterChunkDiffReq_Req{Reqs: half},
		&pb.ManifesterChunkDiffReq_Req{Reqs: half},
		&pb.ManifesterChunkDiffReq_Req{Reqs: half})
	assert.GreaterOrEqual(t, rec.Code, 400, "three requests of %d digests were served", ChunkDiffMaxDigests/2)

	// at the limit is fine
	rec = doChunkDiff(t, context.Background(), srv,
		&pb.ManifesterChunkDiffReq_Req{Bases: reqs[:cdig.Bytes], Reqs: reqs[:ChunkDiffMaxDigests*cdig.Bytes]})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestChunkDiffByteLimits(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 8, ChunkDiffZstdLevel: 1}, mb)
	require.NoError(t, err)

	// more than ChunkDiffMaxBytes of chunks, in few digests
	var big [][]byte
	for i := range ChunkDiffMaxBytes>>20 + 1 {
		c := make([]byte, 1<<20)
		fillPseudoRandom(c, uint64(i))
		big = append(big, c)
	}
	rec := doChunkDiff(t, context.Background(), srv, &pb.ManifesterChunkDiffReq_Req{Reqs: putChunks(t, cs, big...)})
	assert.Equal(t, http.StatusExpectationFailed, rec.Code, "%d bytes of chunks were served", len(big)<<20)

	// a gzip bomb
	var gz bytes.Buffer
	zw, err := gzip.NewWriterLevel(&gz, gzip.BestSpeed)
	require.NoError(t, err)
	_, err = zw.Write(make([]byte, chunkDiffMaxExpandedBytes+1))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	rec = doChunkDiff(t, context.Background(), srv,
		&pb.ManifesterChunkDiffReq_Req{Bases: putChunks(t, cs, gz.Bytes()), ExpandBeforeDiff: ExpandGz})
	assert.Equal(t, http.StatusExpectationFailed, rec.Code, "%d byte gzip expanded without limit", gz.Len())
}

// ExpandGz: if the gzip stream was corrupt after the header, io.ReadAll(gzr) returned but the
// pipe reader was never closed. The chunk-series consumer stayed blocked in pw.Write forever
// (no ctx case), and its producer/fetchers stayed blocked on channels, holding chunk buffers
// and ChunkDiffParallel slots.
func TestChunkDiffGzipErrorLeaksFetchGoroutines(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 4, ChunkDiffZstdLevel: 3}, mb)
	require.NoError(t, err)

	// gzip header, then a deflate block with the reserved type 3: NewReader succeeds, Read fails
	chunks := [][]byte{{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 0x07}}
	for i := range 12 {
		chunks = append(chunks, bytes.Repeat([]byte{byte('a' + i)}, 1024))
	}
	bases := putChunks(t, cs, chunks...)

	const marker = "manifester.(*server).fetchChunkSeries"
	before := countGoroutines(marker)

	ctx, cancel := context.WithCancel(context.Background())
	rec := doChunkDiff(t, ctx, srv, &pb.ManifesterChunkDiffReq_Req{Bases: bases, ExpandBeforeDiff: ExpandGz})
	require.GreaterOrEqual(t, rec.Code, 400)
	cancel() // the request is over; net/http would cancel it here too

	after := waitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before,
		"%d chunk-series goroutines still blocked after the request finished and its context was cancelled", after-before)
}

// missingChunkStore reports the chunks in missing as not found, like the real stores.
type missingChunkStore struct {
	*mockChunkStore
	missing map[string]bool
}

func (m missingChunkStore) Get(ctx context.Context, ns, key string, dst []byte) ([]byte, error) {
	if m.missing[key] {
		return nil, wrapNotFound(os.ErrNotExist)
	}
	return m.mockChunkStore.Get(ctx, ns, key, dst)
}

// A chunk fetch that failed sent nil on its result channel, then returned its error, which
// cancelled the group only after that. The consumer skipped the empty result, and if it
// checked the group before the cancel landed, the diff was served with 200 and the chunk
// silently left out. The daemon then failed with "decompressed data is too short" instead
// of seeing a 404 and remanifesting.
func TestChunkDiffMissingChunkIsNotFound(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	var chunks [][]byte
	for i := range 8 {
		chunks = append(chunks, []byte(fmt.Sprintf("chunk %d", i)))
	}
	digests := putChunks(t, cs, chunks...)

	// a gzip stream split over two chunks
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, err := zw.Write(bytes.Repeat([]byte("styx"), 1000))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	gzDigests := putChunks(t, cs, gz.Bytes()[:gz.Len()/2], gz.Bytes()[gz.Len()/2:])

	nth := func(ds []byte, i int) string { return cdig.FromBytes(ds[i*cdig.Bytes:]).String() }
	for _, tc := range []struct {
		name    string
		missing string
		req     *pb.ManifesterChunkDiffReq_Req
	}{
		{"first", nth(digests, 0), &pb.ManifesterChunkDiffReq_Req{Reqs: digests}},
		{"middle", nth(digests, 4), &pb.ManifesterChunkDiffReq_Req{Reqs: digests}},
		{"last", nth(digests, 7), &pb.ManifesterChunkDiffReq_Req{Reqs: digests}},
		{"base", nth(digests, 7), &pb.ManifesterChunkDiffReq_Req{Bases: digests, Reqs: digests[:cdig.Bytes]}},
		{"gz first", nth(gzDigests, 0), &pb.ManifesterChunkDiffReq_Req{Reqs: gzDigests, ExpandBeforeDiff: ExpandGz}},
		{"gz last", nth(gzDigests, 1), &pb.ManifesterChunkDiffReq_Req{Reqs: gzDigests, ExpandBeforeDiff: ExpandGz}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mb, err := NewManifestBuilder(ManifestBuilderConfig{}, missingChunkStore{cs, map[string]bool{tc.missing: true}})
			require.NoError(t, err)
			srv, err := NewManifestServer(Config{ChunkDiffParallel: 8, ChunkDiffZstdLevel: 1}, mb)
			require.NoError(t, err)
			// it was a race, so try many times
			for range 200 {
				rec := doChunkDiff(t, context.Background(), srv, tc.req)
				require.Equal(t, http.StatusNotFound, rec.Code, "diff with a missing chunk served: %q", rec.Body.String())
			}
		})
	}

	// and the daemon sees a 404 as not found, which makes it remanifest
	assert.True(t, common.IsNotFound(common.HttpErrorFromRes(&http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader("not found")),
	})))
}

// The upstream allow-list was checked for the first URL only: http.DefaultClient follows up
// to 10 redirects to any host, for narinfos, nars and tarballs.
func TestUpstreamRedirectToDisallowedHost(t *testing.T) {
	sk, pk := upstreamKeys(t)
	allowed := newFakeUpstream(t)
	other := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0, allowed.host())
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	request := func(r ManifestReq) *httptest.ResponseRecorder {
		r.DigestAlgo, r.DigestBits = cdig.Algo, int(cdig.Bits)
		body, err := json.Marshal(r)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
		return rec
	}

	// narinfo redirected to another host
	sph := other.addPath(t, sk, "elsewhere", []narFile{{"/f", 1000}}, narinfoOpts{})
	allowed.redirect("/"+sph+".narinfo", other.url()+sph+".narinfo")
	rec := request(ManifestReq{Upstream: allowed.url(), StorePathHash: sph})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// tarball redirected to another host
	other.set("/t.tar", makeTar(t, []tarFile{{"f", tar.TypeReg, "hello"}}))
	allowed.redirect("/t.tar", other.url()+"t.tar")
	rec = request(ManifestReq{Upstream: allowed.url() + "t.tar", BuildMode: ModeGenericTarball})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	assert.Zero(t, other.requestCount(), "manifester followed a redirect to a disallowed host")

	// redirects within allowed hosts are fine
	sph = allowed.addPath(t, sk, "moved", []narFile{{"/f", 1000}}, narinfoOpts{})
	allowed.mu.Lock()
	ni := allowed.files["/"+sph+".narinfo"]
	allowed.mu.Unlock()
	allowed.set("/moved/"+sph+".narinfo", ni)
	allowed.redirect("/"+sph+".narinfo", "/moved/"+sph+".narinfo")
	rec = request(ManifestReq{Upstream: allowed.url(), StorePathHash: sph})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// ChunkDiffParallel = 0 made errgroup Go block forever, and nil PublicKeys failed every nar
// build (after fetching the narinfo) although the comment said it disabled verification.
func TestConfigZeroValues(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{}, mb)
	require.NoError(t, err)

	reqs := putChunks(t, cs, []byte("chunk"))
	done := make(chan int, 1)
	go func() {
		done <- doChunkDiff(t, context.Background(), srv, &pb.ManifesterChunkDiffReq_Req{Reqs: reqs}).Code
	}()
	select {
	case code := <-done:
		assert.Equal(t, http.StatusOK, code)
	case <-time.After(10 * time.Second):
		t.Fatal("chunk diff with a zero-value config hung")
	}

	_, err = NewManifestServer(Config{ChunkDiffParallel: -1}, mb)
	assert.Error(t, err)

	// without keys, nar builds fail clearly before fetching anything
	sk, _ := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := up.addPath(t, sk, "nokeys", []narFile{{"/f", 10}}, narinfoOpts{})
	_, err = mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	assert.ErrorContains(t, err, "no public keys")
	assert.Zero(t, up.requestCount())
}

// shardEnv runs sharded manifest requests for one store path against manifesters that share
// a chunk store.
type shardEnv struct {
	t        *testing.T
	pk       signature.PublicKey
	cs       *mockChunkStore
	up       *fakeUpstream
	sph      string
	cacheKey string
	srv      *server
}

func newShardEnv(t *testing.T, seed string) *shardEnv {
	sk, pk := upstreamKeys(t)
	e := &shardEnv{t: t, pk: pk, cs: &mockChunkStore{data: make(map[string][]byte)}, up: newFakeUpstream(t)}
	// 5 chunks, so every one of 4 shards uploads at least one
	e.sph = e.up.addPath(t, sk, seed, []narFile{{"/big", 4<<16 + 1000}}, narinfoOpts{})
	e.cacheKey = (&ManifestReq{
		Upstream:      e.up.url(),
		StorePathHash: e.sph,
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	}).CacheKey()
	e.srv = e.server(newTestBuilder(t, e.cs, pk, 0, e.up.host()))
	return e
}

func (e *shardEnv) server(mb *ManifestBuilder) *server {
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 4}, mb)
	require.NoError(e.t, err)
	return srv
}

func (e *shardEnv) request(srv *server, total, index int) *httptest.ResponseRecorder {
	body, err := json.Marshal(ManifestReq{
		Upstream:      e.up.url(),
		StorePathHash: e.sph,
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
		ShardTotal:    total,
		ShardIndex:    index,
	})
	require.NoError(e.t, err)
	rec := httptest.NewRecorder()
	srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
	return rec
}

// requestAll sends shards concurrently, like the daemon, and returns the responses.
func (e *shardEnv) requestAll(srv *server, total int, indexes ...int) []*httptest.ResponseRecorder {
	recs := make([]*httptest.ResponseRecorder, len(indexes))
	var wg sync.WaitGroup
	for i, index := range indexes {
		wg.Go(func() { recs[i] = e.request(srv, total, index) })
	}
	wg.Wait()
	return recs
}

// cached reports whether the manifest is in the cache, and checks that every chunk it refers
// to exists if it is.
func (e *shardEnv) cached() bool {
	if !storeHas(e.cs, ManifestCachePath, e.cacheKey) {
		return false
	}
	m := cachedManifest(e.t, e.cs, e.cacheKey)
	var total, missing int
	for _, ent := range m.Entries {
		for _, d := range cdig.FromSliceAlias(ent.Digests) {
			total++
			if !storeHas(e.cs, ChunkReadPath, d.String()) {
				missing++
			}
		}
	}
	require.Positive(e.t, total)
	assert.Zero(e.t, missing,
		"manifest was written to the shared cache while %d of %d referenced chunks are absent", missing, total)
	return true
}

// requireShardOk checks that rec is a successful shard response, and returns whether it
// carries the manifest (checking it matches the cache if so).
func (e *shardEnv) requireShardOk(rec *httptest.ResponseRecorder) bool {
	require.Equal(e.t, http.StatusOK, rec.Code, rec.Body.String())
	if rec.Header().Get(ManifestHeader) == "" {
		return false
	}
	sb, err := zstd.Decompress(nil, rec.Body.Bytes())
	require.NoError(e.t, err)
	var sm pb.SignedMessage
	require.NoError(e.t, proto.Unmarshal(sb, &sm))
	require.NotNil(e.t, sm.Msg)
	require.True(e.t, e.cached(), "manifest returned but not cached")
	return true
}

// A caller can send only some shards of N (the manifester is behind an unauthenticated URL),
// and some of the daemon's own shard requests can fail or be refused with a 429. Shard 0 used
// to write the manifest to the shared cache after uploading only its own 1/N of the chunks,
// so clients got the cached manifest and 404 on the rest (and remanifesting hit the same
// cache entry). The manifest must be cached only once every shard is done.
func TestShardedManifestCachedOnlyWhenEveryShardIsDone(t *testing.T) {
	e := newShardEnv(t, "shard")

	// shard 0 alone doesn't cache
	assert.False(t, e.requireShardOk(e.request(e.srv, 4, 0)), "shard 0 alone returned the manifest")
	assert.False(t, e.cached(), "manifest cached with only shard 0 done")
	assert.Len(t, e.cs.markers, 1)

	// nor do three of four (shard 2's request was refused with a 429, say)
	for _, rec := range e.requestAll(e.srv, 4, 1, 3) {
		assert.False(t, e.requireShardOk(rec))
	}
	assert.False(t, e.cached(), "manifest cached with shard 2 not done")

	// shard 2 fails
	failing := e.server(newTestBuilder(t, failingStore{e.cs}, e.pk, 0, e.up.host()))
	rec := e.request(failing, 4, 2)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "injected chunk put failure")
	assert.False(t, e.cached(), "manifest cached after shard 2 failed")

	// the retry succeeds, and as the last shard to finish, caches and returns the manifest
	assert.True(t, e.requireShardOk(e.request(e.srv, 4, 2)), "last shard didn't return the manifest")
	assert.True(t, e.cached(), "manifest not cached")

	// retrying any shard again is harmless, and returns the manifest since all are done
	assert.True(t, e.requireShardOk(e.request(e.srv, 4, 0)))

	// shard fields are checked
	for _, s := range [][2]int{{-1, 0}, {4, 4}, {4, -1}, {0, 1}, {maxShards + 1, 0}} {
		assert.Equal(t, http.StatusBadRequest, e.request(e.srv, s[0], s[1]).Code, "shard %d of %d", s[1], s[0])
	}
}

// All shards together, as the daemon sends them: at least one returns the manifest.
func TestShardedManifestAllShardsTogether(t *testing.T) {
	for _, total := range []int{1, 2, 4, 7} {
		e := newShardEnv(t, fmt.Sprint("together", total))
		indexes := make([]int, total)
		for i := range indexes {
			indexes[i] = i
		}
		var returned int
		for _, rec := range e.requestAll(e.srv, total, indexes...) {
			if e.requireShardOk(rec) {
				returned++
			}
		}
		assert.Positive(t, returned, "%d shards: no shard returned the manifest", total)
		assert.True(t, e.cached(), "%d shards: manifest not cached", total)
	}
}

// A marker vouches for its shard's chunks only for so long (after that, GC may have deleted
// them), and only for the manifest the shard built.
func TestShardMarkersMustMatch(t *testing.T) {
	e := newShardEnv(t, "stale")
	for _, rec := range e.requestAll(e.srv, 4, 0, 1, 2) {
		assert.False(t, e.requireShardOk(rec))
	}
	e.cs.lock.Lock()
	for k := range e.cs.markers {
		require.True(t, IsShardMarker(k), k)
		e.cs.markers[k] = time.Now().Add(-shardMarkerMaxAge - time.Minute)
	}
	e.cs.lock.Unlock()
	assert.False(t, e.requireShardOk(e.request(e.srv, 4, 3)), "used stale markers")
	assert.False(t, e.cached(), "manifest cached with stale markers")
	// retrying the others refreshes their markers
	var returned int
	for _, rec := range e.requestAll(e.srv, 4, 0, 1, 2) {
		if e.requireShardOk(rec) {
			returned++
		}
	}
	assert.Positive(t, returned)
	assert.True(t, e.cached())

	// shards that chunked differently don't complete each other
	e = newShardEnv(t, "layout")
	styxSk, _, err := signature.GenerateKeypair("styx-test-1", rand.Reader)
	require.NoError(t, err)
	mb, err := NewManifestBuilder(ManifestBuilderConfig{
		PublicKeys:       []signature.PublicKey{e.pk},
		SigningKeys:      []signature.SecretKey{styxSk},
		ChunkSizer:       func(int64) shift.Shift { return 17 },
		AllowedUpstreams: []string{e.up.host()},
	}, e.cs)
	require.NoError(t, err)
	other := e.server(mb)
	for _, rec := range e.requestAll(e.srv, 4, 0, 1) {
		assert.False(t, e.requireShardOk(rec))
	}
	for _, rec := range e.requestAll(other, 4, 2, 3) {
		assert.False(t, e.requireShardOk(rec))
	}
	assert.False(t, e.cached(), "shards with different chunks completed each other")
}
