package manifester

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

// Upstream 404 for the narinfo: RetryHttpRequest turns it into an error, which was wrapped
// with ErrReq, so the StatusNotFound branch in BuildFromNar was unreachable and writeError
// answered 417.
func TestNarinfo404IsReportedAs417(t *testing.T) {
	_, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)
	srv, err := NewManifestServer(Config{AllowedUpstreams: []string{up.host()}, ChunkDiffParallel: 4}, mb)
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

// An unauthenticated caller can send only shard 0 of N. Shard 0 used to write the manifest to
// the shared cache after uploading only its own 1/N of the chunks. Clients then got the
// cached manifest and 404 on the rest (and remanifesting hit the same cache entry). The same
// happened when one of the daemon's own shard requests failed.
func TestShardZeroAloneCachesManifestWithMissingChunks(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := up.addPath(t, sk, "shard", []narFile{{"/big", 4<<16 + 1000}}, narinfoOpts{})

	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)
	srv, err := NewManifestServer(Config{AllowedUpstreams: []string{up.host()}, ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	request := func(total, index int) *httptest.ResponseRecorder {
		body, err := json.Marshal(ManifestReq{
			Upstream:      up.url(),
			StorePathHash: sph,
			DigestAlgo:    cdig.Algo,
			DigestBits:    int(cdig.Bits),
			ShardTotal:    total,
			ShardIndex:    index,
		})
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
		return rec
	}

	cacheKey := (&ManifestReq{
		Upstream:      up.url(),
		StorePathHash: sph,
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	}).CacheKey()
	checkCache := func() bool {
		if !storeHas(cs, ManifestCachePath, cacheKey) {
			return false
		}
		m := cachedManifest(t, cs, cacheKey)
		var total, missing int
		for _, e := range m.Entries {
			for _, d := range cdig.FromSliceAlias(e.Digests) {
				total++
				if !storeHas(cs, ChunkReadPath, d.String()) {
					missing++
				}
			}
		}
		require.Positive(t, total)
		assert.Zero(t, missing,
			"manifest was written to the shared cache while %d of %d referenced chunks are absent", missing, total)
		return true
	}

	// shard 0 alone gives up waiting for the others
	mb.shardWait = 200 * time.Millisecond
	rec := request(4, 0)
	assert.NotEqual(t, http.StatusOK, rec.Code)
	assert.False(t, checkCache(), "manifest cached without the other shards' chunks")

	// all shards together succeed
	mb.shardWait = time.Minute
	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, 4)
	for i := range recs {
		wg.Go(func() { recs[i] = request(len(recs), i) })
	}
	wg.Wait()
	for i, rec := range recs {
		require.Equal(t, http.StatusOK, rec.Code, "shard %d: %s", i, rec.Body.String())
	}
	assert.True(t, checkCache(), "manifest not cached")

	// shard fields are checked
	for _, s := range [][2]int{{-1, 0}, {4, 4}, {4, -1}, {0, 1}, {maxShards + 1, 0}} {
		assert.Equal(t, http.StatusBadRequest, request(s[0], s[1]).Code, "shard %d of %d", s[1], s[0])
	}
}
