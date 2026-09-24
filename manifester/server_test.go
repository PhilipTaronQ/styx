package manifester

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/cdig"
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
