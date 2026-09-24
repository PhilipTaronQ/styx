package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

// fakeShardedManifester answers each shard with f(index), and records the shards it saw.
type fakeShardedManifester struct {
	mu   sync.Mutex
	seen map[int]int // index -> total
}

func newShardClientServer(t *testing.T, f func(w http.ResponseWriter, index int)) (*Server, *fakeShardedManifester) {
	fm := &fakeShardedManifester{seen: make(map[int]int)}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req manifester.ManifestReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fm.mu.Lock()
		fm.seen[req.ShardIndex] = req.ShardTotal
		fm.mu.Unlock()
		f(w, req.ShardIndex)
	}))
	t.Cleanup(ts.Close)
	s := NewServer(Config{ErofsBlockShift: 12, Workers: 1})
	require.NoError(t, s.postInit(&pb.DaemonParams{ManifesterUrl: ts.URL, ManifestCacheUrl: ts.URL, ChunkReadUrl: ts.URL}, nil))
	return s, fm
}

func (fm *fakeShardedManifester) shards() map[int]int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return maps.Clone(fm.seen)
}

func writeManifest(t *testing.T, w http.ResponseWriter, body []byte) {
	b, err := zstd.Compress(nil, body)
	assert.NoError(t, err)
	w.Header().Set(manifester.ManifestHeader, "1")
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(b)
}

var shardTestReq = manifester.ManifestReq{
	Upstream:      "https://cache.example/",
	StorePathHash: "53qwclnym7a6vzs937jjmsfqxlxlsf2y",
	DigestAlgo:    cdig.Algo,
	DigestBits:    int(cdig.Bits),
}

// The manifester returns the manifest from whichever shard finishes last, not from shard 0.
func TestGetNewManifestFromLastShard(t *testing.T) {
	manifest := []byte("signed manifest")
	s, fm := newShardClientServer(t, func(w http.ResponseWriter, index int) {
		if index == 2 {
			writeManifest(t, w, manifest)
		} else {
			fmt.Fprintf(w, "shard %d ok", index)
		}
	})
	got, err := s.getNewManifest(context.Background(), shardTestReq, 4)
	require.NoError(t, err)
	assert.Equal(t, manifest, got)
	assert.Equal(t, map[int]int{0: 4, 1: 4, 2: 4, 3: 4}, fm.shards(), "every shard must be sent")
}

func TestGetNewManifestNeedsEveryShard(t *testing.T) {
	// a shard is refused: even if another returned the manifest (it can't have: that shard
	// never ran), the request fails
	s, _ := newShardClientServer(t, func(w http.ResponseWriter, index int) {
		switch index {
		case 1:
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		case 3:
			writeManifest(t, w, []byte("x"))
		default:
			fmt.Fprintf(w, "shard %d ok", index)
		}
	})
	_, err := s.getNewManifest(context.Background(), shardTestReq, 4)
	assert.Error(t, err)

	// every shard succeeds but none returns the manifest
	s, _ = newShardClientServer(t, func(w http.ResponseWriter, index int) {
		fmt.Fprintf(w, "shard %d ok", index)
	})
	_, err = s.getNewManifest(context.Background(), shardTestReq, 4)
	assert.ErrorContains(t, err, "no manifest")

	// unsharded
	s, fm := newShardClientServer(t, func(w http.ResponseWriter, index int) {
		writeManifest(t, w, []byte("y"))
	})
	got, err := s.getNewManifest(context.Background(), shardTestReq, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), got)
	assert.Equal(t, map[int]int{0: 1}, fm.shards())
}
