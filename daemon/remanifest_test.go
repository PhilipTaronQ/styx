package daemon

import (
	"context"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"
)

// Remanifesting exists to make the manifester rebuild a manifest and re-upload its chunks
// after a chunk read hit NotFound. doRemanifestReqs used to go through
// getManifestFromManifester, which returns the manifest cache entry if there is one, so when
// the chunks were gone but the cached manifest was not (sharded build where a later shard
// failed, bucket GC race), the manifester was never asked and nothing was repaired.
func TestRemanifestSkipsManifestCache(t *testing.T) {
	e := newFetchEnv(t)
	comp, err := zstd.Compress(nil, []byte("cached envelope"))
	require.NoError(t, err)
	e.manifestCacheHit = comp
	req := MountReq{StorePath: testSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	require.NoError(t, e.s.doRemanifestReqs(context.Background(), []MountReq{req}))
	require.EqualValues(t, 1, e.manifestPosts.Load(), "remanifest did not ask the manifester to rebuild")
}
