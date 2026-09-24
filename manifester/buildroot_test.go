package manifester

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ci/gc.go keeps a root only if its key has at least 4 "@"-separated fields and the second
// is an RFC3339 time.
func assertGcCanParseRoots(t *testing.T, roots []string) {
	for _, k := range roots {
		parts := strings.Split(path.Base(k), "@")
		if assert.GreaterOrEqual(t, len(parts), 4, k) {
			_, err := time.Parse(time.RFC3339, parts[1])
			assert.NoError(t, err, k)
		}
	}
}

// Every on-demand manifest build with writeBuildRoot=true must leave its own build root,
// because the bucket GC (ci/gc.go) deletes any manifest and chunk that no root references.
// The root key was "manifest@<RFC3339 seconds>@m@m", written with PutIfNotExists, so a
// second build that finished in the same second found the key taken and silently skipped
// writing its root.
func TestBuildRootPerManifest(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "root/", Typeflag: tar.TypeDir, Mode: 0755}))
	body := "some file content"
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "root/file", Mode: 0644, Size: int64(len(body))}))
	_, err := tw.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Etag", `"etag-`+r.URL.Path+`"`)
		w.Write(buf.Bytes())
	}))
	defer ts.Close()

	cs := &mockChunkStore{data: make(map[string][]byte)}
	builder, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)

	const n = 10
	var cacheKeys []string
	for i := range n {
		res, err := builder.BuildFromTarball(context.Background(), fmt.Sprintf("%s/src-%d.tar", ts.URL, i), 1, 0, "", true)
		require.NoError(t, err)
		cacheKeys = append(cacheKeys, res.CacheKey)
	}

	roots := storeKeysWithPrefix(cs, BuildRootPath)
	require.Len(t, cacheKeys, n)
	require.Len(t, roots, n, "only %d build roots for %d manifests; the rest will be deleted by the next GC", len(roots), n)
	assertGcCanParseRoots(t, roots)
}

// Same for nar manifests.
func TestBuildRootsCollideWithinOneSecond(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)

	const n = 3
	for i := range n {
		sph := up.addPath(t, sk, fmt.Sprintf("root%d", i), []narFile{{"/f", 1000 + i}}, narinfoOpts{})
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", true)
		require.NoError(t, err)
	}
	roots := storeKeysWithPrefix(cs, BuildRootPath+"/")
	assert.Len(t, roots, n, "each on-demand manifest should have its own GC root; got %v", roots)
	assertGcCanParseRoots(t, roots)
}
