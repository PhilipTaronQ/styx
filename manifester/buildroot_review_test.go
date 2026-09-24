package manifester

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every on-demand manifest build with writeBuildRoot=true must leave its own build root,
// because the bucket GC (ci/gc.go) deletes any manifest and chunk that no root references.
// The root key is "manifest@<RFC3339 seconds>@m@m" and is written with PutIfNotExists, so
// a second build that finishes in the same second finds the key taken and silently skips
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

	roots := 0
	for k := range cs.data {
		if strings.HasPrefix(k, BuildRootPath) {
			roots++
		}
	}
	require.Len(t, cacheKeys, n)
	require.Equal(t, n, roots, "only %d build roots for %d manifests; the rest will be deleted by the next GC", roots, n)
}
