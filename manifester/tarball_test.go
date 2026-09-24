package manifester

import (
	"archive/tar"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tarFile struct {
	name string
	typ  byte
	body string
}

func makeTar(t *testing.T, files []tarFile) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: f.name, Typeflag: f.typ, Mode: 0o755, Size: int64(len(f.body))}))
		_, err := tw.Write([]byte(f.body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// Tarball entries named "../x" survived path.Clean, sorted before "/x", and writeNar turned
// the pair into a nar entry named "." (go-nix accepts it). The nar reader then yielded "/x"
// twice, and the signed manifest had duplicate paths. Like tar, refuse ".." components.
func TestTarballDotDotEntryDuplicatesPath(t *testing.T) {
	for _, bad := range [][]tarFile{{
		{"../x/", tar.TypeDir, ""},
		{"x/", tar.TypeDir, ""},
		{"x/a", tar.TypeReg, "hello"},
	}, {
		{"x/../../y", tar.TypeReg, "hello"},
	}, {
		{".", tar.TypeReg, "root is a file"},
		{"x", tar.TypeReg, "hello"},
	}} {
		up := newFakeUpstream(t)
		up.set("/t.tar", makeTar(t, bad))
		cs := &mockChunkStore{data: make(map[string][]byte)}
		mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
		require.NoError(t, err)

		res, err := mb.BuildFromTarball(context.Background(), up.ts.URL+"/t.tar", 0, 0, "", false)
		if !assert.Error(t, err, "tarball %v was accepted", bad) {
			m := cachedManifest(t, cs, res.CacheKey)
			counts := map[string]int{}
			var paths []string
			for _, e := range m.Entries {
				counts[e.Path]++
				paths = append(paths, e.Path)
			}
			for p, c := range counts {
				assert.Equal(t, 1, c, "path %q appears %d times in a signed manifest (entries %q)", p, c, paths)
			}
		}
	}

	// "./" prefixes, as written by "tar -C dir -c .", are fine
	up := newFakeUpstream(t)
	up.set("/t.tar", makeTar(t, []tarFile{
		{"./", tar.TypeDir, ""},
		{"./a/", tar.TypeDir, ""},
		{"./a/b", tar.TypeReg, "hello"},
		{"./c", tar.TypeReg, "world"},
	}))
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	res, err := mb.BuildFromTarball(context.Background(), up.ts.URL+"/t.tar", 0, 0, "", false)
	require.NoError(t, err)
	var paths []string
	for _, e := range cachedManifest(t, cs, res.CacheKey).Entries {
		paths = append(paths, e.Path)
	}
	assert.Equal(t, []string{"/", "/a", "/a/b", "/c"}, paths)
}

// Tarball path depth was unbounded. A single entry at depth D creates D parent directories,
// and sorting them with narPathLess costs O(D) per comparison, so a ~16KB path in a tiny
// tarball cost seconds; a 1MiB PAX path (archive/tar's limit) kept a Lambda busy until its
// timeout.
func TestTarballDeepPathIsSuperlinear(t *testing.T) {
	const depth = 8000
	up := newFakeUpstream(t)
	up.set("/deep.tar", makeTar(t, []tarFile{{strings.Repeat("a/", depth) + "f", tar.TypeReg, "x"}}))
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)

	start := time.Now()
	_, err = mb.BuildFromTarball(context.Background(), up.ts.URL+"/deep.tar", 0, 0, "", false)
	elapsed := time.Since(start)
	t.Logf("tarball with path depth %d: err=%v in %s", depth, err, elapsed)
	assert.Less(t, elapsed, time.Second, "a tiny tarball took %s to process", elapsed)
	assert.Error(t, err)

	// reasonable depths are fine
	up.set("/ok.tar", makeTar(t, []tarFile{{strings.Repeat("a/", 200) + "f", tar.TypeReg, "x"}}))
	_, err = mb.BuildFromTarball(context.Background(), up.ts.URL+"/ok.tar", 0, 0, "", false)
	assert.NoError(t, err)
}

// Same for tarballs: nothing read or closed the pipe after buildFromNar failed, so the
// writeNar goroutine blocked in pw.Write forever.
func TestFailedTarballBuildLeaksWriteNarGoroutine(t *testing.T) {
	big := make([]byte, 64<<16)
	fillPseudoRandom(big, 7)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "big", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(big))}))
	_, err := tw.Write(big)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	up := newFakeUpstream(t)
	up.set("/leak.tar", buf.Bytes())
	cs := failingStore{&mockChunkStore{data: make(map[string][]byte)}}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{ConcurrentChunkOps: 1}, cs)
	require.NoError(t, err)

	const marker = "manifester.(*ManifestBuilder).writeNar"
	const narMarker = "styx/common/nar.NewReader"
	before, narBefore := countGoroutines(marker), countGoroutines(narMarker)
	_, err = mb.BuildFromTarball(context.Background(), up.ts.URL+"/leak.tar", 0, 0, "", false)
	require.Error(t, err)
	after := waitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before, "writeNar goroutine leaked after a failed tarball build")
	after = waitGoroutines(narMarker, narBefore, 3*time.Second)
	assert.LessOrEqual(t, after, narBefore, "nar reader goroutine leaked after a failed tarball build")
}
