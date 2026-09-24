package manifester

import (
	"archive/tar"
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	const narMarker = "go-nix/pkg/nar.NewReader"
	before, narBefore := countGoroutines(marker), countGoroutines(narMarker)
	_, err = mb.BuildFromTarball(context.Background(), up.ts.URL+"/leak.tar", 0, 0, "", false)
	require.Error(t, err)
	after := waitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before, "writeNar goroutine leaked after a failed tarball build")
	after = waitGoroutines(narMarker, narBefore, 3*time.Second)
	assert.LessOrEqual(t, after, narBefore, "nar reader goroutine leaked after a failed tarball build")
}
