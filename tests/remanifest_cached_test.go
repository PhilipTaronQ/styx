package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The bucket GC (ci/gc.go) can leave a manifest cache entry whose chunks are gone: it deletes
// manifests and chunks in arbitrary order, counts per-key delete errors without acting on
// them, and has no grace period for objects written while it runs. The daemon recovers
// from a missing chunk by "remanifesting", which used to go through
// getManifestFromManifester and get the cached manifest back, so the chunks were never
// re-uploaded.
//
// TestRemanifestOnNotFound covers the case where the manifest is gone too (the local chunk
// store keeps manifests and chunks in one directory, so wiping it removes both); this test
// removes only the chunks.
func TestRemanifestWithCachedManifestButMissingChunks(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	sp := "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	mp1 := tb.mount(sp)

	ents, err := os.ReadDir(tb.chunkdir)
	require.NoError(t, err)
	removed, kept := 0, 0
	for _, ent := range ents {
		name := ent.Name()
		// keep manifest cache entries ("v1-..." cache keys) and build roots ("...@...")
		if strings.HasPrefix(name, "v1-") || strings.Contains(name, "@") || strings.Contains(name, ".tmp") {
			kept++
			continue
		}
		require.NoError(t, os.Remove(filepath.Join(tb.chunkdir, name)))
		removed++
	}
	t.Logf("removed %d chunks, kept %d manifest/root files", removed, kept)
	require.NotZero(t, removed)
	require.NotZero(t, kept)

	d1 := tb.debug()
	tb.requireNarHash(mp1, sp)
	requireRemanifested(t, tb.debug().Stats.Sub(d1.Stats))
}
