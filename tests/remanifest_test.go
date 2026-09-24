package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/daemon"
)

func TestRemanifestOnNotFound(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	sp := "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	mp1 := tb.mount(sp)
	d1 := tb.debug()
	requireFirstManifest(t, d1.Stats)

	// wipe chunk store
	ents, err := os.ReadDir(tb.chunkdir)
	require.NoError(t, err)
	for _, ent := range ents {
		os.Remove(filepath.Join(tb.chunkdir, ent.Name()))
	}

	// should succeed anyway
	tb.requireNarHash(mp1, sp)
	requireRemanifested(t, tb.debug().Stats.Sub(d1.Stats))
}

// the mount's manifest was built fresh: not cached, and no errors
func requireFirstManifest(t *testing.T, st daemon.Stats) {
	require.EqualValues(t, 1, st.ManifestCacheReqs)
	require.Zero(t, st.ManifestCacheHits)
	require.EqualValues(t, 1, st.ManifestReqs)
	require.Zero(t, st.ManifestErrs)
}

// after the chunk store (cached manifests included) was wiped, the chunk
// requests failed and the daemon got the manifest rebuilt. A cache hit here
// would mean it was served a manifest whose chunks are gone.
func requireRemanifested(t *testing.T, delta daemon.Stats) {
	require.Zero(t, delta.ManifestCacheHits)
	require.Zero(t, delta.ManifestErrs)
}

func TestRemanifestPrefetch(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// mount to manifest and get chunks in cache, but not locally
	sp := "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	tb.mount(sp)
	tb.umount(sp)
	d1 := tb.debug()
	requireFirstManifest(t, d1.Stats)

	// wipe chunk store
	ents, err := os.ReadDir(tb.chunkdir)
	require.NoError(t, err)
	for _, ent := range ents {
		os.Remove(filepath.Join(tb.chunkdir, ent.Name()))
	}

	// should succeed anyway
	mp1 := tb.materialize(sp)
	tb.requireNarHash(mp1, sp)
	requireRemanifested(t, tb.debug().Stats.Sub(d1.Stats))
}
