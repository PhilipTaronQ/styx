package tests

import (
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
)

// handleMountReq records the image as Requested before fetching the manifest,
// and tryMount returns early if getManifestAndBuildImage fails, so the record
// stays Requested with no manifest. gcTraceImage then fails on it for every
// gc that doesn't include Requested in GcByState, which is what `styx gc`
// sends by default.
func TestGcAfterFailedMount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp1 := tb.mount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")
	require.Equal(t, "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm", tb.nixHash(mp1))

	// the upstream has no narinfo for this, so the manifester fails and so does the mount
	c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
	var mres daemon.Status
	code, err := c.Call(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  "00000000000000000000000000000000-not-in-upstream",
		MountPoint: filepath.Join(t.TempDir(), "mp"),
	}, &mres)
	require.NoError(t, err)
	require.NotEqual(t, http.StatusOK, code)
	t.Log("failed mount:", code, mres.Error)

	// what `styx gc` sends with no flags
	var gres map[string]any
	code, err = c.Call(daemon.GcPath, daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted}, &gres)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code, "gc after a failed mount: %v", gres)
}

// handleGcReq builds the catalogf keys to delete from ParseSph's second result,
// which is the hash string, not the name, so catalogf entries for deleted
// images stay behind (catalogr entries are deleted). Base selection scans
// catalogf by name and takes the last best match, so a stale entry can win
// over a live base; its manifest is gone, so the read gets no base at all.
//
// catalogf orders same-name entries by raw hash bytes: kcyrz < 53qwc < qa22.
func TestGcStaleCatalogBase(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// live base, lowest in catalog order
	mpL := tb.mount("kcyrz2y8si9ry5p8qkmj0gp41n01sa1y-opusfile-0.12")
	require.Equal(t, "0im7spp48afrbfv672bmrvrs0lg4md0qhyic8zkcgyc8xqwz1s5b", tb.nixHash(mpL))
	// will be gc'd, highest in catalog order
	mpG := tb.mount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")
	require.Equal(t, "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm", tb.nixHash(mpG))
	tb.umount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh")

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	require.Equal(t, 1, gc.DeleteImages)

	d1 := tb.debug()
	mpN := tb.mount("53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12")
	require.Equal(t, "0dm2277wfknq81wfwzxrasc9rif30fm03vxahndbqnn4gb9swqpq", tb.nixHash(mpN))
	st := tb.debug().Stats.Sub(d1.Stats)
	t.Logf("stats for new image: %+v", st)
	// with kcyrz still mounted this should diff against it, as in TestDiffChunks
	require.Zero(t, st.BatchReqs, "fetched without a base although a live base exists")
	require.NotZero(t, st.DiffReqs)
}

// GC deletes an unmounted image's records and chunks and punches its slab
// ranges, but nothing removes the image's cachefiles backing file. Mounting
// the same store path again builds a new image whose chunks are at new slab
// addresses; if the kernel keeps using the old cached image, reads go to the
// punched addresses.
func TestGcThenRemount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	const sp = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	const want = "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm"

	mp1 := tb.mount(sp)
	require.Equal(t, want, tb.nixHash(mp1))
	tb.umount(sp)

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Logf("gc: %+v", gc)
	require.Equal(t, 1, gc.DeleteImages)
	require.NotZero(t, gc.DeleteChunks)
	require.NotZero(t, gc.PunchLocs)

	tb.dropCaches()

	d1 := tb.debug()
	mp2 := tb.mount(sp)
	out, err := exec.Command("nix-hash", "--type", "sha256", "--base32", mp2).CombinedOutput()
	d2 := tb.debug()
	t.Logf("stats during re-read: %+v", d2.Stats.Sub(d1.Stats))
	require.NoError(t, err, "nix-hash of remounted image: %s", out)
	require.Equal(t, want, strings.TrimSpace(string(out)))
}
