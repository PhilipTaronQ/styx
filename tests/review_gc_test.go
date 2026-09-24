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
