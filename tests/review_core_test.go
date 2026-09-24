package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/daemon"
)

const (
	reviewSp   = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	reviewSph  = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh"
	reviewHash = "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm"
)

// GC deletes an unmounted image's db record and chunks and punches the chunks out of the
// slab, but leaves the image's cachefiles backing file. Mounting the same store path again
// builds a new image with new chunk addresses; if cachefiles accepts the old backing file
// (same fsid, same size), the kernel serves the stale image, whose chunk addresses were
// punched.
func TestReviewRemountAfterGc(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp1 := tb.mount(reviewSp)
	require.Equal(t, reviewHash, tb.nixHash(mp1))
	tb.umount(reviewSp)

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc:", gc)
	require.Equal(t, 1, gc.DeleteImages)
	require.NotZero(t, gc.PunchLocs)

	unix.Sync()
	tb.dropCaches()

	mp2 := tb.mount(reviewSp)
	require.Equal(t, reviewHash, tb.nixHash(mp2), "remount of a gc'd store path")
}

// restoreMounts trusts DbImage.ImageSize > 0 to mean the image is in its cachefiles backing
// file and never rebuilds, although the record has the upstream and the manifest is still in
// the db. If the backing file was lost (e.g. never written back before a power cut, while
// the bbolt commit that recorded ImageSize was fsynced) the store path comes back empty.
func TestReviewRestoreAfterLostImageFile(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(reviewSp)
	require.Equal(t, reviewHash, tb.nixHash(mp))

	tb.daemon.Stop(true)
	tb.daemon = nil
	require.NoError(t, unix.Unmount(mp, 0))

	files, err := filepath.Glob(filepath.Join(tb.cachedir, "cache", "Ierofs,"+tb.tag, "@*", "D"+reviewSph))
	require.NoError(t, err)
	require.Len(t, files, 1, "image backing file")
	require.NoError(t, os.Remove(files[0]))

	tb.startDaemon()

	d := tb.debug(daemon.DebugReq{IncludeImages: []string{reviewSph}})
	if img, ok := d.Images[reviewSp]; ok {
		t.Logf("after restore: state=%v lastErr=%q imageSize=%d",
			img.Image.GetMountState(), img.Image.GetLastMountError(), img.Image.GetImageSize())
	}
	require.Equal(t, reviewHash, tb.nixHash(mp), "store path after restart with a lost image file")
}
