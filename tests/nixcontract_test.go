package tests

// Tests for the contract between the Nix patch (patches/nix_2_35.patch) and
// the daemon. The patch decides "is this a styx mount?" with statfs on the
// store path and only talks to the daemon when statfs says EROFS; the daemon
// decides from its image records. These tests check what the daemon does
// when those two views drift apart.

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
	"github.com/dnr/styx/erofs"
)

func statfsIsErofs(t *testing.T, p string) bool {
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(p, &st))
	return st.Type == erofs.EROFS_MAGIC
}

// After a reboot the mounts are gone. If Nix garbage-collects a store path
// before styx restores it, the patch's isStyxMount sees a plain directory
// and deletes it without sending /umount, so the daemon's record still says
// Mounted. When the daemon then starts, restoreMounts must not re-create the
// deleted store path and mount over it: Nix no longer considers it valid,
// and a mount there makes the path impossible to substitute again.
func TestNixContractRestoreSkipsDeletedMountPoint(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")

	// simulate a reboot, as in TestReboot
	tb.daemon.Stop(true)
	tb.daemon = nil
	require.NoError(t, unix.Unmount(mp, 0))

	// Nix GC deletes the store path (an empty directory now).
	require.NoError(t, os.RemoveAll(mp))

	tb.startDaemon()

	_, err := os.Lstat(mp)
	if err == nil {
		t.Errorf("restoreMounts re-created deleted mount point %s (erofs mounted: %v)", mp, statfsIsErofs(t, mp))
	} else {
		require.True(t, errors.Is(err, fs.ErrNotExist), "lstat: %v", err)
	}
}

// If a mount goes away without the daemon's involvement (unmounted by an
// admin or a systemd mount unit, or not yet restored after boot), the
// daemon's record still says Mounted. The patch's makeStyxMount clears and
// re-creates the mount point and sends /mount; on Success it registers the
// store path as valid. So Success must mean the path is actually mounted.
func TestNixContractMountSuccessMeansMounted(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	sp := "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	mp := tb.mount(sp)
	require.True(t, statfsIsErofs(t, mp))

	require.NoError(t, unix.Unmount(mp, 0))
	require.False(t, statfsIsErofs(t, mp))

	// What makeStyxMount does before its request: deletePath + createDirs.
	require.NoError(t, os.RemoveAll(mp))
	require.NoError(t, os.MkdirAll(mp, 0o755))

	c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
	var res daemon.Status
	code, err := c.Call(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  sp,
		MountPoint: mp,
	}, &res)
	require.NoError(t, err)
	t.Logf("mount after external unmount: code %d, res %+v", code, res)
	if code == http.StatusOK && res.Success {
		require.True(t, statfsIsErofs(t, mp),
			"daemon reported mount success but %s is not an erofs mount; nix would register an empty directory as valid", mp)
	}
}
