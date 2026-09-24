package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PhilipTaronQ/styx/daemon"
)

func TestSmallImage(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// 144K package
	mp1 := tb.mount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")
	d1 := tb.debug(daemon.DebugReq{IncludeSlabs: true})
	require.Zero(t, d1.Slabs[0].Stats.PresentChunks)
	require.Zero(t, d1.Slabs[0].Stats.PresentBlocks)

	require.Equal(t, "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm", tb.nixHash(mp1))
	// present chunks are recorded in a bbolt batch (MaxBatchDelay 100ms), so
	// poll rather than sleep: the commit can take much longer under -race.
	var d2 *daemon.DebugResp
	require.Eventually(t, func() bool {
		d2 = tb.debug(daemon.DebugReq{IncludeSlabs: true})
		return d2.Slabs[0].Stats.PresentChunks > 0 && d2.Slabs[0].Stats.PresentBlocks > 0
	}, 10*time.Second, 50*time.Millisecond)

	tb.dropCaches()

	require.Equal(t, "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm", tb.nixHash(mp1))
	d3 := tb.debug(daemon.DebugReq{IncludeSlabs: true})
	require.Equal(t, d2.Stats.SlabReads, d3.Stats.SlabReads)
	require.Zero(t, d3.Stats.SlabReadErrs)

	// try explicit unmount
	tb.umount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12")
}
