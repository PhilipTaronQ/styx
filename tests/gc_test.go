package tests

import (
	"flag"
	"math/rand"
	"testing"
	"time"

	"github.com/PhilipTaronQ/styx/daemon"
	"github.com/PhilipTaronQ/styx/pb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var gcUnmounted = map[pb.MountState]bool{
	pb.MountState_Unmounted: true,
}

var gcSharedSeed = flag.Int64("gcshared.seed", 0, "seed for TestGcShared (0 picks one)")

func TestGc(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	sp1 := "xpq4yhadyhazkcsggmqd7rsgvxb3kjy4-gnugrep-3.11"
	sp2 := "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	sp3 := "8vyj9c6g424mz0v3kvzkskhvzhwj6288-bash-interactive-5.2-p15-man"
	sp4 := "xd96wmj058ky40aywv72z63vdw9yzzzb-openssl-3.0.12-man"
	sp5 := "3a7xq2qhxw2r7naqmc53akmx7yvz0mkf-less-is-more.patch"
	mp1 := tb.mount(sp1)
	tb.requireNarHash(mp1, sp1)
	mp2 := tb.mount(sp2)
	tb.requireNarHash(mp2, sp2)
	mp3 := tb.mount(sp3)
	tb.requireNarHash(mp3, sp3)
	mp4 := tb.mount(sp4)
	tb.requireNarHash(mp4, sp4)
	mp5 := tb.mount(sp5)
	tb.requireNarHash(mp5, sp5)

	// unmount 2 and 4
	tb.umount("qa22bifihaxyvn6q2a6w9m0nklqrk9wh")
	tb.umount("xd96wmj058ky40aywv72z63vdw9yzzzb")

	gc1 := tb.gc(daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	t.Log("gc1:", gc1)
	require.Equal(t, 2, gc1.DeleteImages)
	require.Equal(t, 3, gc1.RemainImages)
	require.Equal(t, 806, gc1.DeleteChunks)
	require.Equal(t, 53, gc1.RemainRefChunks)
	require.Equal(t, 53, gc1.RemainHaveChunks)
	require.Equal(t, 0, gc1.RewriteChunks)

	gc2 := tb.gc(daemon.GcReq{DryRunSlow: true, GcByState: gcUnmounted})
	t.Log("gc2:", gc2)
	require.Equal(t, 3, gc2.PunchLocs)
	require.Equal(t, int64(4239360), gc2.PunchBytes)

	gc3 := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc3:", gc3)
	require.Equal(t, int64(4239360), gc3.PunchBytes)

	tb.dropCaches()

	// re-read remaining ones
	d1 := tb.debug()
	tb.requireNarHash(mp1, sp1)
	tb.requireNarHash(mp3, sp3)
	tb.requireNarHash(mp5, sp5)
	d2 := tb.debug()
	require.Zero(t, d2.Stats.Sub(d1.Stats).TotalReqs())
	require.Zero(t, d2.Stats.Sub(d1.Stats).TotalErrs())
}

// randomized test
func TestGcShared(t *testing.T) {
	seed := *gcSharedSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("seed %d (rerun with -gcshared.seed=%d)", seed, seed)
	rnd := rand.New(rand.NewSource(seed))

	tb := newTestBase(t)
	tb.startAll()

	fetch := []string{
		"cd1nbildgzzfryjg82njnn36i4ynyf8h-bash-interactive-5.1-p16-man",
		"8vyj9c6g424mz0v3kvzkskhvzhwj6288-bash-interactive-5.2-p15-man",
		"v35ysx9k1ln4c6r7lj74204ss4bw7l5l-openssl-3.0.12-man",
		"xd96wmj058ky40aywv72z63vdw9yzzzb-openssl-3.0.12-man",
		"z2waz77lsh4pxs0jxgmpf16s7a3g7b7v-openssl-3.0.13-man",
		"53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12",
		"kcyrz2y8si9ry5p8qkmj0gp41n01sa1y-opusfile-0.12",
		"qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12",
	}
	// mount all
	mps := make([]string, len(fetch))
	for _, j := range rnd.Perm(len(mps)) {
		mps[j] = tb.mount(fetch[j])
	}
	// read all (different order)
	for _, j := range rnd.Perm(len(mps)) {
		tb.requireNarHash(mps[j], fetch[j])
	}
	// unmount half
	var remaining []int
	for i, j := range rnd.Perm(len(mps)) {
		if i < len(mps)/2 {
			tb.umount(fetch[j])
		} else {
			remaining = append(remaining, j)
		}
	}

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc:", gc)

	unix.Sync()
	tb.dropCaches()
	unix.Sync()

	// re-read remaining ones
	d1 := tb.debug()
	for _, j := range remaining {
		tb.requireNarHash(mps[j], fetch[j])
	}
	d2 := tb.debug()
	// no errors, no requests
	require.Zero(t, d2.Stats.Sub(d1.Stats).TotalReqs())
	require.Zero(t, d2.Stats.Sub(d1.Stats).TotalErrs())
}
