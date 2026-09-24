package tests

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/PhilipTaronQ/styx/daemon"
)

// collect checks that a default gc at the end of the test frees nothing a
// mounted image still uses:
//
//  1. every mounted image hashes to its NarHash (fetching whatever the test
//     didn't read);
//  2. a default gc (unmounted images only) succeeds;
//  3. after dropping caches, every mounted image hashes right again, with no
//     chunk requests and no errors.
//
// It runs once, before anything is unmounted: from the first mount cleanup to
// run, or from cleanup if the test mounted nothing. It's skipped if the test
// already failed or has no running, initialized daemon.
func (tb *testBase) collect() {
	if tb.collected {
		return
	}
	tb.collected = true
	t := tb.t
	if t.Failed() || t.Skipped() || tb.daemon == nil || !tb.initialized {
		return
	}

	mounts := tb.liveMounts()
	t.Logf("collect: %d mounted images, then gc", len(mounts))
	tb.collectHashes(mounts, "before gc")

	var gc daemon.GcResp
	if err := tb.tryCall(daemon.GcPath, daemon.GcReq{GcByState: gcUnmounted}, &gc); err != nil {
		t.Errorf("collect: default gc failed: %v", err)
		return
	}
	t.Logf("collect: gc: %+v", gc)
	if err := checkGcResp(&gc); err != nil {
		t.Errorf("collect: %v", err)
	}

	unix.Sync()
	if err := dropCaches(); err != nil {
		t.Errorf("collect: dropping caches: %v", err)
		return
	}

	var before, after daemon.DebugResp
	if err := tb.tryCall(daemon.DebugPath, daemon.DebugReq{}, &before); err != nil {
		t.Errorf("collect: %v", err)
		return
	}
	tb.collectHashes(mounts, "after gc")
	if err := tb.tryCall(daemon.DebugPath, daemon.DebugReq{}, &after); err != nil {
		t.Errorf("collect: %v", err)
		return
	}
	delta := after.Stats.Sub(before.Stats)
	if n := delta.TotalReqs(); n != 0 {
		t.Errorf("collect: re-reading %d mounted images after gc made %d chunk requests: %+v", len(mounts), n, delta)
	}
	if n := delta.TotalErrs(); n != 0 {
		t.Errorf("collect: re-reading %d mounted images after gc had %d errors: %+v", len(mounts), n, delta)
	}
}

func (tb *testBase) collectHashes(mounts []testMount, when string) {
	for _, m := range mounts {
		want, err := narinfoHash(m.storePath)
		if err != nil {
			tb.t.Errorf("collect: %v", err)
			continue
		}
		got, err := tryNixHash(m.mp)
		if err != nil {
			tb.t.Errorf("collect: %s: %v", when, err)
		} else if got != want {
			tb.t.Errorf("collect: %s: %s (%s) hashes to %s, want %s", when, m.mp, m.storePath, got, want)
		}
	}
}

// liveMounts returns the entries of tb.mounts that are still mounted. A test
// may have unmounted one itself; the daemon may also have remounted it (see
// TestReboot).
func (tb *testBase) liveMounts() []testMount {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		tb.t.Errorf("collect: %v", err)
		return nil
	}
	mounted := make(map[string]bool)
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) > 4 {
			mounted[f[4]] = true
		}
	}
	var out []testMount
	for _, m := range tb.mounts {
		if mounted[m.mp] {
			out = append(out, m)
		}
	}
	return out
}
