package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
	"github.com/dnr/styx/pb"
)

const (
	lcOpusfile     = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	lcOpusfileSph  = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh"
	lcOpusfileHash = "1rswindywkyq2jmfpxd6n772jii3z5xz6ypfbb63c17k5il39hfm"
	// valid nixbase32, not in the test data
	lcFakeSph = "1b9p07z77phvv2hf6gm9f28syp39f1ag"
)

// lcCall makes a request and returns the status and raw body, without asserting success.
func (tb *testBase) lcCall(path string, req any) (int, string) {
	var raw json.RawMessage
	c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
	code, err := c.Call(path, req, &raw)
	if err != nil && code == 0 {
		tb.t.Fatalf("call %s: %v", path, err)
	}
	return code, string(raw)
}

// A mount that fails before its manifest is stored used to leave its image Requested with
// no manifest, and gc, which keeps Requested images by default, failed tracing it.
func TestGcAfterFailedMount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))

	// the upstream has no narinfo for this, so the manifester fails and so does the mount
	code, body := tb.lcCall(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  lcFakeSph + "-not-in-upstream",
		MountPoint: t.TempDir(),
	})
	require.NotEqual(t, http.StatusOK, code, "mount of a path the upstream doesn't have should fail: %s", body)

	// what `styx gc` sends with no flags
	code, body = tb.lcCall(daemon.GcPath, daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	require.Equal(t, http.StatusOK, code, "default gc failed after an unrelated failed mount: %s", body)
}

// A completed vaporize leaves nothing for gc. Mounting a vaporized store path replaces its
// unsigned manifest with the manifester's signed one, which orphans the old manifest's
// chunks, and only those.
func TestGcAfterVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	tmp := t.TempDir()
	vaporize := func(name, filehash string) {
		src := filepath.Join(tmp, name)
		cmd := fmt.Sprintf("xz -cd %s/nar/%s.nar.xz | nix-store --restore %s", TestdataDir, filehash, src)
		require.NoError(t, exec.Command("sh", "-c", cmd).Run())
		tb.vaporize(src)
	}
	const opensslMan = "v35ysx9k1ln4c6r7lj74204ss4bw7l5l-openssl-3.0.12-man"
	vaporize(lcOpusfile, "0h336qzb63kdqxwc5yjrxq61cjraz8jrav0m5rkrcvsb6w55rbll")
	vaporize(opensslMan, "1mv76iwv027rxgdb0i04www6nkx8hy5bxh8v8vjihr9pl5a37hpy")

	gc := tb.gc(daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	t.Logf("gc after vaporize: %+v", gc)
	require.Zero(t, gc.DeleteChunks, "vaporize left chunks no image refers to")

	d := tb.debug(daemon.DebugReq{IncludeImages: []string{opensslMan[:32]}, IncludeManifests: true})
	require.Contains(t, d.Images, opensslMan)
	vaporizedManifestChunks := len(d.Images[opensslMan].ManifestChunks)

	mp := tb.mount(opensslMan)
	require.Equal(t, "0v60mg7qj7mfd27s1nnldb0041ln08xs1bw7zn1mmjiaq02myzlh", tb.nixHash(mp))
	gc = tb.gc(daemon.GcReq{DryRunFast: true, GcByState: gcUnmounted})
	t.Logf("gc after mounting a vaporized path: %+v", gc)
	require.Equal(t, vaporizedManifestChunks, gc.DeleteChunks,
		"mounting a vaporized path should orphan only the chunks of its vaporized manifest")
}

// gc deletes an unmounted image's record, chunks and slab data, and used to leave its
// cachefiles backing file. Mounting the same store path again builds a new image of the same
// size with new chunk addresses; cachefiles' coherency check only compares the size, so the
// kernel kept the old image, whose chunk addresses were just freed.
func TestGcThenRemount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp1 := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp1))
	tb.umount(lcOpusfile)

	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Logf("gc: %+v", gc)
	require.Equal(t, 1, gc.DeleteImages)
	require.NotZero(t, gc.DeleteChunks)
	require.NotZero(t, gc.PunchLocs)

	unix.Sync()
	tb.dropCaches()

	d1 := tb.debug()
	mp2 := tb.mount(lcOpusfile)
	out, err := exec.Command("nix-hash", "--type", "sha256", "--base32", mp2).CombinedOutput()
	t.Logf("stats during re-read: %+v", tb.debug().Stats.Sub(d1.Stats))
	require.NoError(t, err, "nix-hash of remounted image: %s", out)
	require.Equal(t, lcOpusfileHash, strings.TrimSpace(string(out)))
}

// gc used to leave catalogf entries of deleted images behind (it built their keys from the
// hash instead of the name). Base selection scans catalogf by name and takes the last best
// match, so a stale entry could win over a live base; its manifest was gone, so the read got
// no base at all.
//
// catalogf orders same-name entries by raw hash bytes: kcyrz < 53qwc < qa22.
func TestGcStaleCatalogBase(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// live base, lowest in catalog order
	mpL := tb.mount("kcyrz2y8si9ry5p8qkmj0gp41n01sa1y-opusfile-0.12")
	require.Equal(t, "0im7spp48afrbfv672bmrvrs0lg4md0qhyic8zkcgyc8xqwz1s5b", tb.nixHash(mpL))
	// will be gc'd, highest in catalog order
	mpG := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mpG))
	tb.umount(lcOpusfile)

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

// restoreMounts trusted DbImage.ImageSize > 0 to mean the image is in its cachefiles backing
// file and never rebuilt it. If the backing file was lost (it wasn't synced before bbolt
// committed ImageSize), the store path came back empty after a restart.
func TestRestoreAfterLostImageFile(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))

	tb.daemon.Stop(true)
	tb.daemon = nil
	require.NoError(t, unix.Unmount(mp, 0))

	files, err := filepath.Glob(filepath.Join(tb.cachedir, "cache", "Ierofs,"+tb.tag, "@*", "D"+lcOpusfileSph))
	require.NoError(t, err)
	require.Len(t, files, 1, "image backing file")
	require.NoError(t, os.Remove(files[0]))

	tb.startDaemon()

	d := tb.debug(daemon.DebugReq{IncludeImages: []string{lcOpusfileSph}})
	if img, ok := d.Images[lcOpusfile]; ok {
		t.Logf("after restore: state=%v lastErr=%q imageSize=%d",
			img.Image.GetMountState(), img.Image.GetLastMountError(), img.Image.GetImageSize())
	}
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp), "store path after restart with a lost image file")
}

// After something else unmounted a store path, umount got EINVAL from umount2 and failed
// forever, leaving the image UnmountRequested.
func TestUmountAfterExternalUnmount(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))
	require.NoError(t, unix.Unmount(mp, 0))

	tb.umount(lcOpusfile)
	d := tb.debug(daemon.DebugReq{IncludeImages: []string{lcOpusfileSph}})
	require.Contains(t, d.Images, lcOpusfile)
	require.Equal(t, pb.MountState_Unmounted, d.Images[lcOpusfile].Image.GetMountState())
}

// umount detaches lazily and records Unmounted right away, so gc used to free the image
// while a process still had one of its files open.
func TestGcWhileDetachedMountInUse(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	mp := tb.mount(lcOpusfile)
	var big string
	var bigSize int64
	require.NoError(t, filepath.WalkDir(mp, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			if fi, err := d.Info(); err == nil && fi.Size() > bigSize {
				big, bigSize = p, fi.Size()
			}
		}
		return nil
	}))
	require.Greater(t, bigSize, int64(4096), "want a chunked file")

	f, err := os.Open(big)
	require.NoError(t, err)
	defer f.Close()
	want, err := io.ReadAll(f)
	require.NoError(t, err)

	tb.umount(lcOpusfile) // MNT_DETACH: f keeps the filesystem alive
	gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc with a file open:", gc)
	require.Zero(t, gc.DeleteImages, "gc deleted an image that is still in use")

	unix.Sync()
	tb.dropCaches()

	got := make([]byte, len(want))
	n, err := f.ReadAt(got, 0)
	require.NoError(t, err, "reading still-open %s after umount and gc", big)
	require.Equal(t, len(want), n)
	require.True(t, bytes.Equal(want, got), "contents of still-open %s changed after umount and gc", big)

	// once the last user is gone, the image can go
	require.NoError(t, f.Close())
	gc = tb.gc(daemon.GcReq{GcByState: gcUnmounted})
	t.Log("gc after close:", gc)
	require.Equal(t, 1, gc.DeleteImages)
}

// vaporize reserves slab space in one transaction (preallocateBatch) and links chunks to it
// in a later one (commitPreallocated). gc derives punch ranges from the remaining slab keys
// and the slab Sequence, so it used to punch a reservation along with the garbage chunks
// just before it.
func TestGcDuringVaporize(t *testing.T) {
	tb := newTestBase(t)
	tb.startAll()

	// make the most recently allocated chunks garbage
	mp := tb.mount(lcOpusfile)
	require.Equal(t, lcOpusfileHash, tb.nixHash(mp))
	tb.umount(lcOpusfile)

	name := lcFakeSph + "-gcvaporize"
	src := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.Mkdir(src, 0o755))
	data := make([]byte, 256<<20+12345)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(data)
	require.NoError(t, os.WriteFile(filepath.Join(src, "big"), data, 0o644))

	slab0 := func() daemon.DebugSizeStats {
		for _, si := range tb.debug(daemon.DebugReq{IncludeSlabs: true}).Slabs {
			if si.Index == 0 {
				return si.Stats
			}
		}
		t.Fatal("no slab 0")
		return daemon.DebugSizeStats{}
	}
	before := slab0()

	type result struct {
		code int
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var raw json.RawMessage
		c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
		code, err := c.Call(daemon.VaporizePath, daemon.VaporizeReq{Path: src}, &raw)
		done <- result{code, string(raw), err}
	}()

	// Wait until vaporize has reserved space for "big" (Sequence moved, so the last chunk's
	// computed size grew) but not yet linked any chunk to it.
	for {
		select {
		case res := <-done:
			t.Skipf("inconclusive: vaporize finished (%d %s) before gc could run inside its reservation window", res.code, res.body)
		default:
		}
		st := slab0()
		if st.TotalChunks == before.TotalChunks && st.TotalBlocks > before.TotalBlocks {
			gc := tb.gc(daemon.GcReq{GcByState: gcUnmounted})
			t.Logf("gc during vaporize (reserved %d blocks): %+v", st.TotalBlocks-before.TotalBlocks, gc)
			break
		}
		time.Sleep(time.Millisecond)
	}

	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, http.StatusOK, res.code, "vaporize with a gc in the middle: %s", res.body)

	dst := tb.materialize(name)
	got, err := os.ReadFile(filepath.Join(dst, "big"))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "materialized file differs from the vaporized source")
}
