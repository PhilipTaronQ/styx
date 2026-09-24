package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/common/systemd"
	"github.com/dnr/styx/daemon"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

const (
	nixosKeys = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="

	devnode = "/dev/cachefiles"

	blockShift = 12

	// Stop wakes the cachefiles poll and cancels reads in flight, so it
	// returns promptly unless something is wedged. This is far longer than
	// that, and short enough to report a wedge before the suite timeout.
	stopTimeout = 2 * time.Minute
)

type (
	service interface {
		Stop()
	}

	testBase struct {
		t              *testing.T
		tag            string
		basetmpdir     string
		chunkdir       string
		cachedir       string
		manifesterAddr string
		upstreamHost   string
		upstreamUrl    string
		chunkSizer     func(int64) shift.Shift

		tdserver   *http.Server
		manifester service
		daemon     *daemon.Server
		fdstore    map[string]int
		startFds   map[string]string // see checkLeaks

		initialized bool        // initDaemon succeeded
		mounts      []testMount // from tb.mount, less those tb.umount'ed
		collected   bool        // see collect
	}

	testMount struct {
		mp, storePath string
	}
)

var (
	TestdataDir = "/must-set-with-ldflags"

	_ systemd.FdStore = (*testBase)(nil)
)

func newTestBase(t *testing.T) *testBase {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	// have to be root, skip otherwise so go test ./... works
	if os.Getuid() != 0 {
		t.Skip("tests must be run as root")
	}

	// check nothing else has devnode
	var exitErr *exec.ExitError
	require.ErrorAs(t, exec.Command("fuser", "-s", devnode).Run(), &exitErr,
		"tests require exclusive access to "+devnode)

	tag := fmt.Sprintf("styxtest%x", rand.Uint64())
	t.Log("cache tag/domain", tag)

	basetmpdir := t.TempDir()
	// basetmpdir = "/tmp"
	chunkdir := filepath.Join(basetmpdir, "chunks")
	require.NoError(t, os.Mkdir(chunkdir, 0755))
	cachedir := filepath.Join(basetmpdir, "cache")
	require.NoError(t, os.Mkdir(cachedir, 0755))

	tdport, err := freeport.GetFreePort()
	require.NoError(t, err)

	tb := &testBase{
		t:            t,
		tag:          tag,
		basetmpdir:   basetmpdir,
		chunkdir:     chunkdir,
		cachedir:     cachedir,
		upstreamHost: fmt.Sprintf("localhost:%d", tdport),
		upstreamUrl:  fmt.Sprintf("http://localhost:%d/", tdport),
		fdstore:      make(map[string]int),
	}
	tb.startFds = tb.testFds()
	t.Cleanup(tb.cleanup)
	return tb
}

func (tb *testBase) cleanup() {
	// if the test mounted anything this already ran, from the last mount's cleanup
	tb.collect()
	// Stop the daemon before the manifester: a fetch still in flight retries
	// forever against a stopped manifester, and Stop waits for it.
	if tb.daemon != nil {
		tb.t.Log("stopping daemon")
		tb.stopDaemon(true)
	}
	if tb.manifester != nil {
		tb.t.Log("stopping manifester")
		tb.manifester.Stop()
	}
	if tb.tdserver != nil {
		tb.t.Log("stopping test data server")
		tb.tdserver.Close()
	}
	// the daemon has closed styx.bolt and the mounts from tb.mount have been
	// unmounted (their cleanups ran first)
	tb.checkInvariants()
	tb.checkLeaks()
}

// stopDaemon stops tb.daemon and clears it. If Stop doesn't return within
// stopTimeout it dumps every goroutine and panics: a wedged Stop would
// otherwise hold the test until the suite timeout, and every later test would
// fail anyway because this process still has the devnode open.
func (tb *testBase) stopDaemon(closeDevnode bool) {
	d := tb.daemon
	tb.daemon = nil
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Stop(closeDevnode)
	}()
	select {
	case <-done:
		return
	case <-time.After(stopTimeout):
	}
	tb.t.Errorf("daemon Stop did not return within %v; dumping goroutines", stopTimeout)
	_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
	panic(fmt.Sprintf("%s: daemon Stop hung", tb.t.Name()))
}

func (tb *testBase) startTestDataServer() {
	// http server acting as binary cache
	tb.tdserver = &http.Server{
		Addr:    tb.upstreamHost,
		Handler: http.FileServer(http.Dir(TestdataDir)),
	}
	go tb.tdserver.ListenAndServe()
}

func (tb *testBase) startAll() {
	tb.startManifester()
	tb.startDaemon()
	tb.initDaemon()
}

func (tb *testBase) startManifester() {
	tb.startTestDataServer()

	port, err := freeport.GetFreePort()
	require.NoError(tb.t, err)

	cswcfg := manifester.ChunkStoreWriteConfig{
		ChunkLocalDir: tb.chunkdir,
	}
	cs, err := manifester.NewChunkStoreWrite(cswcfg)
	require.NoError(tb.t, err)

	mbcfg := manifester.ManifestBuilderConfig{
		ConcurrentChunkOps: 10,
		ChunkSizer:         tb.chunkSizer,
	}
	mbcfg.PublicKeys, err = common.LoadPubKeys([]string{nixosKeys})
	require.NoError(tb.t, err)
	mbcfg.SigningKeys, err = common.LoadSecretKeys([]string{"../keys/testsuite.secret"})
	require.NoError(tb.t, err)
	mb, err := manifester.NewManifestBuilder(mbcfg, cs)
	require.NoError(tb.t, err)

	hostport := fmt.Sprintf("localhost:%d", port)
	tb.manifesterAddr = fmt.Sprintf("http://%s/", hostport)
	cfg := manifester.Config{
		Bind:               hostport,
		AllowedUpstreams:   []string{tb.upstreamHost},
		ChunkDiffZstdLevel: 3,
		ChunkDiffParallel:  60,
	}

	m, err := manifester.NewManifestServer(cfg, mb)
	require.NoError(tb.t, err)

	go m.Run()
	tb.t.Log("manifester running on", hostport)
	tb.manifester = m
}

func (tb *testBase) startDaemon() {
	if tb.manifesterAddr == "" {
		tb.t.Error("start manifester before daemon")
	}

	d := daemon.NewServer(daemon.Config{
		DevPath:         devnode,
		CachePath:       tb.cachedir,
		CacheTag:        tb.tag,
		CacheDomain:     tb.tag,
		ErofsBlockShift: blockShift,
		// SmallFileCutoff: 224,
		Workers:   10,
		IsTesting: true,
		FdStore:   tb,
	})
	err := d.Start()
	require.NoError(tb.t, err)
	tb.t.Log("daemon running in", tb.cachedir)
	tb.daemon = d
}

func (tb *testBase) initDaemon() {
	pk, err := os.ReadFile("../keys/testsuite.public")
	require.NoError(tb.t, err)
	var res daemon.Status
	tb.call(daemon.InitPath, &daemon.InitReq{
		PubKeys: []string{string(pk)},
		Params: pb.DaemonParams{
			Params: &pb.GlobalParams{
				DigestAlgo: cdig.Algo,
				DigestBits: cdig.Bits,
			},
			ManifesterUrl:    tb.manifesterAddr,
			ManifestCacheUrl: tb.manifesterAddr,
			ChunkReadUrl:     tb.manifesterAddr,
			ChunkDiffUrl:     tb.manifesterAddr,
		},
	}, &res)
	tb.initialized = true
	tb.t.Log("daemon initialized")
}

// call makes a request on the daemon's socket and fails the test unless it
// succeeds (see tryCall).
func (tb *testBase) call(path string, req, res any) {
	tb.t.Helper()
	require.NoError(tb.t, tb.tryCall(path, req, res))
}

// tryCall makes a request on the daemon's socket. It fails unless the
// response is a 200 and, for a *daemon.Status response, has Success set. On
// an error the daemon sends a Status instead of the usual response, so the
// error includes its message.
func (tb *testBase) tryCall(path string, req, res any) error {
	c := client.NewClient(filepath.Join(tb.cachedir, "styx.sock"))
	var raw json.RawMessage
	code, err := c.Call(path, req, &raw)
	if err != nil {
		return fmt.Errorf("%s: status %d: %w", path, code, err)
	}
	if code != http.StatusOK {
		var st daemon.Status
		_ = json.Unmarshal(raw, &st)
		return fmt.Errorf("%s: status %d: %s", path, code, st.Error)
	}
	if err := json.Unmarshal(raw, res); err != nil {
		return fmt.Errorf("%s: decoding %s: %w", path, raw, err)
	}
	if st, ok := res.(*daemon.Status); ok && !st.Success {
		return fmt.Errorf("%s: no success: %s", path, st.Error)
	}
	return nil
}

func (tb *testBase) mount(storePath string) string {
	mp := tb.t.TempDir()
	tb.t.Cleanup(func() {
		// cleanups run last first, so the first of these to run sees every
		// mount still in place
		tb.collect()
		// if the test unmounted already this will just fail
		_ = unix.Unmount(mp, 0)
	})
	var res daemon.Status
	tb.call(daemon.MountPath, daemon.MountReq{
		Upstream:   tb.upstreamUrl,
		StorePath:  storePath,
		MountPoint: mp,
	}, &res)
	tb.mounts = append(tb.mounts, testMount{mp: mp, storePath: storePath})
	return mp
}

func (tb *testBase) umount(storePath string) {
	var res daemon.Status
	tb.call(daemon.UmountPath, daemon.UmountReq{
		StorePath: storePath,
	}, &res)
	hash, _, _ := strings.Cut(storePath, "-")
	tb.mounts = slices.DeleteFunc(tb.mounts, func(m testMount) bool {
		return strings.HasPrefix(m.storePath, hash)
	})
}

func (tb *testBase) materialize(storePath string) string {
	mp := filepath.Join(tb.t.TempDir(), "mp")
	var res daemon.Status
	tb.call(daemon.MaterializePath, daemon.MaterializeReq{
		Upstream:  tb.upstreamUrl,
		StorePath: storePath,
		DestPath:  mp,
	}, &res)
	return mp
}

func (tb *testBase) vaporize(path string) {
	var res daemon.Status
	tb.call(daemon.VaporizePath, daemon.VaporizeReq{
		Path: path,
	}, &res)
}

func (tb *testBase) tarball(url string) *daemon.TarballResp {
	var res daemon.TarballResp
	tb.call(daemon.TarballPath, daemon.TarballReq{
		UpstreamUrl: url,
	}, &res)
	// TarballResp has no Success field; a successful one names a store path
	require.NotEmpty(tb.t, res.StorePathHash, "tarball response: %+v", res)
	require.NotEmpty(tb.t, res.StorePathName, "tarball response: %+v", res)
	require.NotEmpty(tb.t, res.NarHash, "tarball response: %+v", res)
	return &res
}

func (tb *testBase) nixHash(path string) string {
	h, err := tryNixHash(path)
	require.NoError(tb.t, err)
	return h
}

func tryNixHash(path string) (string, error) {
	b, err := exec.Command("nix-hash", "--type", "sha256", "--base32", path).Output()
	if err != nil {
		return "", fmt.Errorf("nix-hash %s: output %q: %w", path, b, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// narinfoHash returns the NarHash (sha256, nix base32) that the test data's
// narinfo gives for storePath (hash-name, or just the hash).
func narinfoHash(storePath string) (string, error) {
	hash, _, _ := strings.Cut(storePath, "-")
	b, err := os.ReadFile(filepath.Join(TestdataDir, hash+".narinfo"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "NarHash: sha256:"); ok {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("no sha256 NarHash in narinfo for %s", storePath)
}

// requireNarHash checks that path, a mount or materialized copy of
// storePath, hashes to storePath's NarHash.
func (tb *testBase) requireNarHash(path, storePath string) {
	tb.t.Helper()
	want, err := narinfoHash(storePath)
	require.NoError(tb.t, err)
	require.Equal(tb.t, want, tb.nixHash(path), "nar hash of %s (%s)", path, storePath)
}

func (tb *testBase) debug(req ...daemon.DebugReq) *daemon.DebugResp {
	var res daemon.DebugResp
	if len(req) == 0 {
		req = append(req, daemon.DebugReq{})
	}
	tb.call(daemon.DebugPath, req[0], &res)
	return &res
}

func (tb *testBase) prefetch(sph, path string) {
	var res daemon.Status
	req := &daemon.PrefetchReq{Path: path, StorePath: sph}
	tb.call(daemon.PrefetchPath, req, &res)
}

func (tb *testBase) gc(req daemon.GcReq) *daemon.GcResp {
	var res daemon.GcResp
	tb.call(daemon.GcPath, req, &res)
	require.NoError(tb.t, checkGcResp(&res))
	return &res
}

// checkGcResp checks the consistency that GcResp documents: every chunk
// reference gc keeps has a chunk record, and gc deletes no manifest without
// its image.
func checkGcResp(res *daemon.GcResp) error {
	if res.RemainRefChunks != res.RemainHaveChunks {
		return fmt.Errorf("gc keeps %d chunk references but %d chunk records: %+v",
			res.RemainRefChunks, res.RemainHaveChunks, res)
	}
	if res.DeleteManifests > res.DeleteImages {
		return fmt.Errorf("gc deletes %d manifests but only %d images: %+v",
			res.DeleteManifests, res.DeleteImages, res)
	}
	return nil
}

func (tb *testBase) dropCaches() {
	require.NoError(tb.t, dropCaches())
}

func dropCaches() error {
	fd, err := unix.Open("/proc/sys/vm/drop_caches", unix.O_WRONLY, 0)
	if err != nil {
		return err
	}
	unix.Write(fd, []byte("3"))
	return unix.Close(fd)
}

// implement systemd.FdStore
func (tb *testBase) Ready() {}
func (tb *testBase) GetFd(name string) (int, error) {
	if fd, ok := tb.fdstore[name]; ok {
		return fd, nil
	}
	return 0, errors.New("missing")
}
func (tb *testBase) SaveFd(name string, fd int) { tb.fdstore[name] = fd }
func (tb *testBase) RemoveFd(name string)       { delete(tb.fdstore, name) }
