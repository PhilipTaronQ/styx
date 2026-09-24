package daemon

// A harness for testing the daemon without root or kernel support: the devnode is a
// SOCK_SEQPACKET socketpair and cachefiles object fds are plain temp files.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lunixbochs/struc"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

const testDomain = "styxreview"

type testFdStore struct{}

func (testFdStore) Ready() {}

func (testFdStore) GetFd(string) (int, error) { return 0, os.ErrNotExist }

func (testFdStore) SaveFd(string, int) {}

func (testFdStore) RemoveFd(string) {}

func testSph(c byte) string { return strings.Repeat(string(c), 32) }

func newTestServer(t *testing.T, workers int, isTesting bool) *Server {
	t.Helper()
	s := NewServer(Config{
		DevPath:         "/nonexistent",
		CachePath:       t.TempDir(),
		CacheTag:        testDomain,
		CacheDomain:     testDomain,
		ErofsBlockShift: 12,
		Workers:         workers,
		IsTesting:       isTesting,
		FdStore:         testFdStore{},
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() }) // closing twice is a no-op in bbolt
	return s
}

func initTestServer(t *testing.T, s *Server, url string) {
	t.Helper()
	require.NoError(t, s.postInit(&pb.DaemonParams{
		Params:           &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		ManifesterUrl:    url,
		ManifestCacheUrl: url,
		ChunkReadUrl:     url,
		ChunkDiffUrl:     url,
	}, nil))
}

// Returns (daemon end, test end) of a fake devnode. The daemon end is non-blocking so
// that, like /dev/cachefiles, reading it with nothing queued returns instead of blocking.
func testDevnode(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetNonblock(fds[0], true))
	return fds[0], fds[1]
}

func testTempFd(t *testing.T) int {
	t.Helper()
	fd, err := unix.Open(filepath.Join(t.TempDir(), "obj"), unix.O_RDWR|unix.O_CREAT, 0o600)
	require.NoError(t, err)
	return fd
}

func packOpenMsg(t *testing.T, msgId, objectId uint32, fd int, fsid string) []byte {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, struc.Pack(&body, &cachefiles_open{
		Fd:        uint32(fd),
		VolumeKey: []byte("erofs," + testDomain + "\x00"),
		CookieKey: []byte(fsid),
	}))
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_OPEN, Len: uint32(16 + body.Len()), ObjectId: objectId,
	}))
	out.Write(body.Bytes())
	return out.Bytes()
}

func packCloseMsg(t *testing.T, msgId, objectId uint32) []byte {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_CLOSE, Len: 16, ObjectId: objectId,
	}))
	return out.Bytes()
}

func packReadMsg(t *testing.T, msgId, objectId uint32, off, ln uint64) []byte {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, struc.Pack(&out, &cachefiles_msg{
		MsgId: msgId, OpCode: CACHEFILES_OP_READ, Len: 32, ObjectId: objectId,
	}))
	require.NoError(t, struc.Pack(&out, &cachefiles_read{Off: off, Len: ln}))
	return out.Bytes()
}

// Reads one reply that the daemon wrote to the fake devnode, or "" on timeout.
func readDevnodeReply(t *testing.T, fd int, timeout time.Duration) string {
	t.Helper()
	tv := unix.NsecToTimeval(timeout.Nanoseconds())
	require.NoError(t, unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv))
	buf := make([]byte, 256)
	n, err := unix.Read(fd, buf)
	if err == unix.EAGAIN {
		return ""
	}
	require.NoError(t, err)
	return string(buf[:n])
}
