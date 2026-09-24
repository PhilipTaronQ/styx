package daemon

// Tests of local gc and the image lifecycle that need no root and no kernel support. The
// devnode is one end of a SOCK_SEQPACKET socketpair, so on-demand features look enabled.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

const gcTestDomain = "styxgctest"

var gcDefault = map[pb.MountState]bool{pb.MountState_Unmounted: true}

type gcTestFdStore struct{}

func (gcTestFdStore) Ready() {}

func (gcTestFdStore) GetFd(string) (int, error) { return 0, os.ErrNotExist }

func (gcTestFdStore) SaveFd(string, int) {}

func (gcTestFdStore) RemoveFd(string) {}

func gcTestSph(c byte) string { return strings.Repeat(string(c), 32) }

func gcTestStorePath(c byte, name string) string { return gcTestSph(c) + "-" + name }

func newGcTestServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(Config{
		DevPath:         "/nonexistent",
		CachePath:       t.TempDir(),
		CacheTag:        gcTestDomain,
		CacheDomain:     gcTestDomain,
		ErofsBlockShift: 12,
		Workers:         2,
		IsTesting:       true,
		FdStore:         gcTestFdStore{},
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() })
	return s
}

func initGcTestServer(t *testing.T, s *Server, url string) {
	t.Helper()
	require.NoError(t, s.postInit(&pb.DaemonParams{
		Params:           &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		ManifesterUrl:    url,
		ManifestCacheUrl: url,
		ChunkReadUrl:     url,
		ChunkDiffUrl:     url,
	}, nil))
}

// Gives s a fake devnode so mounts are accepted. Nothing reads the other end.
func gcTestDevnode(t *testing.T, s *Server) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetNonblock(fds[0], true))
	t.Cleanup(func() { unix.Close(fds[0]); unix.Close(fds[1]) })
	s.devnode.Store(int32(fds[0]))
}

func gcTestGetImage(t *testing.T, s *Server, sphStr string) *pb.DbImage {
	t.Helper()
	var img *pb.DbImage
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(imageBucket).Get([]byte(sphStr)); v != nil {
			img = &pb.DbImage{}
			return proto.Unmarshal(v, img)
		}
		return nil
	}))
	return img
}

// A mount whose manifest can't be fetched used to leave its image Requested with no
// manifest. gc keeps Requested images by default and gave up on the one it couldn't
// trace, so one failed mount made every default "styx gc" fail.
func TestGcAfterFailedMount(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler()) // manifest cache and manifester both 404
	t.Cleanup(srv.Close)

	s := newGcTestServer(t)
	initGcTestServer(t, s, srv.URL)
	gcTestDevnode(t, s)

	_, err := s.handleMountReq(context.Background(), &MountReq{
		Upstream:   srv.URL,
		StorePath:  gcTestStorePath('0', "missing-1.0"),
		MountPoint: filepath.Join(t.TempDir(), "mp"),
	})
	require.Error(t, err, "mount of a path the manifester can't find must fail")

	img := gcTestGetImage(t, s, gcTestSph('0'))
	require.NotNil(t, img)
	require.Equal(t, pb.MountState_MountError, img.MountState)
	require.NotEmpty(t, img.LastMountError)

	// the request "styx gc" sends with default flags
	_, err = s.handleGcReq(context.Background(), &GcReq{DryRunFast: true, GcByState: gcDefault})
	require.NoError(t, err, "default gc must not be wedged by an earlier failed mount")
}
