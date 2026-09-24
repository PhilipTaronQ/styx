package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/PhilipTaronQ/styx/manifester"
	"github.com/PhilipTaronQ/styx/pb"
)

func TestJsonmwRejectsHugeBody(t *testing.T) {
	called := false
	h := jsonmw(func(ctx context.Context, r *MountReq) (*Status, error) {
		called = true
		return nil, nil
	})

	body := `{"StorePath":"` + strings.Repeat("a", maxRequestBytes) + `"}`
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, MountPath, strings.NewReader(body)))
	require.False(t, called, "handler ran on a request body over the limit")
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)

	w = httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, MountPath, strings.NewReader(`{"StorePath":"x"}`)))
	require.True(t, called)
	require.Equal(t, http.StatusOK, w.Code)
}

// The root socket can mount anything anywhere, so only root may connect to it. The public
// socket is for everyone.
func TestSocketPermissions(t *testing.T) {
	s := newTestServer(t, 2, true)
	s.cfg.PublicSock = filepath.Join(t.TempDir(), "public.sock")
	require.NoError(t, s.startSocketServer())
	t.Cleanup(func() {
		close(s.shutdownChan)
		s.shutdownWait.Wait()
	})

	st, err := os.Stat(filepath.Join(s.cfg.CachePath, Socket))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), st.Mode().Perm(), "root socket")
	st, err = os.Stat(s.cfg.PublicSock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o777), st.Mode().Perm(), "public socket")
}

// Each shard downloads the whole tarball, so the public socket must not choose how many.
func TestPublicTarballIgnoresShards(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			// ResolveUrl's request for the tarball itself
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == manifester.ManifestPath:
			posts.Add(1)
			http.Error(w, "no", http.StatusInternalServerError) // not retried
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	s := newTestServer(t, 2, true)
	initTestServer(t, s, srv.URL)

	_, err := s.handlePublicTarballReq(context.Background(), &TarballReq{
		UpstreamUrl: srv.URL + "/src.tar.gz",
		Shards:      10,
	})
	require.Error(t, err)
	require.EqualValues(t, 1, posts.Load(), "manifest requests for one public tarball request")
}

func TestPruneFakeCache(t *testing.T) {
	s := newTestServer(t, 2, true)
	now := time.Now()
	old := now.Add(-2 * fakeCacheExpiry)
	put := func(sph string, updated time.Time) {
		b, err := proto.Marshal(&pb.FakeCacheData{Narinfo: []byte("x"), Updated: updated.Unix()})
		require.NoError(t, err)
		require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket(fakeCacheBucket).Put([]byte(sph), b)
		}))
	}
	put(testSph('a'), now) // recent
	put(testSph('b'), old) // old, no image
	put(testSph('c'), old) // old, but remanifesting its image needs it
	require.NoError(t, s.imageTx(testSph('c'), func(img *pb.DbImage) error {
		img.MountState = pb.MountState_Mounted
		return nil
	}))

	require.NoError(t, s.pruneFakeCache(now))

	for sph, keep := range map[string]bool{testSph('a'): true, testSph('b'): false, testSph('c'): true} {
		_, err := s.getFakeCacheData(sph)
		if keep {
			require.NoError(t, err, "entry %s", sph)
		} else {
			require.ErrorIs(t, err, errNotFound, "entry %s", sph)
		}
	}
}
