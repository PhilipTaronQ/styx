package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
