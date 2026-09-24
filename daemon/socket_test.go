package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
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
