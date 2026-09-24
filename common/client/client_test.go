package client

import (
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func serveUnix(t *testing.T, status int, body string) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "styx.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// The styx CLI commands return CallAndPrint's error, so a daemon error has
// to come back as one for the command to exit non-zero.
func TestCallAndPrintReturnsDaemonError(t *testing.T) {
	sock := serveUnix(t, http.StatusNotFound, `{"Error":"not mounted"}`)
	err := NewClient(sock).CallAndPrint("/umount", map[string]string{"StorePath": "x"})
	if err == nil {
		t.Fatal("CallAndPrint returned nil for a 404 from the daemon")
	} else if !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("error %q should give the status and the daemon's message", err)
	}
}

func TestCallAndPrintNonObjectErrorBody(t *testing.T) {
	sock := serveUnix(t, http.StatusBadRequest, `null`)
	if err := NewClient(sock).CallAndPrint("/mount", map[string]string{}); err == nil {
		t.Fatal("CallAndPrint returned nil for a 400 from the daemon")
	}
}

func TestCallAndPrintSuccess(t *testing.T) {
	sock := serveUnix(t, http.StatusOK, `{"Success":true}`)
	if err := NewClient(sock).CallAndPrint("/mount", map[string]string{}); err != nil {
		t.Fatalf("CallAndPrint: %v", err)
	}
}
