package tests

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/client"
	"github.com/dnr/styx/daemon"
)

const (
	lcOpusfile     = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
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
