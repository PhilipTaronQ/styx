package manifester

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/cdig"
)

// Upstream 404 for the narinfo: RetryHttpRequest turns it into an error, which was wrapped
// with ErrReq, so the StatusNotFound branch in BuildFromNar was unreachable and writeError
// answered 417.
func TestNarinfo404IsReportedAs417(t *testing.T) {
	_, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)
	srv, err := NewManifestServer(Config{AllowedUpstreams: []string{up.host()}, ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	body, err := json.Marshal(ManifestReq{
		Upstream:      up.url(),
		StorePathHash: sphOf("absent"),
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}
