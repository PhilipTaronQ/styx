package manifester

// Review test for the chunk read path used by the daemon (readSingle -> csread.Get).
// Expected to FAIL on the current code.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/shift"
)

// urlChunkStoreRead.Get reads the whole body and zstd-decompresses it with no limit
// (Decompress falls back to a streaming ReadAll when the frame is bigger than its hint).
// No chunk is bigger than MaxChunkShift, and the digest is only checked after
// decompression, so a small response from the chunk store can make the daemon allocate
// arbitrary amounts of memory.
func TestReviewChunkReadDecompressionIsBounded(t *testing.T) {
	bomb, err := zstd.Compress(nil, make([]byte, 32<<20))
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "zstd")
		w.Write(bomb)
	}))
	defer srv.Close()

	cs := NewChunkStoreReadUrl(srv.URL, ChunkReadPath)
	b, err := cs.Get(context.Background(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil)
	if err == nil {
		require.LessOrEqual(t, int64(len(b)), shift.MaxChunkShift.Size(),
			"%d byte response expanded to %d bytes", len(bomb), len(b))
	}
}
