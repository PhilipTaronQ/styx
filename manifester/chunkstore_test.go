package manifester

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"

	"github.com/PhilipTaronQ/styx/common"
	"github.com/PhilipTaronQ/styx/common/shift"
)

// urlChunkStoreRead.Get used to read the whole body and zstd-decompress it with no limit
// (Decompress falls back to a streaming ReadAll when the frame is bigger than its hint).
// No chunk is bigger than MaxChunkShift, and the digest is only checked after
// decompression, so a small response from the chunk store could make the daemon allocate
// arbitrary amounts of memory.
func TestChunkReadDecompressionIsBounded(t *testing.T) {
	maxChunk := make([]byte, shift.MaxChunkShift.Size())
	ok, err := zstd.Compress(nil, maxChunk)
	require.NoError(t, err)
	bomb, err := zstd.Compress(nil, make([]byte, 32<<20))
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "zstd")
		if r.URL.Path == ChunkReadPath+"ok" {
			w.Write(ok)
		} else {
			w.Write(bomb)
		}
	}))
	defer srv.Close()

	cs := NewChunkStoreReadUrl(srv.URL, ChunkReadPath)
	b, err := cs.Get(context.Background(), "ok", nil)
	require.NoError(t, err)
	require.Equal(t, maxChunk, b)

	b, err = cs.Get(context.Background(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil)
	require.ErrorIs(t, err, common.ErrTooLarge, "%d byte response expanded to %d bytes", len(bomb), len(b))
}
