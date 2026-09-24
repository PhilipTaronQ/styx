package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
)

// buildOps builds the ops that requestChunk would build for digest, without starting them.
func (e *fetchEnv) buildOps(digest cdig.CDig) []*diffOp {
	sphps := e.sphps(digest)
	set := newOpSet(e.s)
	e.s.diffLock.Lock()
	defer e.s.diffLock.Unlock()
	require.NoError(e.t, e.s.db.View(func(tx *bbolt.Tx) error {
		return set.buildDiff(tx, digest, sphps, false)
	}))
	return set.ops
}

// requestChunk for a chunk that is already present (a second kernel read request for
// another page of the chunk, issued before the first request's op wrote it and handled
// after that op finished; or a present bit that survived a crash without its data): buildDiff
// skipped the present target but added its missing neighbours to set.ops and to diffMap,
// then requestChunk hit "buildDiff did not include requested chunk" and never started those
// ops. Their done channels were never closed, so any later read of those neighbours blocked
// forever.
func TestRequestChunkForPresentChunkLeavesNoOps(t *testing.T) {
	e := newFetchEnv(t)
	chunks := testChunks(12, 1)
	e.serveChunks(chunks)
	digests, locs := e.addImage(testSpX, chunks)
	sphps := e.sphps(digests[0])

	// first kernel read of chunk 0: a batch op fetches chunks 0..7 (InitOpSize)
	requestChunkWithin(t, e.s, locs[0], digests[0], sphps, 10*time.Second)
	require.EqualValues(t, 1, e.diffCalls.Load())
	require.Zero(t, e.diffMapLen())

	// second read request for chunk 0, handled after the first op finished
	requestChunkWithin(t, e.s, locs[0], digests[0], sphps, 10*time.Second)
	require.Zero(t, e.diffMapLen(), "requestChunk left diffMap entries for ops it never started")

	// a later read of chunk 8 would find a leaked op in diffMap and wait on it forever
	requestChunkWithin(t, e.s, locs[8], digests[8], e.sphps(digests[8]), 5*time.Second)
}

// Chunk diffs are checked against their digests only after decompressing, so a small zstd
// bomb from the chunk differ (or anything between it and us) used to make the daemon
// allocate whatever the bomb expanded to.
func TestChunkDiffDecompressionIsBounded(t *testing.T) {
	e := newFetchEnv(t)
	chunks := testChunks(2, 6)
	e.serveChunks(chunks)
	digests, _ := e.addImage(testSpX, chunks)
	e.diffBomb.Store(true)

	ops := e.buildOps(digests[0])
	require.Len(t, ops, 1)
	e.s.startDiffOp(context.Background(), ops[0])
	require.ErrorIs(t, ops[0].err, common.ErrTooLarge)
	require.Zero(t, e.diffMapLen())
}
