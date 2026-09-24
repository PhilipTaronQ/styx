package daemon

import (
	"context"
	"testing"

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
