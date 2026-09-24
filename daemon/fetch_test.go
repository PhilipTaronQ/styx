package daemon

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
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

// requestChunk took diffLock without defer and ran buildDiff (manifest reads, proto decoding,
// iterator arithmetic on db contents) under it. handleMessage recovers panics, so a panic
// there left diffLock held and every later chunk request blocked forever. One data-driven
// trigger: an envelope's chunk_shift is not covered by the signature (entryFingerprint), and
// a negative value panics in Shift.Size() when a diff base's chunked manifest is read.
func TestPanicInBuildDiffDoesNotWedge(t *testing.T) {
	e := newFetchEnv(t)
	chunks := testChunks(2, 3)
	e.serveChunks(chunks)
	digests, locs := e.addImage(testSpX, chunks)

	// diff base candidate (same pname) with a chunked manifest envelope, chunk_shift = -1
	sphB, sphStrB, nameB, err := ParseSphAndName(testSpB)
	require.NoError(t, err)
	mdata := []byte("manifest chunk")
	mdig := cdig.Sum(mdata)
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:       common.ManifestContext + "/" + testSpB,
			Type:       pb.EntryType_REGULAR,
			Size:       int64(len(mdata)),
			Digests:    mdig[:],
			ChunkShift: -1,
		},
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
	})
	require.NoError(t, err)
	e.putManifest(sphB, sphStrB, nameB, envelope)
	mlocs, err := e.s.AllocateBatch(withAllocateCtx(context.Background(), makeManifestSph(sphB), true), []uint16{1}, []cdig.CDig{mdig})
	require.NoError(t, err)
	e.s.presentMap.Put(mlocs[0], struct{}{})

	// precondition: building the diff panics
	sphps := e.sphps(digests[0])
	var buildErr error
	e.s.diffLock.Lock()
	_ = e.s.db.View(func(tx *bbolt.Tx) error {
		_, buildErr = newOpSet(e.s).build(tx, locs[0], digests[0], sphps, false)
		return nil
	})
	e.s.diffLock.Unlock()
	require.ErrorContains(t, buildErr, "panic in buildDiff")
	require.Zero(t, e.diffMapLen())

	// falls back to a single read
	requestChunkWithin(t, e.s, locs[0], digests[0], e.sphps(digests[0]), 10*time.Second)

	if !e.s.diffLock.TryLock() {
		t.Fatal("diffLock is still held after a panic in buildDiff; every later chunk request would block forever")
	}
	e.s.diffLock.Unlock()
	require.Zero(t, e.diffMapLen())

	// and later requests still work
	requestChunkWithin(t, e.s, locs[1], digests[1], e.sphps(digests[1]), 10*time.Second)
}

// requestChunk recovers from a failed diff op by reading the chunk directly.
// requestPrefetch (styx prefetch, and materialize) had no fallback, so the same chunk
// differ failure failed the whole request even though every chunk could be read directly.
// ("recompress mismatch", which doDiffOp says should "fall back to single", is the
// deterministic version of this.)
func TestPrefetchFallsBackWhenDiffFails(t *testing.T) {
	e := newFetchEnv(t)
	chunks := testChunks(4, 2)
	e.serveChunks(chunks)
	digests, locs := e.addImage(testSpX, chunks)
	e.diffStatus.Store(http.StatusInternalServerError)

	requestChunkWithin(t, e.s, locs[0], digests[0], e.sphps(digests[0]), 10*time.Second)
	require.EqualValues(t, 1, e.diffCalls.Load(), "requestChunk should have tried a diff first")

	err := e.s.requestPrefetch(context.Background(), digests[1:])
	require.EqualValues(t, 2, e.diffCalls.Load(), "requestPrefetch should have tried a diff")
	require.NoError(t, err, "prefetch failed although every chunk is readable directly")
	require.NoError(t, e.s.db.View(func(tx *bbolt.Tx) error {
		for _, loc := range locs {
			require.True(t, e.s.locPresent(tx, loc))
		}
		return nil
	}))
}

// Diff ops used to read their bases while holding a diffSem slot. Base reads go through the
// kernel, and a base that isn't really in the cache makes the kernel send a READ, which
// needs a slot for the single op that fetches it. With every slot held by an op waiting on
// such a read, the daemon deadlocked. The slab here is a plain file, so the test can't make
// a base read wait on the kernel; instead it checks that an op gets as far as reading its
// bases (which fail, because the slab has no read fd) while every diffSem slot is taken.
func TestDiffOpReadsBasesWithoutDiffSemSlot(t *testing.T) {
	e := newFetchEnv(t)
	baseChunks := testChunks(4, 7)
	e.serveChunks(baseChunks)
	_, baseLocs := e.addImage(testSpB, baseChunks)
	for _, loc := range baseLocs {
		e.s.presentMap.Put(loc, struct{}{})
	}
	chunks := testChunks(4, 8)
	e.serveChunks(chunks)
	digests, _ := e.addImage(testSpX, chunks)

	ops := e.buildOps(digests[0])
	require.Len(t, ops, 1)
	require.True(t, ops[0].anyHasBase(), "precondition: expected a diff against %s", testSpB)

	e.s.stateLock.Lock()
	fds := e.s.readfdBySlab[0]
	delete(e.s.readfdBySlab, 0)
	e.s.stateLock.Unlock()
	defer func() {
		e.s.stateLock.Lock()
		e.s.readfdBySlab[0] = fds
		e.s.stateLock.Unlock()
	}()

	workers := int64(e.s.cfg.Workers)
	require.NoError(t, e.s.diffSem.Acquire(context.Background(), workers))
	released := false
	release := func() {
		if !released {
			released = true
			e.s.diffSem.Release(workers)
		}
	}
	defer release()

	go e.s.startDiffOp(context.Background(), ops[0])
	select {
	case <-ops[0].done:
	case <-time.After(10 * time.Second):
		release()
		<-ops[0].done
		t.Fatal("diff op waited for a diffSem slot before reading its bases")
	}
	release()
	require.ErrorContains(t, ops[0].err, "getKnownChunk")
	require.Zero(t, e.diffMapLen())
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
