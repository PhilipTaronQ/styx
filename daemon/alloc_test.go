package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/pb"
)

const slabTestStorePath = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"

func slabTestSph(t *testing.T) Sph {
	sph, _, _, err := ParseSphAndName(slabTestStorePath)
	require.NoError(t, err)
	return sph
}

func testDigest(i int) cdig.CDig {
	return cdig.Sum(binary.LittleEndian.AppendUint64(nil, uint64(i)))
}

type testAlloc struct {
	loc    erofs.SlabLoc
	blocks uint16
}

// requireNoOverlap checks that every allocation is inside its slab and none overlap.
func requireNoOverlap(t *testing.T, s *Server, allocs []testAlloc) {
	t.Helper()
	limit := uint32(uint64(slabBytes) >> s.blockShift)
	for i, a := range allocs {
		require.GreaterOrEqual(t, a.loc.Addr, uint32(reservedBlocks), "allocation %d at %v", i, a.loc)
		require.LessOrEqual(t, a.loc.Addr+uint32(a.blocks), limit, "allocation %d at %v runs past the end of its slab", i, a.loc)
		for j, b := range allocs[:i] {
			if a.loc.SlabId == b.loc.SlabId {
				overlap := a.loc.Addr < b.loc.Addr+uint32(b.blocks) && b.loc.Addr < a.loc.Addr+uint32(a.blocks)
				require.False(t, overlap, "allocation %d at %v (%d blocks) overlaps %d at %v (%d blocks)",
					i, a.loc, a.blocks, j, b.loc, b.blocks)
			}
		}
	}
}

func setSlabSequence(t *testing.T, s *Server, id uint16, seq uint64) {
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := tx.Bucket(slabBucket).CreateBucketIfNotExists(slabKey(id))
		if err != nil {
			return err
		}
		return sb.SetSequence(seq)
	}))
}

// A batch that runs off the end of a slab has to leave that slab's sequence where it stopped,
// or the next batch hands out the same addresses again.
func TestAllocateBatchSlabRollover(t *testing.T) {
	s := newTestServer(t, 1, false)
	limit := uint64(slabBytes >> s.blockShift)
	setSlabSequence(t, s, 0, limit-20)

	ctx := withAllocateCtx(context.Background(), slabTestSph(t), false)
	var allocs []testAlloc
	n := 0
	for range 3 {
		blocks := []uint16{16, 16, 16}
		digests := []cdig.CDig{testDigest(n), testDigest(n + 1), testDigest(n + 2)}
		n += 3
		locs, err := s.AllocateBatch(ctx, blocks, digests)
		require.NoError(t, err)
		for i, loc := range locs {
			allocs = append(allocs, testAlloc{loc, blocks[i]})
		}
	}
	t.Log(allocs)
	require.Equal(t, uint16(1), allocs[len(allocs)-1].loc.SlabId, "should have moved on to slab 1")
	requireNoOverlap(t, s, allocs)
}

// Same for vaporize's two-phase allocation, and the second phase has to record each chunk in
// the slab it was reserved in.
func TestPreallocateBatchSlabRollover(t *testing.T) {
	s := newTestServer(t, 1, false)
	limit := uint64(slabBytes >> s.blockShift)
	setSlabSequence(t, s, 0, limit-20)

	ctx := withAllocateCtx(context.Background(), slabTestSph(t), false)
	var allocs []testAlloc
	n := 0
	for range 3 {
		blocks := []uint16{16, 16}
		digests := []cdig.CDig{testDigest(n), testDigest(n + 1)}
		n += 2
		locs, wasAllocated, err := s.preallocateBatch(ctx, blocks, digests)
		require.NoError(t, err)
		require.NoError(t, s.commitPreallocated(ctx, blocks, digests, locs, wasAllocated))
		for i, loc := range locs {
			allocs = append(allocs, testAlloc{loc, blocks[i]})
		}
		require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
			for i, loc := range locs {
				sb := tx.Bucket(slabBucket).Bucket(slabKey(loc.SlabId))
				require.NotNil(t, sb)
				require.Equal(t, digests[i][:], sb.Get(addrKey(loc.Addr)), "chunk %d not recorded in slab %d", i, loc.SlabId)
				require.Equal(t, loc, loadLoc(tx.Bucket(chunkBucket).Get(digests[i][:])))
			}
			return nil
		}))
	}
	t.Log(allocs)
	require.Equal(t, uint16(1), allocs[len(allocs)-1].loc.SlabId, "should have moved on to slab 1")
	requireNoOverlap(t, s, allocs)
}

func TestAllocateBatchRejectsZeroBlocks(t *testing.T) {
	s := newTestServer(t, 1, false)
	ctx := withAllocateCtx(context.Background(), slabTestSph(t), false)
	_, err := s.AllocateBatch(ctx, []uint16{16, 0}, []cdig.CDig{testDigest(1), testDigest(2)})
	require.ErrorContains(t, err, "zero-block")
	_, _, err = s.preallocateBatch(ctx, []uint16{0}, []cdig.CDig{testDigest(3)})
	require.ErrorContains(t, err, "zero-block")
}

// End to end through the erofs builder, as getManifestAndBuildImage does it: a file whose
// size is an exact multiple of its chunk size needs a full chunk for its last chunk, or the
// next new chunk gets the same slab address and takes over its address key.
func TestBuildExactMultipleFileGetsOwnSlabSpace(t *testing.T) {
	s := newTestServer(t, 1, false)
	cs := shift.DefaultChunkShift
	ca := bytes.Repeat([]byte{'a'}, int(cs.Size()))
	cb := bytes.Repeat([]byte{'b'}, int(cs.Size()))
	da, db := cdig.Sum(ca), cdig.Sum(cb)
	m := &pb.Manifest{Entries: []*pb.Entry{
		{Path: "/", Type: pb.EntryType_DIRECTORY},
		{Path: "/a", Type: pb.EntryType_REGULAR, Size: cs.Size(), Digests: da[:]}, // exactly one chunk
		{Path: "/b", Type: pb.EntryType_REGULAR, Size: cs.Size(), Digests: db[:]},
	}}
	var image bytes.Buffer
	ctx := withAllocateCtx(context.Background(), slabTestSph(t), false)
	require.NoError(t, s.builder.BuildFromManifestWithSlab(ctx, m, &image, s))

	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		cbk := tx.Bucket(chunkBucket)
		sb := tx.Bucket(slabBucket).Bucket(slabKey(0))
		la, lb := loadLoc(cbk.Get(da[:])), loadLoc(cbk.Get(db[:]))
		require.NotEqual(t, la, lb, "two different chunks were allocated the same slab address")
		require.Equal(t, da[:], sb.Get(addrKey(la.Addr)), "slab entry for /a's chunk names another digest")
		require.Equal(t, db[:], sb.Get(addrKey(lb.Addr)))
		require.Equal(t, uint64(lb.Addr)+uint64(cs.Size()>>s.blockShift), sb.Sequence())
		require.Equal(t, cs.Size()>>s.blockShift, int64(lb.Addr-la.Addr))
		return nil
	}))
}
