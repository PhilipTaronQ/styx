package daemon

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/pb"
)

// End to end through the erofs builder, as getManifestAndBuildImage does it: a file whose
// size is an exact multiple of its chunk size gets 0 blocks for its last chunk
// (common.AppendBlocksList), so AllocateBatch gives the next new chunk the same slab
// address and overwrites the slab bucket's addr->digest entry. A kernel read of the first
// file's last chunk is then served the other chunk's data. Expected to FAIL on the current
// code.
func TestReviewExactMultipleFileSharesSlabAddress(t *testing.T) {
	e := newFetchEnv(t)
	chunks := reviewChunks(2, 5)
	da, db := cdig.Sum(chunks[0]), cdig.Sum(chunks[1])
	m := &pb.Manifest{Entries: []*pb.Entry{
		{Path: "/", Type: pb.EntryType_DIRECTORY},
		{Path: "/a", Type: pb.EntryType_REGULAR, Size: 1 << 16, Digests: da[:]}, // exactly one chunk
		{Path: "/b", Type: pb.EntryType_REGULAR, Size: 1 << 16, Digests: db[:]},
	}}
	sph, _, _, err := ParseSphAndName(reviewSpX)
	require.NoError(t, err)
	var image bytes.Buffer
	require.NoError(t, e.s.builder.BuildFromManifestWithSlab(withAllocateCtx(context.Background(), sph, false), m, &image, e.s))

	var la, lb erofs.SlabLoc
	var atA []byte
	require.NoError(t, e.s.db.View(func(tx *bbolt.Tx) error {
		cb := tx.Bucket(chunkBucket)
		la, lb = loadLoc(cb.Get(da[:])), loadLoc(cb.Get(db[:]))
		atA = bytes.Clone(tx.Bucket(slabBucket).Bucket(slabKey(la.SlabId)).Get(addrKey(la.Addr)))
		return nil
	}))
	require.NotEqual(t, la, lb, "two different 64 KiB chunks were allocated the same slab address")
	require.Equal(t, da[:], atA, "slab entry for /a's chunk names another digest")
}
