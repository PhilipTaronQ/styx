package daemon

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/pb"
)

const slabTestStorePath = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"

// newSlabTestServer returns a server with a database and nothing else: no devnode, no slabs
// open, not initialized.
func newSlabTestServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(Config{
		CachePath:       t.TempDir(),
		CacheDomain:     "slabtest",
		ErofsBlockShift: 12,
		Workers:         1,
	})
	require.NoError(t, s.openDb())
	t.Cleanup(func() { s.db.Close() })
	return s
}

func slabTestSph(t *testing.T) Sph {
	sph, _, _, err := ParseSphAndName(slabTestStorePath)
	require.NoError(t, err)
	return sph
}

// End to end through the erofs builder, as getManifestAndBuildImage does it: a file whose
// size is an exact multiple of its chunk size needs a full chunk for its last chunk, or the
// next new chunk gets the same slab address and takes over its address key.
func TestBuildExactMultipleFileGetsOwnSlabSpace(t *testing.T) {
	s := newSlabTestServer(t)
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
