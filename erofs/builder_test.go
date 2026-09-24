package erofs

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/pb"
)

// bumpSlab allocates the way the daemon's AllocateBatch does: a bump pointer that advances by
// exactly the requested block count.
type bumpSlab struct {
	next   uint32
	allocs []bumpAlloc
}

type bumpAlloc struct {
	addr   uint32
	blocks uint16
}

func (s *bumpSlab) VerifyParams(shift.Shift) error { return nil }

func (s *bumpSlab) AllocateBatch(_ context.Context, blocks []uint16, _ []cdig.CDig) ([]SlabLoc, error) {
	out := make([]SlabLoc, len(blocks))
	for i, b := range blocks {
		out[i] = SlabLoc{SlabId: 0, Addr: s.next}
		s.allocs = append(s.allocs, bumpAlloc{addr: s.next, blocks: b})
		s.next += uint32(b)
	}
	return out, nil
}

func (s *bumpSlab) SlabInfo(uint16) (string, uint32) { return "_slab_0", 1 << 28 }

func chunkedEntry(path string, size int64, fill byte) *pb.Entry {
	cs := shift.DefaultChunkShift
	data := bytes.Repeat([]byte{fill}, int(size))
	var digests []byte
	for off := int64(0); off < size; off += cs.Size() {
		d := cdig.Sum(data[off:min(off+cs.Size(), size)])
		digests = append(digests, d[:]...)
	}
	return &pb.Entry{Path: path, Type: pb.EntryType_REGULAR, Size: size, Digests: digests}
}

func buildNoPanic(m *pb.Manifest, sm SlabManager) (image []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	var out bytes.Buffer
	err = NewBuilder(BuilderConfig{BlockShift: 12}).BuildFromManifestWithSlab(context.Background(), m, &out, sm)
	return out.Bytes(), err
}

// erofs reads a full chunk at the chunk index's block address, including the last chunk of a
// file whose size is an exact multiple of the chunk size. The builder must ask the slab for
// that many blocks, or the next chunk is allocated at the same address.
func TestBuildExactMultipleChunkGetsBlocks(t *testing.T) {
	cs := shift.DefaultChunkShift
	m := &pb.Manifest{Entries: []*pb.Entry{
		{Path: "/", Type: pb.EntryType_DIRECTORY},
		chunkedEntry("/a", cs.Size(), 'a'),
		chunkedEntry("/b", cs.Size(), 'b'),
	}}
	sm := &bumpSlab{}
	_, err := buildNoPanic(m, sm)
	require.NoError(t, err)
	require.Len(t, sm.allocs, 2)
	want := uint16(cs.Size() >> 12)
	for i, a := range sm.allocs {
		require.Equal(t, want, a.blocks, "chunk %d: reserved %d blocks at %d, erofs will read %d", i, a.blocks, a.addr, want)
	}
	require.NotEqual(t, sm.allocs[0].addr, sm.allocs[1].addr, "two different chunks were given the same slab address")
}
