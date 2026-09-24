package common

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PhilipTaronQ/styx/common/shift"
)

// blocksFromFileChunkSize is what AppendBlocksList has to agree with: the slab allocator
// reserves exactly these block counts, while erofs, the manifester and the daemon's reads and
// writes all size chunks with FileChunkSize.
func blocksFromFileChunkSize(size int64, blockShift, chunkShift shift.Shift) []uint16 {
	n := int(chunkShift.Blocks(size))
	var out []uint16
	for i := range n {
		out = append(out, uint16(blockShift.Blocks(chunkShift.FileChunkSize(size, i == n-1))))
	}
	return out
}

// If AppendBlocksList says 0 blocks for the last chunk of a file whose size is an exact
// multiple of the chunk size, the next allocation lands on top of it.
func TestAppendBlocksListExactMultiple(t *testing.T) {
	const blk = shift.Shift(12)
	for _, cs := range []shift.Shift{16, 18, 20} {
		for _, n := range []int64{1, 2, 3} {
			size := n << cs
			got := AppendBlocksList(nil, size, blk, cs)
			want := make([]uint16, n)
			for i := range want {
				want[i] = uint16(cs.Size() >> blk)
			}
			assert.Equal(t, want, got, "size %d, chunk shift %d", size, cs)
		}
	}
}

func TestAppendBlocksListMatchesFileChunkSize(t *testing.T) {
	const blk = shift.Shift(12)
	cs := shift.DefaultChunkShift
	sizes := []int64{1, 100, 4095, 4096, 4097, cs.Size() - 1, cs.Size(), cs.Size() + 1, 2 * cs.Size(), 5*cs.Size() + 123, 1 << 20}
	for _, size := range sizes {
		assert.Equal(t, blocksFromFileChunkSize(size, blk, cs), AppendBlocksList(nil, size, blk, cs), "file size %d", size)
	}
	assert.Empty(t, AppendBlocksList(nil, 0, blk, cs), "an empty file has no chunks")
	assert.Equal(t, []uint16{7, 16}, AppendBlocksList([]uint16{7}, cs.Size(), blk, cs), "appends")
}

// Property: for any size and valid shifts, AppendBlocksList agrees with FileChunkSize, and no
// chunk gets 0 blocks.
func TestAppendBlocksListProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		blk := shift.Shift(9 + r.IntN(4))
		cs := shift.Shift(int(blk) + r.IntN(int(shift.MaxChunkShift-blk)+1))
		var size int64
		switch r.IntN(3) {
		case 0: // exact multiple of the chunk size
			size = (1 + r.Int64N(64)) << cs
		case 1: // near a chunk boundary
			size = max(1, (1+r.Int64N(64))<<cs+r.Int64N(3)-1)
		default:
			size = 1 + r.Int64N(1024<<cs)
		}
		checkAppendBlocksList(t, size, blk, cs)
	}
}

func FuzzAppendBlocksList(f *testing.F) {
	f.Add(int64(1<<16), uint8(12), uint8(16))
	f.Add(int64(3<<20), uint8(12), uint8(20))
	f.Add(int64(1<<16+1), uint8(9), uint8(16))
	f.Add(int64(12345), uint8(12), uint8(18))
	f.Fuzz(func(t *testing.T, size int64, b, c uint8) {
		blk := shift.Shift(9 + int(b)%4)
		cs := blk + shift.Shift(int(c)%int(shift.MaxChunkShift-blk+1))
		if size <= 0 || size > 1<<40 || cs.Blocks(size) > 1<<16 {
			t.Skip()
		}
		checkAppendBlocksList(t, size, blk, cs)
	})
}

func checkAppendBlocksList(t *testing.T, size int64, blk, cs shift.Shift) {
	t.Helper()
	got := AppendBlocksList(nil, size, blk, cs)
	if !assert.Equal(t, blocksFromFileChunkSize(size, blk, cs), got, "size %d, block shift %d, chunk shift %d", size, blk, cs) {
		t.FailNow()
	}
	for i, b := range got {
		if b == 0 || int64(b)<<blk < cs.FileChunkSize(size, i == len(got)-1) {
			t.Fatalf("size %d, block shift %d, chunk shift %d: chunk %d got %d blocks", size, blk, cs, i, b)
		}
	}
}
