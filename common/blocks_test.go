package common

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dnr/styx/common/shift"
)

// The slab allocator reserves exactly the block counts AppendBlocksList
// returns, while erofs, the manifester and shift.FileChunkSize all treat the
// last chunk of a file whose size is an exact multiple of the chunk size as a
// full chunk. If AppendBlocksList says 0 blocks for that chunk, the next
// allocation lands on top of it.
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

// Every chunk must be given at least as many blocks as the bytes that
// FileChunkSize says it holds.
func TestAppendBlocksListCoversFileChunkSize(t *testing.T) {
	const blk = shift.Shift(12)
	cs := shift.DefaultChunkShift
	sizes := []int64{1, 4095, 4096, 4097, cs.Size() - 1, cs.Size(), cs.Size() + 1, 2 * cs.Size(), 5*cs.Size() + 123}
	for _, size := range sizes {
		blocks := AppendBlocksList(nil, size, blk, cs)
		for i, b := range blocks {
			chunk := cs.FileChunkSize(size, i == len(blocks)-1)
			assert.GreaterOrEqual(t, int64(b)<<blk, chunk,
				"size %d: chunk %d of %d holds %d bytes but got %d blocks", size, i, len(blocks), chunk, b)
		}
	}
}
