package common

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dnr/styx/common/shift"
)

// AppendBlocksList sizes the last chunk from chunkShift.Leftover(size), which is 0 when the
// file size is an exact multiple of the chunk size, so that full chunk is allocated 0
// blocks. FileChunkSize (what the daemon fetches, digests and writes for that chunk) says
// it is a full chunk. Expected to FAIL on the current code.
func TestReviewAppendBlocksListExactMultiple(t *testing.T) {
	const blk, cs shift.Shift = 12, shift.DefaultChunkShift
	for _, size := range []int64{100, 1<<16 + 1, 1 << 16, 2 << 16, 1 << 20} {
		n := int(cs.Blocks(size))
		want := make([]uint16, n)
		for i := range want {
			want[i] = uint16(blk.Blocks(cs.FileChunkSize(size, i == n-1)))
		}
		assert.Equal(t, want, AppendBlocksList(nil, size, blk, cs), "file size %d", size)
	}
}
