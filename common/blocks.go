package common

import "github.com/dnr/styx/common/shift"

// AppendBlocksList appends the number of blocks each chunk of a file of the given size takes
// in a slab. Every chunk but the last is a full chunk. The last one holds what's left, which is
// a full chunk when the size is an exact multiple of the chunk size, the same as
// shift.FileChunkSize says. An empty file has no chunks.
func AppendBlocksList(blocks []uint16, size int64, blockShift, chunkShift shift.Shift) []uint16 {
	nChunks := chunkShift.Blocks(size)
	if nChunks <= 0 {
		return blocks
	}
	allButLast := TruncU16(chunkShift.Size() >> blockShift)
	for j := 0; j < int(nChunks)-1; j++ {
		blocks = append(blocks, allButLast)
	}
	lastChunkLen := chunkShift.FileChunkSize(size, true)
	blocks = append(blocks, TruncU16(blockShift.Blocks(lastChunkLen)))
	return blocks
}
