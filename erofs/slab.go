package erofs

import (
	"context"

	"github.com/PhilipTaronQ/styx/common/cdig"
	"github.com/PhilipTaronQ/styx/common/shift"
)

type (
	SlabLoc struct {
		SlabId uint16
		Addr   uint32
	}

	SlabManager interface {
		VerifyParams(blockShift shift.Shift) error
		AllocateBatch(ctx context.Context, blocks []uint16, digests []cdig.CDig) ([]SlabLoc, error)
		SlabInfo(slabId uint16) (tag string, totalBlocks uint32)
	}
)
