package common

import (
	"bytes"
	"sync"

	"github.com/DataDog/zstd"
)

type ZstdCtxPool struct {
	p sync.Pool
}

var globalPool = &ZstdCtxPool{
	p: sync.Pool{New: func() any { return zstd.NewCtx() }},
}

func GetZstdCtxPool() *ZstdCtxPool {
	return globalPool
}

func (z *ZstdCtxPool) Get() zstd.Ctx {
	return z.p.Get().(zstd.Ctx)
}

func (z *ZstdCtxPool) Put(c zstd.Ctx) {
	z.p.Put(c)
}

// DecompressLimit decompresses src, failing with ErrTooLarge if the result would be more than
// limit bytes. Unlike Ctx.Decompress, it never allocates much more than limit bytes, whatever
// src claims or contains.
func DecompressLimit(src []byte, limit int64) ([]byte, error) {
	zr := zstd.NewReader(bytes.NewReader(src))
	defer zr.Close() // frees the C decompression stream
	return ReadAllLimit(zr, limit)
}
