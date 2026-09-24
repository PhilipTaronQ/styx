package common

import (
	"bytes"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These exercise the cgo zstd paths styx relies on, so the sanitizer CI jobs
// have something to instrument.

func testData(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		// compressible but not trivially so
		b[i] = byte('a' + r.IntN(8))
	}
	return b
}

func TestZstdCtxPoolConcurrent(t *testing.T) {
	pool := GetZstdCtxPool()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 20 {
				data := testData(uint64(g*100+i), 1000+g*i*37)
				z := pool.Get()
				comp, err := z.Compress(nil, data)
				pool.Put(z)
				if !assert.NoError(t, err) {
					return
				}
				z = pool.Get()
				out, err := z.Decompress(nil, comp)
				pool.Put(z)
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, data, out)
			}
		})
	}
	wg.Wait()
}

// Mirrors manifester chunk diffing (NewWriterPatcher) and the daemon side
// (NewReaderPatcher).
func TestZstdPatcherRoundTrip(t *testing.T) {
	base := [][]byte{testData(1, 50000), testData(2, 30000)}
	baseData := ContiguousBytes(base)

	// target is base with some edits
	target := bytes.Clone(baseData)
	copy(target[1000:], testData(3, 500))
	target = append(target, testData(4, 2000)...)
	reqs := [][]byte{target[:40000], target[40000:]}

	var diff bytes.Buffer
	zw := zstd.NewWriterPatcher(&diff, 3, baseData, int64(len(target)))
	for _, r := range reqs {
		_, err := zw.Write(r)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	assert.Less(t, diff.Len(), len(target)/4, "diff should be much smaller than target")

	zr := zstd.NewReaderPatcher(&diff, baseData)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.NoError(t, zr.Close())
	assert.Equal(t, target, out)
}
