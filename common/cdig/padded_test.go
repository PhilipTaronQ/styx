package cdig

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchesPadded(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	data[len(data)-1] = 1 // doesn't end in zeros
	block := func(b []byte) []byte {
		out := make([]byte, (len(b)+4095)&^4095)
		copy(out, b)
		return out
	}

	d := Sum(data)
	assert.True(t, d.MatchesPadded(data))
	assert.True(t, d.MatchesPadded(block(data)))
	assert.False(t, d.MatchesPadded(data[:len(data)-1]))
	assert.False(t, d.MatchesPadded(block(data[1:])))

	// the data itself may end in zeros
	withZeros := append(bytes.Clone(data), make([]byte, 100)...)
	dz := Sum(withZeros)
	assert.True(t, dz.MatchesPadded(block(withZeros)))
	assert.True(t, d.MatchesPadded(block(withZeros)))

	// garbage after the data is not padding
	dirty := block(data)
	dirty[len(dirty)-1] = 1
	assert.False(t, d.MatchesPadded(dirty))

	assert.False(t, Sum(nil).MatchesPadded(nil))
	assert.False(t, Sum(nil).MatchesPadded(make([]byte, 4096)))
	assert.True(t, Sum([]byte{0}).MatchesPadded(make([]byte, 4096)))
}
