package cdig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// FromBase64("") used to index dst[0] of an empty slice and panic instead of returning
// ErrInvalid.
func TestFromBase64Empty(t *testing.T) {
	var err error
	assert.NotPanics(t, func() { _, err = FromBase64("") })
	assert.Error(t, err)
}
