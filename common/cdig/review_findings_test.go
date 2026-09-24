package cdig

// Test written during a bug review; expected to FAIL on the unfixed code.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// FromBase64("") indexes dst[0] of an empty slice and panics instead of returning ErrInvalid.
func TestReviewFromBase64Empty(t *testing.T) {
	var err error
	assert.NotPanics(t, func() { _, err = FromBase64("") })
	assert.Error(t, err)
}
