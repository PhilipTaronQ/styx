package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFscachePath(t *testing.T) {
	// Paths observed from cachefiles on a real kernel. The hash directory is
	// always two hex digits ("@%02x"), including when the hash is < 0x10.
	require.Equal(t,
		"cache/Ierofs,styxtest9dbeb96981e633e3/@0e/D_slab_0",
		fscachePath("styxtest9dbeb96981e633e3", "_slab_0"))
	require.Equal(t,
		"cache/Ierofs,styxtestbf4a1ed3c4d64dd8/@1d/D_slab_0",
		fscachePath("styxtestbf4a1ed3c4d64dd8", "_slab_0"))
	require.Equal(t,
		"cache/Ierofs,styxtest9dbeb96981e633e3/@fb/D_slabimg_0",
		fscachePath("styxtest9dbeb96981e633e3", "_slabimg_0"))
}
