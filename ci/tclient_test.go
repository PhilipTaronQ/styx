package ci

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	"github.com/dnr/styx/common"
)

// zstdcodec.Encode used to write the compressed payloads into its input slice and return the
// untouched clone, so nothing was ever compressed.
func TestZstdCodecCompressesPayloads(t *testing.T) {
	names := make([]string, 5000)
	for i := range names {
		names[i] = fmt.Sprintf("package-name-%d-1.2.3", i%50)
	}
	plain, err := converter.GetDefaultDataConverter().ToPayload(names)
	require.NoError(t, err)

	dc := getDataConverter()
	p, err := dc.ToPayload(names)
	require.NoError(t, err)

	var out []string
	require.NoError(t, dc.FromPayload(p, &out))
	require.Equal(t, names, out)

	require.Equal(t, "zst", string(p.Metadata["styx/cmp"]), "payload was not compressed")
	require.Less(t, len(p.Data), len(plain.Data)/4)
}

// zstdcodec.Decode had the same bug as Encode: it returned the clone with the payloads still
// compressed.
func TestZstdCodecDecodesCompressedPayloads(t *testing.T) {
	const val = "hello hello hello hello hello hello"
	plain, err := converter.GetDefaultDataConverter().ToPayload(val)
	require.NoError(t, err)

	zp := common.GetZstdCtxPool()
	z := zp.Get()
	zd, err := z.Compress(nil, plain.Data)
	zp.Put(z)
	require.NoError(t, err)

	md := maps.Clone(plain.Metadata)
	md["styx/cmp"] = []byte("zst")

	var out string
	require.NoError(t, getDataConverter().FromPayload(&commonpb.Payload{Metadata: md, Data: zd}, &out))
	require.Equal(t, val, out)
}

// Encode leaves a payload that compression doesn't shrink as it is, with no marker, so
// Decode has to pass unmarked payloads through.
func TestZstdCodecLeavesSmallPayloadsUncompressed(t *testing.T) {
	const val = "hi"
	plain, err := converter.GetDefaultDataConverter().ToPayload(val)
	require.NoError(t, err)

	dc := getDataConverter()
	p, err := dc.ToPayload(val)
	require.NoError(t, err)
	require.NotContains(t, p.Metadata, "styx/cmp")
	require.Equal(t, plain.Data, p.Data)

	var out string
	require.NoError(t, dc.FromPayload(p, &out))
	require.Equal(t, val, out)
}
