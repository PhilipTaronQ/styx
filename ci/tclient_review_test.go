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

// zstdcodec.Encode writes the compressed payloads into its input slice and returns the
// untouched clone, so nothing is ever compressed.
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

// zstdcodec.Decode has the same shape as Encode: it returns the clone with the payloads
// still compressed, so a compressed payload (for example one written by a fixed Encode
// during a rolling deploy) can't be decoded.
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
