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

	dc := getDataConverter(true)
	p, err := dc.ToPayload(names)
	require.NoError(t, err)

	var out []string
	require.NoError(t, dc.FromPayload(p, &out))
	require.Equal(t, names, out)

	require.Equal(t, "zst", string(p.Metadata["styx/cmp"]), "payload was not compressed")
	require.Less(t, len(p.Data), len(plain.Data)/4)
}

// Until every worker can decode compressed payloads, encoding leaves them alone.
func TestZstdCodecCompressionIsOptIn(t *testing.T) {
	names := make([]string, 5000)
	for i := range names {
		names[i] = fmt.Sprintf("package-name-%d-1.2.3", i%50)
	}
	plain, err := converter.GetDefaultDataConverter().ToPayload(names)
	require.NoError(t, err)

	p, err := getDataConverter(false).ToPayload(names)
	require.NoError(t, err)
	require.NotContains(t, p.Metadata, "styx/cmp")
	require.Equal(t, plain.Data, p.Data)
}

// zstdcodec.Decode had the same bug as Encode: it returned the clone with the payloads still
// compressed. Decoding must work whether or not this process compresses, so that decoders
// can be deployed before encoders.
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

	for _, compress := range []bool{false, true} {
		var out string
		require.NoError(t, getDataConverter(compress).FromPayload(&commonpb.Payload{Metadata: md, Data: zd}, &out))
		require.Equal(t, val, out)
	}
}

// Payloads written before compression worked (all of them, so far) are uncompressed.
func TestZstdCodecDecodesUncompressedPayloads(t *testing.T) {
	const val = "hello hello hello hello hello hello"
	plain, err := converter.GetDefaultDataConverter().ToPayload(val)
	require.NoError(t, err)

	for _, compress := range []bool{false, true} {
		var out string
		require.NoError(t, getDataConverter(compress).FromPayload(plain, &out))
		require.Equal(t, val, out)
	}
}
