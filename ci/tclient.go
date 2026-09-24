package ci

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/dnr/styx/common"
)

// getDataConverter returns a data converter that decodes zstd-compressed payloads, and
// compresses payloads it encodes if compress is set.
//
// Deploy ordering: payloads were never actually compressed before, and a process with an
// older binary can't decode compressed ones. So enable compress (--compress_payloads) only
// once every worker and client that reads this namespace's payloads runs a binary with this
// decoder. Decoding accepts uncompressed payloads, so that's safe in any order.
func getDataConverter(compress bool) converter.DataConverter {
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), zstdcodec{compress: compress})
}

func getTemporalClient(ctx context.Context, paramSrc string, compress bool) (client.Client, string, error) {
	params, err := getParams(paramSrc)
	if err != nil {
		return nil, "", err
	}
	parts := strings.SplitN(params, "~", 3)
	if len(parts) < 3 {
		return nil, "", errors.New("bad params format")
	}
	hostPort, namespace, apiKey := parts[0], parts[1], parts[2]

	dc := getDataConverter(compress)
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})

	co := client.Options{
		HostPort:         hostPort,
		Namespace:        namespace,
		DataConverter:    dc,
		FailureConverter: fc,
		Logger:           log.NewStructuredLogger(slog.Default()),
	}
	if apiKey != "" {
		co.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		// TODO: remove after go sdk does this automatically
		co.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{},
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(
					func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
						return invoker(
							metadata.AppendToOutgoingContext(ctx, "temporal-namespace", namespace),
							method,
							req,
							reply,
							cc,
							opts...,
						)
					},
				),
			},
		}
	}
	c, err := client.DialContext(ctx, co)
	return c, namespace, err
}

type zstdcodec struct {
	compress bool // Encode compresses; Decode always decompresses
}

func (c zstdcodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := slices.Clone(payloads)
	if !c.compress {
		return out, nil
	}
	z := common.GetZstdCtxPool().Get()
	defer common.GetZstdCtxPool().Put(z)
	for i, p := range payloads {
		zd, err := z.Compress(nil, p.Data)
		if err != nil {
			return nil, err
		}
		if len(zd)+24 >= len(p.Data) {
			continue
		}
		np := &commonpb.Payload{
			Metadata: maps.Clone(p.Metadata),
			Data:     zd,
		}
		if np.Metadata == nil {
			np.Metadata = make(map[string][]byte)
		}
		np.Metadata["styx/cmp"] = []byte("zst")
		out[i] = np
	}
	return out, nil
}

func (zstdcodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	z := common.GetZstdCtxPool().Get()
	defer common.GetZstdCtxPool().Put(z)
	out := slices.Clone(payloads)
	for i, p := range payloads {
		cmp := string(p.Metadata["styx/cmp"])
		if cmp != "zstd" && cmp != "zst" {
			continue
		}
		d, err := z.Decompress(nil, p.Data)
		if err != nil {
			return nil, err
		}
		np := &commonpb.Payload{
			Metadata: maps.Clone(p.Metadata),
			Data:     d,
		}
		delete(np.Metadata, "styx/cmp")
		out[i] = np
	}
	return out, nil
}
