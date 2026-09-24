package manifester

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"time"

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/errgroup"
	"github.com/dnr/styx/pb"
)

const (
	// the daemon never asks for more than this many shards
	maxShards = 40

	// how long shard 0 waits for the other shards' chunks before giving up
	defaultShardWait = 2 * time.Minute
)

// Implemented by chunk stores that can cheaply check whether an object exists. Needed for
// sharded builds.
type chunkStoreHas interface {
	Has(ctx context.Context, path, key string) (bool, error)
}

func (l *localChunkStoreWrite) Has(ctx context.Context, path_, key string) (bool, error) {
	// ignore path! mix chunks and manifests in same directory
	_, err := os.Stat(path.Join(l.dir, key))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (s *s3ChunkStoreWrite) Has(ctx context.Context, path, key string) (bool, error) {
	key = path[1:] + key
	_, err := s.s3client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if IsS3NotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func validShard(total, index int) bool {
	return total >= 0 && total <= maxShards && index >= 0 && index < max(total, 1)
}

// In a sharded build, each shard uploads only its share of the chunks, and shard 0 writes
// the manifest to the cache. waitForOtherShards makes shard 0 wait until the other shards'
// chunks exist, so the cache never has a manifest with missing chunks (e.g. if a caller sent
// only shard 0, or another shard failed).
func (b *ManifestBuilder) waitForOtherShards(ctx context.Context, args *BuildArgs, m *pb.Manifest) error {
	if args.ShardTotal <= 1 {
		return nil
	}
	hs, ok := b.cs.(chunkStoreHas)
	if !ok {
		return errors.New("chunk store can't check for chunks from other shards")
	}

	// same assignment as chunkData
	var missing []cdig.CDig
	seen := make(map[cdig.CDig]struct{})
	i := 0
	for _, e := range m.Entries {
		for _, d := range cdig.FromSliceAlias(e.Digests) {
			if _, ok := seen[d]; !ok && i%args.ShardTotal != args.ShardIndex {
				seen[d] = struct{}{}
				missing = append(missing, d)
			}
			i++
		}
	}

	deadline := time.Now().Add(b.shardWait)
	delay := 100 * time.Millisecond
	for {
		var err error
		if missing, err = b.filterMissing(ctx, hs, missing); err != nil {
			return err
		} else if len(missing) == 0 {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("%d chunks from other shards are still missing after %s", len(missing), b.shardWait)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(delay):
		}
		delay = min(2*delay, 2*time.Second)
	}
}

func (b *ManifestBuilder) filterMissing(ctx context.Context, hs chunkStoreHas, digests []cdig.CDig) ([]cdig.CDig, error) {
	has := make([]bool, len(digests))
	eg := errgroup.WithContext(ctx)
	eg.SetLimit(50)
	for i, d := range digests {
		eg.Go(func() (err error) {
			has[i], err = hs.Has(eg, ChunkReadPath, d.String())
			return
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var out []cdig.CDig
	for i, d := range digests {
		if !has[i] {
			out = append(out, d)
		}
	}
	return out, nil
}
