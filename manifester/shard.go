package manifester

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dnr/styx/common/errgroup"
	"github.com/dnr/styx/pb"
)

// Sharded builds
//
// Each shard of a sharded build reads the whole nar and builds the whole manifest, but uploads
// only its share of the chunks (see chunkData). The manifest may only be cached once every
// share is uploaded, and the shards are independent, stateless requests: any of them can
// fail, be refused (a 429 from Lambda), be retried or never be sent at all. So they
// coordinate through the chunk store. When a shard has uploaded its share, it writes a
// completion marker, then looks for the other shards' markers. A shard that finds all of them
// writes the manifest to the cache and returns it; the others return without it.
//
// A shard writes its marker only after all its chunks are uploaded, and S3 is strongly
// consistent, so the shard whose marker lands last sees every other marker: once every shard
// has succeeded, at least one of them has cached the manifest (two that finish together may
// both do it, which is harmless). If any shard fails or never runs, its marker is missing, so
// no shard finds a full set and nothing is cached. A retry repeats the same steps: markers are
// plain overwrites, and chunk and cache writes are PutIfNotExists.
//
// Marker keys name the manifest cache key, the shard count and index, and a hash of the
// manifest's digest sequence, which determines exactly which chunks each shard uploaded. So
// shards that built different manifests (a tarball ref that moved between shards, a deploy
// that changed chunking) never complete each other.
//
// Markers are empty objects in ManifestCachePath, named "<cache key>.shard-<i>-of-<n>.<hash>".
// No cache key or chunk digest contains a ".". Bucket GC (ci/gc.go) recognises them with
// IsShardMarker: nothing refers to them, so it deletes them with the leaves once they're
// older than its grace window, and never reads one as a manifest.
// A marker vouches for its chunks only while they can't have been garbage-collected, so
// markers older than shardMarkerMaxAge are ignored: every shard of a request runs within
// that, and it's far shorter than GC's grace window.

const (
	// the daemon never asks for more than this many shards
	maxShards = 40

	shardMarkerMaxAge = time.Hour
)

// Implemented by chunk stores that can hold shard completion markers. Needed for sharded
// builds.
type shardMarkerStore interface {
	// PutShardMarker writes an empty marker object, replacing any that's there.
	PutShardMarker(ctx context.Context, key string) error
	// ShardMarkerTime returns when a marker was last written, or ok = false if it doesn't
	// exist.
	ShardMarkerTime(ctx context.Context, key string) (t time.Time, ok bool, err error)
}

func (l *localChunkStoreWrite) PutShardMarker(ctx context.Context, key string) error {
	// same directory as chunks and manifests (see PutIfNotExists)
	fn := path.Join(l.dir, key)
	if err := os.WriteFile(fn, nil, 0o644); err != nil {
		return err
	}
	// truncating an empty file may not update its modification time
	now := time.Now()
	return os.Chtimes(fn, now, now)
}

func (l *localChunkStoreWrite) ShardMarkerTime(ctx context.Context, key string) (time.Time, bool, error) {
	fi, err := os.Stat(path.Join(l.dir, key))
	if errors.Is(err, fs.ErrNotExist) {
		return time.Time{}, false, nil
	} else if err != nil {
		return time.Time{}, false, err
	}
	return fi.ModTime(), true, nil
}

func (s *s3ChunkStoreWrite) PutShardMarker(ctx context.Context, key string) error {
	key = ManifestCachePath[1:] + key
	_, err := s.s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(nil),
		ContentType: aws.String("application/octet-stream"),
	})
	return err
}

func (s *s3ChunkStoreWrite) ShardMarkerTime(ctx context.Context, key string) (time.Time, bool, error) {
	key = ManifestCachePath[1:] + key
	res, err := s.s3client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if IsS3NotFound(err) {
		return time.Time{}, false, nil
	} else if err != nil {
		return time.Time{}, false, err
	}
	return aws.ToTime(res.LastModified), true, nil
}

func validShard(total, index int) bool {
	return total >= 0 && total <= maxShards && index >= 0 && index < max(total, 1)
}

// IsShardMarker reports whether key (relative to ManifestCachePath) is a shard completion
// marker rather than a manifest.
func IsShardMarker(key string) bool {
	return strings.Contains(key, ".shard-")
}

func shardMarkerKey(cacheKey, layout string, index, total int) string {
	return fmt.Sprintf("%s.shard-%d-of-%d.%s", cacheKey, index, total, layout)
}

// shardLayout identifies the chunks of m and their order, which determines each shard's share.
func shardLayout(m *pb.Manifest) string {
	h := sha256.New()
	h.Write([]byte("styx-shard-layout\n"))
	for _, e := range m.Entries {
		h.Write(e.Digests)
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:18])
}

// shardsDone is called by each shard once its share of m's chunks is uploaded. It records
// that, and reports whether every shard of the build has now done the same, in which case
// this shard should cache the manifest. Unsharded builds are always done.
func (b *ManifestBuilder) shardsDone(ctx context.Context, args *BuildArgs, cacheKey string, m *pb.Manifest) (bool, error) {
	if args.ShardTotal <= 1 {
		return true, nil
	}
	ms, ok := b.cs.(shardMarkerStore)
	if !ok {
		return false, errors.New("chunk store can't hold shard markers")
	}

	layout := shardLayout(m)
	if err := ms.PutShardMarker(ctx, shardMarkerKey(cacheKey, layout, args.ShardIndex, args.ShardTotal)); err != nil {
		return false, fmt.Errorf("shard marker write error: %w", err)
	}

	var notDone atomic.Int32
	eg := errgroup.WithContext(ctx)
	for i := range args.ShardTotal {
		if i == args.ShardIndex {
			continue
		}
		eg.Go(func() error {
			t, ok, err := ms.ShardMarkerTime(eg, shardMarkerKey(cacheKey, layout, i, args.ShardTotal))
			if err != nil {
				return err
			} else if !ok || time.Since(t) > shardMarkerMaxAge {
				notDone.Add(1)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return false, fmt.Errorf("shard marker read error: %w", err)
	}
	if n := notDone.Load(); n > 0 {
		log.Printf("shard %d/%d of %s done, waiting for %d more", args.ShardIndex, args.ShardTotal, cacheKey, n)
		return false, nil
	}
	log.Printf("shard %d/%d of %s done, all shards done", args.ShardIndex, args.ShardTotal, cacheKey)
	return true, nil
}
