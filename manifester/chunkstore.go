package manifester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/DataDog/zstd"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/shift"
)

const (
	// Signed manifest envelopes are small: manifests over SmallManifestCutoff are chunked,
	// so the envelope holds only their digests and metadata. This is a generous limit.
	MaxEnvelopeBytes = 16 << 20

	// Each attempt at reading a chunk or manifest envelope gets this long.
	readAttemptTimeout = time.Minute
)

// RefreshAge is how old an existing object must be for PutIfNotExists to refresh its
// modification time instead of just reusing it. Bucket GC (ci/gc.go) never deletes an object
// modified within twice this long, and rechecks the modification time just before deleting,
// so a build that reuses an object a running GC has condemned keeps it alive.
const RefreshAge = 7 * 24 * time.Hour

type (
	ChunkStoreWrite interface {
		PutIfNotExists(ctx context.Context, path, key string, data []byte) ([]byte, error)
		Get(ctx context.Context, path, key string, dst []byte) ([]byte, error)
	}

	ChunkStoreRead interface {
		// Data will be appended to dst and returned.
		// If dst is given, it must be big enough to hold the full chunk!
		Get(ctx context.Context, key string, dst []byte) ([]byte, error)
	}

	ChunkStoreWriteConfig struct {
		// One of these is required:
		ChunkBucket      string
		ChunkLocalDir    string
		ZstdEncoderLevel int
	}

	localChunkStoreWrite struct {
		dir string
		zp  *common.ZstdCtxPool
	}

	s3ChunkStoreWrite struct {
		bucket   string
		s3client *s3.Client
		zp       *common.ZstdCtxPool
		zlevel   int
	}

	urlChunkStoreRead struct {
		url     string
		zp      *common.ZstdCtxPool
		maxSize int64 // of the data
		maxBody int64 // of the response body
	}
)

func newLocalChunkStoreWrite(dir string) (*localChunkStoreWrite, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &localChunkStoreWrite{dir: dir, zp: common.GetZstdCtxPool()}, nil
}

func (l *localChunkStoreWrite) PutIfNotExists(ctx context.Context, path_, key string, data []byte) ([]byte, error) {
	// ignore path! mix chunks and manifests in same directory
	z := l.zp.Get()
	defer l.zp.Put(z)
	fn := path.Join(l.dir, key)
	if _, err := os.Stat(fn); err == nil {
		return nil, nil
	} else if d, err := z.CompressLevel(nil, data, 1); err != nil {
		return nil, err
	} else if out, err := os.CreateTemp(l.dir, key+".tmp*"); err != nil {
		return nil, err
	} else if n, err := out.Write(d); err != nil || n != len(d) {
		_ = out.Close()
		_ = os.Remove(out.Name())
		return nil, err
	} else if err := out.Close(); err != nil {
		_ = os.Remove(out.Name())
		return nil, err
	} else if err := os.Rename(out.Name(), fn); err != nil {
		_ = os.Remove(out.Name())
		return nil, err
	} else {
		return d, nil
	}
}

func (l *localChunkStoreWrite) Get(ctx context.Context, path_, key string, data []byte) ([]byte, error) {
	// ignore path! mix chunks and manifests in same directory
	b, err := os.ReadFile(path.Join(l.dir, key))
	if err != nil {
		return nil, wrapNotFound(err)
	}
	z := l.zp.Get()
	defer l.zp.Put(z)
	return z.Decompress(data, b)
}

func newS3ChunkStoreWrite(bucket string, zlevel int) (*s3ChunkStoreWrite, error) {
	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithEC2IMDSRegion())
	if err != nil {
		return nil, err
	}
	s3client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.EndpointOptions.DisableHTTPS = true
		o.RetryMaxAttempts = 15
	})
	return &s3ChunkStoreWrite{
		bucket:   bucket,
		s3client: s3client,
		zp:       common.GetZstdCtxPool(),
		zlevel:   zlevel,
	}, nil
}

func (s *s3ChunkStoreWrite) PutIfNotExists(ctx context.Context, path, key string, data []byte) ([]byte, error) {
	if path != ChunkReadPath && path != ManifestCachePath && path != BuildRootPath {
		panic("path must be ChunkReadPath or ManifestCachePath")
	}
	key = path[1:] + key
	head, err := s.s3client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err == nil {
		if time.Since(aws.ToTime(head.LastModified)) < RefreshAge {
			return nil, nil
		} else if exists, err := s.touch(ctx, key); err != nil || exists {
			return nil, err
		}
		// deleted since the head: write it again
	} else if !IsS3NotFound(err) {
		return nil, err
	}
	z := s.zp.Get()
	defer s.zp.Put(z)
	// TODO: use buffer pool here (requires caller to return it?)
	d, err := z.CompressLevel(nil, data, s.zlevel)
	if err != nil {
		return nil, err
	}
	_, err = s.s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          &s.bucket,
		Key:             &key,
		Body:            bytes.NewReader(d),
		CacheControl:    aws.String("public, max-age=31536000"),
		ContentType:     aws.String("application/octet-stream"),
		ContentEncoding: aws.String("zstd"),
	})
	return d, err
}

// touch updates key's modification time by copying it onto itself, and reports whether key
// still exists. (Our keys need no URL escaping in CopySource.)
func (s *s3ChunkStoreWrite) touch(ctx context.Context, key string) (bool, error) {
	_, err := s.s3client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            &s.bucket,
		Key:               &key,
		CopySource:        aws.String(s.bucket + "/" + key),
		MetadataDirective: s3types.MetadataDirectiveReplace,
		CacheControl:      aws.String("public, max-age=31536000"),
		ContentType:       aws.String("application/octet-stream"),
		ContentEncoding:   aws.String("zstd"),
	})
	// CopyObject doesn't model NoSuchKey, so check the code
	var ae interface{ ErrorCode() string }
	if errors.As(err, &ae) && ae.ErrorCode() == "NoSuchKey" {
		return false, nil
	}
	return err == nil, err
}

func (s *s3ChunkStoreWrite) Get(ctx context.Context, path, key string, data []byte) ([]byte, error) {
	key = path[1:] + key
	res, err := s.s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("get(%q): %w", key, wrapNotFound(err))
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read(%q): %w", key, err)
	}
	z := s.zp.Get()
	defer s.zp.Put(z)
	return z.Decompress(data, b)
}

func NewChunkStoreWrite(cfg ChunkStoreWriteConfig) (ChunkStoreWrite, error) {
	if len(cfg.ChunkLocalDir) > 0 {
		return newLocalChunkStoreWrite(cfg.ChunkLocalDir)
	} else if len(cfg.ChunkBucket) > 0 {
		return newS3ChunkStoreWrite(cfg.ChunkBucket, cfg.ZstdEncoderLevel)
	}
	return nil, errors.New("chunk store configuration is missing")
}

// path should be either ChunkReadPath or ManifestCachePath
func NewChunkStoreReadUrl(url, path string) ChunkStoreRead {
	if path != ChunkReadPath && path != ManifestCachePath {
		panic("path must be ChunkReadPath or ManifestCachePath")
	}
	maxSize := shift.MaxChunkShift.Size()
	if path == ManifestCachePath {
		maxSize = MaxEnvelopeBytes
	}
	return &urlChunkStoreRead{
		url:     strings.TrimSuffix(url, "/") + path,
		zp:      common.GetZstdCtxPool(),
		maxSize: maxSize,
		maxBody: int64(zstd.CompressBound(int(maxSize))),
	}
}

func (s *urlChunkStoreRead) Get(ctx context.Context, key string, dst []byte) ([]byte, error) {
	b, hdr, err := common.RetryHttpRequestBody(ctx, http.MethodGet, s.url+key, "", nil, s.maxBody, readAttemptTimeout)
	if err != nil {
		return nil, err
	}
	// the caller checks the digest only after this, so don't trust the body to decompress to
	// a reasonable size
	if hdr.Get("Content-Encoding") == "zstd" {
		if dst == nil {
			return common.DecompressLimit(b, s.maxSize)
		} else {
			// fast path, assume buffer is big enough (this can't write past its capacity)
			z := s.zp.Get()
			defer s.zp.Put(z)
			n, err := z.DecompressInto(dst[len(dst):cap(dst)], b)
			if err != nil {
				return nil, err
			}
			return dst[:len(dst)+n], nil
		}
	} else if int64(len(b)) > s.maxSize {
		return nil, fmt.Errorf("%w: %d bytes", common.ErrTooLarge, len(b))
	} else if dst == nil {
		return b, nil
	} else {
		return append(dst, b...), nil
	}
}

type wrapNotFoundErr struct{ error }

var _ common.NotFoundable = wrapNotFoundErr{}

func (wrapNotFoundErr) IsNotFound() bool { return true }

func wrapNotFound(err error) error {
	if errors.Is(err, fs.ErrNotExist) || IsS3NotFound(err) {
		return wrapNotFoundErr{err}
	}
	return err
}

func IsS3NotFound(err error) bool {
	// the S3 sdk is inconsistent about these, just check both
	var nsk *s3types.NoSuchKey
	var nf *s3types.NotFound
	return errors.As(err, &nsk) || errors.As(err, &nf)
}
