package manifester

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common"
)

// refreshFakeS3 is just enough S3 for PutIfNotExists: HEAD, PUT and in-place copy.
type refreshFakeS3 struct {
	mu     sync.Mutex
	mod    map[string]time.Time
	puts   []string
	copies []string
	// if set, delete the object before handling a copy of it
	goneOnCopy bool
}

func (f *refreshFakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodHead:
		mod, ok := f.mod[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		f.copies = append(f.copies, key)
		if f.goneOnCopy {
			delete(f.mod, key)
		}
		w.Header().Set("Content-Type", "application/xml")
		if _, ok := f.mod[key]; !ok || r.Header.Get("X-Amz-Copy-Source") != "test-bucket/"+key {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>gone</Message></Error>`)
			return
		}
		now := time.Now()
		f.mod[key] = now
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><LastModified>`+
			now.UTC().Format("2006-01-02T15:04:05.000Z")+`</LastModified><ETag>"x"</ETag></CopyObjectResult>`)
	case r.Method == http.MethodPut:
		f.puts = append(f.puts, key)
		f.mod[key] = time.Now()
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unsupported "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func newRefreshTestStore(t *testing.T, f *refreshFakeS3) *s3ChunkStoreWrite {
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return &s3ChunkStoreWrite{
		bucket: "test-bucket",
		s3client: s3.New(s3.Options{
			Region:                     "us-east-1",
			BaseEndpoint:               aws.String(srv.URL),
			UsePathStyle:               true,
			Credentials:                aws.AnonymousCredentials{},
			RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
			ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
			RetryMaxAttempts:           1,
		}),
		zp:     common.GetZstdCtxPool(),
		zlevel: 1,
	}
}

// Bucket GC skips objects modified within its grace window and rechecks the modification
// time before deleting, so PutIfNotExists must refresh old objects that it reuses.
func TestPutIfNotExistsRefreshesOldObjects(t *testing.T) {
	ctx := context.Background()
	f := &refreshFakeS3{mod: map[string]time.Time{
		"chunk/recent": time.Now().Add(-time.Hour),
		"chunk/old":    time.Now().Add(-RefreshAge - time.Hour),
	}}
	cs := newRefreshTestStore(t, f)

	d, err := cs.PutIfNotExists(ctx, ChunkReadPath, "new", []byte("data"))
	require.NoError(t, err)
	require.NotNil(t, d, "new object should be written")

	d, err = cs.PutIfNotExists(ctx, ChunkReadPath, "recent", []byte("data"))
	require.NoError(t, err)
	require.Nil(t, d, "recent object should be reused")

	d, err = cs.PutIfNotExists(ctx, ChunkReadPath, "old", []byte("data"))
	require.NoError(t, err)
	require.Nil(t, d, "old object should be reused")

	f.mu.Lock()
	defer f.mu.Unlock()
	require.WithinDuration(t, time.Now(), f.mod["chunk/old"], time.Minute, "old object was not refreshed")

	require.Equal(t, []string{"chunk/new"}, f.puts)
	require.Equal(t, []string{"chunk/old"}, f.copies)
}

// If the object is deleted between the head and the refresh (by a GC that condemned it),
// PutIfNotExists writes it again.
func TestPutIfNotExistsRewritesObjectDeletedDuringRefresh(t *testing.T) {
	f := &refreshFakeS3{
		mod:        map[string]time.Time{"manifest/old": time.Now().Add(-RefreshAge - time.Hour)},
		goneOnCopy: true,
	}
	cs := newRefreshTestStore(t, f)

	d, err := cs.PutIfNotExists(context.Background(), ManifestCachePath, "old", []byte("data"))
	require.NoError(t, err)
	require.NotNil(t, d)

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []string{"manifest/old"}, f.copies)
	require.Equal(t, []string{"manifest/old"}, f.puts)
}
