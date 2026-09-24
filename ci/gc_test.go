package ci

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PhilipTaronQ/styx/common"
	"github.com/PhilipTaronQ/styx/common/cdig"
	"github.com/PhilipTaronQ/styx/manifester"
	"github.com/PhilipTaronQ/styx/pb"
)

// Tests for the bucket GC in gc.go, run against a minimal in-memory S3 fake.

type fakeObj struct {
	data []byte
	mod  time.Time
}

type fakeS3 struct {
	mu       sync.Mutex
	objs     map[string]fakeObj
	pageSize int

	// afterList, if set, is called after a ListObjectsV2 page has been computed and before
	// it is returned, without the lock held. token is "" for the first page.
	afterList func(prefix, token string)
	// failDelete, if set, reports whether a DeleteObjects entry should fail with a per-key
	// error (and the object be kept). Called with the lock held.
	failDelete func(key string) bool
	// loseDeleteResponse, if set, reports whether a DeleteObjects request should do its
	// deletes and then fail with a 500 as if the response were lost. Called with the lock held.
	loseDeleteResponse func(keys []string) bool
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objs: make(map[string]fakeObj), pageSize: 2}
}

func (f *fakeS3) put(key string, data []byte) { f.putAt(key, data, time.Now()) }

// putOld writes an object last modified long before any GC grace window.
func (f *fakeS3) putOld(key string, data []byte) {
	f.putAt(key, data, time.Now().Add(-365*24*time.Hour))
}

func (f *fakeS3) putAt(key string, data []byte, mod time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = fakeObj{data: data, mod: mod}
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[key]
	return ok
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && key == "" && q.Get("list-type") == "2":
		f.serveList(w, q)
	case r.Method == http.MethodPost && key == "" && q.Has("delete"):
		f.serveDelete(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && key != "":
		f.serveGet(w, r, key)
	default:
		http.Error(w, "unsupported "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

type fakeListContents struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
}

type fakeListResult struct {
	XMLName               xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string
	Prefix                string
	KeyCount              int
	MaxKeys               int
	IsTruncated           bool
	ContinuationToken     string `xml:",omitempty"`
	NextContinuationToken string `xml:",omitempty"`
	Contents              []fakeListContents
}

func (f *fakeS3) serveList(w http.ResponseWriter, q url.Values) {
	prefix := q.Get("prefix")
	token := q.Get("continuation-token")
	f.mu.Lock()
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) && k > token {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := fakeListResult{Name: "test-bucket", Prefix: prefix, MaxKeys: f.pageSize, ContinuationToken: token}
	if len(keys) > f.pageSize {
		keys = keys[:f.pageSize]
		res.IsTruncated = true
		res.NextContinuationToken = keys[len(keys)-1]
	}
	for _, k := range keys {
		o := f.objs[k]
		res.Contents = append(res.Contents, fakeListContents{
			Key:          k,
			LastModified: o.mod.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"x"`,
			Size:         int64(len(o.data)),
			StorageClass: "STANDARD",
		})
	}
	res.KeyCount = len(res.Contents)
	f.mu.Unlock()
	if f.afterList != nil {
		f.afterList(prefix, token)
	}
	writeXML(w, http.StatusOK, res)
}

type fakeS3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string
	Message string
	Key     string `xml:",omitempty"`
}

func (f *fakeS3) serveGet(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	o, ok := f.objs[key]
	f.mu.Unlock()
	if !ok {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeXML(w, http.StatusNotFound, fakeS3Error{Code: "NoSuchKey", Message: "The specified key does not exist.", Key: key})
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
	w.Header().Set("Last-Modified", o.mod.UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", `"x"`)
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		w.Write(o.data)
	}
}

type fakeDeleteReq struct {
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
	Quiet bool `xml:"Quiet"`
}

type fakeDeleted struct{ Key string }

type fakeDeleteError struct{ Key, Code, Message string }

type fakeDeleteResult struct {
	XMLName xml.Name          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []fakeDeleted     `xml:"Deleted"`
	Error   []fakeDeleteError `xml:"Error"`
}

func (f *fakeS3) serveDelete(w http.ResponseWriter, r *http.Request) {
	body, err := readS3Body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req fakeDeleteReq
	if err := xml.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var res fakeDeleteResult
	var keys []string
	f.mu.Lock()
	for _, o := range req.Objects {
		keys = append(keys, o.Key)
		if f.failDelete != nil && f.failDelete(o.Key) {
			res.Error = append(res.Error, fakeDeleteError{Key: o.Key, Code: "InternalError", Message: "injected"})
			continue
		}
		delete(f.objs, o.Key)
		if !req.Quiet {
			res.Deleted = append(res.Deleted, fakeDeleted{Key: o.Key})
		}
	}
	lose := f.loseDeleteResponse != nil && f.loseDeleteResponse(keys)
	f.mu.Unlock()
	if lose {
		writeXML(w, http.StatusInternalServerError, fakeS3Error{Code: "InternalError", Message: "injected"})
		return
	}
	writeXML(w, http.StatusOK, res)
}

// readS3Body reads a request body, undoing aws-chunked encoding if the SDK used it.
func readS3Body(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		return b, nil
	}
	var out []byte
	for {
		line, rest, ok := bytes.Cut(b, []byte("\r\n"))
		if !ok {
			return nil, errors.New("bad aws-chunked body")
		}
		sizeStr, _, _ := strings.Cut(string(line), ";")
		n, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
		if int64(len(rest)) < n+2 {
			return nil, errors.New("short aws-chunked body")
		}
		out = append(out, rest[:n]...)
		b = rest[n+2:]
	}
}

func writeXML(w http.ResponseWriter, status int, v any) {
	b, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	w.Write(b)
}

func newTestGC(t *testing.T, f *fakeS3) (*gc, *strings.Builder) {
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cli := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(srv.URL),
		UsePathStyle:               true,
		Credentials:                aws.AnonymousCredentials{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           1,
	})
	sb := &strings.Builder{}
	return &gc{
		now:     time.Now(),
		stage:   func(string) {},
		summary: sb,
		zp:      common.GetZstdCtxPool(),
		s3:      cli,
		bucket:  "test-bucket",
		age:     gcMaxAge,
		grace:   gcGrace,
	}, sb
}

const testStorePath = "/nix/store/00000000000000000000000000000000-test"

func testDigest(i int) cdig.CDig {
	var d cdig.CDig
	d[0] = byte(i)
	d[1] = byte(i >> 8)
	d[23] = 0x5a
	return d
}

func testChunkKey(d cdig.CDig) string { return manifester.ChunkReadPath[1:] + d.String() }

func testManifestKey(cacheKey string) string { return manifester.ManifestCachePath[1:] + cacheKey }

func testRootKey(kind string, tm time.Time) string {
	return manifester.BuildRootPath[1:] + strings.Join([]string{kind, tm.UTC().Format(time.RFC3339), "m", "m"}, "@")
}

// testManifestObj returns a manifest cache object (uncompressed SignedMessage with an
// inline manifest) whose single file references digs.
func testManifestObj(t *testing.T, digs ...cdig.CDig) []byte {
	var all []byte
	for _, d := range digs {
		all = append(all, d[:]...)
	}
	mb, err := proto.Marshal(&pb.Manifest{
		Entries: []*pb.Entry{{
			Path:    "/f",
			Type:    pb.EntryType_REGULAR,
			Size:    int64(len(digs)) << 16,
			Digests: all,
		}},
		Meta: &pb.ManifestMeta{Narinfo: &pb.NarInfo{StorePath: testStorePath}},
	})
	require.NoError(t, err)
	b, err := proto.Marshal(&pb.SignedMessage{Msg: &pb.Entry{
		Path:       "manifest",
		Type:       pb.EntryType_REGULAR,
		Size:       int64(len(mb)),
		InlineData: mb,
	}})
	require.NoError(t, err)
	return b
}

// testChunkedManifestObj returns a manifest cache object whose manifest is stored in the
// chunk store, in one chunk with digest body.
func testChunkedManifestObj(t *testing.T, body cdig.CDig) []byte {
	b, err := proto.Marshal(&pb.SignedMessage{Msg: &pb.Entry{
		Path:    "manifest",
		Type:    pb.EntryType_REGULAR,
		Size:    100,
		Digests: body[:],
	}})
	require.NoError(t, err)
	return b
}

func testRootObj(t *testing.T, cacheKeys ...string) []byte {
	b, err := proto.Marshal(&pb.BuildRoot{
		Meta:     &pb.BuildRootMeta{BuildTime: time.Now().Unix()},
		Manifest: cacheKeys,
	})
	require.NoError(t, err)
	return b
}

// A manifester request that overlaps a GC run writes its chunks, then its manifest cache
// entry, then its build root (manifester/build.go BuildFromNar). GC takes its snapshot of
// build roots first and lists objects later. Here the chunk is uploaded after GC listed the
// roots, and the manifest and root are written after GC listed manifest/, so the manifest
// and root survive; the chunk must too, or every client that gets that manifest from the
// cache fails to read the file.
func TestGCKeepsObjectsWrittenDuringGC(t *testing.T) {
	f := newFakeS3()
	g, _ := newTestGC(t, f)

	// a fresh CI root keeping one manifest and its chunk
	keptDig := testDigest(1)
	keptMan := testManifestKey("v1-kept")
	f.putOld(testChunkKey(keptDig), []byte("kept chunk"))
	f.putOld(keptMan, testManifestObj(t, keptDig))
	f.put(testRootKey("build", g.now.Add(-time.Hour)), testRootObj(t, "v1-kept"))

	// the concurrent manifester request
	newDig := testDigest(2)
	newMan := testManifestKey("v1-new")
	newManData := testManifestObj(t, newDig)
	newRoot := testRootKey("manifest", g.now.Add(time.Second))
	newRootData := testRootObj(t, "v1-new")
	var chunkOnce, manOnce sync.Once
	f.afterList = func(prefix, token string) {
		switch prefix {
		case manifester.BuildRootPath[1:]:
			chunkOnce.Do(func() { f.put(testChunkKey(newDig), []byte("new chunk")) })
		case manifester.ManifestCachePath[1:]:
			manOnce.Do(func() {
				f.put(newMan, newManData)
				f.put(newRoot, newRootData)
			})
		}
	}

	require.NoError(t, g.run(context.Background()))

	require.True(t, f.has(testChunkKey(keptDig)), "chunk referenced by a fresh root was deleted")
	require.True(t, f.has(keptMan), "manifest referenced by a fresh root was deleted")
	require.True(t, f.has(newRoot))
	require.True(t, f.has(newMan))
	require.True(t, f.has(testChunkKey(newDig)),
		"GC deleted a chunk uploaded after it started, while the manifest %s and root %s that reference it survived", newMan, newRoot)
}

// Unreachable objects are kept until they're older than the grace window.
func TestGCKeepsRecentUnreachableObjects(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)
	recent := testChunkKey(testDigest(1))
	old := testChunkKey(testDigest(2))
	f.putAt(recent, []byte("c"), g.now.Add(-gcGrace+time.Hour))
	f.putAt(old, []byte("c"), g.now.Add(-gcGrace-time.Hour))

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.True(t, f.has(recent))
	require.False(t, f.has(old))
}

// A build that overlaps GC can reuse an old cached manifest and old chunks that no root
// reached when GC traced, and write a root for them after GC's first listing of roots.
// GC lists the roots again before removing and keeps what the new roots reach.
func TestGCKeepsObjectsReachedByRootsWrittenDuringGC(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)

	dig := testDigest(1)
	man := testManifestKey("v1-reused")
	f.putOld(testChunkKey(dig), []byte("chunk"))
	f.putOld(man, testManifestObj(t, dig))
	root := testRootKey("manifest", g.now.Add(time.Second))
	rootData := testRootObj(t, "v1-reused")
	var once sync.Once
	f.afterList = func(prefix, token string) {
		if prefix == manifester.ManifestCachePath[1:] {
			once.Do(func() { f.put(root, rootData) })
		}
	}

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.True(t, f.has(root))
	require.True(t, f.has(man), "manifest reached by a root written during GC was deleted")
	require.True(t, f.has(testChunkKey(dig)), "chunk reached by a root written during GC was deleted")
}

// A build that reuses an old chunk refreshes it (manifester.RefreshAge). If that happens
// after GC's last listing of roots, GC can't know about the new referrer, but it rechecks
// each object's modification time just before deleting it.
func TestGCKeepsObjectsRefreshedDuringGC(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)

	dig := testDigest(1)
	chunk := testChunkKey(dig)
	f.putOld(chunk, []byte("chunk"))
	gone := testChunkKey(testDigest(2))
	f.putOld(gone, []byte("chunk"))

	man := testManifestKey("v1-new")
	manData := testManifestObj(t, dig)
	root := testRootKey("manifest", g.now.Add(time.Second))
	rootData := testRootObj(t, "v1-new")
	var rootLists atomic.Int32
	f.afterList = func(prefix, token string) {
		if prefix != manifester.BuildRootPath[1:] || token != "" {
			return
		}
		if rootLists.Add(1) == 2 {
			// the manifester refreshes the chunk it reuses, then writes the manifest and
			// the root, after GC's second listing of roots
			f.put(chunk, []byte("chunk"))
			f.put(man, manData)
			f.put(root, rootData)
		}
	}

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.Equal(t, int32(2), rootLists.Load())
	require.True(t, f.has(chunk), "chunk refreshed during GC was deleted")
	require.False(t, f.has(gone))
}

// GC deletes manifests before chunks. If the manifest's delete fails but its chunks'
// deletes succeed, the manifest stays in the cache without its chunks, and the daemon's
// remanifest recovery gets the same cached manifest back and can't recover.
func TestGCPartialDeleteKeepsManifestChunks(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)

	var digs []cdig.CDig
	for i := range 5 {
		d := testDigest(10 + i)
		digs = append(digs, d)
		f.putOld(testChunkKey(d), []byte("chunk"))
	}
	man := testManifestKey("v1-stale")
	f.putOld(man, testManifestObj(t, digs...))
	staleRoot := testRootKey("build", g.now.Add(-gcMaxAge-24*time.Hour))
	f.putOld(staleRoot, testRootObj(t, "v1-stale"))
	other := testChunkKey(testDigest(99))
	f.putOld(other, []byte("chunk"))

	f.failDelete = func(key string) bool { return key == man }

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())

	require.False(t, f.has(staleRoot))
	require.False(t, f.has(other))
	// invariant: a manifest left in the cache still has all of its chunks
	require.True(t, f.has(man), "test setup: manifest delete should have failed")
	for _, d := range digs {
		require.True(t, f.has(testChunkKey(d)), "manifest %s survived GC but its chunk %s was deleted", man, d)
	}
}

// If a DeleteObjects for manifests fails outright (here it deletes and then loses its
// response), GC doesn't know which manifests are left, so it stops before deleting chunks.
func TestGCLostDeleteResponseKeepsChunks(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)

	dig := testDigest(1)
	chunk := testChunkKey(dig)
	f.putOld(chunk, []byte("chunk"))
	man := testManifestKey("v1-stale")
	f.putOld(man, testManifestObj(t, dig))
	f.loseDeleteResponse = func(keys []string) bool { return slices.Contains(keys, man) }

	require.Error(t, g.run(context.Background()))
	t.Log(sb.String())
	require.False(t, f.has(man))
	require.True(t, f.has(chunk), "GC deleted chunks after a failed manifest delete")
}

// A chunked manifest whose body chunk is missing used to fail the trace and abort GC, every
// week until its root expired 210 days later. Now GC deletes that manifest and carries on.
func TestGCSkipsUnreadableChunkedManifest(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)

	okDig := testDigest(1)
	f.putOld(testChunkKey(okDig), []byte("chunk"))
	f.putOld(testManifestKey("v1-ok"), testManifestObj(t, okDig))
	broken := testManifestKey("v1-broken")
	f.putOld(broken, testChunkedManifestObj(t, testDigest(2))) // body chunk is missing
	garbage := testChunkKey(testDigest(3))
	f.putOld(garbage, []byte("chunk"))
	f.put(testRootKey("build", g.now.Add(-time.Hour)), testRootObj(t, "v1-broken", "v1-ok"))

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.Contains(t, sb.String(), "unreadable manifest")
	require.False(t, f.has(broken), "unreadable manifest was kept")
	require.False(t, f.has(garbage), "GC did not collect")
	require.True(t, f.has(testManifestKey("v1-ok")))
	require.True(t, f.has(testChunkKey(okDig)))
}

func TestGCDryRunDeletesNothing(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)
	g.dryRun = true
	chunk := testChunkKey(testDigest(1))
	f.putOld(chunk, []byte("chunk"))
	man := testManifestKey("v1-stale")
	f.putOld(man, testManifestObj(t, testDigest(1)))

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.Contains(t, sb.String(), "dry run: would delete 1 manifests and narinfos")
	require.Contains(t, sb.String(), "dry run: would delete 1 chunks, nars and other files")
	require.True(t, f.has(chunk))
	require.True(t, f.has(man))
}

func TestGCLocalRequiresBucket(t *testing.T) {
	require.ErrorContains(t, GCLocal(context.Background(), GCConfig{MaxAge: gcMaxAge}), "bucket is required")
}

// remove() runs DeleteObjects batches concurrently and counts the per-key errors of each.
// Run with -race.
func TestGCRemoveConcurrentDeleteErrors(t *testing.T) {
	f := newFakeS3()
	f.pageSize = 1000
	g, sb := newTestGC(t, f)
	const n = 3000
	for i := range n {
		f.putOld(testChunkKey(testDigest(1000+i)), []byte("c"))
	}
	f.failDelete = func(string) bool { return true }

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.Contains(t, sb.String(), fmt.Sprintf("delete errors: %d", n))
}

// list() runs its listings concurrently and each may call gc.logln, which writes to the
// shared summary. Run with -race.
func TestGCListConcurrentUnexpectedFiles(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)
	f.putOld("nixcache/unexpected", []byte("x"))
	// one-character names are not valid base64 digests, so each is "unexpected"
	for _, c := range "ABCDEFGH" {
		f.putOld(manifester.ChunkReadPath[1:]+string(c), []byte("x"))
	}

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
}

// A chunk key that is valid base64 but shorter than a digest used to panic in
// cdig.FromBytes, crashing the heavy worker on every GC.
func TestGCShortChunkKey(t *testing.T) {
	f := newFakeS3()
	g, _ := newTestGC(t, f)
	short := manifester.ChunkReadPath[1:] + "AAAA" // 3 bytes
	long := manifester.ChunkReadPath[1:] + testDigest(1).String() + "AAAA"
	f.putOld(short, []byte("x"))
	f.putOld(long, []byte("x"))

	require.NoError(t, g.run(context.Background()))
	require.False(t, f.has(short))
	require.False(t, f.has(long))
}

// Sanity check of the fake: pagination and a normal collection.
func TestGCFakeBasics(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)
	var keep, drop []cdig.CDig
	for i := range 7 {
		keep = append(keep, testDigest(100+i))
		drop = append(drop, testDigest(200+i))
	}
	for _, d := range append(append([]cdig.CDig{}, keep...), drop...) {
		f.putOld(testChunkKey(d), []byte("c"))
	}
	f.putOld(testManifestKey("v1-keep"), testManifestObj(t, keep...))
	f.putOld(testManifestKey("v1-drop"), testManifestObj(t, drop...))
	f.put(testRootKey("build", g.now.Add(-time.Hour)), testRootObj(t, "v1-keep"))

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())

	require.True(t, f.has(testManifestKey("v1-keep")))
	require.False(t, f.has(testManifestKey("v1-drop")))
	for _, d := range keep {
		require.True(t, f.has(testChunkKey(d)), "kept chunk "+d.String())
	}
	for _, d := range drop {
		require.False(t, f.has(testChunkKey(d)), "dropped chunk "+d.String())
	}
}

// A sharded build writes an empty completion marker under manifest/ for each shard. Nothing
// refers to one, so GC deletes it once it's past the grace window, and it must never be
// traced as a manifest: if its delete failed, tracing it as one failed the whole GC run.
func TestGCShardMarkers(t *testing.T) {
	f := newFakeS3()
	g, sb := newTestGC(t, f)
	recent := testManifestKey("v1-recent.shard-0-of-2.layout")
	old := testManifestKey("v1-old.shard-1-of-2.layout")
	stuck := testManifestKey("v1-stuck.shard-0-of-3.layout")
	f.putAt(recent, nil, g.now.Add(-gcGrace+time.Hour))
	f.putAt(old, nil, g.now.Add(-gcGrace-time.Hour))
	f.putAt(stuck, nil, g.now.Add(-gcGrace-time.Hour))
	f.failDelete = func(key string) bool { return key == stuck }

	require.NoError(t, g.run(context.Background()))
	t.Log(sb.String())
	require.True(t, f.has(recent), "a marker within the grace window was deleted")
	require.False(t, f.has(old), "an old marker was kept")
	require.True(t, f.has(stuck), "test setup: the marker's delete should have failed")
	for _, key := range []string{recent, old, stuck} {
		phase, _, known := g.classify(key)
		require.True(t, known)
		require.Equal(t, phaseLeaves, phase, "%s isn't deleted with the leaves", key)
	}
}
