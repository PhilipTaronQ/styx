package manifester

// Tests written during a bug review. Each one demonstrates a finding and is expected to FAIL
// on the unfixed code.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/nar"
	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/nix-community/go-nix/pkg/nixbase32"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

// ---- fixtures ----

type reviewFile struct {
	name string // absolute nar path, e.g. "/big"; must be given in nar order
	size int
}

func reviewFill(b []byte, seed uint64) {
	x := seed | 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = byte(x)
	}
}

func reviewSeed(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(h[:8])
}

func reviewSph(s string) string {
	h := sha256.Sum256([]byte("sph:" + s))
	return nixbase32.EncodeToString(h[:20])
}

// nar with a root directory containing regular files filled with pseudo-random data
func reviewNar(t *testing.T, seed string, files []reviewFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	require.NoError(t, err)
	require.NoError(t, nw.WriteHeader(&nar.Header{Path: "/", Type: nar.TypeDirectory}))
	for i, f := range files {
		require.NoError(t, nw.WriteHeader(&nar.Header{Path: f.name, Type: nar.TypeRegular, Size: int64(f.size)}))
		data := make([]byte, f.size)
		reviewFill(data, reviewSeed(seed)+uint64(i))
		_, err := nw.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, nw.Close())
	return buf.Bytes()
}

type reviewNarinfoOpts struct {
	noFileHash bool
	noNarHash  bool
	noSig      bool
	refs       []string
}

func reviewNarinfo(t *testing.T, sk signature.SecretKey, sph, name string, narData []byte, o reviewNarinfoOpts) string {
	t.Helper()
	sum := sha256.Sum256(narData)
	nh := "sha256:" + nixbase32.EncodeToString(sum[:])
	var sb strings.Builder
	fmt.Fprintf(&sb, "StorePath: /nix/store/%s-%s\n", sph, name)
	fmt.Fprintf(&sb, "URL: nar/%s.nar\n", sph)
	fmt.Fprintf(&sb, "Compression: none\n")
	if !o.noFileHash {
		fmt.Fprintf(&sb, "FileHash: %s\nFileSize: %d\n", nh, len(narData))
	}
	if !o.noNarHash {
		fmt.Fprintf(&sb, "NarHash: %s\n", nh)
	}
	fmt.Fprintf(&sb, "NarSize: %d\n", len(narData))
	fmt.Fprintf(&sb, "References: %s\n", strings.Join(o.refs, " "))
	text := sb.String()
	if !o.noSig && !o.noNarHash {
		ni, err := narinfo.Parse(strings.NewReader(text))
		require.NoError(t, err)
		sig, err := sk.Sign(nil, ni.Fingerprint())
		require.NoError(t, err)
		text += "Sig: " + sig.String() + "\n"
	}
	return text
}

// a fake binary cache
type reviewUpstream struct {
	ts    *httptest.Server
	mu    sync.Mutex
	files map[string][]byte
}

func newReviewUpstream(t *testing.T) *reviewUpstream {
	u := &reviewUpstream{files: make(map[string][]byte)}
	u.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		body, ok := u.files[r.URL.Path]
		u.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(u.ts.Close)
	return u
}

func (u *reviewUpstream) set(p string, b []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[p] = b
}

func (u *reviewUpstream) url() string { return u.ts.URL + "/" }

func (u *reviewUpstream) host() string {
	pu, _ := url.Parse(u.ts.URL)
	return pu.Host
}

func (u *reviewUpstream) addPath(t *testing.T, sk signature.SecretKey, seed string, files []reviewFile, o reviewNarinfoOpts) string {
	t.Helper()
	sph := reviewSph(seed)
	narData := reviewNar(t, seed, files)
	u.set("/"+sph+".narinfo", []byte(reviewNarinfo(t, sk, sph, "review-"+seed, narData, o)))
	u.set("/nar/"+sph+".nar", narData)
	return sph
}

func reviewKeys(t *testing.T) (signature.SecretKey, signature.PublicKey) {
	sk, pk, err := signature.GenerateKeypair("upstream-review-1", rand.Reader)
	require.NoError(t, err)
	return sk, pk
}

func newReviewBuilder(t *testing.T, cs ChunkStoreWrite, pk signature.PublicKey, concurrent int) *ManifestBuilder {
	t.Helper()
	styxSk, _, err := signature.GenerateKeypair("styx-review-1", rand.Reader)
	require.NoError(t, err)
	mb, err := NewManifestBuilder(ManifestBuilderConfig{
		ConcurrentChunkOps: concurrent,
		PublicKeys:         []signature.PublicKey{pk},
		SigningKeys:        []signature.SecretKey{styxSk},
	}, cs)
	require.NoError(t, err)
	return mb
}

func reviewHas(cs *mockChunkStore, ns, key string) bool {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	_, ok := cs.data[ns+"/"+key]
	return ok
}

func reviewKeysWithPrefix(cs *mockChunkStore, prefix string) []string {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	var out []string
	for k := range cs.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func reviewCachedManifest(t *testing.T, cs *mockChunkStore, cacheKey string) *pb.Manifest {
	t.Helper()
	cs.lock.Lock()
	sb, ok := cs.data[ManifestCachePath+"/"+cacheKey]
	cs.lock.Unlock()
	require.True(t, ok, "manifest not in cache")
	var sm pb.SignedMessage
	require.NoError(t, proto.Unmarshal(sb, &sm))
	require.NotEmpty(t, sm.Msg.GetInlineData(), "test expects a small inline manifest")
	var m pb.Manifest
	require.NoError(t, proto.Unmarshal(sm.Msg.InlineData, &m))
	return &m
}

func reviewCall(f func() error) (panicked any, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	return nil, f()
}

// number of goroutines whose stack mentions substr
func reviewGoroutines(substr string) int {
	buf := make([]byte, 32<<20)
	n := runtime.Stack(buf, true)
	c := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, substr) {
			c++
		}
	}
	return c
}

func reviewWaitGoroutines(substr string, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	for {
		c := reviewGoroutines(substr)
		if c <= want || time.Now().After(deadline) {
			return c
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type reviewFailingStore struct{ *mockChunkStore }

func (f reviewFailingStore) PutIfNotExists(ctx context.Context, p, k string, d []byte) ([]byte, error) {
	if p == ChunkReadPath {
		return nil, errors.New("injected chunk put failure")
	}
	return f.mockChunkStore.PutIfNotExists(ctx, p, k, d)
}

// ---- findings ----

// An unauthenticated caller can send only shard 0 of N. Shard 0 writes the manifest to the
// shared cache after uploading only its own 1/N of the chunks. Clients then get the cached
// manifest and 404 on the rest (and remanifesting hits the same cache entry).
func TestReviewShardZeroAloneCachesManifestWithMissingChunks(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	sph := up.addPath(t, sk, "shard", []reviewFile{{"/big", 4<<16 + 1000}}, reviewNarinfoOpts{})

	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)
	srv, err := NewManifestServer(Config{AllowedUpstreams: []string{up.host()}, ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	body, err := json.Marshal(ManifestReq{
		Upstream:      up.url(),
		StorePathHash: sph,
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
		ShardTotal:    4,
		ShardIndex:    0,
	})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	cacheKey := (&ManifestReq{
		Upstream:      up.url(),
		StorePathHash: sph,
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	}).CacheKey()
	m := reviewCachedManifest(t, cs, cacheKey)
	var total, missing int
	for _, e := range m.Entries {
		for _, d := range cdig.FromSliceAlias(e.Digests) {
			total++
			if !reviewHas(cs, ChunkReadPath, d.String()) {
				missing++
			}
		}
	}
	require.Positive(t, total)
	assert.Zero(t, missing,
		"manifest was written to the shared cache while %d of %d referenced chunks are absent", missing, total)
}

// Build roots are keyed "manifest@<RFC3339 seconds>@m@m" and written with PutIfNotExists, so
// all but one of the manifests built in the same second lose their GC root.
func TestReviewBuildRootsCollideWithinOneSecond(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)

	const n = 3
	for i := range n {
		sph := up.addPath(t, sk, fmt.Sprintf("root%d", i), []reviewFile{{"/f", 1000 + i}}, reviewNarinfoOpts{})
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", true)
		require.NoError(t, err)
	}
	roots := reviewKeysWithPrefix(cs, BuildRootPath+"/")
	assert.Len(t, roots, n, "each on-demand manifest should have its own GC root; got %v", roots)
}

// narinfo without NarHash: ni.Fingerprint() dereferences a nil hash before the signature is
// checked. lambdaurl runs the handler in a goroutine without recover, so this kills the process.
func TestReviewNarinfoWithoutNarHashPanics(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	sph := up.addPath(t, sk, "nonarhash", []reviewFile{{"/f", 1000}}, reviewNarinfoOpts{noNarHash: true})
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)

	p, err := reviewCall(func() error {
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
		return err
	})
	assert.Nil(t, p, "unauthenticated narinfo made BuildFromNar panic before signature verification")
	assert.Error(t, err)
}

// A correctly signed narinfo without FileHash (FileHash is not part of the fingerprint, and
// some caches omit it) makes BuildFromNar panic after the nar was fully processed.
func TestReviewSignedNarinfoWithoutFileHashPanics(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	sph := up.addPath(t, sk, "nofilehash", []reviewFile{{"/f", 1000}}, reviewNarinfoOpts{noFileHash: true})
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)

	p, err := reviewCall(func() error {
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
		return err
	})
	assert.Nil(t, p, "signed narinfo without FileHash made BuildFromNar panic")
	assert.NoError(t, err)
}

// Upstream 404 for the narinfo: RetryHttpRequest turns it into an error wrapped with ErrReq, the
// StatusNotFound branch in BuildFromNar is unreachable, and writeError answers 417.
func TestReviewNarinfo404IsReportedAs417(t *testing.T) {
	_, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)
	srv, err := NewManifestServer(Config{AllowedUpstreams: []string{up.host()}, ChunkDiffParallel: 4}, mb)
	require.NoError(t, err)

	body, err := json.Marshal(ManifestReq{
		Upstream:      up.url(),
		StorePathHash: reviewSph("absent"),
		DigestAlgo:    cdig.Algo,
		DigestBits:    int(cdig.Bits),
	})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.handleManifest(rec, httptest.NewRequest(http.MethodPost, ManifestPath, bytes.NewReader(body)))
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}

// Unbounded narinfo input: go-nix NarInfo.Fingerprint builds the reference list with repeated
// string +=, which is quadratic, and it runs before the signature check.
func TestReviewNarinfoManyReferencesIsQuadratic(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	sph := reviewSph("refs")
	narData := reviewNar(t, "refs", []reviewFile{{"/f", 1000}})
	refs := make([]string, 80000)
	for i := range refs {
		refs[i] = "a"
	}
	ni := reviewNarinfo(t, sk, sph, "refs", narData, reviewNarinfoOpts{noSig: true, refs: refs})
	t.Logf("narinfo is %d bytes", len(ni))
	up.set("/"+sph+".narinfo", []byte(ni))
	up.set("/nar/"+sph+".nar", narData)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newReviewBuilder(t, cs, pk, 0)

	start := time.Now()
	_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	elapsed := time.Since(start)
	require.Error(t, err) // unsigned
	assert.Less(t, elapsed, time.Second,
		"a %d byte unsigned narinfo took %s to reject", len(ni), elapsed)
}

// Tarball entries named "../x" survive path.Clean, sort before "/x", and writeNar turns the
// pair into a nar entry named "." (go-nix accepts it). The nar reader then yields "/x" twice,
// and the signed manifest has duplicate paths.
func TestReviewTarballDotDotEntryDuplicatesPath(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "../x/", Typeflag: tar.TypeDir, Mode: 0o755}))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "x/", Typeflag: tar.TypeDir, Mode: 0o755}))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "x/a", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5}))
	_, err := tw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	up := newReviewUpstream(t)
	up.set("/t.tar", buf.Bytes())
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)

	res, err := mb.BuildFromTarball(context.Background(), up.ts.URL+"/t.tar", 0, 0, "", false)
	require.NoError(t, err)
	m := reviewCachedManifest(t, cs, res.CacheKey)
	counts := map[string]int{}
	var paths []string
	for _, e := range m.Entries {
		counts[e.Path]++
		paths = append(paths, e.Path)
	}
	for p, c := range counts {
		assert.Equal(t, 1, c, "path %q appears %d times in a signed manifest (entries %q)", p, c, paths)
	}
}

// The chunkdiff protocol documents a max of 256 digests per side, but the server doesn't
// enforce it (nor ChunkDiffMaxBytes, nor the number of Req entries, nor repeats).
func TestReviewChunkDiffDigestLimitNotEnforced(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 8, ChunkDiffZstdLevel: 3}, mb)
	require.NoError(t, err)

	var reqs []byte
	for i := range ChunkDiffMaxDigests + 44 {
		c := []byte(fmt.Sprintf("review chunk %06d payload", i))
		d := cdig.Sum(c)
		_, err := cs.PutIfNotExists(context.Background(), ChunkReadPath, d.String(), c)
		require.NoError(t, err)
		reqs = append(reqs, d[:]...)
	}
	r := &pb.ManifesterChunkDiffReq{
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		Req:    []*pb.ManifesterChunkDiffReq_Req{{Bases: reqs[:cdig.Bytes], Reqs: reqs}},
	}
	body, err := proto.Marshal(r)
	require.NoError(t, err)
	hr := httptest.NewRequest(http.MethodPost, ChunkDiffPath, bytes.NewReader(body))
	hr.Header.Set(common.CTHdr, common.CTProto)
	rec := httptest.NewRecorder()
	srv.handleChunkDiff(rec, hr)
	assert.GreaterOrEqual(t, rec.Code, 400,
		"request with %d req digests (documented max %d) was served", len(reqs)/cdig.Bytes, ChunkDiffMaxDigests)
}

// ExpandGz: if the gzip stream is corrupt after the header, io.ReadAll(gzr) returns but the pipe
// reader is never closed. The chunk-series consumer stays blocked in pw.Write forever (no ctx
// case), and its producer/fetchers stay blocked on channels, holding chunk buffers and
// ChunkDiffParallel slots.
func TestReviewChunkDiffGzipErrorLeaksFetchGoroutines(t *testing.T) {
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{}, cs)
	require.NoError(t, err)
	srv, err := NewManifestServer(Config{ChunkDiffParallel: 4, ChunkDiffZstdLevel: 3}, mb)
	require.NoError(t, err)

	var bases []byte
	put := func(c []byte) {
		d := cdig.Sum(c)
		_, err := cs.PutIfNotExists(context.Background(), ChunkReadPath, d.String(), c)
		require.NoError(t, err)
		bases = append(bases, d[:]...)
	}
	// gzip header, then a deflate block with the reserved type 3: NewReader succeeds, Read fails
	put([]byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 0x07})
	for i := range 12 {
		put(bytes.Repeat([]byte{byte('a' + i)}, 1024))
	}
	r := &pb.ManifesterChunkDiffReq{
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
		Req:    []*pb.ManifesterChunkDiffReq_Req{{Bases: bases, ExpandBeforeDiff: ExpandGz}},
	}
	body, err := proto.Marshal(r)
	require.NoError(t, err)

	const marker = "manifester.(*server).fetchChunkSeries"
	before := reviewGoroutines(marker)

	ctx, cancel := context.WithCancel(context.Background())
	hr := httptest.NewRequest(http.MethodPost, ChunkDiffPath, bytes.NewReader(body)).WithContext(ctx)
	hr.Header.Set(common.CTHdr, common.CTProto)
	rec := httptest.NewRecorder()
	srv.handleChunkDiff(rec, hr)
	require.GreaterOrEqual(t, rec.Code, 400)
	cancel() // the request is over; net/http would cancel it here too

	after := reviewWaitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before,
		"%d chunk-series goroutines still blocked after the request finished and its context was cancelled", after-before)
}

// When a chunk upload fails mid-nar, buildFromNar returns without calling nar.Reader.Close, so
// go-nix's parser goroutine stays parked forever (Lambda reuses the process across invocations).
func TestReviewFailedNarBuildLeaksNarReaderGoroutine(t *testing.T) {
	sk, pk := reviewKeys(t)
	up := newReviewUpstream(t)
	sph := up.addPath(t, sk, "leak", []reviewFile{{"/big", 64 << 16}, {"/z", 1000}}, reviewNarinfoOpts{})
	cs := reviewFailingStore{&mockChunkStore{data: make(map[string][]byte)}}
	mb := newReviewBuilder(t, cs, pk, 1)

	const marker = "go-nix/pkg/nar.NewReader"
	before := reviewGoroutines(marker)
	_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	require.Error(t, err)
	after := reviewWaitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before, "nar reader goroutine leaked after a failed build")
}

// Same for tarballs: nothing reads or closes the pipe after buildFromNar fails, so the writeNar
// goroutine blocks in pw.Write forever.
func TestReviewFailedTarballBuildLeaksWriteNarGoroutine(t *testing.T) {
	big := make([]byte, 64<<16)
	reviewFill(big, 7)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "big", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(big))}))
	_, err := tw.Write(big)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	up := newReviewUpstream(t)
	up.set("/leak.tar", buf.Bytes())
	cs := reviewFailingStore{&mockChunkStore{data: make(map[string][]byte)}}
	mb, err := NewManifestBuilder(ManifestBuilderConfig{ConcurrentChunkOps: 1}, cs)
	require.NoError(t, err)

	const marker = "manifester.(*ManifestBuilder).writeNar"
	before := reviewGoroutines(marker)
	_, err = mb.BuildFromTarball(context.Background(), up.ts.URL+"/leak.tar", 0, 0, "", false)
	require.Error(t, err)
	after := reviewWaitGoroutines(marker, before, 3*time.Second)
	assert.LessOrEqual(t, after, before, "writeNar goroutine leaked after a failed tarball build")
}
