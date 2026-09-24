package manifester

// Fixtures for building manifests from a fake binary cache.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
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

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/nix-community/go-nix/pkg/nixbase32"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common/nar"
	"github.com/dnr/styx/pb"
)

type narFile struct {
	name string // absolute nar path, e.g. "/big"; must be given in nar order
	size int
}

func fillPseudoRandom(b []byte, seed uint64) {
	x := seed | 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = byte(x)
	}
}

func seedOf(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(h[:8])
}

func sphOf(s string) string {
	h := sha256.Sum256([]byte("sph:" + s))
	return nixbase32.EncodeToString(h[:20])
}

// nar with a root directory containing regular files filled with pseudo-random data
func makeNar(t *testing.T, seed string, files []narFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	require.NoError(t, err)
	require.NoError(t, nw.WriteHeader(&nar.Header{Path: "/", Type: nar.TypeDirectory}))
	for i, f := range files {
		require.NoError(t, nw.WriteHeader(&nar.Header{Path: f.name, Type: nar.TypeRegular, Size: int64(f.size)}))
		data := make([]byte, f.size)
		fillPseudoRandom(data, seedOf(seed)+uint64(i))
		_, err := nw.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, nw.Close())
	return buf.Bytes()
}

type narinfoOpts struct {
	noFileHash bool
	noNarHash  bool
	noSig      bool
	refs       []string
}

func makeNarinfo(t *testing.T, sk signature.SecretKey, sph, name string, narData []byte, o narinfoOpts) string {
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
type fakeUpstream struct {
	ts        *httptest.Server
	mu        sync.Mutex
	files     map[string][]byte
	redirects map[string]string
	requests  int
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	u := &fakeUpstream{files: make(map[string][]byte), redirects: make(map[string]string)}
	u.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.requests++
		body, ok := u.files[r.URL.Path]
		target, redirect := u.redirects[r.URL.Path]
		u.mu.Unlock()
		if redirect {
			http.Redirect(w, r, target, http.StatusFound)
			return
		} else if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(u.ts.Close)
	return u
}

func (u *fakeUpstream) set(p string, b []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[p] = b
}

func (u *fakeUpstream) redirect(p, target string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.redirects[p] = target
}

func (u *fakeUpstream) requestCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.requests
}

func (u *fakeUpstream) url() string { return u.ts.URL + "/" }

func (u *fakeUpstream) host() string {
	pu, _ := url.Parse(u.ts.URL)
	return pu.Host
}

func (u *fakeUpstream) addPath(t *testing.T, sk signature.SecretKey, seed string, files []narFile, o narinfoOpts) string {
	t.Helper()
	sph := sphOf(seed)
	narData := makeNar(t, seed, files)
	u.set("/"+sph+".narinfo", []byte(makeNarinfo(t, sk, sph, "test-"+seed, narData, o)))
	u.set("/nar/"+sph+".nar", narData)
	return sph
}

func upstreamKeys(t *testing.T) (signature.SecretKey, signature.PublicKey) {
	sk, pk, err := signature.GenerateKeypair("upstream-test-1", rand.Reader)
	require.NoError(t, err)
	return sk, pk
}

// newTestBuilder returns a builder that accepts pk's narinfos, from allowed upstreams.
func newTestBuilder(t *testing.T, cs ChunkStoreWrite, pk signature.PublicKey, concurrent int, allowed ...string) *ManifestBuilder {
	t.Helper()
	styxSk, _, err := signature.GenerateKeypair("styx-test-1", rand.Reader)
	require.NoError(t, err)
	mb, err := NewManifestBuilder(ManifestBuilderConfig{
		ConcurrentChunkOps: concurrent,
		PublicKeys:         []signature.PublicKey{pk},
		SigningKeys:        []signature.SecretKey{styxSk},
		AllowedUpstreams:   allowed,
	}, cs)
	require.NoError(t, err)
	return mb
}

func storeHas(cs *mockChunkStore, ns, key string) bool {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	_, ok := cs.data[ns+"/"+key]
	return ok
}

func storeKeysWithPrefix(cs *mockChunkStore, prefix string) []string {
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

func cachedManifest(t *testing.T, cs *mockChunkStore, cacheKey string) *pb.Manifest {
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

func callRecover(f func() error) (panicked any, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	return nil, f()
}

// number of goroutines whose stack mentions substr
func countGoroutines(substr string) int {
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

func waitGoroutines(substr string, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	for {
		c := countGoroutines(substr)
		if c <= want || time.Now().After(deadline) {
			return c
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type failingStore struct{ *mockChunkStore }

func (f failingStore) PutIfNotExists(ctx context.Context, p, k string, d []byte) ([]byte, error) {
	if p == ChunkReadPath {
		return nil, errors.New("injected chunk put failure")
	}
	return f.mockChunkStore.PutIfNotExists(ctx, p, k, d)
}
