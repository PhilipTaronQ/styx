package common

// Review tests for the shared fetch helpers. Each test demonstrates one finding; they are
// expected to FAIL on the current code.

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/pb"
)

// entryFingerprint covers path, size and inline data or digests, but not chunk_shift,
// digest_bytes or manifest_meta. The daemon uses the envelope's chunk_shift to allocate
// and slice chunked manifests, and handleTarballReq takes the narinfo and resolved
// upstream of a chunked tarball manifest from manifest_meta. Anyone who can write the
// manifest cache can change them without breaking the signature.
func TestReviewSignatureCoversAllUsedEntryFields(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("review-test-1", rand.Reader)
	require.NoError(t, err)

	entry := &pb.Entry{
		Path:    ManifestContext + "/53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12",
		Type:    pb.EntryType_REGULAR,
		Size:    100000,
		Digests: make([]byte, 2*24),
		ManifestMeta: &pb.ManifestMeta{
			GenericTarballResolved: "https://example.org/good.tar.gz",
		},
	}
	signed, err := SignMessageAsEntry([]signature.SecretKey{sk}, &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192}, entry)
	require.NoError(t, err)
	_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, signed)
	require.NoError(t, err, "precondition: untampered envelope verifies")

	for name, tamper := range map[string]func(*pb.Entry){
		"chunk_shift": func(e *pb.Entry) { e.ChunkShift = -1 },
		"manifest_meta": func(e *pb.Entry) {
			e.ManifestMeta.GenericTarballResolved = "https://attacker.example/evil.tar.gz"
		},
	} {
		t.Run(name, func(t *testing.T) {
			var sm pb.SignedMessage
			require.NoError(t, proto.Unmarshal(signed, &sm))
			tamper(sm.Msg)
			tampered, err := proto.Marshal(&sm)
			require.NoError(t, err)
			_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, tampered)
			require.Error(t, err, "envelope with tampered %s still verifies", name)
		})
	}
}

// RetryHttpRequest retries forever with retry.Delay(1s) and no MaxDelay, so the delay
// doubles without bound. The kernel read path calls it with context.Background(), so a
// chunk store outage of T leaves reads blocked for up to about another T after the store
// comes back (and holds a diffSem slot and a cachefiles worker the whole time).
func TestReviewRetryHttpRequestRecoversPromptly(t *testing.T) {
	start := time.Now()
	const recoverAfter = 8 * time.Second

	var mu sync.Mutex
	var attempts []time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		el := time.Since(start)
		mu.Lock()
		attempts = append(attempts, el.Round(100*time.Millisecond))
		mu.Unlock()
		if el < recoverAfter {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RetryHttpRequest(ctx, http.MethodGet, srv.URL, "", nil)
	require.NoError(t, err)
	res.Body.Close()

	lag := time.Since(start) - recoverAfter
	mu.Lock()
	defer mu.Unlock()
	require.Less(t, lag, 3*time.Second,
		"server recovered at %v but the request only succeeded %v later; attempts at %v",
		recoverAfter, lag.Round(100*time.Millisecond), attempts)
}
