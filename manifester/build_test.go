package manifester

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// narinfo without NarHash: ni.Fingerprint() dereferenced a nil hash before the signature was
// checked. lambdaurl runs the handler in a goroutine without recover, so this killed the
// process.
func TestNarinfoWithoutNarHashPanics(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := up.addPath(t, sk, "nonarhash", []narFile{{"/f", 1000}}, narinfoOpts{noNarHash: true})
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)

	p, err := callRecover(func() error {
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
		return err
	})
	assert.Nil(t, p, "unauthenticated narinfo made BuildFromNar panic before signature verification")
	assert.Error(t, err)
}

// A correctly signed narinfo without FileHash (FileHash is not part of the fingerprint, and
// some caches omit it) made BuildFromNar panic after the nar was fully processed.
func TestSignedNarinfoWithoutFileHashPanics(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := up.addPath(t, sk, "nofilehash", []narFile{{"/f", 1000}}, narinfoOpts{noFileHash: true})
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)

	p, err := callRecover(func() error {
		_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
		return err
	})
	assert.Nil(t, p, "signed narinfo without FileHash made BuildFromNar panic")
	assert.NoError(t, err)
}

// An empty or malformed store path hash made CacheKey panic, and a narinfo for a different
// store path than the one requested was accepted.
func TestBuildFromNarChecksStorePath(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := up.addPath(t, sk, "storepath", []narFile{{"/f", 1000}}, narinfoOpts{})
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)

	for _, bad := range []string{"", "x", sph[:31] + "e", sph + "0"} {
		p, err := callRecover(func() error {
			_, err := mb.BuildFromNar(context.Background(), up.url(), bad, 0, 0, "", false)
			return err
		})
		assert.Nil(t, p, "store path hash %q", bad)
		assert.Error(t, err, "store path hash %q", bad)
	}

	// serve the (correctly signed) narinfo for sph under another hash
	other := sphOf("other")
	up.set("/"+other+".narinfo", up.files["/"+sph+".narinfo"])
	_, err := mb.BuildFromNar(context.Background(), up.url(), other, 0, 0, "", false)
	assert.Error(t, err)

	_, err = mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	assert.NoError(t, err)
}

// Unbounded narinfo input: go-nix NarInfo.Fingerprint builds the reference list with repeated
// string +=, which is quadratic, and it runs before the signature check.
func TestNarinfoManyReferencesIsQuadratic(t *testing.T) {
	sk, pk := upstreamKeys(t)
	up := newFakeUpstream(t)
	sph := sphOf("refs")
	narData := makeNar(t, "refs", []narFile{{"/f", 1000}})
	refs := make([]string, 80000)
	for i := range refs {
		refs[i] = "a"
	}
	ni := makeNarinfo(t, sk, sph, "refs", narData, narinfoOpts{noSig: true, refs: refs})
	t.Logf("narinfo is %d bytes", len(ni))
	up.set("/"+sph+".narinfo", []byte(ni))
	up.set("/nar/"+sph+".nar", narData)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	mb := newTestBuilder(t, cs, pk, 0)

	start := time.Now()
	_, err := mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	elapsed := time.Since(start)
	require.Error(t, err) // unsigned
	assert.Less(t, elapsed, time.Second,
		"a %d byte unsigned narinfo took %s to reject", len(ni), elapsed)

	// and there's a limit on the size
	up.set("/"+sph+".narinfo", []byte(ni+strings.Repeat("References: a\n", maxNarinfoSize/14)))
	_, err = mb.BuildFromNar(context.Background(), up.url(), sph, 0, 0, "", false)
	assert.ErrorContains(t, err, "larger than")
}

func TestNarinfoFingerprint(t *testing.T) {
	sk, _ := upstreamKeys(t)
	narData := makeNar(t, "fp", []narFile{{"/f", 10}})
	for _, refs := range [][]string{nil, {"a"}, {"00000000000000000000000000000000-x", "11111111111111111111111111111111-y"}} {
		text := makeNarinfo(t, sk, sphOf("fp"), "fp", narData, narinfoOpts{refs: refs})
		ni, err := narinfo.Parse(strings.NewReader(text))
		require.NoError(t, err)
		assert.Equal(t, ni.Fingerprint(), narinfoFingerprint(ni))
	}
}
