package daemon

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

// Remanifesting exists to make the manifester rebuild a manifest and re-upload its chunks
// after a chunk read hit NotFound. doRemanifestReqs used to go through
// getManifestFromManifester, which returns the manifest cache entry if there is one, so when
// the chunks were gone but the cached manifest was not (sharded build where a later shard
// failed, bucket GC race), the manifester was never asked and nothing was repaired.
func TestRemanifestSkipsManifestCache(t *testing.T) {
	e := newFetchEnv(t)
	comp, err := zstd.Compress(nil, []byte("cached envelope"))
	require.NoError(t, err)
	e.manifestCacheHit = comp
	req := MountReq{StorePath: testSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	require.NoError(t, e.s.doRemanifestReqs(context.Background(), []MountReq{req}))
	require.EqualValues(t, 1, e.manifestPosts.Load(), "remanifest did not ask the manifester to rebuild")
}

// Chunked manifests are catalogued and allocated under the "manifest sph" (the image sph
// with one bit flipped), so a diff op for manifest chunks records the manifest sph in
// usingSph. When such a fetch hit NotFound, appendRemanifestReqs turned usingSph straight
// into MountReqs: it looked the flipped hash up in the image bucket (no entry, so no
// upstream or nar size) and asked the manifester to rebuild a store path hash that doesn't
// exist, so remanifesting could never recover a missing manifest chunk.
func TestRemanifestForManifestChunkAsksForImage(t *testing.T) {
	e := newFetchEnv(t)
	const upstream = "https://cache.example.org/"

	sph, sphStr, spName, err := ParseSphAndName(testSpX)
	require.NoError(t, err)
	manifestSph := makeManifestSph(sph)

	// the chunk store has lost this manifest chunk: not served, so chunkdiff and the
	// direct read both 404
	mdata := testChunks(1, 4)[0]
	mdig := cdig.Sum(mdata)
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:    common.ManifestContext + "/" + testSpX,
			Type:    pb.EntryType_REGULAR,
			Size:    int64(len(mdata)),
			Digests: mdig[:],
		},
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
	})
	require.NoError(t, err)

	// db state as handleMountReq + getManifestAndBuildImage leave it just before
	// readChunks: image record, envelope, manifest catalog entries, manifest chunk allocation
	require.NoError(t, e.s.db.Update(func(tx *bbolt.Tx) error {
		img, err := proto.Marshal(&pb.DbImage{
			StorePath:  testSpX,
			Upstream:   upstream,
			NarSize:    1234,
			MountState: pb.MountState_Requested,
		})
		if err != nil {
			return err
		}
		if err := tx.Bucket(imageBucket).Put([]byte(sphStr), img); err != nil {
			return err
		}
		if err := tx.Bucket(manifestBucket).Put([]byte(sphStr), envelope); err != nil {
			return err
		}
		mkey := bytes.Join([][]byte{[]byte(isManifestPrefix), []byte(spName), {0}, manifestSph[:]}, nil)
		if err := tx.Bucket(catalogFBucket).Put(mkey, nil); err != nil {
			return err
		}
		return tx.Bucket(catalogRBucket).Put(manifestSph[:], []byte(isManifestPrefix+spName))
	}))
	mlocs, err := e.s.AllocateBatch(withAllocateCtx(context.Background(), manifestSph, true), []uint16{16}, []cdig.CDig{mdig})
	require.NoError(t, err)

	// what getManifestAndBuildImage's readChunks does for the first missing manifest chunk
	_ = e.s.requestChunk(context.Background(), mlocs[0], mdig, []SphPrefix{SphPrefixFromBytes(manifestSph[:])})

	e.mu.Lock()
	reqs := e.manifestReqs
	e.mu.Unlock()
	require.NotEmpty(t, reqs, "expected a remanifest request after NotFound")
	for _, r := range reqs {
		require.Equal(t, sphStr, r.StorePathHash, "remanifest asked for the wrong store path hash (manifest sph is %s)", manifestSph.String())
		require.Equal(t, upstream, r.Upstream, "remanifest lost the image's upstream")
	}
}

// doRemanifestReqs dedups through remanifestCache. The first caller used to run the
// remanifest on its own context and cache the result, including a context.Canceled caused
// by that caller going away. Every other caller for the next remanifestCacheExpiry (1
// minute) got that failure without the manifester being asked again. For a kernel read
// that hit NotFound, that meant EIO.
func TestRemanifestCacheDoesNotShareCancellation(t *testing.T) {
	e := newFetchEnv(t)
	e.manifesterHang = true
	req := MountReq{StorePath: testSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	ctx1, cancel1 := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- e.s.doRemanifestReqs(ctx1, []MountReq{req}) }()
	select {
	case <-e.manifestStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("manifester never got the first request")
	}
	cancel1() // e.g. `styx materialize` interrupted, or nix killed during a mount
	require.Error(t, <-errc)

	// another caller with a live context; the manifester would succeed now
	err := e.s.doRemanifestReqs(context.Background(), []MountReq{req})
	require.NoError(t, err, "second caller got the first caller's cancellation")
	require.EqualValues(t, 2, e.manifestPosts.Load(), "second caller never reached the manifester")
}

// A caller that goes away doesn't cancel the remanifest for others still waiting on it.
func TestRemanifestSurvivesFirstCallerLeaving(t *testing.T) {
	e := newFetchEnv(t)
	e.manifesterGate = make(chan struct{})
	req := MountReq{StorePath: testSpX[:32], Upstream: "http://upstream.invalid/", NarSize: 1000}

	ctx1, cancel1 := context.WithCancel(context.Background())
	errc1 := make(chan error, 1)
	go func() { errc1 <- e.s.doRemanifestReqs(ctx1, []MountReq{req}) }()
	select {
	case <-e.manifestStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("manifester never got the first request")
	}

	// a second caller joins the same request
	errc2 := make(chan error, 1)
	go func() { errc2 <- e.s.doRemanifestReqs(context.Background(), []MountReq{req}) }()
	require.Eventually(t, func() bool {
		rr, ok := e.s.remanifestCache.Get(req.StorePath)
		if !ok {
			return false
		}
		rr.mu.Lock()
		defer rr.mu.Unlock()
		return rr.waiters == 2
	}, 10*time.Second, 10*time.Millisecond)

	cancel1()
	require.Error(t, <-errc1)
	close(e.manifesterGate)
	require.NoError(t, <-errc2, "the first caller leaving cancelled the second caller's remanifest")
	require.EqualValues(t, 1, e.manifestPosts.Load(), "the second caller should have shared the first request")
}

const testTarballUrl = "https://tarballs.example.org/opusfile-0.12.tar.gz"

// putFakeCacheData records sphStr as built from testTarballUrl, like handleTarballReq does.
func (e *fetchEnv) putFakeCacheData(sphStr string) {
	b, err := proto.Marshal(&pb.FakeCacheData{Narinfo: []byte("narinfo"), Upstream: testTarballUrl})
	require.NoError(e.t, err)
	require.NoError(e.t, e.s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(fakeCacheBucket).Put([]byte(sphStr), b)
	}))
}

func (e *fetchEnv) requireTarballReqs() {
	e.t.Helper()
	e.mu.Lock()
	reqs := e.manifestReqs
	e.mu.Unlock()
	require.NotEmpty(e.t, reqs, "the manifester was never asked")
	for _, r := range reqs {
		require.Equal(e.t, manifester.ModeGenericTarball, r.BuildMode, "asked for a nar manifest from %s", r.Upstream)
		require.Equal(e.t, testTarballUrl, r.Upstream)
		require.Empty(e.t, r.StorePathHash)
	}
}

// Tarball images are substituted from our fake binary cache, so the image records its
// upstream as http://localhost:7444, which only this machine can read. Remanifesting sent
// that to the manifester as a binary cache, so missing chunks of a tarball image could never
// be recovered. It should build the tarball again instead.
func TestRemanifestTarballImageRebuildsTarball(t *testing.T) {
	e := newFetchEnv(t)
	sphStr := testSpX[:32]
	e.putFakeCacheData(sphStr)
	req := MountReq{StorePath: sphStr, Upstream: "http://" + fakeCacheBind + "/", NarSize: 1000}

	require.NoError(t, e.s.doRemanifestReqs(context.Background(), []MountReq{req}))
	e.requireTarballReqs()
}

// Without the tarball url there's nothing to rebuild, so don't ask the manifester at all.
func TestRemanifestTarballImageWithoutFakeCacheData(t *testing.T) {
	e := newFetchEnv(t)
	req := MountReq{StorePath: testSpX[:32], Upstream: "http://" + fakeCacheBind + "/", NarSize: 1000}

	require.Error(t, e.s.doRemanifestReqs(context.Background(), []MountReq{req}))
	require.Zero(t, e.manifestPosts.Load(), "sent a request the manifester can't do anything with")
}

// Mounting a tarball image looks for its manifest in the manifest cache under the tarball
// url. On a miss it asked the manifester for a nar manifest from that url, which isn't a
// binary cache. It should build the tarball.
func TestTarballImageManifestCacheMissRebuildsTarball(t *testing.T) {
	e := newFetchEnv(t)
	e.putFakeCacheData(testSpX[:32])
	req := MountReq{StorePath: testSpX, Upstream: "http://" + fakeCacheBind + "/", NarSize: 1000}

	// the test manifester's response isn't a real envelope, so this fails after the request
	_, _, err := e.s.getManifestAndBuildImage(context.Background(), &req)
	require.Error(t, err)
	e.requireTarballReqs()
}
