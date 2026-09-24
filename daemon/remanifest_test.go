package daemon

import (
	"bytes"
	"context"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
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
