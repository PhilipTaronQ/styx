package daemon

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

// Chunked manifests are catalogued and allocated under the "manifest sph" (the image sph
// with one bit flipped), so a diff op for manifest chunks records the manifest sph in
// usingSph. When such a fetch hits NotFound, appendRemanifestReqs turns usingSph straight
// into MountReqs: it looks the flipped hash up in the image bucket (no entry, so no
// upstream or nar size) and asks the manifester to rebuild a store path hash that doesn't
// exist. Remanifesting can never recover a missing manifest chunk.
// Expected to FAIL on the current code.
func TestReviewRemanifestForManifestChunkAsksForImage(t *testing.T) {
	e := newFetchEnv(t)
	const upstream = "https://cache.example.org/"

	sph, sphStr, spName, err := ParseSphAndName(reviewSpX)
	require.NoError(t, err)
	manifestSph := makeManifestSph(sph)

	// the chunk store has lost this manifest chunk: not served, so chunkdiff and the
	// direct read both 404
	mdata := reviewChunks(1, 4)[0]
	mdig := cdig.Sum(mdata)
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:    common.ManifestContext + "/" + reviewSpX,
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
			StorePath:  reviewSpX,
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
