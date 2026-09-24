package daemon

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/PhilipTaronQ/styx/common"
	"github.com/PhilipTaronQ/styx/common/cdig"
	"github.com/PhilipTaronQ/styx/erofs"
	"github.com/PhilipTaronQ/styx/pb"
)

// withManifestSlab sets up the manifest slab file, as the daemon does at startup.
func (e *fetchEnv) withManifestSlab() {
	require.NoError(e.t, e.s.setupManifestSlab())
	e.t.Cleanup(func() {
		e.s.stateLock.Lock()
		fd := e.s.readfdBySlab[manifestSlabOffset].readFd
		e.s.stateLock.Unlock()
		_ = unix.Close(fd)
	})
}

func (e *fetchEnv) putImage(storePath string, state pb.MountState) {
	_, sphStr, _, err := ParseSphAndName(storePath)
	require.NoError(e.t, err)
	img, err := proto.Marshal(&pb.DbImage{StorePath: storePath, Upstream: "http://upstream.invalid/", MountState: state})
	require.NoError(e.t, err)
	require.NoError(e.t, e.s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(imageBucket).Put([]byte(sphStr), img)
	}))
}

func (e *fetchEnv) allocate(sph Sph, forManifest bool, data []byte) erofs.SlabLoc {
	blocks := []uint16{uint16(e.s.blockShift.Blocks(int64(len(data))))}
	locs, err := e.s.AllocateBatch(withAllocateCtx(context.Background(), sph, forManifest), blocks, []cdig.CDig{cdig.Sum(data)})
	require.NoError(e.t, err)
	return locs[0]
}

func requireManifestSlab(t *testing.T, loc erofs.SlabLoc, what string) {
	t.Helper()
	require.GreaterOrEqual(t, loc.SlabId, uint16(manifestSlabOffset), "%s was put in data slab %d", what, loc.SlabId)
}

func requireDataSlab(t *testing.T, loc erofs.SlabLoc, what string) {
	t.Helper()
	require.Less(t, loc.SlabId, uint16(manifestSlabOffset), "%s was put in manifest slab %d", what, loc.SlabId)
}

// A digest had one loc, and AllocateBatch reused it whatever slab it was in. So a manifest
// chunk with the same bytes as a data chunk was placed in a data slab, where reading it
// goes through erofs and cachefiles. buildDiff reads manifest chunks of diff bases under
// diffLock, and if the data wasn't there, the kernel's READ for it needed diffLock too: a
// deadlock. The other way round, a data chunk with the same bytes as a manifest chunk was
// placed in the manifest slab, which isn't a cachefiles blob, so an erofs image can't refer
// to it.
func TestManifestAndDataChunksGetSeparateSlabs(t *testing.T) {
	e := newFetchEnv(t)
	e.withManifestSlab()
	sphX, _, _, err := ParseSphAndName(testSpX)
	require.NoError(t, err)
	sphB, _, _, err := ParseSphAndName(testSpB)
	require.NoError(t, err)
	chunks := testChunks(2, 21)

	// data chunk first, then a manifest chunk with the same bytes
	requireDataSlab(t, e.allocate(sphX, false, chunks[0]), "data chunk")
	requireManifestSlab(t, e.allocate(makeManifestSph(sphB), true, chunks[0]), "manifest chunk with the bytes of a data chunk")

	// manifest chunk first, then a data chunk with the same bytes
	requireManifestSlab(t, e.allocate(makeManifestSph(sphX), true, chunks[1]), "manifest chunk")
	requireDataSlab(t, e.allocate(sphB, false, chunks[1]), "data chunk with the bytes of a manifest chunk")
}

// Image B has a file whose bytes are image X's chunked manifest. The manifest chunk is read
// from the manifest slab, and GC keeps and collects each one on its own.
func TestManifestChunkLikeDataChunkIsReadAndCollectedSeparately(t *testing.T) {
	e := newFetchEnv(t)
	e.withManifestSlab()
	sphX, sphStrX, nameX, err := ParseSphAndName(testSpX)
	require.NoError(t, err)

	// X's manifest
	xChunks := testChunks(2, 22)
	xDigests := []cdig.CDig{cdig.Sum(xChunks[0]), cdig.Sum(xChunks[1])}
	mb, err := proto.Marshal(&pb.Manifest{Entries: []*pb.Entry{
		{Path: "/", Type: pb.EntryType_DIRECTORY},
		{Path: "/file", Type: pb.EntryType_REGULAR, Size: 2 << 16, Digests: cdig.ToSliceAlias(xDigests)},
	}})
	require.NoError(t, err)
	mdig := cdig.Sum(mb)

	// B, with one file that is X's manifest, fully present
	_, bLocs := e.addImage(testSpB, [][]byte{mb})
	requireDataSlab(t, bLocs[0], "B's data chunk")
	require.NoError(t, e.s.gotNewChunk(bLocs[0], mdig, bytes.Clone(mb)))
	e.putImage(testSpB, pb.MountState_Unmounted)

	// X, with a chunked manifest, present in the manifest slab
	envelope, err := proto.Marshal(&pb.SignedMessage{
		Msg: &pb.Entry{
			Path:    common.ManifestContext + "/" + testSpX,
			Type:    pb.EntryType_REGULAR,
			Size:    int64(len(mb)),
			Digests: mdig[:],
		},
		Params: &pb.GlobalParams{DigestAlgo: cdig.Algo, DigestBits: cdig.Bits},
	})
	require.NoError(t, err)
	e.putManifest(sphX, sphStrX, nameX, envelope)
	mloc := e.allocate(makeManifestSph(sphX), true, mb)
	requireManifestSlab(t, mloc, "X's manifest chunk")
	require.NoError(t, e.s.gotNewChunk(mloc, mdig, bytes.Clone(mb)))
	for _, c := range xChunks {
		e.allocate(sphX, false, c)
	}
	e.putImage(testSpX, pb.MountState_Mounted)

	readX := func() {
		t.Helper()
		require.NoError(t, e.s.db.View(func(tx *bbolt.Tx) error {
			m, mdigs, err := e.s.getManifestLocal(tx, sphStrX)
			if err != nil {
				return err
			}
			require.Equal(t, []cdig.CDig{mdig}, mdigs)
			require.Len(t, m.Entries, 2)
			return nil
		}))
	}
	readX()

	// collect B: its data chunk goes, X's manifest chunk and data chunks stay
	resp, err := e.s.handleGcReq(context.Background(), &GcReq{
		GcByState: map[pb.MountState]bool{pb.MountState_Unmounted: true},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.DeleteImages)
	require.Equal(t, 1, resp.DeleteChunks)
	require.Equal(t, 3, resp.RemainRefChunks)
	require.Equal(t, 3, resp.RemainHaveChunks)
	readX()
	require.NoError(t, e.s.db.View(func(tx *bbolt.Tx) error {
		sb := tx.Bucket(slabBucket).Bucket(slabKey(bLocs[0].SlabId))
		require.Nil(t, sb.Get(addrKey(bLocs[0].Addr)), "B's data chunk is still allocated")
		msb := tx.Bucket(slabBucket).Bucket(slabKey(mloc.SlabId))
		require.NotNil(t, msb.Get(addrKey(mloc.Addr)), "X's manifest chunk was collected")
		return nil
	}))

	// collect X too: nothing is left
	e.putImage(testSpX, pb.MountState_Unmounted)
	resp, err = e.s.handleGcReq(context.Background(), &GcReq{
		GcByState: map[pb.MountState]bool{pb.MountState_Unmounted: true},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.DeleteImages)
	require.Equal(t, 3, resp.DeleteChunks)
	require.Zero(t, resp.RemainRefChunks)
}
