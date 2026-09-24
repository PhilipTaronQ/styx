package daemon

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/erofs"
	"github.com/dnr/styx/pb"
)

const (
	repairSpA = "qa22bifihaxyvn6q2a6w9m0nklqrk9wh-opusfile-0.12"
	repairSpB = "3a7xq2qhxw2r7naqmc53akmx7yvz0mkf-less-is-more.patch"
)

type repairEnv struct {
	t    *testing.T
	s    *Server
	slab *os.File
}

func newRepairEnv(t *testing.T) *repairEnv {
	s := newTestServer(t, 1, false)
	p := s.slabDataPath(0)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	f, err := os.Create(p)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	require.NoError(t, f.Truncate(1<<30))
	return &repairEnv{t: t, s: s, slab: f}
}

func randChunk(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, 0))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32() | 1)
	}
	return b
}

func sphOf(t *testing.T, sp string) Sph {
	sph, _, _, err := ParseSphAndName(sp)
	require.NoError(t, err)
	return sph
}

// putChunk writes a chunk record the way an old AllocateBatch did, which may take over the
// address key of an earlier chunk.
func (e *repairEnv) putChunk(d cdig.CDig, addr uint32, sp string, present bool) {
	sph := sphOf(e.t, sp)
	require.NoError(e.t, e.s.db.Update(func(tx *bbolt.Tx) error {
		sb, err := tx.Bucket(slabBucket).CreateBucketIfNotExists(slabKey(0))
		require.NoError(e.t, err)
		require.NoError(e.t, tx.Bucket(chunkBucket).Put(d[:], locValue(0, addr, sph)))
		require.NoError(e.t, sb.Put(addrKey(addr), d[:]))
		if present {
			require.NoError(e.t, sb.Put(addrKey(addr|presentMask), []byte{}))
		}
		return nil
	}))
}

func (e *repairEnv) setSeq(seq uint64) { setSlabSequence(e.t, e.s, 0, seq) }

func (e *repairEnv) write(addr uint32, data []byte) {
	_, err := e.slab.WriteAt(data, int64(addr)<<e.s.blockShift)
	require.NoError(e.t, err)
}

func (e *repairEnv) read(addr uint32, n int) []byte {
	b := make([]byte, n)
	_, err := e.slab.ReadAt(b, int64(addr)<<e.s.blockShift)
	require.NoError(e.t, err)
	return b
}

func (e *repairEnv) putImage(sp string, state pb.MountState, size int64) string {
	_, sphStr, _, err := ParseSphAndName(sp)
	require.NoError(e.t, err)
	require.NoError(e.t, e.s.imageTx(sphStr, func(img *pb.DbImage) error {
		img.StorePath = sp
		img.Upstream = "http://upstream/"
		img.MountState = state
		img.ImageSize = size
		return nil
	}))
	// stand-in for cachefiles' copy of the image
	p := filepath.Join(e.s.cfg.CachePath, fscachePath(e.s.cfg.CacheDomain, sphStr))
	require.NoError(e.t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(e.t, os.WriteFile(p, []byte("old image"), 0o644))
	return p
}

func (e *repairEnv) image(sp string) *pb.DbImage {
	_, sphStr, _, err := ParseSphAndName(sp)
	require.NoError(e.t, err)
	var img pb.DbImage
	require.NoError(e.t, e.s.db.View(func(tx *bbolt.Tx) error {
		return proto.Unmarshal(tx.Bucket(imageBucket).Get([]byte(sphStr)), &img)
	}))
	return &img
}

type repairChunk struct {
	loc     erofs.SlabLoc
	present bool
	keyedBy cdig.CDig // digest the address key names
}

func (e *repairEnv) chunk(d cdig.CDig) repairChunk {
	var c repairChunk
	require.NoError(e.t, e.s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(chunkBucket).Get(d[:])
		require.NotNil(e.t, v, "chunk %s", d)
		c.loc = loadLoc(v)
		sb := tx.Bucket(slabBucket).Bucket(slabKey(c.loc.SlabId))
		c.present = sb.Get(addrKey(c.loc.Addr|presentMask)) != nil
		c.keyedBy = cdig.FromBytes(sb.Get(addrKey(c.loc.Addr)))
		return nil
	}))
	return c
}

func (e *repairEnv) seq() uint64 {
	var seq uint64
	require.NoError(e.t, e.s.db.View(func(tx *bbolt.Tx) error {
		seq = tx.Bucket(slabBucket).Bucket(slabKey(0)).Sequence()
		return nil
	}))
	return seq
}

func (e *repairEnv) dbDump() map[string]string {
	out := make(map[string]string)
	require.NoError(e.t, e.s.db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			var walk func(prefix string, b *bbolt.Bucket) error
			walk = func(prefix string, b *bbolt.Bucket) error {
				out[prefix+"#seq"] = strconv.FormatUint(b.Sequence(), 10)
				return b.ForEach(func(k, v []byte) error {
					if v == nil {
						return walk(prefix+"/"+string(k), b.Bucket(k))
					}
					out[prefix+"/"+string(k)] = string(v)
					return nil
				})
			}
			return walk(string(name), b)
		})
	}))
	return out
}

func requireFileGone(t *testing.T, p string) {
	_, err := os.Stat(p)
	require.ErrorIs(t, err, os.ErrNotExist, "%s should have been removed", p)
}

// Old allocation: file /a of image A is exactly one 64 KiB chunk and got 0 blocks at 4. Image
// B's 4 KiB chunk was allocated at 4 too, and B's next chunk at 5. B's chunks were written,
// then A's chunk was fetched as a neighbor and its 64 KiB written at 4, over both of B's.
func TestRepairSlabOverlapClobbered(t *testing.T) {
	e := newRepairEnv(t)
	ca, cb, cc, cz := randChunk(1, 64<<10), randChunk(2, 4<<10), randChunk(3, 64<<10), randChunk(4, 1000)
	da, db, dc, dz := cdig.Sum(ca), cdig.Sum(cb), cdig.Sum(cc), cdig.Sum(cz)
	e.putChunk(da, 4, repairSpA, false)
	e.putChunk(db, 4, repairSpB, true)
	e.putChunk(dc, 5, repairSpB, true)
	e.putChunk(dz, 21, repairSpB, true)
	e.setSeq(22)
	e.write(4, cb)
	e.write(5, cc)
	e.write(21, cz)
	e.write(4, ca)                    // clobbers cb and most of cc
	e.write(22, randChunk(5, 32<<10)) // stale data past the end of allocated space
	imgA := e.putImage(repairSpA, pb.MountState_Unmounted, 1000)
	imgB := e.putImage(repairSpB, pb.MountState_Mounted, 2000)

	require.NoError(t, e.s.repairSlabOverlaps())

	// A's chunk moved to its own space and has to be fetched again
	a := e.chunk(da)
	require.NotEqual(t, uint32(4), a.loc.Addr)
	require.GreaterOrEqual(t, a.loc.Addr, uint32(22))
	require.Equal(t, da, a.keyedBy)
	require.False(t, a.present)
	require.Equal(t, make([]byte, 64<<10), e.read(a.loc.Addr, 64<<10), "new space should be empty")

	// B's chunks keep their addresses, but their data was wrong, so it's dropped
	for _, d := range []cdig.CDig{db, dc} {
		c := e.chunk(d)
		require.Equal(t, d, c.keyedBy)
		require.False(t, c.present, "chunk %s holds another chunk's data", d)
	}
	require.Equal(t, uint32(4), e.chunk(db).loc.Addr)
	require.Equal(t, uint32(5), e.chunk(dc).loc.Addr)
	require.Equal(t, make([]byte, 17<<12), e.read(4, 17<<12), "dropped data should be punched out")

	// the chunk after that was fine
	z := e.chunk(dz)
	require.True(t, z.present)
	require.Equal(t, cz, e.read(21, len(cz)))

	// A has to be rebuilt, B's image is fine
	require.Zero(t, e.image(repairSpA).ImageSize)
	requireFileGone(t, imgA)
	require.Equal(t, int64(2000), e.image(repairSpB).ImageSize)
	require.FileExists(t, imgB)

	// new allocations don't land on anything
	require.GreaterOrEqual(t, e.seq(), uint64(a.loc.Addr)+16)
	before := e.dbDump()
	require.NoError(t, e.s.repairSlabOverlaps())
	require.Equal(t, before, e.dbDump(), "repair should only run once")
}

// Old allocation again, but A's chunk was written first. It fits in the space B's chunk was
// given, so A keeps the address and B's chunk moves.
func TestRepairSlabOverlapFirstWriterKeeps(t *testing.T) {
	e := newRepairEnv(t)
	ca, cb, cc := randChunk(1, 64<<10), randChunk(2, 64<<10), randChunk(3, 3000)
	da, db, dc := cdig.Sum(ca), cdig.Sum(cb), cdig.Sum(cc)
	e.putChunk(da, 4, repairSpA, true)
	e.write(4, ca)
	e.putChunk(db, 4, repairSpB, false) // B's reads were served A's data, so it was never written
	e.putChunk(dc, 20, repairSpB, true)
	e.write(20, cc)
	e.setSeq(21)
	e.putImage(repairSpA, pb.MountState_Mounted, 1000)
	imgB := e.putImage(repairSpB, pb.MountState_MountError, 2000)

	require.NoError(t, e.s.repairSlabOverlaps())

	a := e.chunk(da)
	require.Equal(t, uint32(4), a.loc.Addr)
	require.Equal(t, da, a.keyedBy)
	require.True(t, a.present)
	require.Equal(t, ca, e.read(4, len(ca)))

	b := e.chunk(db)
	require.GreaterOrEqual(t, b.loc.Addr, uint32(21))
	require.Equal(t, db, b.keyedBy)
	require.False(t, b.present)

	c := e.chunk(dc)
	require.True(t, c.present)
	require.Equal(t, cc, e.read(20, len(cc)))

	require.Equal(t, int64(1000), e.image(repairSpA).ImageSize)
	require.Zero(t, e.image(repairSpB).ImageSize)
	requireFileGone(t, imgB)
}

// The last chunk allocated got 0 blocks, and nothing has been allocated after it yet.
func TestRepairSlabOverlapTail(t *testing.T) {
	e := newRepairEnv(t)
	ca := randChunk(1, 64<<10)
	da := cdig.Sum(ca)
	e.putChunk(da, 4, repairSpA, true)
	e.write(4, ca)
	e.setSeq(4)
	e.putImage(repairSpA, pb.MountState_Mounted, 1000)

	require.NoError(t, e.s.repairSlabOverlaps())

	a := e.chunk(da)
	require.Equal(t, uint32(4), a.loc.Addr)
	require.True(t, a.present)
	require.Equal(t, int64(1000), e.image(repairSpA).ImageSize)

	ctx := withAllocateCtx(context.Background(), sphOf(t, repairSpB), false)
	locs, err := e.s.AllocateBatch(ctx, []uint16{1}, []cdig.CDig{testDigest(1)})
	require.NoError(t, err)
	require.GreaterOrEqual(t, locs[0].Addr, uint32(4+16))
}

func TestRepairSlabOverlapNothingToDo(t *testing.T) {
	e := newRepairEnv(t)
	ca, cb := randChunk(1, 64<<10), randChunk(2, 5000)
	da, db := cdig.Sum(ca), cdig.Sum(cb)
	e.putChunk(da, 4, repairSpA, true)
	e.putChunk(db, 20, repairSpA, true)
	e.setSeq(22)
	e.write(4, ca)
	e.write(20, cb)
	imgA := e.putImage(repairSpA, pb.MountState_Unmounted, 1000)
	before := e.dbDump()

	require.NoError(t, e.s.repairSlabOverlaps())

	after := e.dbDump()
	delete(after, "meta/"+string(metaSlabOverlapRepaired))
	require.Equal(t, before, after)
	require.FileExists(t, imgA)
	require.True(t, bytes.Equal(ca, e.read(4, len(ca))))
}
