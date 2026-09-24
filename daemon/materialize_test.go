package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/pb"
)

// cancelAfter is a context that cancels itself once Err has been called n
// times, so a test can cancel materialize at an exact point in its work.
type cancelAfter struct {
	context.Context
	cancel context.CancelFunc
	left   atomic.Int64
}

func newCancelAfter(n int64) *cancelAfter {
	ctx, cancel := context.WithCancel(context.Background())
	c := &cancelAfter{Context: ctx, cancel: cancel}
	c.left.Store(n)
	return c
}

func (c *cancelAfter) Err() error {
	if c.left.Add(-1) < 0 {
		c.cancel()
	}
	return c.Context.Err()
}

type materializeFixture struct {
	s    *Server
	m    *pb.Manifest
	want map[string][]byte // file path in the manifest -> contents
}

// newMaterializeFixture returns a server whose only slab holds the chunks of
// a manifest with one file of each of the given sizes, without a devnode or
// cachefiles. With copyFileRange the slab is also the slab's cache fd, so
// materialize uses copy_file_range; otherwise it falls back to plain reads.
func newMaterializeFixture(t *testing.T, sizes []int64, copyFileRange bool) *materializeFixture {
	const blockShift = 12
	cshift := shift.DefaultChunkShift
	dir := t.TempDir()
	s := NewServer(Config{CachePath: dir, ErofsBlockShift: blockShift})

	db, err := bbolt.Open(filepath.Join(dir, "db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	s.db = db

	slab, err := os.Create(filepath.Join(dir, "slab"))
	require.NoError(t, err)
	t.Cleanup(func() { slab.Close() })

	f := &materializeFixture{
		s:    s,
		m:    &pb.Manifest{Entries: []*pb.Entry{{Path: "/", Type: pb.EntryType_DIRECTORY}}},
		want: make(map[string][]byte),
	}
	var addr uint32
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		cb, err := tx.CreateBucket(chunkBucket)
		if err != nil {
			return err
		}
		for i, size := range sizes {
			data := make([]byte, size)
			for j := range data {
				data[j] = byte(i + j*7 + j>>16)
			}
			var digests []byte
			for off := int64(0); off < size; off += cshift.Size() {
				chunk := data[off:min(off+cshift.Size(), size)]
				dig := cdig.Sum(chunk)
				if _, err := slab.WriteAt(chunk, int64(addr)<<blockShift); err != nil {
					return err
				} else if err := cb.Put(dig[:], locValue(0, addr, Sph{})); err != nil {
					return err
				}
				digests = append(digests, dig[:]...)
				addr += uint32(s.blockShift.Blocks(int64(len(chunk))))
			}
			p := fmt.Sprintf("/file%d", i)
			f.m.Entries = append(f.m.Entries, &pb.Entry{
				Path:    p,
				Type:    pb.EntryType_REGULAR,
				Size:    size,
				Digests: digests,
			})
			f.want[p] = data
		}
		return nil
	}))
	// copy_file_range copies whole blocks
	require.NoError(t, slab.Truncate(int64(addr)<<blockShift))

	fds := slabFds{readFd: int(slab.Fd())}
	if copyFileRange {
		fds.cacheFd = fds.readFd
	}
	s.readfdBySlab[0] = fds
	return f
}

func copyModes(t *testing.T, run func(t *testing.T, copyFileRange bool)) {
	t.Run("copy_file_range", func(t *testing.T) { run(t, true) })
	t.Run("plain", func(t *testing.T) { run(t, false) })
}

func TestMaterializeFiles(t *testing.T) {
	copyModes(t, func(t *testing.T, copyFileRange bool) {
		f := newMaterializeFixture(t, []int64{100, 70000, 3 << 16, 5<<16 + 1}, copyFileRange)
		dest := filepath.Join(t.TempDir(), "out")
		require.NoError(t, f.s.materialize(context.Background(), dest, f.m))
		for p, want := range f.want {
			got, err := os.ReadFile(filepath.Join(dest, p))
			require.NoError(t, err)
			require.Equal(t, want, got, "contents of %s", p)
		}
	})
}

// handleMaterializeReq runs with the request's context, which net/http
// cancels when the client disconnects, as nix does after styx-timeout. Nix
// then falls back to writing the path itself, so materialize must stop.
func TestMaterializeStopsWhenCancelled(t *testing.T) {
	copyModes(t, func(t *testing.T, copyFileRange bool) {
		sizes := make([]int64, 50)
		for i := range sizes {
			sizes[i] = 1000
		}
		f := newMaterializeFixture(t, sizes, copyFileRange)
		dest := filepath.Join(t.TempDir(), "out")

		const allowed = 5
		err := f.s.materialize(newCancelAfter(allowed), dest, f.m)
		require.ErrorIs(t, err, context.Canceled)
		ents, err := os.ReadDir(dest)
		require.NoError(t, err)
		require.LessOrEqual(t, len(ents), allowed,
			"materialize created %d of %d files after being cancelled once it had checked its context %d times",
			len(ents), len(sizes), allowed)
	})
}

// A single large file must stop too, within a chunk or so of the context
// being cancelled, not only between files.
func TestMaterializeStopsMidFileWhenCancelled(t *testing.T) {
	copyModes(t, func(t *testing.T, copyFileRange bool) {
		const chunks = 64
		cshift := shift.DefaultChunkShift
		f := newMaterializeFixture(t, []int64{chunks << cshift}, copyFileRange)
		dest := filepath.Join(t.TempDir(), "out")

		// one check per entry, then one per chunk
		const allowed = 4
		err := f.s.materialize(newCancelAfter(allowed), dest, f.m)
		require.ErrorIs(t, err, context.Canceled)
		st, err := os.Stat(filepath.Join(dest, "file0"))
		require.NoError(t, err)
		require.LessOrEqual(t, st.Size(), int64(allowed)<<cshift,
			"materialize wrote %d of %d bytes of a file after being cancelled once it had checked its context %d times",
			st.Size(), int64(chunks)<<cshift, allowed)
	})
}
