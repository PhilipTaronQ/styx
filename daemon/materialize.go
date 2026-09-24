package daemon

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"

	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/PhilipTaronQ/styx/common"
	"github.com/PhilipTaronQ/styx/common/cdig"
	"github.com/PhilipTaronQ/styx/common/errgroup"
	"github.com/PhilipTaronQ/styx/erofs"
	"github.com/PhilipTaronQ/styx/pb"
)

var errCachefdNotFound = errors.New("cache fd not found for slab")

var zeroTimeval = []unix.Timeval{{}, {}}

// for tests: called just before materialize copies a chunk
var testHookMaterializeChunk func(erofs.SlabLoc)

func (s *Server) handleMaterializeReq(ctx context.Context, r *MaterializeReq) (*Status, error) {
	if s.p() == nil {
		return nil, mwErr(http.StatusPreconditionFailed, "styx is not initialized, call 'styx init --params=...'")
	} else if r.Upstream == "" {
		return nil, mwErr(http.StatusBadRequest, "invalid upstream")
	} else if !strings.HasPrefix(r.DestPath, "/") {
		return nil, mwErr(http.StatusBadRequest, "dest must be absolute path")
	}

	_, sphStr, _, err := ParseSphAndName(r.StorePath)
	if err != nil {
		return nil, err
	}

	// An Unmounted or Materialized image stays in that state while we copy it, so without
	// this gc could delete its chunks and punch them mid-copy, and we'd copy the holes as
	// zeros.
	defer s.holdForGc(sphStr)()

	common.NormalizeUpstream(&r.Upstream)

	shouldHaveManifest := false
	_ = s.imageTx(sphStr, func(img *pb.DbImage) error {
		switch img.MountState {
		case pb.MountState_Unknown:
			// we have no record of this, set it up
			img.StorePath = r.StorePath
			img.Upstream = r.Upstream
			img.MountState = pb.MountState_Requested
			return nil

		case pb.MountState_Mounted, pb.MountState_Unmounted, pb.MountState_Materialized:
			// we should already have a manifest locally. leave img alone.
			shouldHaveManifest = true
			return errors.New("rollback")

		default:
			// other states are errors/races, leave img alone.
			return errors.New("rollback")
		}
	})

	var m *pb.Manifest
	if shouldHaveManifest {
		// read locally
		if m, _, err = s.loadManifest(ctx, sphStr); err != nil {
			// fall back to remote manifest
			log.Print("error getting manifest locally, trying remote")
			shouldHaveManifest = false
		}
	}
	if !shouldHaveManifest {
		// get manifest and allocate
		m, _, err = s.getManifestAndBuildImage(ctx, &MountReq{
			Upstream:  r.Upstream,
			StorePath: r.StorePath,
			NarSize:   r.NarSize,
		})
	}
	if err != nil {
		return nil, err
	}

	// prefetch all
	haveReq := make(map[cdig.CDig]struct{})
	var reqs []cdig.CDig
	for _, e := range m.Entries {
		for _, d := range cdig.FromSliceAlias(e.Digests) {
			if _, ok := haveReq[d]; !ok {
				haveReq[d] = struct{}{}
				reqs = append(reqs, d)
			}
		}
	}
	if len(reqs) > 0 {
		if err = s.requestPrefetch(ctx, reqs); err != nil {
			return nil, err
		}
	}

	// copy to dest. stop if the client gives up: it may fall back to writing
	// the same destination itself.
	err = s.materialize(ctx, r.DestPath, m)
	if err != nil {
		return nil, err
	}

	// record as materialized/error, unless in some other state
	_ = s.imageTx(sphStr, func(img *pb.DbImage) error {
		if img.MountState != pb.MountState_Requested {
			return errors.New("rollback")
		}
		img.MountState = pb.MountState_Materialized
		return nil
	})

	return nil, nil
}

func (s *Server) materialize(ctx context.Context, dest string, m *pb.Manifest) error {
	ents := m.Entries
	locs := make(map[cdig.CDig]erofs.SlabLoc)
	err := s.db.View(func(tx *bbolt.Tx) error {
		cb := tx.Bucket(chunkBucket)
		for it := newDigestIterator(ents); it.ent() != nil; it.next(1) {
			dig := it.digest()
			if _, ok := locs[dig]; ok {
				continue
			}
			loc := cb.Get(dig[:])
			if loc == nil {
				return fmt.Errorf("missing reference for chunk %s", dig)
			}
			locs[dig] = loadLoc(loc)
		}
		return nil
	})
	if err != nil {
		return err
	}

	var cloneFailed atomic.Bool
	readFds := s.dupCacheFds()
	defer func() {
		for _, fds := range readFds {
			_ = unix.Close(fds.cacheFd)
		}
	}()

	// create all directories first
	for _, ent := range ents {
		if ent.Type == pb.EntryType_DIRECTORY {
			p := filepath.Join(dest, ent.Path)
			if err = os.MkdirAll(p, 0o755); err != nil {
				return err
			}
			_ = unix.Lutimes(p, zeroTimeval)
		}
	}

	// create files/symlinks in parallel
	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())

	wasBare := false
	for i, ent := range ents {
		p := filepath.Join(dest, ent.Path)

		if wasBare {
			return errors.New("bare file must be only entry")
		} else if i == 0 {
			p = dest
			wasBare = ent.Type == pb.EntryType_REGULAR
		}

		eg.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			switch ent.Type {
			case pb.EntryType_DIRECTORY:
				return nil // done above
			case pb.EntryType_REGULAR:
				return s.materializeFile(ctx, p, ent, locs, readFds, &cloneFailed)
			case pb.EntryType_SYMLINK:
				if i == 0 {
					return errors.New("bare file can't be symlink")
				}
				err := unix.Symlink(string(ent.InlineData), p)
				if err == unix.EEXIST {
					// handle overlayfs store in interactive vm, shouldn't happen normally
					_ = os.Remove(p)
					err = unix.Symlink(string(ent.InlineData), p)
				}
				if err == nil {
					err = unix.Lutimes(p, zeroTimeval)
				}
				return err
			default:
				return errors.New("unknown entry type in manifest")
			}
		})
	}

	return eg.Wait()
}

func (s *Server) materializeFile(
	ctx context.Context,
	path string,
	ent *pb.Entry,
	locs map[cdig.CDig]erofs.SlabLoc,
	readFds map[uint16]slabFds,
	cloneFailed *atomic.Bool,
) (retErr error) {
	var dst *os.File
	defer func() {
		if dst != nil {
			retErr = cmp.Or(retErr, dst.Close())
			_ = unix.Lutimes(path, zeroTimeval)
		}
	}()
tryAgain:
	var err error
	dst, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fs.FileMode(ent.FileMode()))
	if err != nil {
		return err
	}

	if len(ent.InlineData) > 0 {
		_, err = dst.Write(ent.InlineData)
		return err
	}

	var buf []byte
	digs := cdig.FromSliceAlias(ent.Digests)
	cshift := ent.ChunkShiftDef()
	roundedUp := false
	for i, dig := range digs {
		// check between chunks too, so a large file stops within one chunk
		// (at most 1<<shift.MaxChunkShift bytes) of the client giving up.
		if err := ctx.Err(); err != nil {
			return err
		}
		loc := locs[dig]
		size := cshift.FileChunkSize(ent.Size, i == len(digs)-1)
		// The chunk is recorded present, but its data can still be missing: lost in a crash,
		// or punched under us. A copy wouldn't notice, so check, and fetch it again.
		for refetched := false; ; refetched = true {
			if !cloneFailed.Load() {
				sizeUp := int(s.blockShift.Roundup(size))
				roundedUp = sizeUp != int(size)
				err = s.cloneChunk(readFds[loc.SlabId].cacheFd, loc, dst, int64(i)<<cshift, sizeUp)
				// err = unix.IoctlFileCloneRange(
				// 	int(dst.Fd()),
				// 	&unix.FileCloneRange{
				// 		Src_fd:      int64(cfd),
				// 		Src_offset:  uint64(loc.Addr) << s.blockShift,
				// 		Src_length:  uint64(sizeUp),
				// 		Dest_offset: uint64(i) << common.ChunkShift,
				// 	})
				switch err {
				case syscall.EINVAL, syscall.EOPNOTSUPP, syscall.EXDEV, io.ErrShortWrite, errCachefdNotFound:
					log.Printf("CopyFileRange: %s, using plain copy", err)
					cloneFailed.Store(true)
					dst.Close()
					goto tryAgain
				}
			} else {
				if buf == nil {
					buf = s.chunkPool.Get(int(cshift.Size()))
					defer s.chunkPool.Put(buf)
				}
				b := buf[:size]
				if testHookMaterializeChunk != nil {
					testHookMaterializeChunk(loc)
				}
				if err = s.getKnownChunk(loc, b); err == nil {
					if err = dig.Check(b); err != nil {
						err = fmt.Errorf("%w %d:%d: %w", errMissingChunk, loc.SlabId, loc.Addr, err)
					} else {
						_, err = dst.Write(b)
					}
				}
			}
			if err == nil {
				break
			} else if refetched || !errors.Is(err, errMissingChunk) {
				return err
			}
			log.Printf("materialize: %v, fetching chunk %s again", err, dig)
			if err = s.refetchChunk(ctx, loc, dig); err != nil {
				return fmt.Errorf("fetching chunk %s again: %w", dig, err)
			}
		}
	}
	if roundedUp {
		// we have to round up blocks when using CopyFileRange, so truncate the last one
		return unix.Ftruncate(int(dst.Fd()), ent.Size)
	}
	return nil
}

// cloneChunk copies sizeUp bytes of the chunk at loc from its slab's backing file cfd to dst
// at woff. The copy reads a hole in the backing file as zeros, so check that the chunk's space
// has data before and after the copy. A chunk's data only goes away by being punched (or in a
// crash, which the first check catches), so data at both checks means the copy got it.
func (s *Server) cloneChunk(cfd int, loc erofs.SlabLoc, dst *os.File, woff int64, sizeUp int) error {
	if cfd <= 0 {
		return errCachefdNotFound
	}
	roff := int64(loc.Addr) << s.blockShift
	if err := checkSlabData(cfd, loc, roff, sizeUp); err != nil {
		return err
	}
	if testHookMaterializeChunk != nil {
		testHookMaterializeChunk(loc)
	}
	coff := roff // CopyFileRange advances it
	rsize, err := unix.CopyFileRange(cfd, &coff, int(dst.Fd()), &woff, sizeUp, 0)
	if err != nil {
		return err
	} else if rsize != sizeUp {
		// we rounded size up to our erofs block size (4k for now), but it's possible
		// the target fs is using larger blocks. in that case this may fail.
		return io.ErrShortWrite
	}
	return checkSlabData(cfd, loc, roff, sizeUp)
}

// checkSlabData returns errMissingChunk if [off, off+n) of fd has a hole.
func checkSlabData(fd int, loc erofs.SlabLoc, off int64, n int) error {
	hole, err := unix.Seek(fd, off, unix.SEEK_HOLE)
	if err == unix.ENXIO {
		hole = off // at or past the end of the file
	} else if err != nil {
		return nil // can't tell: the filesystem may not support SEEK_HOLE
	}
	if hole < off+int64(n) {
		return fmt.Errorf("%w %d:%d: hole in slab", errMissingChunk, loc.SlabId, loc.Addr)
	}
	return nil
}
