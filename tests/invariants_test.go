package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/daemon"
	"github.com/dnr/styx/pb"
)

// End-of-test checks, run from cleanup after the daemon has stopped.
//
// The db layout is copied from daemon/const.go, daemon/daemon.go and
// daemon/catalog.go:
//
//	chunk:    digest -> slab u16 LE, addr u32 LE, then 10-byte sph prefixes
//	slab:     slab id u16 BE -> bucket of addr u32 BE -> digest,
//	          and addr|1<<31 -> "" when the chunk is present
//	image:    sph string -> pb.DbImage
//	manifest: sph string -> pb.SignedMessage
//	catalogf: name \0 sph -> ""   (name has "M/" prefix for manifests)
//	catalogr: sph -> name
const (
	invPresentMask    = 1 << 31
	invReservedBlocks = 4
	invManifestSlab   = 10000
	invSphPrefixBytes = 10
	invManifestPrefix = "M/"
	invMaxReports     = 40
)

type invLoc struct {
	slab uint16
	addr uint32
}

func invSlabKey(id uint16) []byte { return binary.BigEndian.AppendUint16(nil, id) }
func invAddrKey(a uint32) []byte  { return binary.BigEndian.AppendUint32(nil, a) }

func invDecodeLoc(v []byte) invLoc {
	return invLoc{slab: binary.LittleEndian.Uint16(v), addr: binary.LittleEndian.Uint32(v[2:])}
}

// Get returns nil for a key stored with a nil value in some cases, so check
// existence with a cursor.
func invHas(b *bbolt.Bucket, k []byte) bool {
	if b == nil {
		return false
	}
	ck, _ := b.Cursor().Seek(k)
	return ck != nil && bytes.Equal(ck, k)
}

func invHasSphPrefix(loc []byte, sph daemon.Sph) bool {
	for rest := loc[6:]; len(rest) >= invSphPrefixBytes; rest = rest[invSphPrefixBytes:] {
		if bytes.Equal(rest[:invSphPrefixBytes], sph[:invSphPrefixBytes]) {
			return true
		}
	}
	return false
}

func (tb *testBase) invSlabFile(files map[uint16]*os.File, id uint16) *os.File {
	if f, ok := files[id]; ok {
		return f
	}
	var p string
	if id == invManifestSlab {
		p = filepath.Join(tb.cachedir, "_manifests_"+strconv.Itoa(int(id)))
	} else {
		pat := filepath.Join(tb.cachedir, "cache", "Ierofs,"+tb.tag, "@*", "D_slab_"+strconv.Itoa(int(id)))
		if m, _ := filepath.Glob(pat); len(m) == 1 {
			p = m[0]
		}
	}
	var f *os.File
	if p != "" {
		f, _ = os.Open(p)
	}
	files[id] = f
	return f
}

// checkInvariants opens styx.bolt read-only and checks that the buckets agree
// with each other and with the slab files. Violations are test errors;
// bookkeeping oddities that don't affect data are logged as warnings.
func (tb *testBase) checkInvariants() {
	t := tb.t
	dbPath := filepath.Join(tb.cachedir, "styx.bolt")
	if _, err := os.Stat(dbPath); err != nil {
		return
	}
	db, err := bbolt.Open(dbPath, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		t.Errorf("invariant: open %s: %v", dbPath, err)
		return
	}
	defer db.Close()

	var errs, warns []string
	bad := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }
	warn := func(format string, a ...any) { warns = append(warns, fmt.Sprintf(format, a...)) }

	files := make(map[uint16]*os.File)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	checked := make(map[cdig.CDig]bool)

	_ = db.View(func(tx *bbolt.Tx) error {
		cb := tx.Bucket([]byte("chunk"))
		slabroot := tx.Bucket([]byte("slab"))
		ib := tx.Bucket([]byte("image"))
		mb := tx.Bucket([]byte("manifest"))
		cfb := tx.Bucket([]byte("catalogf"))
		crb := tx.Bucket([]byte("catalogr"))
		if cb == nil || slabroot == nil || ib == nil || mb == nil || cfb == nil || crb == nil {
			bad("missing a top-level bucket")
			return nil
		}

		present := func(l invLoc) bool {
			sb := slabroot.Bucket(invSlabKey(l.slab))
			return invHas(sb, invAddrKey(l.addr|invPresentMask))
		}

		// read a chunk's bytes from its slab file and check them against the digest
		readChunk := func(d cdig.CDig, size int64) ([]byte, error) {
			v := cb.Get(d[:])
			if v == nil {
				return nil, fmt.Errorf("chunk %s has no chunk record", d)
			}
			l := invDecodeLoc(v)
			if !present(l) {
				return nil, fmt.Errorf("chunk %s at %d/%d is not marked present", d, l.slab, l.addr)
			}
			f := tb.invSlabFile(files, l.slab)
			if f == nil {
				return nil, fmt.Errorf("chunk %s: no backing file for slab %d", d, l.slab)
			}
			b := make([]byte, size)
			if _, err := f.ReadAt(b, int64(l.addr)<<blockShift); err != nil {
				return nil, fmt.Errorf("chunk %s: read %d bytes at %d/%d: %w", d, size, l.slab, l.addr, err)
			}
			if err := d.Check(b); err != nil {
				return nil, fmt.Errorf("chunk %s at %d/%d (%d bytes) is marked present but its bytes don't match: %w",
					d, l.slab, l.addr, size, err)
			}
			return b, nil
		}

		// chunk -> slab
		cur := cb.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if len(k) != cdig.Bytes {
				bad("chunk key %x has length %d", k, len(k))
				continue
			}
			d := cdig.FromBytes(k)
			if len(v) < 6 || (len(v)-6)%invSphPrefixBytes != 0 {
				bad("chunk %s: loc value has length %d", d, len(v))
				continue
			}
			if len(v) == 6 {
				bad("chunk %s: no store path references (prefetch fails with 'missing sph references')", d)
			}
			l := invDecodeLoc(v)
			sb := slabroot.Bucket(invSlabKey(l.slab))
			if sb == nil {
				bad("chunk %s: points at slab %d, which has no bucket", d, l.slab)
				continue
			}
			if got := sb.Get(invAddrKey(l.addr)); !bytes.Equal(got, k) {
				bad("chunk %s: slab %d addr %d holds %x, not this digest", d, l.slab, l.addr, got)
			}
			if l.addr < invReservedBlocks || uint64(l.addr) >= sb.Sequence() {
				bad("chunk %s: addr %d outside allocated range [%d, %d) of slab %d",
					d, l.addr, invReservedBlocks, sb.Sequence(), l.slab)
			}
		}

		// slab -> chunk
		scur := slabroot.Cursor()
		for sk, sv := scur.First(); sk != nil; sk, sv = scur.Next() {
			if sv != nil || len(sk) != 2 {
				bad("slab root has non-bucket key %x", sk)
				continue
			}
			id := binary.BigEndian.Uint16(sk)
			sb := slabroot.Bucket(sk)
			c := sb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if len(k) != 4 {
					bad("slab %d: key %x has length %d", id, k, len(k))
					continue
				}
				addr := binary.BigEndian.Uint32(k)
				if addr&invPresentMask != 0 {
					if !invHas(sb, invAddrKey(addr&^invPresentMask)) {
						warn("slab %d: present marker for addr %d, which holds no chunk", id, addr&^invPresentMask)
					}
					continue
				}
				if len(v) != cdig.Bytes {
					bad("slab %d addr %d: value has length %d", id, addr, len(v))
					continue
				}
				cv := cb.Get(v)
				if cv == nil {
					bad("slab %d addr %d: chunk %s has no chunk record", id, addr, cdig.FromBytes(v))
					continue
				}
				if len(cv) >= 6 {
					if l := invDecodeLoc(cv); l.slab != id || l.addr != addr {
						bad("slab %d addr %d: chunk %s record points to %d/%d instead", id, addr, cdig.FromBytes(v), l.slab, l.addr)
					}
				}
			}
		}

		// images -> manifests -> chunks
		icur := ib.Cursor()
		for k, v := icur.First(); k != nil; k, v = icur.Next() {
			sphStr := string(k)
			var img pb.DbImage
			if err := proto.Unmarshal(v, &img); err != nil {
				bad("image %s: unmarshal: %v", sphStr, err)
				continue
			}
			sph, _, err := daemon.ParseSph(sphStr)
			if err != nil {
				bad("image key %q is not a store path hash", sphStr)
				continue
			}
			// gc with the default flags deletes Unmounted images and traces the
			// manifests of everything else, so those manifests must be readable.
			kept := img.MountState != pb.MountState_Unmounted
			mv := mb.Get(k)
			if mv == nil {
				if kept {
					bad("image %s (%s, %s) has no manifest; gc keeps %s images and needs their manifests",
						sphStr, img.StorePath, img.MountState, img.MountState)
				}
				continue
			}
			var sm pb.SignedMessage
			if err := proto.Unmarshal(mv, &sm); err != nil || sm.Msg == nil {
				bad("image %s: manifest envelope doesn't unmarshal: %v", sphStr, err)
				continue
			}
			ent := sm.Msg
			data := ent.InlineData
			if len(data) == 0 {
				mdigs := cdig.FromSliceAlias(ent.Digests)
				cs := ent.ChunkShiftDef()
				msph := sph
				msph[0] ^= 1 // makeManifestSph
				var buf bytes.Buffer
				ok := true
				for i, md := range mdigs {
					if cv := cb.Get(md[:]); len(cv) >= 6 && kept && !invHasSphPrefix(cv, msph) {
						bad("image %s: manifest chunk %s doesn't reference the manifest sph", sphStr, md)
					}
					b, err := readChunk(md, cs.FileChunkSize(ent.Size, i == len(mdigs)-1))
					if err != nil {
						if kept {
							bad("image %s (%s): manifest chunk %d/%d: %v", sphStr, img.MountState, i, len(mdigs), err)
						}
						ok = false
						break
					}
					buf.Write(b)
				}
				if !ok {
					continue
				}
				data = buf.Bytes()
			}
			var m pb.Manifest
			if err := proto.Unmarshal(data, &m); err != nil {
				bad("image %s: manifest doesn't unmarshal: %v", sphStr, err)
				continue
			}
			for _, e := range m.Entries {
				ds := cdig.FromSliceAlias(e.Digests)
				cs := e.ChunkShiftDef()
				for i, d := range ds {
					cv := cb.Get(d[:])
					if cv == nil {
						if kept {
							bad("image %s (%s): %s chunk %d has no chunk record", sphStr, img.MountState, e.Path, i)
						}
						continue
					}
					if kept && len(cv) >= 6 && !invHasSphPrefix(cv, sph) {
						bad("image %s: %s chunk %d (%s) doesn't reference the image", sphStr, e.Path, i, d)
					}
					if checked[d] || len(cv) < 6 || !present(invDecodeLoc(cv)) {
						continue
					}
					checked[d] = true
					if _, err := readChunk(d, cs.FileChunkSize(e.Size, i == len(ds)-1)); err != nil {
						bad("image %s: %s chunk %d: %v", sphStr, e.Path, i, err)
					}
				}
			}
		}

		// manifests -> images
		mcur := mb.Cursor()
		for k, _ := mcur.First(); k != nil; k, _ = mcur.Next() {
			if ib.Get(k) == nil {
				bad("manifest %s has no image record", k)
			}
		}

		// catalog forward <-> reverse, and catalog -> images
		fcur := cfb.Cursor()
		for k, _ := fcur.First(); k != nil; k, _ = fcur.Next() {
			name, h, ok := bytes.Cut(k, []byte{0})
			if !ok || len(h) != len(daemon.Sph{}) {
				bad("catalogf key %q is malformed", k)
				continue
			}
			if rv := crb.Get(h); !bytes.Equal(rv, name) {
				bad("catalogf %q: catalogr has %q", name, rv)
			}
			isph := daemon.SphFromBytes(h)
			if bytes.HasPrefix(name, []byte(invManifestPrefix)) {
				isph[0] ^= 1
			}
			if ib.Get([]byte(isph.String())) == nil {
				bad("catalog entry %q -> %s has no image record", name, isph.String())
			}
		}
		rcur := crb.Cursor()
		for k, v := rcur.First(); k != nil; k, v = rcur.Next() {
			fk := append(append(bytes.Clone(v), 0), k...)
			if !invHas(cfb, fk) {
				bad("catalogr %x -> %q has no catalogf entry", k, v)
			}
		}

		if gb := tx.Bucket([]byte("gcstate")); gb != nil {
			if k, _ := gb.Cursor().First(); k != nil {
				warn("gcstate still has unfinished punch records")
			}
		}
		return nil
	})

	for i, w := range warns {
		if i == invMaxReports {
			t.Logf("invariant warning: ... %d more", len(warns)-i)
			break
		}
		t.Log("invariant warning:", w)
	}
	for i, e := range errs {
		if i == invMaxReports {
			t.Errorf("invariant violated: ... %d more", len(errs)-i)
			break
		}
		t.Error("invariant violated:", e)
	}
}

// fds of this process that point into the test's temp dirs, at the devnode, or
// at cachefiles object fds (anon inodes)
func (tb *testBase) testFds() map[string]string {
	out := make(map[string]string)
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return out
	}
	root := filepath.Dir(tb.basetmpdir) + "/"
	for _, ent := range ents {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", ent.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, root) || target == devnode || strings.Contains(target, "cachefiles") {
			out[ent.Name()] = target
		}
	}
	return out
}

// checkLeaks reports mounts and fds the test left behind. Mounts are errors;
// fds are only logged ("leak:" prefix) since the daemon runs in this process
// and a leak there would otherwise fail every test.
func (tb *testBase) checkLeaks() {
	t := tb.t
	root := filepath.Dir(tb.basetmpdir) + "/"
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if f := strings.Fields(line); len(f) > 4 && strings.HasPrefix(f[4], root) {
				t.Errorf("leak: mount left at %s (%s)", f[4], line)
			}
		}
	}
	for fd, target := range tb.testFds() {
		if tb.startFds[fd] == target {
			continue // was open before this test started
		}
		t.Logf("leak: fd %s -> %s still open after the daemon stopped", fd, target)
	}
}
