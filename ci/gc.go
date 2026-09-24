package ci

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nix-community/go-nix/pkg/narinfo"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/errgroup"
	"github.com/dnr/styx/manifester"
	"github.com/dnr/styx/pb"
)

type (
	gc struct {
		now     time.Time
		stage   func(string)
		summary *strings.Builder
		zp      *common.ZstdCtxPool
		s3      *s3.Client
		bucket  string
		age     time.Duration
		// Objects modified less than grace before now are never deleted.
		grace time.Duration
		lim   struct{ trace, chunk, list, del, batch int }

		summaryMu    sync.Mutex // guards summary
		toDelete     sync.Map   // key -> size
		delCount     atomic.Int64
		delSize      atomic.Int64
		totalCount   atomic.Int64
		totalSize    atomic.Int64
		recent       atomic.Int64 // unreachable but modified within grace
		rescued      atomic.Int64 // condemned, then reached by a root written during gc
		refreshed    atomic.Int64 // condemned, then deleted or modified before removal
		delErrors    atomic.Int64
		tracedRoots  sync.Map
		traced       sync.Map
		goodNi       sync.Map
		goodNar      sync.Map
		goodManifest sync.Map
		goodChunk    sync.Map
	}

	GCConfig struct {
		Bucket string
		MaxAge time.Duration
	}
)

// gcGrace is how long an unreachable object is kept after it was last modified. A manifester
// build that overlaps a GC writes its chunks and manifest before the root that keeps them, and
// the chunk store refreshes old objects that it reuses (see manifester.RefreshAge), so the
// objects such a build refers to are always newer than this.
const gcGrace = 2 * manifester.RefreshAge

func GCLocal(ctx context.Context, cfg GCConfig) error {
	var sb strings.Builder
	s3, err := getS3Cli()
	if err != nil {
		return err
	}
	gc := gc{
		now:     time.Now(),
		stage:   func(s string) { log.Println("======================", "STAGE", s) },
		summary: &sb,
		zp:      common.GetZstdCtxPool(),
		s3:      s3,
		bucket:  cfg.Bucket,
		age:     cfg.MaxAge,
		grace:   gcGrace,
		lim: struct{ trace, chunk, list, del, batch int }{
			trace: 10,
			chunk: 3,
			list:  5,
			del:   10,
			batch: 100,
		},
	}
	return gc.run(ctx)
}

// logln and logf may be called concurrently.
func (gc *gc) logln(args ...any) {
	log.Println(args...)
	gc.summaryMu.Lock()
	defer gc.summaryMu.Unlock()
	fmt.Fprintln(gc.summary, args...)
}
func (gc *gc) logf(msg string, args ...any) {
	log.Printf(msg, args...)
	gc.summaryMu.Lock()
	defer gc.summaryMu.Unlock()
	fmt.Fprintf(gc.summary, msg+"\n", args...)
}

func (gc *gc) writeBuildRoot(ctx context.Context, br *pb.BuildRoot, key string) error {
	data, err := proto.Marshal(br)
	if err != nil {
		return err
	}

	key = manifester.BuildRootPath[1:] + key
	z := gc.zp.Get()
	defer gc.zp.Put(z)
	d, err := z.Compress(nil, data)
	if err != nil {
		return err
	}
	_, err = gc.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          &gc.bucket,
		Key:             &key,
		Body:            bytes.NewReader(d),
		CacheControl:    aws.String("public, max-age=31536000"),
		ContentType:     aws.String("application/octet-stream"),
		ContentEncoding: aws.String("zstd"),
	})
	return err
}

func (gc *gc) readOne(ctx context.Context, key string, dst []byte) ([]byte, error) {
	res, err := gc.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &gc.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if aws.ToString(res.ContentEncoding) == "zstd" {
		z := gc.zp.Get()
		defer gc.zp.Put(z)
		if dst == nil {
			body, err = z.Decompress(nil, body)
		} else {
			var n int
			n, err = z.DecompressInto(dst, body)
			if err == nil {
				body = dst[:n]
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

func (gc *gc) listPrefix(ctx context.Context, prefix string, f func(s3types.Object) error) error {
	var token *string
	for {
		res, err := gc.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &gc.bucket,
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return err
		}
		for _, c := range res.Contents {
			if err := f(c); err != nil {
				return err
			}
		}
		if res.NextContinuationToken == nil {
			return nil
		}
		token = res.NextContinuationToken
	}
}

// run traces the build roots, lists the bucket, then lists the roots again and traces those
// written since the first listing, by builds that overlap this GC: they may refer to old
// objects that the list just condemned. Then it removes what's still unreachable.
func (gc *gc) run(ctx context.Context) error {
	start := time.Now()
	if gc.grace <= 0 {
		return errors.New("gc grace window must be positive")
	}
	if roots, err := gc.loadRoots(ctx, true); err != nil {
		gc.logln("gc loadRoots error:", err)
		return err
	} else if err := gc.trace(ctx, roots); err != nil {
		gc.logln("gc trace error:", err)
		return err
	} else if err := gc.list(ctx); err != nil {
		gc.logln("gc list error:", err)
		return err
	} else if roots, err := gc.loadRoots(ctx, false); err != nil {
		gc.logln("gc reload roots error:", err)
		return err
	} else if err := gc.trace(ctx, roots); err != nil {
		gc.logln("gc trace new roots error:", err)
		return err
	} else if err := gc.remove(ctx); err != nil {
		gc.logln("gc remove error:", err)
		return err
	}
	gc.logf("gc done in %s", time.Since(start))
	return nil
}

// Remove deletes in phases, referrers before what they refer to, so that a delete that fails
// or is cut short never leaves a surviving referrer without its referents.
const (
	phaseRoots  = iota // build roots
	phaseRefs          // manifests and narinfos
	phaseLeaves        // chunks, nars and anything unexpected
	numPhases
)

// cutoff is the latest modification time of an object that GC may delete.
func (gc *gc) cutoff() time.Time { return gc.now.Add(-gc.grace) }

// classify reports key's remove phase, whether the traced roots reach it, and whether it's a
// kind of object that GC knows about.
func (gc *gc) classify(key string) (phase int, live, known bool) {
	has := func(m *sync.Map, k any) bool { _, ok := m.Load(k); return ok }
	if strings.HasPrefix(key, manifester.BuildRootPath[1:]) {
		return phaseRoots, has(&gc.tracedRoots, path.Base(key)), true
	} else if strings.HasPrefix(key, manifester.ManifestCachePath[1:]) {
		return phaseRefs, has(&gc.goodManifest, path.Base(key)), true
	} else if key == "nixcache/nix-cache-info" {
		return phaseLeaves, true, true
	} else if rest, ok := strings.CutPrefix(key, "nixcache/nar/"); ok {
		return phaseLeaves, has(&gc.goodNar, rest), true
	} else if rest, ok := strings.CutSuffix(key, ".narinfo"); ok && strings.HasPrefix(rest, "nixcache/") {
		return phaseRefs, has(&gc.goodNi, strings.TrimPrefix(rest, "nixcache/")), true
	} else if rest, ok := strings.CutPrefix(key, manifester.ChunkReadPath[1:]); ok {
		b, err := base64.RawURLEncoding.DecodeString(rest)
		if err != nil || len(b) != cdig.Bytes {
			return phaseLeaves, false, false
		}
		return phaseLeaves, has(&gc.goodChunk, cdig.FromBytes(b)), true
	}
	return phaseLeaves, false, false
}

// consider condemns o if nothing reaches it and it's older than the grace window.
func (gc *gc) consider(o s3types.Object) {
	key, size := aws.ToString(o.Key), aws.ToInt64(o.Size)
	gc.totalCount.Add(1)
	gc.totalSize.Add(size)
	_, live, known := gc.classify(key)
	if !known {
		gc.logln("unexpected file", key)
	}
	if live {
		return
	} else if o.LastModified == nil || !o.LastModified.Before(gc.cutoff()) {
		gc.recent.Add(1)
		return
	}
	gc.toDelete.Store(key, size)
	gc.delCount.Add(1)
	gc.delSize.Add(size)
}

func (gc *gc) logRoot(first bool, args ...any) {
	if first {
		gc.logln(args...)
	}
}

// loadRoots lists build roots and returns the fresh ones that haven't been traced yet. On the
// first call, it also condemns stale roots.
func (gc *gc) loadRoots(ctx context.Context, first bool) ([]string, error) {
	gc.stage("GC LOAD ROOTS")
	var roots []string
	err := gc.listPrefix(ctx, manifester.BuildRootPath[1:], func(o s3types.Object) error {
		key := aws.ToString(o.Key)
		base := path.Base(key)
		parts := strings.Split(base, "@") // "build", time, relid, styx commit
		fresh := false
		if len(parts) < 4 {
			gc.logRoot(first, "bad root key", base)
		} else if tm, err := time.Parse(time.RFC3339, parts[1]); err != nil {
			gc.logRoot(first, "bad root key", base, "time parse error", err)
		} else if gc.now.Sub(tm) > gc.age {
			gc.logRoot(first, "stale root", base)
		} else {
			fresh = true
		}
		if fresh {
			if _, loaded := gc.tracedRoots.LoadOrStore(base, true); !loaded {
				log.Println("using root", base)
				roots = append(roots, base)
			}
		}
		if first {
			gc.consider(o)
		}
		return nil
	})
	return roots, err
}

func (gc *gc) trace(ctx context.Context, roots []string) error {
	gc.stage("GC TRACE")

	eg := errgroup.WithContext(ctx)
	eg.SetWorkLimit(cmp.Or(gc.lim.trace, 50))
	for _, key := range roots {
		key := key
		eg.Go(func() error { return gc.traceRoot(eg, key) })
	}
	return eg.Wait()
}

func (gc *gc) traceRoot(eg *errgroup.Group, key string) error {
	var root pb.BuildRoot
	if b, err := gc.readOne(eg, manifester.BuildRootPath[1:]+key, nil); err != nil {
		return err
	} else if err = proto.Unmarshal(b, &root); err != nil {
		return err
	}
	log.Printf("tracing root %s, %d sph, %d manifest", key, len(root.StorePathHash), len(root.Manifest))
	for _, sph := range root.StorePathHash {
		if _, loaded := gc.traced.LoadOrStore(sph, true); loaded {
			continue
		}
		eg.Go(func() error { return gc.traceSph(eg, sph) })
	}
	for _, mc := range root.Manifest {
		if _, loaded := gc.traced.LoadOrStore(mc, true); loaded {
			continue
		}
		eg.Go(func() error { return gc.traceManifest(eg, mc) })
	}
	return nil
}

func (gc *gc) traceSph(eg *errgroup.Group, sph string) error {
	// TODO: "nixcache" should be derived from configuration
	key := "nixcache/" + sph + ".narinfo"
	b, err := gc.readOne(eg, key, nil)
	if err != nil {
		if manifester.IsS3NotFound(err) {
			return nil // ignore if not found
		}
		return err
	}
	ni, err := narinfo.Parse(bytes.NewReader(b))
	if err != nil {
		return err
	}
	gc.goodNi.Store(sph, struct{}{})
	if path.Dir(ni.URL) != "nar" {
		return errors.New("unexpected nar url " + ni.URL)
	}
	gc.goodNar.Store(path.Base(ni.URL), struct{}{})
	log.Printf("traced %s = %s", key, path.Base(ni.StorePath)[33:])
	return nil
}

func (gc *gc) traceManifest(eg *errgroup.Group, mc string) error {
	key := manifester.ManifestCachePath[1:] + mc

	b, err := gc.readOne(eg, key, nil)
	if err != nil {
		if manifester.IsS3NotFound(err) {
			return nil // ignore if not found
		}
		return err
	}
	gc.goodManifest.Store(mc, struct{}{})
	// don't verify signature, assume it's good
	var sm pb.SignedMessage
	err = proto.Unmarshal(b, &sm)
	if err != nil {
		return err
	}

	var chunks, mchunks int
	b = sm.Msg.InlineData
	if b == nil {
		cshift := sm.Msg.ChunkShiftDef()
		b = make([]byte, sm.Msg.Size)
		subeg := errgroup.WithContext(eg)
		subeg.SetLimit(cmp.Or(gc.lim.chunk, 5))
		for i, dig := range cdig.FromSliceAlias(sm.Msg.Digests) {
			gc.goodChunk.Store(dig, struct{}{})
			mchunks++
			subeg.Go(func() error {
				key := manifester.ChunkReadPath[1:] + dig.String()
				start := i << cshift
				end := min(len(b), (i+1)<<cshift)
				_, err := gc.readOne(subeg, key, b[start:end])
				return err
			})
		}
		if err := subeg.Wait(); err != nil {
			return err
		}
	}

	var m pb.Manifest
	err = proto.Unmarshal(b, &m)
	if err != nil {
		return err
	}
	for _, ent := range m.Entries {
		for _, dig := range cdig.FromSliceAlias(ent.Digests) {
			gc.goodChunk.Store(dig, struct{}{})
			chunks++
		}
	}
	log.Printf("traced %s = %s, %d chunks, %d manifest chunks",
		key, path.Base(m.Meta.Narinfo.StorePath)[33:], chunks, mchunks)
	return nil
}

func (gc *gc) list(ctx context.Context) error {
	gc.stage("GC LIST")
	prefixes := []string{"nixcache/", manifester.ManifestCachePath[1:]}
	for _, pchar := range "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" {
		prefixes = append(prefixes, manifester.ChunkReadPath[1:]+string(pchar))
	}
	eg := errgroup.WithContext(ctx)
	eg.SetLimit(cmp.Or(gc.lim.list, 5))
	for _, prefix := range prefixes {
		eg.Go(func() error {
			return gc.listPrefix(eg, prefix, func(o s3types.Object) error {
				gc.consider(o)
				return nil
			})
		})
	}
	return eg.Wait()
}

func (gc *gc) remove(ctx context.Context) error {
	gc.stage("GC REMOVE")

	gc.logf("total : %9d objects, %14d bytes", gc.totalCount.Load(), gc.totalSize.Load())
	gc.logf("remove: %9d objects, %14d bytes", gc.delCount.Load(), gc.delSize.Load())
	gc.logf("keep  : %9d objects, %14d bytes", gc.totalCount.Load()-gc.delCount.Load(), gc.totalSize.Load()-gc.delSize.Load())
	gc.logf("recent: %9d unreachable objects modified within %s", gc.recent.Load(), gc.grace)

	var phases [numPhases][]string
	gc.toDelete.Range(func(k, _ any) bool {
		phase, _, _ := gc.classify(k.(string))
		phases[phase] = append(phases[phase], k.(string))
		return true
	})
	err := gc.removePhases(ctx, phases)
	if n := gc.rescued.Load(); n > 0 {
		gc.logf("kept %d objects reached by roots written during gc", n)
	}
	if n := gc.refreshed.Load(); n > 0 {
		gc.logf("kept %d objects deleted or modified during gc", n)
	}
	if n := gc.delErrors.Load(); n > 0 {
		gc.logf("delete errors: %d", n)
	}
	return err
}

func (gc *gc) removePhases(ctx context.Context, phases [numPhases][]string) error {
	for phase, keys := range phases {
		// Check reachability again here: roots written during GC, or referrers whose
		// deletes failed, may reach these.
		keys = slices.DeleteFunc(keys, func(key string) bool {
			_, live, _ := gc.classify(key)
			if live {
				gc.rescued.Add(1)
			}
			return live
		})
		slices.Sort(keys)
		failed, err := gc.deleteKeys(ctx, keys)
		if err != nil {
			// Some of this phase's deletes may or may not have happened, so don't touch
			// what they refer to.
			return err
		}
		if phase == phaseRefs && len(failed) > 0 {
			gc.logf("tracing %d manifests and narinfos that failed to delete", len(failed))
			if err := gc.keepReferents(ctx, failed); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteKeys deletes keys and returns those that S3 reported per-key errors for.
func (gc *gc) deleteKeys(ctx context.Context, keys []string) ([]string, error) {
	var mu sync.Mutex
	var failed []string
	eg := errgroup.WithContext(ctx)
	eg.SetLimit(cmp.Or(gc.lim.del, 20))
	for batch := range slices.Chunk(keys, cmp.Or(gc.lim.batch, 100)) {
		eg.Go(func() error {
			batch, err := gc.recheck(eg, batch)
			if err != nil || len(batch) == 0 {
				return err
			}
			res, err := gc.s3.DeleteObjects(eg, &s3.DeleteObjectsInput{
				Bucket: &gc.bucket,
				Delete: makeBatchDelete(batch),
			})
			if err != nil {
				return err
			}
			gc.delErrors.Add(int64(len(res.Errors)))
			mu.Lock()
			defer mu.Unlock()
			for _, e := range res.Errors {
				failed = append(failed, aws.ToString(e.Key))
			}
			return nil
		})
	}
	err := eg.Wait()
	return failed, err
}

// keepReferents traces manifests and narinfos that failed to delete, so that the next phase
// keeps the chunks and nars they refer to.
func (gc *gc) keepReferents(ctx context.Context, keys []string) error {
	eg := errgroup.WithContext(ctx)
	eg.SetLimit(cmp.Or(gc.lim.trace, 50))
	for _, key := range keys {
		if mc, ok := strings.CutPrefix(key, manifester.ManifestCachePath[1:]); ok {
			eg.Go(func() error { return gc.traceManifest(eg, mc) })
		} else if rest, ok := strings.CutSuffix(key, ".narinfo"); ok && strings.HasPrefix(rest, "nixcache/") {
			sph := strings.TrimPrefix(rest, "nixcache/")
			eg.Go(func() error { return gc.traceSph(eg, sph) })
		}
	}
	return eg.Wait()
}

// recheck returns the keys that still exist and are older than the grace window. The chunk
// store refreshes old objects that it reuses, so this keeps objects that a build overlapping
// this GC has started referring to since they were listed.
func (gc *gc) recheck(ctx context.Context, keys []string) ([]string, error) {
	del := make([]bool, len(keys))
	eg := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	for i, key := range keys {
		eg.Go(func() error {
			res, err := gc.s3.HeadObject(eg, &s3.HeadObjectInput{
				Bucket: &gc.bucket,
				Key:    &key,
			})
			if manifester.IsS3NotFound(err) {
				gc.refreshed.Add(1)
				return nil
			} else if err != nil {
				return err
			} else if res.LastModified == nil || !res.LastModified.Before(gc.cutoff()) {
				gc.refreshed.Add(1)
				return nil
			}
			del[i] = true
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for i, key := range keys {
		if del[i] {
			out = append(out, key)
		}
	}
	return out, nil
}

func makeBatchDelete(keys []string) *s3types.Delete {
	keys = slices.Clone(keys)
	objs := make([]s3types.ObjectIdentifier, len(keys))
	for i := range keys {
		objs[i].Key = &keys[i]
	}
	return &s3types.Delete{
		Objects: objs,
		Quiet:   aws.Bool(true),
	}
}
