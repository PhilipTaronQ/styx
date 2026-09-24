package manifester

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DataDog/zstd"
	"github.com/aws/aws-lambda-go/lambdaurl"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/common"
	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/common/errgroup"
	"github.com/dnr/styx/common/shift"
	"github.com/dnr/styx/pb"
)

const (
	DefaultSmallFileCutoff = 224
	// maxSmallFileCutoff = 480

	SmallManifestCutoff = 32 * 1024

	defaultChunkDiffParallel = 60

	// max size of json-encoded stats. if we add more stats, may need to increase this
	statsSpace = 256
)

type (
	server struct {
		cfg *Config
		mb  *ManifestBuilder

		httpServer *http.Server
	}

	Config struct {
		Bind string

		ChunkDiffZstdLevel int
		ChunkDiffParallel  int
	}
)

func NewManifestServer(cfg Config, mb *ManifestBuilder) (*server, error) {
	if cfg.ChunkDiffParallel < 0 {
		return nil, fmt.Errorf("ChunkDiffParallel must be positive, not %d", cfg.ChunkDiffParallel)
	} else if cfg.ChunkDiffParallel == 0 {
		// a limit of 0 would make every chunk fetch block forever
		cfg.ChunkDiffParallel = defaultChunkDiffParallel
	}
	return &server{
		cfg: &cfg,
		mb:  mb,
	}, nil
}

func (s *server) validateManifestReq(r *ManifestReq, upstreamHost string) error {
	if r.DigestAlgo != s.mb.params.DigestAlgo {
		return fmt.Errorf("mismatched digest algo (this server uses %s, not %s)",
			s.mb.params.DigestAlgo, r.DigestAlgo)
	} else if r.DigestBits != cdig.Bits {
		return fmt.Errorf("mismatched digest bits (this server uses %d, not %d)",
			cdig.Bits, r.DigestBits)
	} else if !validShard(r.ShardTotal, r.ShardIndex) {
		return fmt.Errorf("invalid shard %d of %d", r.ShardIndex, r.ShardTotal)
	}

	// the builder's allow-list, which its upstream client also applies to every redirect
	if !slices.Contains(s.mb.allowedUpstreams, upstreamHost) {
		return fmt.Errorf("invalid upstream %q", upstreamHost)
	}

	return nil
}

func (s *server) handleManifest(w http.ResponseWriter, req *http.Request) {
	var r ManifestReq
	if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
		log.Println("json parse error:", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	upstreamUrl, err := url.Parse(r.Upstream)
	if err != nil {
		log.Println("bad upstream url:", r.Upstream)
		w.WriteHeader(http.StatusBadRequest)
		return
	} else if err := s.validateManifestReq(&r, upstreamUrl.Host); err != nil {
		log.Println("validation error:", err, "for", r)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Println("req", r.StorePathHash, "from", r.Upstream)

	ctx := req.Context()
	mres, err := s.mb.Build(ctx, r.BuildMode, r.Upstream, r.StorePathHash, r.ShardTotal, r.ShardIndex, "", true)

	if err != nil {
		log.Println("build error:", err)
		writeError(w, err)
		return
	}

	if mres == nil {
		// another shard caches and returns the manifest
		w.Write([]byte(fmt.Sprintf("shard %d/%d ok", r.ShardIndex, r.ShardTotal)))
		return
	}

	w.Header().Set(ManifestHeader, "1")
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(mres.Bytes)
}

func (s *server) handleChunkDiff(w http.ResponseWriter, req *http.Request) {
	reqBody, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil || len(reqBody) == 0 {
		log.Println("body read error:", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var r pb.ManifesterChunkDiffReq
	switch req.Header.Get(common.CTHdr) {
	case common.CTJson:
		var jr DeprecatedChunkDiffReq
		if err := json.Unmarshal(reqBody, &jr); err != nil {
			writeError(w, fmt.Errorf("%w: json unmarshal: %w", ErrReq, err))
			return
		}
		// translate json to proto for backwards compatibility
		r.Params = &pb.GlobalParams{
			DigestAlgo: cdig.Algo,
			DigestBits: cdig.Bits,
		}
		r.Req = []*pb.ManifesterChunkDiffReq_Req{{
			Bases:            jr.Bases,
			Reqs:             jr.Reqs,
			ExpandBeforeDiff: jr.ExpandBeforeDiff,
		}}
	case common.CTProto:
		if err := proto.Unmarshal(reqBody, &r); err != nil {
			writeError(w, fmt.Errorf("%w: proto unmarshal: %w", ErrReq, err))
			return
		}
	default:
		writeError(w, fmt.Errorf("%w: invalid content-type", ErrReq))
		return
	}

	if r.Params.GetDigestAlgo() != cdig.Algo || r.Params.GetDigestBits() != cdig.Bits {
		writeError(w, fmt.Errorf("%w: parameter mismatch", ErrReq))
		return
	} else if err := checkChunkDiffReq(&r); err != nil {
		writeError(w, err)
		return
	}
	baseBudget, reqBudget := newDiffBudget(), newDiffBudget()

	// load requested chunks
	start := time.Now()

	n := len(r.Req)
	baseDatas := make([][]byte, n)
	reqDatas := make([][]byte, n)
	stats := ChunkDiffStats{Reqs: n}

	// fetch all in parallel: normal requests have just one "req" so no real limit.
	// [multi-]recompress diffs will call gzip or xz for each "expand". lambda has limited cpu
	// and 1024 fd limit, so limit the number of chunk series we do in parallel.
	expandGrp := errgroup.WithContext(req.Context())
	expandGrp.SetLimit(4)

	egCtx := errgroup.WithContext(req.Context())
	egCtx.SetLimit(s.cfg.ChunkDiffParallel)

	for i, ri := range r.Req {
		if ri.ExpandBeforeDiff != "" {
			stats.Expands++
		}
		stats.BaseChunks += len(ri.Bases) / cdig.Bytes
		stats.ReqChunks += len(ri.Reqs) / cdig.Bytes

		expandGrp.Go(func() (err error) {
			baseDatas[i], err = s.expand(egCtx, cdig.FromSliceAlias(ri.Bases), ri.ExpandBeforeDiff, baseBudget)
			return
		})
		expandGrp.Go(func() (err error) {
			reqDatas[i], err = s.expand(egCtx, cdig.FromSliceAlias(ri.Reqs), ri.ExpandBeforeDiff, reqBudget)
			return
		})
	}

	// wait for all
	if err := expandGrp.Wait(); err != nil {
		egCtx.Cancel(err) // stop fetches that are still running
		log.Println("chunk read error:", err)
		writeError(w, err)
		return
	}
	dlDone := time.Now()

	w.Header().Set(common.CTHdr, "application/octet-stream")

	// write lengths of req data (for recompress; if not recompressing, caller should know req
	// data length already)
	lensHdr, reqDataLen, err := makeLengthsHeader(reqDatas)
	if err != nil {
		log.Println("proto marshal error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set(LengthsHeader, lensHdr)

	cw := countWriter{w: w}
	baseData := common.ContiguousBytes(baseDatas)
	zw := zstd.NewWriterPatcher(&cw, s.cfg.ChunkDiffZstdLevel, baseData, reqDataLen+statsSpace)
	for _, data := range reqDatas {
		if _, err := zw.Write(data); err != nil {
			log.Printf("zstd write error %v", err)
			w.WriteHeader(http.StatusInternalServerError) // this will fail if zstd wrote anything
			return
		}
	}

	// flush to get accurate count in cw
	if err := zw.Flush(); err != nil {
		log.Printf("zstd flush error %v", err)
		w.WriteHeader(http.StatusInternalServerError) // this will fail if zstd wrote anything
		return
	}

	stats.BaseBytes = len(baseData)
	stats.ReqBytes = int(reqDataLen)
	stats.DiffBytes = cw.c
	stats.DlTotalMs = dlDone.Sub(start).Milliseconds()
	stats.ZstdMs = time.Since(dlDone).Milliseconds()
	statsEnc, err := json.Marshal(stats)
	if err != nil || len(statsEnc) > statsSpace {
		statsEnc = []byte("{}")
	}
	if pad := statsSpace - len(statsEnc); pad > 0 {
		statsEnc = append(statsEnc, bytes.Repeat([]byte{'\n'}, pad)...)
	}
	if _, err := zw.Write(statsEnc); err != nil {
		log.Printf("zstd write stats error %v", err)
		w.WriteHeader(http.StatusInternalServerError) // this will fail if zstd wrote anything
		return
	}
	if err := zw.Close(); err != nil {
		log.Printf("zstd close error %v", err)
		w.WriteHeader(http.StatusInternalServerError) // this will fail if zstd wrote anything
		return
	}

	log.Printf("diff done %#v", stats)
}

func (s *server) expand(egCtx *errgroup.Group, digests []cdig.CDig, expand string, budget *diffBudget) ([]byte, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	switch expand {
	case ExpandGz:
		pr, pw := io.Pipe()
		go func() {
			pw.CloseWithError(s.fetchChunkSeries(egCtx, digests, pw, &budget.fetch))
		}()
		// if we stop reading early, make writes to the write end fail
		defer pr.CloseWithError(errExpandDone)
		gzr, err := gzip.NewReader(pr)
		if err != nil {
			return nil, err
		}
		return io.ReadAll(budgetReader{gzr, &budget.expand})

	case ExpandXz:
		decompress := exec.CommandContext(egCtx, common.XzBin, "-d")
		pw, err := decompress.StdinPipe()
		if err != nil {
			return nil, err
		}
		pr, err := decompress.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err = decompress.Start(); err != nil {
			return nil, err
		}
		fetchErr := make(chan error, 1)
		go func() {
			err := s.fetchChunkSeries(egCtx, digests, pw, &budget.fetch)
			pw.Close()
			fetchErr <- err
		}()
		out, readErr := io.ReadAll(budgetReader{pr, &budget.expand})
		if readErr != nil {
			egCtx.Cancel(readErr) // kill xz, which may be blocked writing to us
		}
		waitErr := decompress.Wait() // also closes pw, so the fetch can't block writing to it
		// a failed fetch truncates xz's input (or kills it), so its error is the real one
		return common.ValOrErr(out, cmp.Or(<-fetchErr, readErr, waitErr))

	default:
		var out bytes.Buffer
		out.Grow(len(digests) << shift.DefaultChunkShift)
		err := s.fetchChunkSeries(egCtx, digests, &out, &budget.fetch)
		return common.ValOrErr(out.Bytes(), err)
	}
}

func (s *server) fetchChunkSeries(egCtx *errgroup.Group, digests []cdig.CDig, out io.Writer, budget *atomic.Int64) error {
	// TODO: ew, use separate setting?
	cs := s.mb.cs

	// Each fetch sends its error along with its data: a failed fetch cancels egCtx only
	// after its result is sent, so checking egCtx for errors could miss it, and the failed
	// chunk would read as empty.
	type result struct {
		b   []byte
		err error
	}
	chs := make(chan chan result, egCtx.Limit())
	go func() {
		for i := 0; i < len(digests) && egCtx.Err() == nil; i++ {
			digest := digests[i]
			digestStr := digest.String()
			ch := make(chan result, 1)
			chs <- ch
			egCtx.Go(func() error {
				b, err := cs.Get(egCtx, ChunkReadPath, digestStr, nil)
				ch <- result{b, err}
				return err
			})
		}
		close(chs)
	}()

	// read every result, so that the producer isn't left blocked, but stop writing at the
	// first error
	var err error
	for ch := range chs {
		r := <-ch
		if err != nil {
			continue
		}
		err = cmp.Or(r.err, context.Cause(egCtx))
		if err == nil && budget.Add(-int64(len(r.b))) < 0 {
			err = errChunkDiffTooBig
		}
		if err == nil {
			_, err = out.Write(r.b)
		}
		if err != nil {
			egCtx.Cancel(err)
		}
	}
	return cmp.Or(err, context.Cause(egCtx))
}

const (
	// gz and xz expansion can produce much more than ChunkDiffMaxBytes
	chunkDiffMaxExpandedBytes = 4 * ChunkDiffMaxBytes
)

var (
	errChunkDiffTooBig = fmt.Errorf("%w: chunk diff data is too big", ErrReq)
	errExpandDone      = errors.New("expansion finished")
)

// checkChunkDiffReq enforces the documented limits on digests (and on the number of
// requests, which is otherwise unbounded).
func checkChunkDiffReq(r *pb.ManifesterChunkDiffReq) error {
	if len(r.Req) > ChunkDiffMaxDigests {
		return fmt.Errorf("%w: too many requests (%d)", ErrReq, len(r.Req))
	}
	var bases, reqs int
	for _, ri := range r.Req {
		if len(ri.Bases)%cdig.Bytes != 0 || len(ri.Reqs)%cdig.Bytes != 0 {
			return fmt.Errorf("%w: digest lists must be a multiple of %d bytes", ErrReq, cdig.Bytes)
		}
		bases += len(ri.Bases) / cdig.Bytes
		reqs += len(ri.Reqs) / cdig.Bytes
	}
	if bases > ChunkDiffMaxDigests || reqs > ChunkDiffMaxDigests {
		return fmt.Errorf("%w: too many digests (%d bases, %d reqs, max %d each)",
			ErrReq, bases, reqs, ChunkDiffMaxDigests)
	}
	return nil
}

// diffBudget is the number of bytes left for one side (bases or reqs) of a chunk diff, as
// fetched from the chunk store and after expansion.
type diffBudget struct {
	fetch, expand atomic.Int64
}

func newDiffBudget() *diffBudget {
	b := &diffBudget{}
	b.fetch.Store(ChunkDiffMaxBytes)
	b.expand.Store(chunkDiffMaxExpandedBytes)
	return b
}

type budgetReader struct {
	r    io.Reader
	left *atomic.Int64
}

func (br budgetReader) Read(p []byte) (int, error) {
	n, err := br.r.Read(p)
	if br.left.Add(-int64(n)) < 0 {
		return n, errChunkDiffTooBig
	}
	return n, err
}

func (s *server) handleChunk(w http.ResponseWriter, r *http.Request) {
	// This is for local testing only, real usage goes to s3 directly!
	localWrite, ok := s.mb.cs.(*localChunkStoreWrite)
	if !ok {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if l := len(parts); l < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if parts[l-2] != "chunk" && parts[l-2] != "manifest" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[len(parts)-1]

	fn := path.Join(localWrite.dir, key)
	f, err := os.Open(fn)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer f.Close()
	// log.Println("get chunk", key)
	w.Header().Set("Content-Encoding", "zstd")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, f)
}

func (s *server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc(ManifestPath, s.handleManifest)
	mux.HandleFunc(ChunkDiffPath, s.handleChunkDiff)
	mux.HandleFunc(ChunkReadPath, s.handleChunk)
	mux.HandleFunc(ManifestCachePath, s.handleChunk)

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambdaurl.Start(mux)
		return nil
	}

	s.httpServer = &http.Server{
		Addr:    s.cfg.Bind,
		Handler: mux,
	}
	return s.httpServer.ListenAndServe()
}

func (s *server) Stop() {
	_ = s.httpServer.Close()
}

func writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrReq):
		code = http.StatusExpectationFailed
	case common.IsNotFound(err):
		code = http.StatusNotFound
	case errors.Is(err, ErrInternal):
		code = http.StatusInternalServerError
	}
	http.Error(w, err.Error(), code)
}

func makeLengthsHeader(reqDatas [][]byte) (string, int64, error) {
	var lens pb.Lengths
	lens.Length = make([]int64, len(reqDatas))
	var reqDataLen int64
	for i, data := range reqDatas {
		lens.Length[i] = int64(len(data))
		reqDataLen += int64(len(data))
	}
	lensEnc, err := proto.Marshal(&lens)
	if err != nil {
		return "", 0, err
	}
	return base64.RawURLEncoding.EncodeToString(lensEnc), reqDataLen, nil
}
