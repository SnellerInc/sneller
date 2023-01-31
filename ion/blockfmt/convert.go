// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package blockfmt

import (
	"compress/flate"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"runtime"

	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion"
	"github.com/SnellerInc/sneller/jsonrl"
	"github.com/SnellerInc/sneller/xsv"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/exp/slices"
)

const (
	// DefaultMaxReadsInFlight is the default maximum
	// number of outstanding read operations in Converter.Run
	DefaultMaxReadsInFlight = 400
	// DefaultMaxBytesInFlight is the default maximum
	// number of bytes in flight in Converter.Run
	DefaultMaxBytesInFlight = 80 * 1024 * 1024
)

// RowFormat is the interface through which
// input streams are converted into aligned
// output blocks.
type RowFormat interface {
	// Convert should read data from r and write
	// rows into dst. For each row written to dst,
	// the provided list of constants should also be inserted.
	Convert(r io.Reader, dst *ion.Chunker, constants []ion.Field) error
	// Name is the name of the format
	// that will be included in an index description.
	Name() string
}

// Input is a combination of
// an input stream and a row-formatting function.
// Together they produce output blocks.
type Input struct {
	// Path and ETag are used to
	// populate the ObjectInfo
	// in an Index built from a Converter.
	Path, ETag string
	// Size is the size of the input, in bytes
	Size int64
	// R is the source of unformatted data
	R io.ReadCloser
	// F is the formatter that produces output blocks
	F RowFormat
	// Err is an error specific
	// to this input that is populated
	// by Converter.Run.
	Err error
}

type jsonConverter struct {
	name         string
	decomp       func(r io.Reader) (io.Reader, error)
	hints        *jsonrl.Hint
	isCloudtrail bool
}

func (j *jsonConverter) Name() string {
	return j.name
}

func (j *jsonConverter) Convert(r io.Reader, dst *ion.Chunker, cons []ion.Field) error {
	rc := r
	var err, err2 error
	if j.decomp != nil {
		rc, err = j.decomp(r)
		if err != nil {
			return err
		}
	}
	if j.isCloudtrail {
		err = jsonrl.ConvertCloudtrail(rc, dst, cons)
	} else {
		err = jsonrl.Convert(rc, dst, j.hints, cons)
	}
	if j.decomp != nil {
		// if the decompressor (i.e. gzip.Reader)
		// has a Close() method, then use that;
		// this lets us check the integrity of
		// gzip checksums, etc.
		if cc, ok := rc.(io.Closer); ok {
			err2 = cc.Close()
		}
	}
	if err == nil {
		err = err2
	}
	return err
}

type xsvConverter struct {
	name   string
	ch     xsv.RowChopper
	decomp func(r io.Reader) (io.Reader, error)
	hints  *xsv.Hint
}

func (t *xsvConverter) Convert(r io.Reader, dst *ion.Chunker, cons []ion.Field) error {
	rc := r
	var err, err2 error
	if t.decomp != nil {
		rc, err = t.decomp(r)
		if err != nil {
			return err
		}
	}

	err = xsv.Convert(rc, dst, t.ch, t.hints, cons)
	if t.decomp != nil {
		// if the decompressor (i.e. gzip.Reader)
		// has a Close() method, then use that;
		// this lets us check the integrity of
		// gzip checksums, etc.
		if cc, ok := rc.(io.Closer); ok {
			err2 = cc.Close()
		}
	}
	if err == nil {
		err = err2
	}
	return err
}

func (t *xsvConverter) Name() string {
	return t.name
}

type ionConverter struct{}

func (i ionConverter) Name() string { return "ion" }

func (i ionConverter) Convert(r io.Reader, dst *ion.Chunker, cons []ion.Field) error {
	_, err := dst.ReadFrom(r, cons)
	if err != nil {
		return fmt.Errorf("converting UnsafeION: %w", err)
	}
	return nil
}

// UnsafeION converts raw ion by
// decoding and re-encoding it.
//
// NOTE: UnsafeION is called UnsafeION
// because the ion package has not been
// hardened against arbitrary user input.
// FIXME: harden the ion package against
// malicious input and then rename this
// to something else.
func UnsafeION() RowFormat {
	return ionConverter{}
}

func isCompressed(r RowFormat) bool {
	switch r := r.(type) {
	case *jsonConverter:
		return r.decomp != nil
	case *xsvConverter:
		return r.decomp != nil
	default:
		return false
	}
}

// SuffixToFormat is a list of known
// filename suffixes that correspond
// to known constructors for RowFormat
// objects.
var SuffixToFormat = make(map[string]func(hints []byte) (RowFormat, error))

func MustSuffixToFormat(suffix string) RowFormat {
	f := SuffixToFormat[suffix]
	if f == nil {
		panic(fmt.Sprintf("cannot find suffix %q", suffix))
	}
	rf, err := f(nil) // create the format (without hints)
	if err != nil {
		panic(err)
	}
	return rf
}

func init() {
	decompressors := map[string]func(r io.Reader) (io.Reader, error){
		"": nil,
		".gz": func(r io.Reader) (io.Reader, error) {
			rz, err := gzip.NewReader(r)
			err = noEOF(err, gzip.ErrHeader)
			return rz, err
		},
		".zst": func(r io.Reader) (io.Reader, error) {
			rz, err := zstd.NewReader(r)
			err = noEOF(err, zstd.ErrMagicMismatch)
			return rz, err
		},
	}

	// JSON formats
	for dn, dc := range decompressors {
		decName := dn
		decomp := dc
		SuffixToFormat[".json"+decName] = func(h []byte) (RowFormat, error) {
			var hints *jsonrl.Hint
			if h != nil {
				var err error
				hints, err = jsonrl.ParseHint(h)
				if err != nil {
					return nil, err
				}
			}

			return &jsonConverter{
				name:   "json" + decName,
				decomp: decomp,
				hints:  hints,
			}, nil
		}
	}

	// Cloudtrail JSON format (only GZIP needed)
	SuffixToFormat[".cloudtrail.json.gz"] = func(h []byte) (RowFormat, error) {
		if h != nil {
			return nil, errors.New("cloudtrail doesn't support hints")
		}
		return &jsonConverter{
			name:         "cloudtrail.json.gz",
			decomp:       decompressors[".gz"],
			isCloudtrail: true,
		}, nil
	}

	// CSV encoder
	for dn, dc := range decompressors {
		decName := dn
		decomp := dc
		SuffixToFormat[".csv"+decName] = func(h []byte) (RowFormat, error) {
			if h == nil {
				return nil, errors.New("CSV requires hints")
			}
			hints, err := xsv.ParseHint(h)
			if err != nil {
				return nil, err
			}
			return &xsvConverter{
				name:   "csv" + decName,
				decomp: decomp,
				hints:  hints,
				ch: &xsv.CsvChopper{
					SkipRecords: hints.SkipRecords,
					Separator:   hints.Separator,
				},
			}, nil
		}
	}

	// TSV encoder
	for dn, dc := range decompressors {
		decName := dn
		decomp := dc
		SuffixToFormat[".tsv"+decName] = func(h []byte) (RowFormat, error) {
			if h == nil {
				return nil, errors.New("TSV requires hints")
			}
			hints, err := xsv.ParseHint(h)
			if err != nil {
				return nil, err
			}
			if hints.Separator != 0 && hints.Separator != '\t' {
				return nil, errors.New("TSV doesn't support a custom separator")
			}
			return &xsvConverter{
				name:   "tsv" + decName,
				decomp: decomp,
				hints:  hints,
				ch: &xsv.TsvChopper{
					SkipRecords: hints.SkipRecords,
				},
			}, nil
		}
	}
}

// Template is a templated constant field.
type Template struct {
	Field string // Field is the name of the field to be generated.
	// Eval should generate an ion datum
	// from the input object.
	Eval func(in *Input) (ion.Datum, error)
}

// Converter performs single- or
// multi-stream conversion of a list of inputs
// in parallel.
type Converter struct {
	// Prepend, if R is not nil,
	// is a blockfmt-formatted stream
	// of data to prepend to the output stream.
	Prepend struct {
		// R should read data from Trailer
		// starting at offset Trailer.Blocks[0].Offset.
		// Converter will read bytes up to Trailer.Offset.
		R       io.ReadCloser
		Trailer *Trailer
	}
	// Constants is the list of templated constants
	// to be inserted into the ingested data.
	Constants []ion.Field

	// Inputs is the list of input
	// streams that need to be converted
	// into the output format.
	Inputs []Input
	// Output is the Uploader to which
	// data will be written. The Uploader
	// will be wrapped in a CompressionWriter
	// or MultiWriter depending on the number
	// of input streams and the parallelism setting.
	Output Uploader
	// Comp is the name of the compression
	// algorithm used for uploaded data blocks.
	Comp string
	// Align is the pre-compression alignment
	// of chunks written to the uploader.
	Align int
	// FlushMeta is the maximum interval
	// at which metadata is flushed.
	// Note that metadata may be flushed
	// below this interval if there is not
	// enough input data to make the intervals this wide.
	FlushMeta int
	// TargetSize is the target size of
	// chunks written to the Uploader.
	TargetSize int
	// Parallel is the maximum parallelism of
	// uploads. If Parallel is <= 0, then
	// GOMAXPROCS is used instead.
	Parallel int
	// MinInputBytesPerCPU is used to determine
	// the level of parallelism used for converting data.
	// If this setting is non-zero, then the converter
	// will try to ensure that there are at least this
	// many bytes of input data for each independent
	// parallel stream used for conversion.
	//
	// Picking a larger setting for MinInputBytesPerCPU
	// will generally increase the effiency of the
	// conversion (in bytes converted per CPU-second)
	// and also the compactness of the output data.
	MinInputBytesPerCPU int64
	// MaxReadsInFlight is the maximum number of
	// prefetched reads in flight. If this is less
	// than or equal to zero, then DefaultMaxReadsInFlight is used.
	MaxReadsInFlight int

	// DisablePrefetch, if true, disables
	// prefetching of inputs.
	DisablePrefetch bool

	// trailer built by the writer. This is only
	// set if the object was written successfully.
	trailer *Trailer
}

// static errors known to be fatal to decoding
var isFatal = []error{
	jsonrl.ErrNoMatch,
	jsonrl.ErrTooLarge,
	ion.ErrTooLarge,
	gzip.ErrHeader,
	zstd.ErrReservedBlockType,
	zstd.ErrMagicMismatch,
	zstd.ErrUnknownDictionary,
	zstd.ErrWindowSizeExceeded,
	zstd.ErrWindowSizeTooSmall,
	zstd.ErrBlockTooSmall,

	// these can be produced from the first
	// fs.File.Read call on at least s3.File
	fs.ErrNotExist,
	s3.ErrETagChanged,

	// TODO: ion errors from transcoding?
}

func noEOF(err, sub error) error {
	if errors.Is(err, io.EOF) {
		return sub
	}
	return err
}

// IsFatal returns true if the error
// is an error known to be fatal when
// returned from blockfmt.Format.Convert.
// (A fatal error is one that will not
// disappear on a retry.)
func IsFatal(err error) bool {
	for i := range isFatal {
		if errors.Is(err, isFatal[i]) {
			return true
		}
	}
	var cie flate.CorruptInputError
	return errors.As(err, &cie)
}

func (c *Converter) parallel() int {
	p := c.Parallel
	if p == 0 {
		p = runtime.GOMAXPROCS(0)
	}
	// clamp to # inputs:
	if p > len(c.Inputs) {
		p = len(c.Inputs)
	}
	if c.MinInputBytesPerCPU == 0 {
		return p
	}

	// calculate the number of input bytes
	// based on the raw input sizes and the
	// presence or absence of compression
	//
	// without actually opening the files,
	// just make a conservative guess on the
	// compression ratio
	insize := int64(0)
	const compressionRatio = 5
	for i := range c.Inputs {
		size := c.Inputs[i].Size
		if isCompressed(c.Inputs[i].F) {
			size *= compressionRatio
		}
		insize += size
	}
	max := int(c.MinInputBytesPerCPU / insize)
	if max == 0 {
		max = 1
	}
	if max < p {
		p = max
	}
	return p
}

// MultiStream returns whether the configuration of Converter
// would lead to a multi-stream upload.
func (c *Converter) MultiStream() bool {
	return c.parallel() > 1
}

// Run runs the conversion operation
// and returns the first error it ecounters.
// Additionally, it will populate c.Inputs[*].Err
// with any errors associated with the inputs.
// Note that Run stops at the first encountered
// error, so if one of the Inputs has Err set,
// then subsequent items in Inputs may not
// have been processed at all.
func (c *Converter) Run() error {
	// keep this deterministic:
	slices.SortFunc(c.Constants, func(x, y ion.Field) bool {
		return x.Label < y.Label
	})
	if len(c.Inputs) == 0 && c.Prepend.R == nil {
		return errors.New("no inputs or merge sources")
	}
	p := c.parallel()
	if p > 1 {
		return c.runMulti(p)
	}
	return c.runSingle()
}

func (c *Converter) prefetch() int {
	if c.MaxReadsInFlight > 0 {
		return c.MaxReadsInFlight
	}
	return DefaultMaxReadsInFlight
}

func (c *Converter) runSingle() error {
	cname := c.Comp
	if cname == "zstd" {
		cname = "zstd-better"
	}
	comp := getCompressor(cname)
	if comp == nil {
		return fmt.Errorf("compression %q unavailable", c.Comp)
	}
	w := &CompressionWriter{
		Output:     c.Output,
		Comp:       comp,
		InputAlign: c.Align,
		TargetSize: c.TargetSize,
		// try to make the blocks at least
		// half the target size
		MinChunksPerBlock: c.FlushMeta / (c.Align * 2),
	}
	if len(c.Constants) > 0 {
		w.Trailer.Sparse.consts = ion.NewStruct(nil, c.Constants)
	}
	cn := ion.Chunker{
		W:          w,
		Align:      w.InputAlign,
		RangeAlign: c.FlushMeta,
	}
	err := c.fastPrepend(w)
	if err != nil {
		return err
	}
	err = c.runPrepend(&cn)
	if err != nil {
		return err
	}
	ready := make([]chan struct{}, len(c.Inputs))
	next := 1
	inflight := int64(0) // # bytes being prefetched
	for i := range c.Inputs {
		// make sure that prefetching has completed
		// on this entry if we had queued it up
		var saved chan struct{}
		if ready[i] != nil {
			<-ready[i]
			saved, ready[i] = ready[i], nil
			inflight -= c.Inputs[i].Size
		}
		// fast-forward the prefetch pointer
		// if we had a run of large files
		if next <= i {
			next = i + 1
		}
		// start readahead on inputs that we will need
		for !c.DisablePrefetch && inflight < DefaultMaxBytesInFlight && (next-i) < c.prefetch() && next < len(c.Inputs) {
			if saved != nil {
				ready[next] = saved
				saved = nil
			} else {
				ready[next] = make(chan struct{}, 1)
			}
			go func(r io.Reader, done chan struct{}) {
				r.Read([]byte{})
				done <- struct{}{}
			}(c.Inputs[next].R, ready[next])
			inflight += c.Inputs[next].Size
			next++
		}

		err := c.Inputs[i].F.Convert(c.Inputs[i].R, &cn, c.Constants)
		err2 := c.Inputs[i].R.Close()
		if err == nil {
			err = err2
		}
		if err != nil {
			// wait for prefetching to stop
			for _, c := range ready[i:next] {
				if c != nil {
					<-c
				}
			}
			// close everything we haven't already closed
			tail := c.Inputs[i+1:]
			for j := range tail {
				tail[j].R.Close()
			}
			c.Inputs[i].Err = err
			return err
		}
	}
	err = cn.Flush()
	if err != nil {
		return err
	}
	err = w.Close()
	c.trailer = &w.Trailer
	return err
}

type trailerWriter interface {
	writeStart(r io.Reader, t *Trailer) error
}

func (c *Converter) fastPrepend(tw trailerWriter) error {
	if c.Prepend.R == nil {
		return nil
	}
	return tw.writeStart(c.Prepend.R, c.Prepend.Trailer)
}

type compressWriter interface {
	writeCompressed(p []byte) error
	setSymbols(st *ion.Symtab)
}

var (
	_ compressWriter = &CompressionWriter{}
	_ compressWriter = &singleStream{}
)

// fastWriter exists to bypass ion.Chunker + (*CompressionWriter|*MultiWriter)
// when we are prepending complete blocks; we can simply re-use the compressed
// data as long as the blocks are (mostly) full
type fastWriter struct {
	slowpath   bool
	configured bool
	zd         zion.Decoder
	tmp        []byte
	dst        *ion.Chunker
	trailer    *Trailer
	inner      compressWriter
	skipped    int
	targetFull int
	maxchunks  int
}

var _ ZionWriter = &fastWriter{}

func (f *fastWriter) ConfigureZion(_ []string) bool {
	f.zd.SetWildcard() // decompress everything
	f.configured = true
	// fall back to slow path if blocks aren't 90% full
	f.targetFull = (9 * f.dst.Align) / 10
	return true
}

// consume raw compressed zion data;
// we always decompress it but we often avoid
// re-compressing the input if the block is sufficiently full
func (f *fastWriter) Write(p []byte) (int, error) {
	var err error
	if !f.configured {
		panic("fastWriter not configured as ZionWriter?")
	}
	f.tmp, err = f.zd.Decode(p, f.tmp[:0])
	if err != nil {
		return 0, err
	}
	// for as long as we have mostly-full blocks,
	// just update the chunker symbol table
	//
	// TODO: figure out if we can judge "full" blocks
	// without decompressing *all* of the data
	if !f.slowpath {
		if f.maxchunks > 0 && len(f.tmp) >= f.targetFull {
			if ion.IsBVM(f.tmp) || ion.TypeOf(f.tmp) == ion.AnnotationType {
				_, err = f.dst.Symbols.Unmarshal(f.tmp)
				if err != nil {
					return 0, err
				}
			}
			f.skipped += f.dst.Align
			f.maxchunks--
			return len(p), f.inner.writeCompressed(p)
		}
		f.slowpath = true
		if f.skipped > 0 {
			f.dst.FastForward(f.skipped)
			f.inner.setSymbols(&f.dst.Symbols)
			for _, p := range f.dst.WalkTimeRanges {
				min, max, ok := f.trailer.Sparse.MinMax(p)
				if ok {
					f.dst.SetTimeRange(p, min, max)
				}
			}
		}
	}
	return f.dst.Write(f.tmp)
}

func (c *Converter) runPrepend(cn *ion.Chunker) error {
	if c.Prepend.R == nil {
		return nil
	}
	t := c.Prepend.Trailer
	cn.WalkTimeRanges = collectRanges(t)
	d := Decoder{}
	size := int64(0)
	if len(t.Blocks) > 0 {
		size = t.Offset - t.Blocks[0].Offset
	}
	if size == 0 {
		return nil
	}
	dst := (io.Writer)(cn)

	// if we are appending to a short block (i.e. size < RangeAlign)
	// then try to consume all but the final chunk without re-compressing
	if len(t.Blocks) == 1 &&
		c.Comp == "zion" && t.Algo == "zion" && // not changing compression
		cn.Align == 1<<t.BlockShift && // not changing block size
		t.Blocks[0].Chunks > 1 && // more than 1 chunk to use fast-path
		cn.RangeAlign >= t.Blocks[0].Chunks<<t.BlockShift {
		dst = &fastWriter{
			dst:       cn,
			trailer:   t,
			inner:     cn.W.(compressWriter),
			maxchunks: t.Blocks[0].Chunks - 1, // skip over all but the last chunk
		}
	}
	d.Set(c.Prepend.Trailer, len(t.Blocks))
	_, err := d.Copy(dst, io.LimitReader(c.Prepend.R, size))
	c.Prepend.R.Close()
	cn.WalkTimeRanges = nil
	return err
}

func (c *Converter) runMulti(p int) error {
	cname := c.Comp
	if cname == "zstd" {
		cname = "zstd-better"
	}
	comp := getCompressor(cname)
	if comp == nil {
		return fmt.Errorf("compression %q unavailable", c.Comp)
	}
	w := &MultiWriter{
		Output:     c.Output,
		Algo:       c.Comp,
		InputAlign: c.Align,
		TargetSize: c.TargetSize,
		// try to make the blocks at least
		// half the target size
		MinChunksPerBlock: c.FlushMeta / (c.Align * 2),
	}
	if len(c.Constants) > 0 {
		w.Trailer.Sparse.consts = ion.NewStruct(nil, c.Constants)
	}
	err := c.fastPrepend(w)
	if err != nil {
		return err
	}
	startc := make(chan *Input, p)
	readyc := startc
	if p >= len(c.Inputs) {
		p = len(c.Inputs)
	} else if !c.DisablePrefetch {
		max := c.prefetch()
		if max > len(c.Inputs) {
			max = len(c.Inputs)
		}
		readyc = doPrefetch(startc, max, DefaultMaxBytesInFlight)
	}
	errs := make(chan error, p)
	// NOTE: consume must be called
	// before the send on errs so that
	// the consumption of inputs happens
	// strictly before we return from this
	// function call
	consume := func(in chan *Input) {
		for in := range in {
			in.R.Close()
		}
	}
	for i := 0; i < p; i++ {
		wc, err := w.Open()
		if err != nil {
			close(readyc)
			return err
		}
		go func(i int) {
			cn := ion.Chunker{
				W:          wc,
				Align:      w.InputAlign,
				RangeAlign: c.FlushMeta,
			}
			if i == 0 {
				err := c.runPrepend(&cn)
				if err != nil {
					consume(startc)
					errs <- fmt.Errorf("prepend: %w", err)
					return
				}
			}
			for in := range startc {
				err := in.F.Convert(in.R, &cn, c.Constants)
				err2 := in.R.Close()
				if err == nil {
					err = err2
				}
				if err != nil {
					consume(startc)
					in.Err = err
					errs <- fmt.Errorf("%s: %w", in.Path, err)
					return
				}
			}
			err := cn.Flush()
			if err != nil {
				consume(startc)
				errs <- err
				return
			}
			errs <- wc.Close()
		}(i)
	}
	for i := range c.Inputs {
		readyc <- &c.Inputs[i]
	}
	// will cause readyc to be closed
	// when the queue has been drained:
	close(readyc)
	var extra int
	var outerr error
	for i := 0; i < p; i++ {
		err := <-errs
		if outerr == nil {
			outerr = err
		} else {
			extra++
		}
	}
	if outerr != nil {
		if extra > 0 {
			return fmt.Errorf("%w (and %d other errors)", outerr, extra)
		}
		return outerr
	}
	// don't finalize unless everything
	// up to this point succeeded
	if err := w.Close(); err != nil {
		return err
	}
	c.trailer = &w.Trailer
	return nil
}

func (c *Converter) Trailer() *Trailer {
	return c.trailer
}
