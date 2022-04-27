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
	"runtime"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/jsonrl"
	"github.com/klauspost/compress/zstd"
)

// RowFormat is the interface through which
// input streams are converted into aligned
// output blocks.
type RowFormat interface {
	Convert(r io.Reader, dst *ion.Chunker) error
	// Name is the name of the format
	// that will be included in an index description.
	Name() string
	UseHints(schema []byte) error
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
	decomp   func(r io.Reader) (io.Reader, error)
	compname string
	hints    *jsonrl.Hint
}

func (j *jsonConverter) Name() string {
	if j.compname == "" {
		return "json"
	}
	return "json." + j.compname
}

func (j *jsonConverter) Convert(r io.Reader, dst *ion.Chunker) error {
	rc := r
	var err, err2 error
	if j.decomp != nil {
		rc, err = j.decomp(r)
		if err != nil {
			return err
		}
	}
	err = jsonrl.Convert(rc, dst, j.hints)
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

func (j *jsonConverter) UseHints(hints []byte) error {
	s, err := jsonrl.ParseHint(hints)
	if err != nil {
		return err
	}
	j.hints = s
	return nil
}

type ionConverter struct{}

func (i ionConverter) Name() string { return "ion" }

func (i ionConverter) Convert(r io.Reader, dst *ion.Chunker) error {
	_, err := dst.ReadFrom(r)
	if err != nil {
		return fmt.Errorf("converting UnsafeION: %w", err)
	}
	return nil
}

func (i ionConverter) UseHints(schema []byte) error {
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

// SuffixToFormat is a list of known
// filename suffixes that correspond
// to known constructors for RowFormat
// objects.
var SuffixToFormat = map[string]func() RowFormat{
	".json": func() RowFormat {
		return &jsonConverter{decomp: nil}
	},
	".json.zst": func() RowFormat {
		return &jsonConverter{
			decomp: func(r io.Reader) (io.Reader, error) {
				return zstd.NewReader(r)
			},
			compname: "zst",
		}
	},
	".json.gz": func() RowFormat {
		return &jsonConverter{
			decomp: func(r io.Reader) (io.Reader, error) {
				return gzip.NewReader(r)
			},
			compname: "gz",
		}
	},
}

// Converter performs single- or
// multi-stream conversion of a list of inputs
// in parallel.
type Converter struct {
	// Prepend, if R is not nil,
	// is a blockfmt-formatted stream
	// of data to prepend to the output stream.
	Prepend struct {
		R       io.ReadCloser
		Trailer *Trailer
	}
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
	// trailer built by the writer. This is only
	// set if the object was written successfully.
	trailer *Trailer
}

// static errors known to be fatal to decoding
var isFatal = []error{
	jsonrl.ErrNoMatch,
	jsonrl.ErrTooLarge,
	gzip.ErrHeader,
	zstd.ErrReservedBlockType,
	zstd.ErrMagicMismatch,
	zstd.ErrUnknownDictionary,
	zstd.ErrWindowSizeExceeded,
	zstd.ErrWindowSizeTooSmall,
	zstd.ErrBlockTooSmall,

	// TODO: ion errors from transcoding?
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

// MultiStream returns whether the configuration of Converter
// would lead to a multi-stream upload.
func (c *Converter) MultiStream() bool {
	return len(c.Inputs) > 1 && (c.Parallel <= 0 || c.Parallel > 1)
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
	if len(c.Inputs) == 0 {
		return errors.New("no inputs or merge sources")
	}
	if len(c.Inputs) == 0 {
		// proxy uploader is never closed by `runSingle()` or `runMulti()`
		// in this case -> finish uploading the uncommitted parts + trailer
		return c.Output.Close(nil)
	}
	if c.MultiStream() {
		return c.runMulti()
	}
	return c.runSingle()
}

func (c *Converter) runSingle() error {
	comp := compr.Compression(c.Comp)
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
	cn := ion.Chunker{
		W:          w,
		Align:      w.InputAlign,
		RangeAlign: c.FlushMeta,
	}
	err := c.runPrepend(&cn)
	if err != nil {
		return err
	}
	for i := range c.Inputs {
		err := c.Inputs[i].F.Convert(c.Inputs[i].R, &cn)
		err2 := c.Inputs[i].R.Close()
		if err == nil {
			err = err2
		}
		if err != nil {
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

func (c *Converter) runPrepend(cn *ion.Chunker) error {
	if c.Prepend.R == nil {
		return nil
	}
	cn.WalkTimeRanges = collectRanges(c.Prepend.Trailer)
	d := Decoder{}
	d.Set(c.Prepend.Trailer, 0)
	_, err := d.Copy(cn, c.Prepend.R)
	c.Prepend.R.Close()
	cn.WalkTimeRanges = nil
	return err
}

func (c *Converter) runMulti() error {
	comp := compr.Compression(c.Comp)
	if comp == nil {
		return fmt.Errorf("compression %q unavailable", c.Comp)
	}
	w := &MultiWriter{
		Output:     c.Output,
		Comp:       comp,
		InputAlign: c.Align,
		TargetSize: c.TargetSize,
		// try to make the blocks at least
		// half the target size
		MinChunksPerBlock: c.FlushMeta / (c.Align * 2),
	}
	p := c.Parallel
	if p <= 0 {
		p = runtime.GOMAXPROCS(0)
	}
	if p >= len(c.Inputs) {
		p = len(c.Inputs)
	}
	ic := make(chan *Input, p)
	errs := make(chan error, p)
	for i := 0; i < p; i++ {
		wc, err := w.Open()
		if err != nil {
			close(ic)
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
					errs <- fmt.Errorf("prepend: %w", err)
					return
				}
			}
			for in := range ic {
				err := in.F.Convert(in.R, &cn)
				err2 := in.R.Close()
				if err == nil {
					err = err2
				}
				if err != nil {
					in.Err = err
					errs <- fmt.Errorf("%s: %w", in.Path, err)
					return
				}
			}
			err := cn.Flush()
			if err != nil {
				errs <- err
				return
			}
			errs <- wc.Close()
		}(i)
	}
	var outerr error
loop:
	for i := range c.Inputs {
		select {
		case ic <- &c.Inputs[i]:
		case outerr = <-errs:
			p--
			break loop
		}
	}
	close(ic)
	var extra int
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
