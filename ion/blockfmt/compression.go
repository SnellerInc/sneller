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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"sync"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

var debugFree = false

// this is just a Blockdesc, but before
// we have frozen the ranges and built
// a sparse index of the offsets
type blockpart struct {
	offset int64
	chunks int
	ranges []TimeRange
}

func toDescs(lst []blockpart) []Blockdesc {
	out := make([]Blockdesc, len(lst))
	for i := range lst {
		out[i].Offset = lst[i].offset
		out[i].Chunks = lst[i].chunks
	}
	return out
}

// CompressionWriter is a single-stream
// io.Writer that accepts blocks from an
// ion.Chunker and concatenates and compresses
// them into an output format that allows for
// seeking through the decompressed blocks without
// actually performing any decompression in advance.
type CompressionWriter struct {
	// Output is the destination to which
	// the compressed data should be uploaded.
	Output Uploader
	// Comp is the compression algorithm to use.
	Comp compr.Compressor
	// InputAlign is the expected input alignment
	// of data blocks. CompressionWriter will disallow
	// calls to Write that do not have length
	// equal to InputAlign.
	InputAlign int
	// TargetSize is the target size of flushes
	// to Output. If TargetSize is zero, then
	// the output will be flushed around
	// Output.MinPartSize
	TargetSize int
	// Trailer is the trailer being
	// built by the compression writer
	Trailer
	// MinChunksPerBlock sets the minimum
	// number of chunks per output block.
	// Below this threshold, chunks are merged
	// into adjacent blocks.
	// See also MultiWriter.MinChunksPerBlock
	MinChunksPerBlock int

	// intermediate blocks, before we have
	// merged them and stuck them in Trailer
	blocks []blockpart

	buffer, alt []byte // buffered data
	bg          chan error
	partnum     int64 // previous part number
	offset      int64 // current real output offset
	minsize     int
	lastblock   int64
	flushblocks int
	skipChecks  bool

	// metadata to be attached
	// to the next block
	futureRange
}

// WrittenBlocks returns the number of blocks
// written to the CompressionWriter (i.e. the number
// of calls to w.Write)
func (w *CompressionWriter) WrittenBlocks() int { return len(w.blocks) }

type futureRange struct {
	buffered []TimeRange
}

type minMaxer interface {
	SetMinMax(path []string, min, max ion.Datum)
}

var _ minMaxer = &futureRange{}

// SetMinMax Sets the `min` and `max` values for the next ION chunk.
// This method should only be called once for each path.
func (f *futureRange) SetMinMax(path []string, min, max ion.Datum) {
	ts, ok := NewRange(path, min, max).(*TimeRange)
	if !ok {
		return // only supporting timestamp ranges right now
	}
	f.buffered = append(f.buffered, *ts)
}

func (f *futureRange) pop() []TimeRange {
	ret := f.buffered
	f.buffered = nil
	return ret
}

func (w *CompressionWriter) target() int {
	if w.minsize == 0 {
		w.minsize = w.Output.MinPartSize()
	}
	if w.TargetSize == 0 {
		w.TargetSize = w.minsize
	}
	return w.TargetSize
}

func (w *CompressionWriter) Flush() error {
	if w.flushblocks == 0 {
		if w.lastblock != w.offset {
			panic("flush blocks = 0 but lastblock != current offset")
		}
		return nil
	}
	w.blocks = append(w.blocks, blockpart{
		offset: w.lastblock,
		chunks: w.flushblocks,
		ranges: w.futureRange.pop(),
	})
	w.lastblock = w.offset
	w.flushblocks = 0
	return nil
}

// SkipChecks disable some runtime checks
// of the input data, which is ordinarily
// expected to be ion data. Do not use this
// except for testing.
func (w *CompressionWriter) SkipChecks() {
	w.skipChecks = true
}

// Write implements io.Writer.
// Each call to Write must be of w.InputAlign bytes.
func (w *CompressionWriter) Write(p []byte) (n int, err error) {
	if len(p) != w.InputAlign {
		return 0, fmt.Errorf("CompressionWriter.Write(%d) not equal to align of %d", len(p), w.InputAlign)
	}
	// layering violation here,
	// but we really need to be
	// pedantic about this:
	if w.flushblocks == 0 && !w.skipChecks && !ion.IsBVM(p) {
		return 0, fmt.Errorf("blockfmt.CompressionWriter.Write: blocks flushed, but no BVM")
	}
	w.flushblocks++
	before := len(w.buffer)
	w.buffer = appendFrame(w.buffer, w.Comp, p)
	w.offset += int64(len(w.buffer) - before)
	if len(w.buffer) >= w.target() {
		pn := w.partnum + 1
		w.partnum++
		if w.bg == nil {
			// no background uploads yet
			w.bg = make(chan error, 1)
			w.alt = make([]byte, 0, w.target())
		} else {
			// wait for previous background
			// upload to finish
			err := <-w.bg
			if err != nil {
				return 0, err
			}
		}
		buf := w.buffer
		go func() {
			w.bg <- w.Output.Upload(pn, buf)
		}()
		// swap buffers while the upload
		// is using the current buffer
		w.buffer, w.alt = w.alt[:0], w.buffer[:0]
	}
	return len(p), nil
}

// use a []blockpart to populate dst.Sparse and dst.Blocks,
// taking care to coalesce blocks where they fall below
// the provided minimum size
func finalize(dst *Trailer, src []blockpart, min int) {
	src = coalesce(src, min)
	for i := range src {
		for j := range src[i].ranges {
			r := &src[i].ranges[j]
			dst.Sparse.push(r.path, r.min, r.max)
		}
		dst.Sparse.bump()
	}
	dst.Blocks = toDescs(src)
}

// Close closes the compression writer
// and finalizes the output upload.
func (w *CompressionWriter) Close() error {
	// if there is a background upload happening,
	// then let's stop it
	if w.bg != nil {
		err := <-w.bg
		if err != nil {
			return err
		}
	}
	if w.flushblocks != 0 {
		println("flushblocks =", w.flushblocks)
		panic("missing Flush before Close")
	}
	finalize(&w.Trailer, w.blocks, w.MinChunksPerBlock)
	w.Trailer.Offset = w.offset
	trailer := w.Trailer.trailer(w.Comp, w.InputAlign)
	w.offset += int64(len(trailer))
	w.buffer = append(w.buffer, trailer...)
	return w.Output.Close(w.buffer)
}

func (t *Trailer) trailer(comp compr.Compressor, align int) []byte {
	var st ion.Symtab
	var buf ion.Buffer

	t.Version = 1
	t.Algo = comp.Name()
	t.BlockShift = bits.TrailingZeros(uint(align))

	t.Encode(&buf, &st)
	tail := buf.Bytes()
	buf.Set(nil)
	st.Marshal(&buf, true)
	buf.UnsafeAppend(tail)
	size := uint32(len(buf.Bytes()))
	// encode trailer with 4-byte suffix
	// so that we can easily determine
	// the start offset of the trailer
	// if we read the last few bytes of
	// the stream
	return append(buf.Bytes(), uint8(size), uint8(size>>8), uint8(size>>16), uint8(size>>24))
}

func fill(t *Trailer, rd io.ReaderAt, insize int64) error {
	guess := 1024 * 1024
	if insize < int64(guess) {
		guess = int(insize)
	}
	if guess <= 4 {
		return fmt.Errorf("size %d too small to possibly be valid", guess)
	}
	buf := malloc(guess)
	n, err := rd.ReadAt(buf, insize-int64(guess))
	if n == len(buf) {
		// if we got all the bytes, we don't
		// care if we hit an EOF error
		err = nil
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		free(buf)
		return err
	}
	tsize := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if guess < int(tsize)+4 {
		guess = int(tsize) + 4
		buf = realloc(buf, guess)
		n, err := rd.ReadAt(buf, insize-int64(guess))
		if n == len(buf) {
			err = nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			free(buf)
			return err
		}
	}
	defer free(buf)
	// now we know we've got enough data
	trailer := buf[len(buf)-int(tsize)-4:]
	st := new(ion.Symtab)
	trailer, err = st.Unmarshal(trailer)
	if err != nil {
		return err
	}
	err = t.Decode(st, trailer)
	if err != nil {
		return err
	}
	return nil
}

// append a compressed frame to dst
// that is wrapped in an ion 'blob' tag
func appendFrame(dst []byte, comp compr.Compressor, src []byte) []byte {
	base := len(dst)
	dst = append(dst,
		byte((ion.BlobType)<<4)|0xe,
		0, 0, 0, 0,
	)
	dst = comp.Compress(src, dst)
	size := len(dst) - base - 5
	dst[base+1] = byte(size>>21) & 0x7f
	dst[base+2] = byte(size>>14) & 0x7f
	dst[base+3] = byte(size>>7) & 0x7f
	dst[base+4] = byte(size&0x7f) | 0x80
	return dst
}

// ReadTrailer reads a trailer from an io.ReaderAt
// that has a backing size of 'size'.
func ReadTrailer(src io.ReaderAt, size int64) (*Trailer, error) {
	t := new(Trailer)
	if err := fill(t, src, size); err != nil {
		return nil, err
	}
	return t, nil
}

var decompScratch sync.Pool

func malloc(size int) []byte {
	r := decompScratch.Get()
	if r != nil {
		buf := r.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

func free(buf []byte) {
	if debugFree {
		rand.Read(buf)
	}
	//lint:ignore SA6002 inconsequential
	decompScratch.Put(buf)
}

func realloc(buf []byte, size int) []byte {
	if cap(buf) >= size {
		return buf[:size]
	}
	free(buf)
	return malloc(size)
}

type Decoder struct {
	BlockShift int
	Offset     int64
	Algo       string

	decomp compr.Decompressor
	frame  [5]byte
	tmp    []byte
}

// Set sets fields in the decoder in order
// to prepare it for reading blocks from the
// trailer t up to (but not including) lastblock.
// To prepare for reading the whole trailer,
// use Set(t, len(t.Blocks)).
func (d *Decoder) Set(t *Trailer, lastblock int) {
	d.BlockShift = t.BlockShift
	d.Algo = t.Algo
	if lastblock >= len(t.Blocks) {
		d.Offset = t.Offset
	} else {
		d.Offset = t.Blocks[lastblock].Offset
	}
}

func (d *Decoder) realloc(size int) []byte {
	if d.tmp == nil {
		d.tmp = malloc(size)
		return d.tmp
	}
	d.tmp = realloc(d.tmp, size)
	return d.tmp
}

func (d *Decoder) free() {
	if d.tmp != nil {
		free(d.tmp)
		d.tmp = nil
	}
}

func (d *Decoder) decompressBlocks(src io.Reader, upto int, dst []byte) (int, error) {
	off, count := 0, 0
	bs := 1 << d.BlockShift

	// while we have input data
	// and at least one block worth of
	// output space available,
	// decompress blocks:
	block := 0
	for count < upto && len(dst)-off >= bs {
		n, err := io.ReadFull(src, d.frame[:])
		count += n
		if err != nil {
			return off, err
		}
		if ion.TypeOf(d.frame[:]) != ion.BlobType {
			return 0, fmt.Errorf("decoding data: expected a blob; got %s", ion.TypeOf(d.frame[:]))
		}
		size := ion.SizeOf(d.frame[:]) - 5
		if size < 0 || size > (upto-count) {
			return off, fmt.Errorf("unexpected frame size %d", size)
		}
		buf := d.realloc(size)
		n, err = io.ReadFull(src, buf)
		if n != len(buf) && err == nil {
			err = io.ErrUnexpectedEOF
		}
		count += n
		if err != nil {
			return off, err
		}
		err = d.decomp.Decompress(buf, dst[off:off+bs])
		if err != nil {
			return 0, fmt.Errorf("decompress @ offset %d of %d block %d size %d: %w", count-n, upto, block, size, err)
		}
		off += bs
		block++
	}
	return off, nil
}

// Decompress decodes d.Trailer and puts its
// contents into dst. len(dst) must be equal to
// d.Trailer.Decompressed(). src must read the data
// that is referenced by the d.Trailer.
func (d *Decoder) Decompress(src io.Reader, dst []byte) (int, error) {
	if d.tmp != nil {
		panic("concurrent blockfmt.Decoder calls")
	}
	defer d.free()
	d.decomp = compr.Decompression(d.Algo)
	if d.decomp == nil {
		return 0, fmt.Errorf("decompression %q not supported", d.Algo)
	}
	return d.decompressBlocks(src, int(d.Offset), dst)
}

// CopyBytes incrementally decompresses data from src
// and writes it to dst. It returns the number of
// bytes written to dst and the first error encountered,
// if any.
func (d *Decoder) CopyBytes(dst io.Writer, src []byte) (int64, error) {
	size := 1 << d.BlockShift
	if size > vm.PageSize {
		return 0, fmt.Errorf("block size %d exceeds vm.PageSize (%d)", size, vm.PageSize)
	}
	tmp := vm.Malloc()[:size]
	defer vm.Free(tmp)
	algo := d.Algo
	// this path is performance-sensitive,
	// so disable xxhash checking in zstd
	// (costs about 15% of total time!)
	if algo == "zstd" {
		algo = "zstd-nocrc"
	}
	decomp := compr.Decompression(algo)
	nn := int64(0)
	for len(src) > 0 {
		if ion.TypeOf(src) != ion.BlobType {
			return nn, fmt.Errorf("decoding data: expected a blob; got %s", ion.TypeOf(src))
		}
		size := ion.SizeOf(src)
		if size < 5 || size > len(src) {
			return nn, fmt.Errorf("unexpected frame size %d", size)
		}
		err := decomp.Decompress(src[5:size], tmp)
		if err != nil {
			return nn, err
		}
		src = src[size:]
		n, err := dst.Write(tmp)
		nn += int64(n)
		if err != nil {
			return nn, err
		}
	}
	return nn, nil
}

// Copy incrementally decompresses data from src
// and writes it to dst. It returns the number of
// bytes written to dst and the first error encountered,
// if any.
//
// Copy always calls dst.Write with memory allocated
// via sneller/vm.Malloc, so dst may be an io.Writer
// returned via a vm.QuerySink.
func (d *Decoder) Copy(dst io.Writer, src io.Reader) (int64, error) {
	if d.tmp != nil {
		panic("concurrent blockfmt.Decoder calls")
	}
	defer d.free()
	d.decomp = compr.Decompression(d.Algo)
	if d.decomp == nil {
		return 0, fmt.Errorf("decompression %q not supported", d.Algo)
	}
	nn := int64(0)
	size := 1 << d.BlockShift
	if size > vm.PageSize {
		return 0, fmt.Errorf("size %d above vm.PageSize (%d)", size, vm.PageSize)
	}
	vmm := vm.Malloc()[:size]
	defer vm.Free(vmm)
	for {
		_, err := io.ReadFull(src, d.frame[:])
		if err == io.EOF {
			// we are done
			return nn, nil
		}
		if err != nil {
			return nn, err
		}
		if ion.TypeOf(d.frame[:]) != ion.BlobType {
			return 0, fmt.Errorf("decoding data: expected a blob; got %s", ion.TypeOf(d.frame[:]))
		}
		size := ion.SizeOf(d.frame[:]) - 5
		if size < 0 {
			return nn, fmt.Errorf("unexpected frame size %d", size)
		}
		buf := d.realloc(size)
		n, err := io.ReadFull(src, buf)
		if n != len(buf) && err == nil {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return nn, err
		}
		err = d.decomp.Decompress(buf, vmm)
		if err != nil {
			return nn, err
		}
		n, err = dst.Write(vmm)
		nn += int64(n)
		if err != nil {
			return nn, err
		}
	}
}
