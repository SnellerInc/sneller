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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion"
	"github.com/SnellerInc/sneller/ion/zion/zll"
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

func toDescs(dst []Blockdesc, src []blockpart) []Blockdesc {
	for i := range src {
		dst = append(dst, Blockdesc{
			src[i].offset,
			src[i].chunks,
		})
	}
	return dst
}

type Compressor interface {
	Name() string
	Compress(src, dst []byte) ([]byte, error)
	io.Closer
}

var zionEncPool = sync.Pool{
	New: func() any { return &zion.Encoder{} },
}

type zionCompressor struct {
	enc *zion.Encoder
}

func (z *zionCompressor) Compress(src, dst []byte) ([]byte, error) {
	return z.enc.Encode(src, dst)
}

func (z *zionCompressor) Close() error {
	zionEncPool.Put(z.enc)
	z.enc = nil
	return nil
}

func (z *zionCompressor) Name() string {
	if z.enc == nil {
		// closed?
		return "zion"
	}
	// zion+zstd, zion+iguana_v0, etc.
	return "zion+" + z.enc.Algo.String()
}

type encoderNopCloser struct {
	compr.Compressor
}

func (e *encoderNopCloser) Close() error { return nil }

func (e *encoderNopCloser) Compress(src, dst []byte) ([]byte, error) {
	return e.Compressor.Compress(src, dst), nil
}

// CompressorByName produces the Compressor
// associated with the provided algorithm name,
// or nil if no such algorithm is known to the library.
//
// Valid values include:
//
//	"zstd"
//	"zion"
//	"zion+zstd" (equivalent to "zion")
//	"zion+iguana_v0"
func CompressorByName(algo string) Compressor {
	return getCompressor(algo)
}

func getCompressor(algo string) Compressor {
	switch algo {
	case "zion+iguana_v0":
		e := zionEncPool.Get().(*zion.Encoder)
		e.Reset()
		e.Algo = zll.CompressIguanaV0
		return &zionCompressor{enc: e}
	case "zion", "zion+zstd":
		e := zionEncPool.Get().(*zion.Encoder)
		e.Reset()
		e.Algo = zll.CompressZstd
		return &zionCompressor{enc: e}
	default:
		c := compr.Compression(algo)
		if c != nil {
			return &encoderNopCloser{c}
		}
		return nil
	}
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
	Comp Compressor
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

func pickPrefix(t *Trailer, minchunks int) (index int, offset int64) {
	for index < len(t.Blocks) && t.Blocks[index].Chunks >= minchunks {
		index++
	}
	offset = t.Offset
	if index < len(t.Blocks) {
		offset = t.Blocks[index].Offset
	}
	return index, offset
}

// consume maybe *some* of an existing object
// without doing any heavy lifting w.r.t compression
func (w *CompressionWriter) writeStart(r io.Reader, t *Trailer) error {
	if t.Algo != w.Comp.Name() || 1<<t.BlockShift != w.InputAlign {
		return nil // not directly compatible
	}
	j, offset := pickPrefix(t, w.MinChunksPerBlock)
	if j == 0 || offset < int64(w.Output.MinPartSize()) {
		return nil
	}
	pn, err := uploadReader(w.Output, w.partnum+1, r, offset)
	if err != nil {
		return err
	}
	w.partnum = pn - 1
	w.lastblock = offset
	w.offset = w.lastblock
	// set the currently-output state:
	w.Trailer.Blocks = t.Blocks[:j]
	w.Trailer.Sparse = t.Sparse.Trim(j)
	// set the state of what we expect to consume:
	t.Blocks = t.Blocks[j:]
	t.Sparse = t.Sparse.Slice(j, t.Sparse.Blocks())
	return nil
}

// upload w.buffer; swap w.alt and w.buffer
func (w *CompressionWriter) upload() error {
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
			return err
		}
	}
	buf := w.buffer
	go func() {
		w.bg <- w.Output.Upload(pn, buf)
	}()
	// swap buffers while the upload
	// is using the current buffer
	w.buffer, w.alt = w.alt[:0], w.buffer[:0]
	return nil
}

func (w *CompressionWriter) setSymbols(st *ion.Symtab) {
	w.Comp.(*zionCompressor).enc.SetSymbols(st)
}

func (w *CompressionWriter) writeCompressed(p []byte) error {
	before := len(w.buffer)
	w.buffer = appendRawFrame(w.buffer, p)
	return w.checkFlush(before)
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
	before := len(w.buffer)
	w.buffer, err = appendFrame(w.buffer, w.Comp, p)
	if err != nil {
		return
	}
	return len(p), w.checkFlush(before)
}

func (w *CompressionWriter) checkFlush(before int) error {
	w.flushblocks++
	w.offset += int64(len(w.buffer) - before)
	if len(w.buffer) >= w.target() {
		err := w.upload()
		if err != nil {
			return err
		}
	}
	return nil
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
	dst.Blocks = toDescs(dst.Blocks, src)
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
	trailer := w.Trailer.trailer(w.Comp.Name(), w.InputAlign)
	w.offset += int64(len(trailer))
	w.buffer = append(w.buffer, trailer...)
	w.Comp.Close()
	w.Comp = nil
	return w.Output.Close(w.buffer)
}

func (t *Trailer) trailer(compname string, align int) []byte {
	var st ion.Symtab
	var buf ion.Buffer

	t.Version = 1
	t.Algo = compname
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

func appendRawFrame(dst, src []byte) []byte {
	size := len(src)
	dst = append(dst,
		byte((ion.BlobType)<<4)|0xe,
		byte(size>>21)&0x7f,
		byte(size>>14)&0x7f,
		byte(size>>7)&0x7f,
		byte(size&0x7f)|0x80)
	dst = append(dst, src...)
	return dst
}

// append a compressed frame to dst
// that is wrapped in an ion 'blob' tag
func appendFrame(dst []byte, comp Compressor, src []byte) ([]byte, error) {
	base := len(dst)
	var err error
	dst = append(dst,
		byte((ion.BlobType)<<4)|0xe,
		0, 0, 0, 0,
	)
	dst, err = comp.Compress(src, dst)
	if err != nil {
		return nil, err
	}
	size := len(dst) - base - 5
	dst[base+1] = byte(size>>21) & 0x7f
	dst[base+2] = byte(size>>14) & 0x7f
	dst[base+3] = byte(size>>7) & 0x7f
	dst[base+4] = byte(size&0x7f) | 0x80
	return dst, nil
}

// ReadTrailer reads a trailer from an io.ReaderAt
// that has a backing size of 'size'.
func ReadTrailer(src io.ReaderAt, size int64) (*Trailer, error) {
	t := new(Trailer)
	err := t.ReadFrom(src, size)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Trailer) ReadFrom(src io.ReaderAt, size int64) error {
	return fill(t, src, size)
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

type decompressor interface {
	Decompress(src, dst []byte) error
	io.Closer
}

type decompressNopCloser struct {
	compr.Decompressor
}

func (d decompressNopCloser) Close() error { return nil }

var zionDecompPool = sync.Pool{
	New: func() any { return &zion.Decoder{} },
}

type zionDecompressor struct {
	dec *zion.Decoder
}

func (z *zionDecompressor) Close() error {
	z.dec.Reset()
	zionDecompPool.Put(z.dec)
	z.dec = nil
	return nil
}

func noppad(buf []byte) {
	for len(buf) > 0 {
		wrote, padded := ion.NopPadding(buf, len(buf))
		buf = buf[(wrote + padded):]
	}
}

func (z *zionDecompressor) Decompress(src, dst []byte) error {
	ret, err := z.dec.Decode(src, dst[:0])
	if err != nil {
		return err
	}
	if &ret[0] != &dst[0] || len(ret) > len(dst) {
		return fmt.Errorf("blockfmt: zion.Decode output %d (> %d) bytes", len(ret), len(dst))
	}
	// in order to produce bit-identical results
	// to the original input buffer, we need to
	// include the nop pad that was presumably
	// at the end of the input data
	tail := dst[len(ret):]
	if len(tail) > 0 {
		noppad(tail)
	}
	return nil
}

func getAlgo(algo string) decompressor {
	switch algo {
	case "zion", "zion+zstd", "zion+iguana_v0":
		d := zionDecompPool.Get().(*zion.Decoder)
		d.Reset()
		return &zionDecompressor{d}
	default:
		d := compr.Decompression(algo)
		if d == nil {
			return nil
		}
		return decompressNopCloser{d}
	}
}

// Decoder is used to decode blocks from a Trailer
// and an associated data stream.
type Decoder struct {
	// BlockShift is the log2 of the block size.
	// BlockShift is set automatically by Decoder.Set.
	BlockShift int
	// Offset is the offset at which to begin decoding.
	// Offset is set automatically by Decoder.Set.
	Offset int64
	// Algo is the algorithm to use for decompressing
	// the input data blocks.
	// Algo is set automatically by Decoder.Set.
	Algo string

	// Fields is the dereference push-down hint
	// for the fields that should be decompressed
	// from the input. Note that the zero value (nil)
	// means all fields, but a zero-length slice explicitly
	// means zero fields (i.e. decode empty structures).
	Fields []string

	// Malloc should return a slice with the given size.
	// If Malloc is nil, then make([]byte, size) is used.
	// If Malloc is non-nil, then Free should be set.
	Malloc func(size int) []byte
	// If Malloc is set, then Free should be
	// set to a corresponding function to release
	// data allocated via Malloc.
	Free func([]byte)

	decomp decompressor
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

func (d *Decoder) malloc(size int) []byte {
	if d.Malloc != nil {
		return d.Malloc(size)
	}
	return malloc(size)
}

func (d *Decoder) drop(buf []byte) {
	if d.Free != nil {
		d.Free(buf)
	} else {
		free(buf)
	}
}

func (d *Decoder) free() {
	if d.decomp != nil {
		d.decomp.Close()
		d.decomp = nil
	}
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

func (d *Decoder) setupZion(dec *zion.Decoder) {
	if d.Fields == nil {
		dec.SetWildcard()
	} else {
		dec.SetComponents(d.Fields)
	}
}

func (d *Decoder) getDecomp(algo string) error {
	d.decomp = getAlgo(algo)
	if d.decomp == nil {
		return fmt.Errorf("decompression %q not supported", d.Algo)
	}
	if z, ok := d.decomp.(*zionDecompressor); ok {
		d.setupZion(z.dec)
	}
	return nil
}

// Decompress decodes d.Trailer and puts its
// contents into dst. len(dst) must be equal to
// d.Trailer.Decompressed(). src must read the data
// that is referenced by the d.Trailer.
func (d *Decoder) Decompress(src io.Reader, dst []byte) (int, error) {
	if d.tmp != nil {
		panic("concurrent blockfmt.Decoder calls")
	}
	err := d.getDecomp(d.Algo)
	if err != nil {
		return 0, err
	}
	defer d.free()
	return d.decompressBlocks(src, int(d.Offset), dst)
}

func (d *Decoder) copyZion(w io.Writer, src []byte) (int64, error) {
	nn := int64(0)
	for len(src) > 0 {
		if ion.TypeOf(src) != ion.BlobType {
			return nn, fmt.Errorf("decoding data: expected a blob; got %s", ion.TypeOf(src))
		}
		size := ion.SizeOf(src)
		if size < 5 || size > len(src) {
			return nn, fmt.Errorf("unexpected frame size %d", size)
		}
		_, err := w.Write(src[5:size])
		if err != nil {
			return nn, err
		}
		src = src[size:]
		nn += int64(1 << d.BlockShift) // we know the decompressed size already
	}
	return nn, nil
}

// same as d.copyZion(), but for an io.Reader
func (d *Decoder) copyZionFrom(w io.Writer, src io.Reader) (int64, error) {
	nn := int64(0)
	defer d.free()
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
		_, err = w.Write(buf)
		if err != nil {
			return nn, err
		}
		nn += 1 << d.BlockShift
	}
}

// ZionWriter is an optional interface implemented by
// an io.Writer passed to Decoder.CopyBytes or Decoder.Copy.
// An io.Writer that implements ZionWriter may receive raw
// zion-encoded data rather than decompressed ion data.
type ZionWriter interface {
	// ConfigureZion is called with the set of
	// top-level path components that the caller
	// expects the callee to handle. ConfigureZion
	// should return true if the callee can handle
	// extracting the provided field list directly
	// from encoded zion data, or false if it cannot.
	ConfigureZion(blocksize int64, fields []string) bool
}

func (d *Decoder) acceptsZion(w io.Writer) bool {
	zw, ok := w.(ZionWriter)
	return ok && zw.ConfigureZion(int64(1)<<d.BlockShift, d.Fields)
}

// CopyBytes incrementally decompresses data from src
// and writes it to dst. It returns the number of
// bytes written to dst and the first error encountered,
// if any. If dst implements ZionWriter and d.Algo is "zion"
// then compressed data may be passed directly to dst
// (see ZionWriter for more details).
func (d *Decoder) CopyBytes(dst io.Writer, src []byte) (int64, error) {
	size := 1 << d.BlockShift
	if strings.HasPrefix(d.Algo, "zion") {
		if d.acceptsZion(dst) {
			return d.copyZion(dst, src)
		}
	}
	vmm := d.malloc(size)
	defer d.drop(vmm)
	algo := d.Algo
	// this path is performance-sensitive,
	// so disable xxhash checking in zstd
	// (costs about 15% of total time!)
	if algo == "zstd" {
		algo = "zstd-nocrc"
	}
	err := d.getDecomp(algo)
	if err != nil {
		return 0, err
	}
	nn := int64(0)
	for len(src) > 0 {
		if ion.TypeOf(src) != ion.BlobType {
			return nn, fmt.Errorf("decoding data: expected a blob; got %s", ion.TypeOf(src))
		}
		size := ion.SizeOf(src)
		if size < 5 || size > len(src) {
			return nn, fmt.Errorf("unexpected frame size %d", size)
		}
		err := d.decomp.Decompress(src[5:size], vmm)
		if err != nil {
			return nn, err
		}
		src = src[size:]
		n, err := dst.Write(vmm)
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
// via d.Malloc, so dst may be an io.Writer
// returned via a vm.QuerySink provided that d.Malloc
// is set to vm.Malloc.
func (d *Decoder) Copy(dst io.Writer, src io.Reader) (int64, error) {
	if d.tmp != nil {
		panic("concurrent blockfmt.Decoder calls")
	}
	if strings.HasPrefix(d.Algo, "zion") && d.acceptsZion(dst) {
		return d.copyZionFrom(dst, src)
	}
	defer d.free()
	err := d.getDecomp(d.Algo)
	if err != nil {
		return 0, err
	}
	nn := int64(0)
	size := 1 << d.BlockShift
	vmm := d.malloc(size)
	defer d.drop(vmm)
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
