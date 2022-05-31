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
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/ion"
)

const (
	partstride = 42
	flushover  = partstride / 3
)

// MultiWriter is a multi-stream writer
// that turns multiple streams of input blocks
// into a single output stream of compressed blocks.
//
// MultiWriter tries to keep blocks written by
// each stream close together in the output so
// that sparse indexes that are built on each
// of the streams end up pointing to contiguous
// regions of output.
type MultiWriter struct {
	// Output is the Uploader used to
	// upload parts to backing storage.
	Output Uploader
	// Comp is the compression algorithm
	// used to compress blocks.
	Comp compr.Compressor
	// InputAlign is the expected size
	// of input blocks that are provided
	// to io.Write in each stream.
	InputAlign int
	// TargetSize is the target size of
	// each output part written to backing
	// storage.
	TargetSize int

	// MinChunksPerBlock is the desired
	// number of chunks per block.
	// If it is set, then metadata blocks
	// are coalesced so that they are at
	// least this size (unless the total
	// number of chunks is less than
	// MinChunksPerBlock).
	MinChunksPerBlock int

	// Trailer is the trailer that
	// is appended to the output stream.
	// The fields in Trailer are only
	// valid once MultiWriter.Close has
	// been called.
	Trailer

	// all of this is used to compute
	// the output block mapping once
	// Close() has been called.
	lock     sync.Mutex
	spans    []span
	nextpart int64

	// unallocated is the list of descriptors
	// in the tail(s) of each input stream that
	// could not be flushed on their own due to
	// insufficient part sizes
	unallocated struct {
		buf    []byte
		blocks []blockpart
	}
	refcount   int32
	skipChecks bool
}

type span struct {
	// id of the stream that produced the span
	tid int
	// part numer of the span
	partnum int64
	// blockmap is the partly-finished
	// list of block descriptors that
	// needs to be updated when the
	// object is finalized; each Offset
	// points to the offset within the
	// span rather than the final offset
	blockmap []blockpart
	outsize  int64
}

type singleStream struct {
	futureRange
	parent  *MultiWriter
	buf     []byte // compressed buffer
	tid     int    // stream id
	curspan span   // current span

	lastblock   int64
	flushblocks int
}

var _ minMaxer = &singleStream{}

func (m *MultiWriter) init() {
	if m.TargetSize == 0 {
		m.TargetSize = m.Output.MinPartSize()
	}
	if m.InputAlign == 0 {
		m.InputAlign = 1024 * 1024
	}
	if m.nextpart == 0 {
		m.nextpart = 1
	}
	if m.Comp == nil || m.Output == nil {
		panic("can't use MultiWriter w/o Comp and Output fields")
	}
}

// SkipChecks disable some runtime checks
// of the input data, which is ordinarily
// expected to be ion data. Do not use this
// except for testing.
func (m *MultiWriter) SkipChecks() {
	m.skipChecks = true
}

// Open opens a stream for writing output.
// Each call to io.Write on the provided io.WriteCloser
// must be m.InputAlign bytes. The stream must be closed
// before the MultiWriter can be closed.
// Blocks written to a single stream are coalesced
// into large, contiguous "spans" of blocks in order
// to reduce index fragmentation caused by writing multiple
// streams of output parts simultaneously.
func (m *MultiWriter) Open() (io.WriteCloser, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.init()
	tid := int(atomic.AddInt32(&m.refcount, 1)) - 1

	// allocate a starting span for this stream eagerly
	// so that we can predict output span ordering in tests
	s := &singleStream{parent: m, tid: tid}
	s.curspan.partnum = m.nextpart
	m.nextpart++
	return s, nil
}

// Flush implements ion.Flusher
func (s *singleStream) Flush() error {
	if s.flushblocks > 0 {
		// add any recent metadata
		// to the blocks written since the last Flush
		s.curspan.blockmap = append(s.curspan.blockmap, blockpart{
			offset: s.lastblock,
			chunks: s.flushblocks,
			ranges: s.futureRange.pop(),
		})
		s.lastblock = int64(len(s.buf))
		s.flushblocks = 0
	}
	// actually flush only if we've buffered
	// enough to satisfy the upload invariants
	if len(s.buf) >= s.parent.TargetSize {
		s.curspan.outsize = int64(len(s.buf))
		n := s.curspan.partnum
		err := s.parent.Output.Upload(n, s.buf)
		if err != nil {
			return err
		}
		s.buf = s.buf[:0]
		// flush span and assign a new part number;
		// reset the local state back to zero
		s.parent.lock.Lock()
		s.parent.spans = append(s.parent.spans, s.curspan)
		s.curspan.partnum = s.parent.nextpart
		s.parent.nextpart++
		s.parent.lock.Unlock()
		s.curspan.blockmap = nil
		s.curspan.outsize = 0
		s.lastblock = 0
	}
	return nil
}

// Write implements io.Writer
func (s *singleStream) Write(p []byte) (int, error) {
	if len(p) != s.parent.InputAlign {
		return 0, fmt.Errorf("input buffer length %d not equal to alignment %d", len(p), s.parent.InputAlign)
	}
	// layering violation here,
	// but we really need to be pedantic about this:
	if s.flushblocks == 0 && !s.parent.skipChecks && !ion.IsBVM(p) {
		return 0, fmt.Errorf("blockfmt.MultiWriter: flush, but then no BVM")
	}
	s.flushblocks++
	s.buf = appendFrame(s.buf, s.parent.Comp, p)
	return len(p), nil
}

// promote returns true if the current span
// is pushed into s.parent.unallocated, or
// false if s.parent.unallocated was merged
// into the current span and the stream now
// needs to be flushed
func (s *singleStream) promote() bool {
	if len(s.buf) == 0 {
		panic("shouldn't have called promote (no data)")
	}
	if len(s.curspan.blockmap) == 0 {
		panic("data but no blocks?")
	}
	if s.curspan.outsize != 0 {
		panic("already wrote out data???")
	}
	s.parent.lock.Lock()
	defer s.parent.lock.Unlock()

	// first, try seeing if we can merge
	// with the unallocated blocks that
	// another stream has failed to write out;
	// we may be able to use the combined data
	// to produce a properly-sized chunk
	u := &s.parent.unallocated
	if len(u.buf)+len(s.buf) >= s.parent.TargetSize {
		off := int64(len(s.buf))
		// sanity-check the blocks we are about to coalesce
		// into this span
		if len(u.blocks) == 0 {
			panic("unallocated data but no blocks?")
		}
		if u.blocks[0].offset != 0 {
			panic("beginning of unallocated blocks not 0?")
		}
		for i := range u.blocks {
			// adjust offset to trailing position
			// in current span
			u.blocks[i].offset += off
		}
		s.curspan.blockmap = append(s.curspan.blockmap, u.blocks...)
		s.buf = append(s.buf, u.buf...)
		s.lastblock = int64(len(s.buf))
		u.buf = u.buf[:0]
		u.blocks = nil
		return false
	}

	// the current blockmap offsets are
	// the offset within the current span,
	// but those offsets have to be adjusted
	// to be the current offset within the final span
	blocks := s.curspan.blockmap
	if s.curspan.outsize != 0 || blocks[0].offset != 0 {
		panic("flush span didn't shift outsize to zero")
	}
	adj := int64(len(u.buf))
	for i := range blocks {
		blocks[i].offset += adj
	}
	u.buf = append(u.buf, s.buf...)
	u.blocks = append(u.blocks, blocks...)

	// reset current span just in case
	s.curspan = span{tid: -2}
	s.flushblocks = 0
	s.buf = nil
	s.tid = -2
	return true
}

// Close implements io.Closer
func (s *singleStream) Close() error {
	defer atomic.AddInt32(&s.parent.refcount, -1)
	if s.flushblocks != 0 {
		return fmt.Errorf("singleStream.Close() missing call to Flush() first")
	}
	if len(s.buf) == 0 {
		if len(s.curspan.blockmap) != 0 {
			panic("unflushed blocks?")
		}
		return nil
	}
	if len(s.buf) >= s.parent.TargetSize || !s.promote() {
		return s.Flush()
	}
	// the promote() call took care of the data;
	// it will be handled in MultiWriter.Close()
	if len(s.buf) != 0 || len(s.curspan.blockmap) != 0 {
		panic("didn't actually flush!")
	}
	return nil
}

func (m *MultiWriter) finalize() {
	// turn the final unallocated data
	// into a logical span
	if len(m.unallocated.buf) > 0 {
		m.spans = append(m.spans, span{
			tid:      -1,
			partnum:  m.nextpart,
			outsize:  int64(len(m.unallocated.buf)),
			blockmap: m.unallocated.blocks,
		})
	}
	// now that we've computed all of our spans,
	// sort them so that we can assign offsets
	sort.Slice(m.spans, func(i, j int) bool {
		return m.spans[i].partnum < m.spans[j].partnum
	})
	// adjust the block offsets in each span
	// by the previous span offsets so that
	// they actually reflect the final output offsets;
	// also take the opportunity to do some sanity checking
	offset := int64(0)
	part := int64(0)
	var all []blockpart
	for i := range m.spans {
		if m.spans[i].partnum == part {
			panic("part re-used")
		}
		part = m.spans[i].partnum
		prev := int64(0)
		for j := range m.spans[i].blockmap {
			block := &m.spans[i].blockmap[j]
			if block.offset < prev {
				panic("blocks out-of-order")
			}
			all = append(all, blockpart{
				offset: block.offset + offset,
				chunks: block.chunks,
				ranges: block.ranges,
			})
			prev = block.offset
		}
		if m.spans[i].outsize <= prev {
			panic("span outsize < offset")
		}
		offset += m.spans[i].outsize
	}
	finalize(&m.Trailer, all, m.MinChunksPerBlock)
	m.Trailer.Offset = offset
}

// merge adjacent blocks below the minimum
// chunks-per-block threshold and return
// the new slice of blocks (aliases 'blocks')
func coalesce(blocks []blockpart, min int) []blockpart {
	if min <= 1 || len(blocks) <= 1 {
		return blocks
	}
	// for tracking the number of chunks
	// above a certain chunk number:
	above := make([]int, len(blocks))
	for i := len(blocks) - 1; i > 0; i-- {
		above[i-1] = above[i] + blocks[i].chunks
	}
	// for each block, merge with subsequent
	// blocks until the combined size reaches
	// the minimum chunks per block
	for i := 0; i < len(blocks); i++ {
		j := i + 1
		for j < len(blocks) &&
			(blocks[i].chunks < min || above[i] < min) {
			above[i] -= blocks[j].chunks
			copy(above[i:], above[i+1:])
			above = above[:len(above)-1]
			blocks[i].merge(&blocks[j])
			copy(blocks[j:], blocks[j+1:])
			blocks = blocks[:len(blocks)-1]
		}
	}
	return blocks
}

// Close closes the MultiWriter.
// Close is only safe to call once Close
// has been called on each outstanding output
// stream created by calling Open.
//
// Once Close returns, the Trailer field
// will be populated with the trailer that
// was assembled from all the constituent
// spans of input data.
func (m *MultiWriter) Close() error {
	if atomic.LoadInt32(&m.refcount) != 0 {
		panic("race between stream Close() and MultiWriter Close()")
	}
	m.finalize()
	// compute the final sparse index:
	m.unallocated.buf = append(m.unallocated.buf, m.Trailer.trailer(m.Comp, m.InputAlign)...)
	return m.Output.Close(m.unallocated.buf)
}
