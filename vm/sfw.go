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

package vm

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/SnellerInc/sneller/ion"
)

// QuerySink represents a sink for query outputs.
// Every query writes into a QuerySink.
type QuerySink interface {
	// Open() opens a new stream for output.
	// Each stream is only safe to use from
	// a single goroutine. Multiple streams
	// may be opened for concurrent output.
	Open() (io.WriteCloser, error)
	io.Closer
}

// HACK:
// some QuerySink implementations want a hint
// about the size of the table to be written
type sizedConsumer interface {
	hint(size int64)
}

// RowConsumer represents part of a QuerySink
// that consumes vectors of rows.
// (It is often the case that the io.WriteCloser
// returned from QuerySink.Open can be cast into
// a RowConsumer, in which case the caller may choose
// to use this interface instead of re-materializing
// the data.)
type RowConsumer interface {
	// symbolize is called every time
	// the current symbol table changes
	symbolize(st *ion.Symtab) error
	// writeRows writes rows delimited by 'delims'
	// from buf into the rest of the query.
	// The implementation of WriteRows may clobber
	// 'delims,' but it should *not* write into
	// the source data buffer.
	writeRows(buf []byte, delims [][2]uint32) error

	// Close indicates that the caller has
	// finished writing row data.
	io.Closer
}

// AsRowConsumer converts and arbitrary stream
// into a RowConsumer. If the destination implements
// RowConsumer, that implementation will be returned
// directly. Otherwise, the returned RowConsumer will
// serialize the row data passed to it before writing
// to the destination.
//
// Use this function when you've been given the
// return value of QuerySink.Open() and you want
// to write row data to it.
func AsRowConsumer(dst io.WriteCloser) RowConsumer {
	if rc, ok := dst.(RowConsumer); ok {
		return rc
	}
	return Rematerialize(dst)
}

// RowSplitter is a QueryConsumer that implements io.WriteCloser
// so that materialized data can be fed to a RowConsumer
type RowSplitter struct {
	RowConsumer             // automatically adopts writeRows() and Close()
	st, shared  ion.Symtab  // current symbol table
	delims      [][2]uint32 // buffer of delimiters; allocated lazily
	delimhint   int
	symbolized  bool // seen any symbol tables

	vmcache []byte
}

// default number of rows to process per batch
const defaultDelims = 512

// Splitter takes a QueryConsumer and a default batch size
// and produces a RowSplitter that splits materialized row data
// into individual rows for consumption by a RowConsumer
func Splitter(q RowConsumer) *RowSplitter {
	return &RowSplitter{RowConsumer: q, delimhint: defaultDelims}
}

func (q *RowSplitter) writeSanitized(src []byte, delims [][2]uint32) error {
	if Allocated(src) {
		return q.writeRows(src, delims)
	}
	if q.vmcache == nil {
		q.vmcache = Malloc()
	}
	if len(src) > PageSize {
		return fmt.Errorf("cannot sanitize write of size %d (PageSize = %d)", len(src), PageSize)
	}
	n := copy(q.vmcache, src)
	return q.writeRows(q.vmcache[:n], delims)
}

func (q *RowSplitter) Close() error {
	if q.vmcache != nil {
		Free(q.vmcache)
		q.vmcache = nil
	}
	return q.RowConsumer.Close()
}

// Write implements io.Writer
//
// NOTE: each call to Write must contain
// zero or more complete ion objects.
// The data passed to Write may contain a symbol table,
// but if it does, it must come first.
func (q *RowSplitter) Write(buf []byte) (int, error) {
	if !q.symbolized && (len(buf) < 4 || !ion.IsBVM(buf)) {
		return 0, fmt.Errorf("first RowSplitter.Write does not have a new symbol table")
	}
	boff := int32(0)
	// if we have a symbol table, then parse it
	// (ion.Symtab.Unmarshal takes care of the BVM resetting the table)
	if len(buf) >= 4 && ion.IsBVM(buf) || ion.TypeOf(buf) == ion.AnnotationType {
		rest, err := q.st.Unmarshal(buf)
		if err != nil {
			return 0, fmt.Errorf("RowSplitter.Write: %w", err)
		}
		q.symbolized = true
		boff = int32(len(buf) - len(rest))
		q.st.CloneInto(&q.shared)
		err = q.symbolize(&q.shared)
		if err != nil {
			return 0, err
		}
	}

	// allocate q.delims lazily
	if len(q.delims) < q.delimhint {
		if !Allocated(buf) {
			// when we know we are receiving non-VM buffers,
			// we should limit the number of delimiters processed
			// in one go so that we can try to guarantee that
			// the objects can safely be copied into a single vm "page"
			q.delims = make([][2]uint32, 32)
		} else {
			q.delims = make([][2]uint32, q.delimhint)
		}
	}

	for int(boff) < len(buf) {
		count, next := scan(buf[boff:], 0, q.delims)
		if int(next) > len(buf[boff:]) {
			panic("last object extends past end of buffer?")
		}
		if count != 0 {
			err := q.writeSanitized(buf[boff:boff+next], q.delims[:count])
			if err != nil {
				return int(boff), err
			}
		}
		boff += next
	}
	return len(buf), nil
}

// QueryBuffer is an in-memory implementation
// of QuerySink that can be trivially converted
// to a Table. It can be used to force a sub-query
// to be fully materialized before being consumed
// by another query. It also guarantees that the
// input chunks are padded to a fixed alignment.
type QueryBuffer struct {
	lock      sync.Mutex
	buf       bytes.Buffer
	chunksize int
	tail      []byte // used to make nop pads
}

// Size returns the number of bytes in the table.
func (q *QueryBuffer) Size() int64 {
	return int64(q.buf.Len())
}

// Alignment returns the alignment of the table.
func (q *QueryBuffer) Alignment() int {
	return q.chunksize
}

// SetAlignment sets the alignment to which
// subsequent calls to Write will be padded.
func (q *QueryBuffer) SetAlignment(align int) {
	q.chunksize = align
}

// Reset resets the buffer so that it contains no data.
func (q *QueryBuffer) Reset() {
	q.buf.Reset()
}

// Bytes returns all of the bytes written to the buffer.
func (q *QueryBuffer) Bytes() []byte {
	return q.buf.Bytes()
}

// Open implements QueryConsumer.Open
func (q *QueryBuffer) Open() (io.WriteCloser, error) {
	return q, nil
}

// Write implements io.Writer
func (q *QueryBuffer) Write(buf []byte) (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.chunksize == 0 {
		q.chunksize = defaultAlign
	}
	if len(buf) > q.chunksize {
		return 0, fmt.Errorf("chunk of %d bytes too big for QueryBuffer", len(buf))
	}

	q.buf.Grow(q.chunksize)
	q.buf.Write(buf)
	nopsize := q.chunksize - len(buf)
	for nopsize > 0 {
		if cap(q.tail) < nopsize {
			q.tail = make([]byte, nopsize)
		}
		q.tail = q.tail[:nopsize]
		wrote, padded := ion.NopPadding(q.tail, len(q.tail))
		q.buf.Write(q.tail[:wrote+padded])
		nopsize -= (wrote + padded)
	}
	return len(buf), nil
}

// Close implements io.Closer
func (q *QueryBuffer) Close() error { return nil }

// Table produces a view of the data in the QueryBuffer
func (q *QueryBuffer) Table() *BufferedTable {
	return &BufferedTable{buf: q.Bytes(), align: q.chunksize}
}

// Rematerializer is a RowConsumer that
// rematerializes row data into contiguous
// blocks of ion data.
type Rematerializer struct {
	buf    ion.Buffer
	out    io.WriteCloser
	stsize int
	empty  bool
}

// Rematerialize returns a RowConsumer that guarantees
// that the row data is fully rematerialized before being
// written to 'dst'
func Rematerialize(dst io.WriteCloser) *Rematerializer {
	r := &Rematerializer{empty: true, out: dst}
	return r
}

func (m *Rematerializer) flush() error {
	if m.empty {
		return nil
	}
	buf := m.buf.Bytes()
	_, err := m.out.Write(buf)
	m.buf.Set(buf[:m.stsize])
	m.empty = true
	return err
}

// symbolize implements RowConsumer.symbolize
func (m *Rematerializer) symbolize(st *ion.Symtab) error {
	err := m.flush()
	if err != nil {
		return err
	}
	m.buf.Reset()
	st.Marshal(&m.buf, true)
	m.stsize = m.buf.Size()
	return nil
}

// writeRows implements RowConsumer.writeRows
func (m *Rematerializer) writeRows(buf []byte, delims [][2]uint32) error {
	if m.stsize == 0 {
		return fmt.Errorf("Rematerializer.WriteRows() before symbolize()")
	}
	for i := range delims {
		if delims[i][1] == 0 {
			continue
		}
		size := int(delims[i][1]) + 8 // generous slack
		if defaultAlign-m.buf.Size() < size {
			err := m.flush()
			if err != nil {
				return err
			}
		}
		off := delims[i][0]
		end := off + delims[i][1]
		m.buf.BeginStruct(-1)
		m.buf.UnsafeAppend(buf[off:end])
		m.buf.EndStruct()
		m.empty = false
	}
	return nil
}

// Close implements io.Closer
func (m *Rematerializer) Close() error {
	err := m.flush()
	err2 := m.out.Close()
	if err == nil {
		err = err2
	}
	return err
}

// Locked turns an io.Writer into a goroutine-safe
// io.Writer where each write is serialized against
// other writes. Locked takes into account whether
// dst implements RowConsumer and optimizes the
// calls to Write accordingly.
func Locked(dst io.Writer) io.Writer {
	if _, ok := dst.(*sink); ok {
		return dst
	}
	return &sink{dst: dst}
}

// LockedSink returns a QuerySink for which
// all calls to Open return a wrapper of dst
// that serializes calls to io.Writer.Write.
// (See also Locked.)
func LockedSink(dst io.Writer) QuerySink {
	if s, ok := dst.(*sink); ok {
		return s
	}
	return &sink{dst: dst}
}

// trivial vm.QuerySink for
// producing an output stream
type sink struct {
	lock sync.Mutex
	dst  io.Writer
}

func (s *sink) Write(p []byte) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.dst.Write(p)
}

func (s *sink) Open() (io.WriteCloser, error) { return s, nil }
func (s *sink) Close() error                  { return nil }
