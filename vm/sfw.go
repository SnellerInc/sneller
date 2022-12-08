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
	"github.com/SnellerInc/sneller/ion/zion/zll"
	"golang.org/x/exp/slices"
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

// auxbindings is passed along with
// symtab to rowConsumer.symbolize to indicate
// that some variable bindings should be assumed
// to arrive via rowParams rather than via the
// primary delimiters provided in writeRows
type auxbindings struct {
	// bound[i] corresponds to the variable that
	// is bound to future rowParams.auxbound[i] values
	bound []string
}

func (a *auxbindings) reset() {
	a.bound = a.bound[:0]
}

// push creates a new binding and returns its index
// (to be used in rowParams.auxbound)
func (a *auxbindings) push(x string) int {
	n := len(a.bound)
	a.bound = append(a.bound, x)
	return n
}

// id returns the id associated with a binding in a,
// or (-1, false) if no such binding is present
func (a *auxbindings) id(x string) (int, bool) {
	for i := len(a.bound) - 1; i >= 0; i-- {
		if a.bound[i] == x {
			return i, true
		}
	}
	return -1, false
}

// dummy auxbindings for most rowConsumers
// rowParams is auxilliary data passed
// to rowConsumer.writeRows that indicates
// things like extra variable bindings, etc.
type rowParams struct {
	// auxbound contains a list of auxilliary bindings
	// corresponding to the aux binding list provided
	// in symbolize(); the length of each vmref slice
	// will be the same as the number of rows passed to writeRows()
	auxbound [][]vmref
}

// RowConsumer represents part of a QuerySink
// that consumes vectors of rows.
// (It is often the case that the io.WriteCloser
// returned from QuerySink.Open can be cast into
// a RowConsumer, in which case the caller may choose
// to use this interface instead of re-materializing
// the data.)
type rowConsumer interface {
	// symbolize is called every time
	// the current symbol table changes
	//
	// aux provides a list of bindings that supersedes
	// the bindings provided by delims in writeRows;
	// see auxbindings
	symbolize(st *symtab, aux *auxbindings) error
	// writeRows writes a slice of vmrefs
	// (pointing to the inside of each row)
	// into the next sub-query
	//
	// the implementation of writeRows *may*
	// re-use the delims slice, but it *must not*
	// write to the memory pointed to by delims;
	// it must allocate new memory for new output
	//
	// the rules for modification of delims also
	// apply to all of params.auxbound[*]; the slices
	// themselves may be modified, but the values pointed
	// to by the vmrefs should not be modified
	writeRows(delims []vmref, params *rowParams) error

	// next returns the next io.WriteCloser
	// in the chain of query operators;
	// this may be nil
	next() rowConsumer

	// Close indicates that the caller has
	// finished writing row data.
	io.Closer
}

// asRowConsumer converts and arbitrary stream
// into a RowConsumer. If the destination implements
// RowConsumer, that implementation will be returned
// directly. Otherwise, the returned RowConsumer will
// serialize the row data passed to it before writing
// to the destination.
//
// Use this function when you've been given the
// return value of QuerySink.Open() and you want
// to write row data to it.
func asRowConsumer(dst io.WriteCloser) rowConsumer {
	if s, ok := dst.(*rowSplitter); ok {
		s.drop()
		ret := s.rowConsumer
		// most common case
		return ret
	}
	if rc, ok := dst.(rowConsumer); ok {
		return rc
	}
	return Rematerialize(dst)
}

// rowSplitter is a QuerySink that implements io.WriteCloser
// so that materialized data can be fed to a RowConsumer
type rowSplitter struct {
	rowConsumer            // automatically adopts writeRows() and Close()
	st          ion.Symtab // input symbol table
	shared      symtab     // current symbol table
	delims      []vmref    // buffer of delimiters; allocated lazily
	delimhint   int
	symbolized  bool // seen any symbol tables
	params      rowParams
	aux         auxbindings

	vmcache []byte

	pos *int64

	// zstate is non-nil and configured if ConfigureZion has been called
	zstate *zionState
	zout   zionConsumer // cast from rowConsumer
}

// default number of rows to process per batch
const defaultDelims = 512

// splitter takes a rowConsumer and a default batch size
// and produces a rowSplitter that splits materialized row data
// into individual rows for consumption by a RowConsumer
func splitter(q rowConsumer) *rowSplitter {
	s := &rowSplitter{rowConsumer: q, delimhint: defaultDelims}
	if tee, ok := q.(*teeSplitter); ok {
		s.pos = &tee.pos
	}
	return s
}

// write vmm-allocated bytes w/o copying
func (q *rowSplitter) writeVM(src []byte, delims []vmref) error {
	for len(src) > 0 {
		n, nb := scanvmm(src, delims)
		if nb == 0 {
			panic("no progress")
		} else if int(nb) > len(src) {
			panic("scanned past end of src")
		}
		if n > 0 {
			err := q.writeRows(delims[:n], &q.params)
			if err != nil {
				return err
			}
		}
		src = src[nb:]
	}
	return nil
}

// write non-vmm bytes by copying immediately after scanning
func (q *rowSplitter) writeVMCopy(src []byte, delims []vmref) error {
	if q.vmcache == nil {
		q.vmcache = Malloc()
	}

	const (
		// startGranule is the desired size
		// of copies into the vmm region
		startGranule = 32 * 1024
		// minDelims is the desired minimum
		// number of delimiters passed to the core
		minDelims = 32
	)
	granule := startGranule
	for len(src) > 0 {
		// copy data until we reach minDelims
		// or the input data is exhausted
		nd := 0
		mem := q.vmcache[:0]
	scancopy:
		for nd < minDelims && len(mem)+granule < PageSize && len(src) > 0 {
			off := len(mem)
			copied := copy(mem[off:off+granule], src)
			nnd, bytes := scanvmm(mem[off:off+copied], delims[nd:])
			if nnd == 0 {
				// just a nop pad:
				if bytes > 0 {
					src = src[bytes:]
					continue scancopy
				}
				if nd > 0 {
					break scancopy // just take what we have
				}
				// granule not large enough
				// to fit a single object,
				// so let's grow it and try again
				granule *= 2
				if granule > PageSize {
					return fmt.Errorf("object > PageSize(%d)", PageSize)
				}
				continue scancopy
			}
			if bytes == 0 {
				// should never be zero if nnd != 0
				panic("zero added bytes")
			}
			nd += nnd                  // added delims
			mem = mem[:off+int(bytes)] // only keep good data
			src = src[bytes:]          // chomp off input
		}
		err := q.writeRows(delims[:nd], &q.params)
		if err != nil {
			return err
		}
	}
	return nil
}

// EndSegmentWriter is implemented by
// some io.WriteClosers returned by
// QuerySink.Open.
//
// See also: HintEndSegment.
type EndSegmentWriter interface {
	EndSegment()
}

// HintEndSegment calls EndSegment() on w
// if it can be cast to an EndSegmentWriter.
//
// Callers that partition data into logical
// segments that begin with a fresh symbol table
// can use HintEndSegment as a hint to release temporary
// resources (like vm memory) that are specific to
// the most-recently-processed segment.
func HintEndSegment(w io.Writer) {
	if esw, ok := w.(EndSegmentWriter); ok {
		esw.EndSegment()
	}
}

// EndSegment implements blockfmt.SegmentHintWriter.EndSegment
func (q *rowSplitter) EndSegment() {
	// since we know we will have to re-build the symbol table
	// anyway, we can free the symbol table memory so that
	// interleaved queries can use the same vm buffers
	q.symbolized = false
	q.shared.Reset()
	for rc := q.rowConsumer; rc != nil; rc = rc.next() {
		if esw, ok := rc.(EndSegmentWriter); ok {
			esw.EndSegment()
		}
	}
}

func (q *rowSplitter) drop() {
	noLeakCheck(q)
	q.shared.Reset()
	if q.vmcache != nil {
		Free(q.vmcache)
		q.vmcache = nil
	}
}

func (q *rowSplitter) Close() error {
	noLeakCheck(q)
	err := q.rowConsumer.Close()
	// the child may have references to q.shared,
	// so it needs to be closed before we can drop it:
	q.shared.Reset()
	if q.vmcache != nil {
		Free(q.vmcache)
		q.vmcache = nil
	}
	return err
}

func (q *rowSplitter) ConfigureZion(fields []string) bool {
	// populate q.zdec and q.zout
	// conditional on configuring zion input
	if q.zstate == nil {
		out, ok := q.rowConsumer.(zionConsumer)
		if !ok || !out.zionOk() {
			return false
		}
		q.zout = out
		q.zstate = new(zionState)
		q.zstate.shape.Symtab = &zionSymtab{parent: q}
	}
	q.zstate.components = fields
	return true
}

// zionSymtab implements zll.Symtab
type zionSymtab struct {
	parent *rowSplitter
}

func (z *zionSymtab) Symbolize(x string) (ion.Symbol, bool) {
	return z.parent.st.Symbolize(x)
}

// this is straight out of z.parent.Write
func (z *zionSymtab) Unmarshal(src []byte) ([]byte, error) {
	q := z.parent
	rest, err := q.st.Unmarshal(src)
	if err != nil {
		return rest, err
	}
	if !q.shared.resident() {
		leakCheck(q)
	}
	// TODO: optmize this; we are re-serializing the
	// symbol list each time here...
	q.shared.resetNoFree()
	q.st.CloneInto(&q.shared.Symtab)
	q.shared.build()

	q.aux.reset()
	err = q.symbolize(&q.shared, &q.aux)
	if err != nil {
		return rest, err
	}
	return rest, nil
}

func (q *rowSplitter) writeZion(src []byte) (int, error) {
	rest, err := q.zstate.shape.Decode(src)
	if err != nil {
		return 0, err
	}
	q.zstate.buckets.Reset(&q.zstate.shape, rest)
	err = q.zout.writeZion(q.zstate)
	if err != nil {
		return 0, err
	}
	return len(src), nil
}

// Write implements io.Writer
//
// NOTE: each call to Write must contain
// zero or more complete ion objects.
// The data passed to Write may contain a symbol table,
// but if it does, it must come first.
func (q *rowSplitter) Write(buf []byte) (int, error) {
	if q.zstate != nil && zll.IsMagic(buf) {
		return q.writeZion(buf)
	}
	if !q.symbolized && (len(buf) < 4 || !ion.IsBVM(buf)) {
		return 0, fmt.Errorf("first rowSplitter.Write does not have a new symbol table")
	}
	boff := int32(0)
	// if we have a symbol table, then parse it
	// (ion.Symtab.Unmarshal takes care of the BVM resetting the table)
	if len(buf) >= 4 && ion.IsBVM(buf) || ion.TypeOf(buf) == ion.AnnotationType {
		rest, err := q.st.Unmarshal(buf)
		if err != nil {
			return 0, fmt.Errorf("rowSplitter.Write: %w", err)
		}
		q.symbolized = true
		boff = int32(len(buf) - len(rest))

		if !q.shared.resident() {
			leakCheck(q)
		}
		// TODO: optmize this; we are re-serializing the
		// symbol list each time here...
		q.shared.resetNoFree()
		q.st.CloneInto(&q.shared.Symtab)
		q.shared.build()

		q.aux.reset()
		err = q.symbolize(&q.shared, &q.aux)
		if err != nil {
			return 0, err
		}
	}
	// we round up rather than down for each
	// call to Write so that a LIMIT that is
	// satisfied on the first block yields a
	// non-zero number of bytes scanned
	if q.pos != nil {
		*q.pos += int64(len(buf))
	}
	// allocate q.delims lazily
	if len(q.delims) < q.delimhint {
		q.delims = make([]vmref, q.delimhint)
	}
	var err error
	if Allocated(buf) {
		err = q.writeVM(buf[boff:], q.delims)
	} else {
		err = q.writeVMCopy(buf[boff:], q.delims)
	}
	return len(buf), err
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
	aux    []ion.Symbol
	auxpos []int

	empty bool
}

// Rematerialize returns a RowConsumer that guarantees
// that the row data is fully rematerialized before being
// written to 'dst'
func Rematerialize(dst io.WriteCloser) *Rematerializer {
	r := &Rematerializer{empty: true, out: dst}
	return r
}

func (m *Rematerializer) next() rowConsumer { return nil }

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
func (m *Rematerializer) symbolize(st *symtab, aux *auxbindings) error {
	err := m.flush()
	if err != nil {
		return err
	}
	m.buf.Reset()
	m.aux = shrink(m.aux, len(aux.bound))
	m.auxpos = shrink(m.auxpos, len(aux.bound))
	for i := range aux.bound {
		m.aux[i] = st.Intern(aux.bound[i])
		m.auxpos[i] = i
	}
	// produce aux symbols in sorted order
	slices.SortFunc(m.auxpos, func(i, j int) bool {
		return m.aux[i] < m.aux[j]
	})
	st.Marshal(&m.buf, true)
	m.stsize = m.buf.Size()
	return nil
}

func (m *Rematerializer) writeRows(delims []vmref, rp *rowParams) error {
	if m.stsize == 0 {
		panic("WriteRows() called before Symbolize()")
	}
	if len(m.aux) == 0 {
		return m.writeRowsFast(delims)
	}
	for i := range delims {
		mem := delims[i].mem()
		var sym ion.Symbol
		var err error
		m.buf.BeginStruct(-1)

		// let BeginField handle the sorting
		for _, pos := range m.auxpos {
			m.buf.BeginField(m.aux[pos])
			m.buf.UnsafeAppend(rp.auxbound[pos][i].mem())
		}

		for len(mem) > 0 {
			before := mem
			sym, mem, err = ion.ReadLabel(mem)
			if err != nil {
				return fmt.Errorf("vm.Rematerializer: writeRows: %x %w", before, err)
			}
			size := ion.SizeOf(mem)
			m.buf.BeginField(sym)
			m.buf.UnsafeAppend(mem[:size])
			mem = mem[size:]
		}
		m.buf.EndStruct()
		m.empty = false
	}
	return nil
}

// writeRows implements RowConsumer.writeRows
func (m *Rematerializer) writeRowsFast(delims []vmref) error {
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
		m.buf.UnsafeAppendFields(delims[i].mem())
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
// dst is the result of another call to Locked or
// LockedSink and optimizes accordingly.
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

func (s *sink) EndSegment() {
	s.lock.Lock()
	defer s.lock.Unlock()
	HintEndSegment(s.dst)
}

func (s *sink) Open() (io.WriteCloser, error) { return s, nil }
func (s *sink) Close() error                  { return nil }
