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

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/heap"
	"github.com/SnellerInc/sneller/ion"
)

// SortDirection selectes ordering of non-null values: ascending or descending.
type SortDirection int

const (
	SortAscending  SortDirection = +1
	SortDescending SortDirection = -1
)

func (v SortDirection) String() string {
	switch v {
	case SortAscending:
		return "ASC"
	case SortDescending:
		return "DESC"
	default:
		return fmt.Sprintf("<SortDirection: %d>", int(v))
	}
}

// SortNullsOrder selects ordering of null values.
type SortNullsOrder int

const (
	SortNullsFirst SortNullsOrder = 0
	SortNullsLast  SortNullsOrder = 1
)

func (v SortNullsOrder) String() string {
	switch v {
	case SortNullsFirst:
		return "NULLS FIRST"
	case SortNullsLast:
		return "NULLS LAST"
	default:
		return fmt.Sprintf("<SortNullsOrder: %d>", int(v))
	}
}

// SortOrdering select ordering of null and non-null values
type SortOrdering struct {
	Direction  SortDirection
	NullsOrder SortNullsOrder
}

func (o SortOrdering) String() string {
	return o.Direction.String() + " " + o.NullsOrder.String()
}

// Compare compares two Ion values according to ordering settings.
// Similarly to bytes.Compare, Compare returns
// -1 if a < b, 0 if a == b, or 1 if a > b
func (o SortOrdering) Compare(a, b []byte) int {
	return int(o.Direction) * compareIonValues(a, b, o.Direction, o.NullsOrder)
}

// SortColumn represents a single entry in the 'ORDER BY' clause:
// "column-name [ASC|DESC] [NULLS FIRST|NULLS LAST]"
type SortColumn struct {
	Node     expr.Node
	Ordering SortOrdering
}

type SortLimit struct {
	Limit  int
	Offset int
}

// Order implements a QuerySink that applies
// an ordering to its output rows.
type Order struct {
	dst         io.Writer    // final destination
	columns     []SortColumn // columns to sort
	limit       *SortLimit   // optional limit parameters
	parallelism int          // number of threads
	prog        prog         // program for capturing fields

	// symbol table for the whole input
	symtab *ion.Symtab

	// lock for writing to `symtab`
	symtabLock sync.Mutex

	// collection of k-top rows
	kheap kheap

	// lock for writing to the heap
	recordsLock sync.Mutex
}

// NewOrder constructs a new Order QuerySink that
// sorts the provided columns (in left-to-right order).
// If limit is non-nil, then the number of rows output
// by the Order will be less than or equal to the limit.
func NewOrder(dst io.Writer, columns []SortColumn, limit *SortLimit, parallelism int) (*Order, error) {
	if limit == nil {
		limit = &SortLimit{Limit: 100000}
	}
	s := &Order{
		columns:     columns,
		limit:       limit,
		parallelism: parallelism,
		dst:         dst,
	}
	s.prog.begin()
	mem0 := s.prog.initMem()
	var mem []*value
	for i := range columns {
		val, err := s.prog.compileStore(mem0, columns[i].Node, stackSlotFromIndex(regV, i), true)
		if err != nil {
			return nil, err
		}
		mem = append(mem, val)
	}
	s.prog.returnValue(s.prog.mergeMem(mem...))

	return s, nil
}

// setSymbolTable sets symbol table for the sorted data.
//
// It's expected that all input chunks have exactly the same symtab.
// Otherwise we either have to:
// 1. align all symtabs into a common one and recode all records;
// 2. preprend possibliy each record with symtab.
func (s *Order) setSymbolTable(st *symtab) error {
	if s.symtab == nil {
		s.symtab = new(ion.Symtab)
		st.Symtab.CloneInto(s.symtab)
		return nil
	}
	_, ok := s.symtab.Merge(&st.Symtab)
	if !ok {
		return fmt.Errorf("symtab changed, can't sort data")
	}
	return nil
}

func (s *Order) orderList() []SortOrdering {
	orders := make([]SortOrdering, len(s.columns))
	for i := range s.columns {
		orders[i] = s.columns[i].Ordering
	}
	return orders
}

// Open implements QuerySink.Open
func (s *Order) Open() (io.WriteCloser, error) {
	kt := &sortstateKtop{parent: s}
	kt.kheap.fields = s.orderList()
	// we'll trim this later:
	kt.kheap.limit = s.limit.Limit + s.limit.Offset
	return splitter(kt), nil
}

// Close implements QuerySink.Close
func (s *Order) Close() error {
	s.prog.reset()
	// wait for all sorting threads to
	// indicate that they have been closed;
	// after this returns we can access
	// s.sub safely
	// s.wg.Wait()

	return s.finalizeKtop()
}

func (s *Order) finalizeKtop() error {
	var globalst ion.Symtab
	var tmp ion.Buffer

	// once we have accumulated this many data bytes,
	// flush the output buffer:
	const flushAt = PageSize / 2

	// temporary buffer for flushing:
	var out []byte
	flush := func() error {
		slice := tmp.Size()
		if slice == 0 {
			return nil
		}
		globalst.Marshal(&tmp, true)
		out = append(out[:0], tmp.Bytes()[slice:]...)
		out = append(out, tmp.Bytes()[:slice]...)
		globalst.Reset()
		tmp.Reset()
		_, err := s.dst.Write(out)
		return err
	}

	off := s.limit.Offset
	if off >= len(s.kheap.heaporder) {
		return flush() // symbol table + no data
	}
	// reverse the max-heap ordering
	// to end up with the final desired ordering,
	// taking care to ignore the top N OFFSET values;
	// we currently have LIMIT+OFFSET and we just want LIMIT
	want := len(s.kheap.heaporder) - off
	if s.limit.Limit < want {
		want = s.limit.Limit
	}
	final := make([]krecord, want)
	i := len(final) - 1
	for i >= 0 {
		n := heap.PopSlice(&s.kheap.heaporder, s.kheap.greater)
		final[i] = s.kheap.records[n]
		i--
	}
	for i := range final {
		rec := &final[i]
		rec.data.Encode(&tmp, &globalst)
		if tmp.Size() >= flushAt {
			err := flush()
			if err != nil {
				return err
			}
		}
	}
	return flush()
}

// ----------------------------------------------------------------------

func symbolize(sort *Order, dst *prog, findbc *bytecode, st *symtab, aux *auxbindings, global bool) error {
	if global {
		sort.symtabLock.Lock()
		defer sort.symtabLock.Unlock()
		err := sort.setSymbolTable(st)
		if err != nil {
			return err
		}

		return symbolizeLocal(sort, dst, findbc, st, aux)
	} else {
		return symbolizeLocal(sort, dst, findbc, st, aux)
	}
}

func symbolizeLocal(sort *Order, dst *prog, findbc *bytecode, st *symtab, aux *auxbindings) error {
	err := recompile(st, &sort.prog, dst, findbc, aux, "symbolizeLocal")
	if err != nil {
		return fmt.Errorf("sortstate.symbolize(): %w", err)
	}
	return nil
}

func bcfind(sort *Order, findbc *bytecode, delims []vmref, rp *rowParams) (out []vRegData, err error) {
	if findbc.compiled == nil {
		panic("bcfind() called before symbolize()")
	}

	// FIXME: don't encode knowledge about vectorization width here...
	blockCount := (len(delims) + bcLaneCount - 1) / bcLaneCount
	regCount := blockCount * len(sort.columns)
	minimumVStackSize := findbc.vstacksize + regCount*vRegSize

	findbc.ensureVStackSize(minimumVStackSize)
	findbc.allocStacks()

	findbc.prepare(rp)
	err = evalfind(findbc, delims, len(sort.columns))
	if err != nil {
		return
	}

	out = vRegDataFromVStackCast(&findbc.vstack, regCount)
	return
}

// ----------------------------------------------------------------------

type sortstateKtop struct {
	// the parent context for this sorting operation
	parent *Order

	// most recent aux bindings
	// passed to symbolize()
	aux *auxbindings
	// auxyms[i] corresponds to aux.bound[i]
	// for the most recent symbol table
	auxsyms []ion.Symbol

	// see the comment in `sortstateMulticolumn`
	parentNotified bool

	// bytecode for locating columns
	findbc bytecode
	prog   prog
	// most recent symbolize() symtab
	st *symtab

	kheap   kheap
	scratch ion.Buffer
	colbuf  [][]byte

	// if prefilter is true,
	// then filtbc is a program that
	// prefilters input rows
	prefilter bool
	recent    []byte
	filtbc    bytecode
	filtprog  prog
}

func (s *sortstateKtop) invalidatePrefilter() {
	s.prefilter = false
	s.recent = s.recent[:0]
	s.filtbc.reset()
	s.filtprog = prog{}
}

func (s *sortstateKtop) next() rowConsumer { return nil }

func (s *sortstateKtop) EndSegment() {
	s.findbc.dropScratch() // restored in symbolize()
	s.filtbc.dropScratch() // restored in s.symbolize()
}

func (s *sortstateKtop) symbolize(st *symtab, aux *auxbindings) error {
	if s.prefilter && s.filtprog.isStale(st, aux) {
		s.invalidatePrefilter()
	} else {
		s.filtbc.restoreScratch(st)
	}
	s.st = st
	s.aux = aux
	s.auxsyms = s.auxsyms[:0]
	for i := range s.aux.bound {
		s.auxsyms = append(s.auxsyms, st.Intern(s.aux.bound[i]))
	}
	return symbolize(s.parent, &s.prog, &s.findbc, st, aux, false)
}

func (s *sortstateKtop) bcfind(delims []vmref, rp *rowParams) ([]vRegData, error) {
	return bcfind(s.parent, &s.findbc, delims, rp)
}

func (s *sortstateKtop) bcfilter(delims []vmref, rp *rowParams) ([]vmref, error) {
	s.filtbc.prepare(rp)
	valid := evalfilterbc(&s.filtbc, delims)
	if s.filtbc.err != 0 {
		return nil, fmt.Errorf("ktop prefilter: %w", s.filtbc.err)
	}
	if valid > 0 {
		// the assembly already did the compression for us:
		for i := range rp.auxbound {
			rp.auxbound[i] = sanitizeAux(rp.auxbound[i], valid) // ensure rp.auxbound[i][valid:] is zeroed up to the lane multiple
		}
	}
	return delims[:valid], nil
}

// peek at the "greatest" retained element
// and compile a filter that drops everything
// that is larger/smaller than this element
func (s *sortstateKtop) maybePrefilter() error {
	if s.prefilter || len(s.kheap.records) == 0 {
		// already enabled
		return nil
	}
	rec := &s.kheap.records[s.kheap.heaporder[0]]
	if bytes.Equal(rec.order, s.recent) {
		// equivalent state
		return nil
	}

	p := &s.filtprog
	p.begin()

	prevequal := p.validLanes() // all the previous columns are equal
	result := p.missing()

	data := rec.order
	colnum := 0
	for len(data) > 0 {
		size := ion.SizeOf(data)
		f := data[:size]
		data = data[size:]
		t := ion.TypeOf(f)
		var (
			imm     *value
			cmplt   func(*value, *value) *value
			cmpeq   func(*value, *value) *value
			typeset expr.TypeSet
		)

		cmplt = p.less
		cmpeq = p.equals

		switch t {
		case ion.FloatType:
			fp, _, err := ion.ReadFloat64(f)
			if err != nil {
				return err
			}
			imm = p.constant(fp)
			typeset = expr.NumericType
		case ion.UintType:
			u, _, err := ion.ReadUint(f)
			if err != nil {
				return err
			}
			imm = p.constant(u)
			typeset = expr.NumericType
		case ion.IntType:
			i, _, err := ion.ReadInt(f)
			if err != nil {
				return err
			}
			imm = p.constant(i)
			typeset = expr.NumericType
		case ion.TimestampType:
			d, _, err := ion.ReadTime(f)
			if err != nil {
				return err
			}
			imm = p.constant(d)
			typeset = expr.TimeType
		case ion.StringType:
			s, _, err := ion.ReadString(f)
			if err != nil {
				return err
			}
			imm = p.constant(s)
			typeset = expr.StringType | expr.SymbolType
		default:
			return nil
		}

		v, err := compile(p, s.parent.columns[colnum].Node)
		if err != nil {
			return err
		}
		colnum++

		validtype := p.checkTag(v, typeset)

		// v[i] < recent[i]
		var less *value
		switch s.parent.columns[0].Ordering.Direction {
		case SortAscending:
			less = p.and(validtype, cmplt(v, imm))
		case SortDescending:
			less = p.and(validtype, cmplt(imm, v))
		default:
			return fmt.Errorf("unrecognized sort direction %d", s.parent.columns[0].Ordering.Direction)
		}

		// v[i] == recent[i]
		equal := p.and(validtype, cmpeq(v, imm))

		result = p.or(result, p.and(prevequal, less)) // column is strictly less
		prevequal = p.and(prevequal, equal)           // prevequal &= (col[i] == imm[i])
	} // for

	p.returnBK(p.validLanes(), result)

	p.symbolize(s.st, s.aux)
	err := p.compile(&s.filtbc, s.st, "sortstateKtop")
	s.filtbc.symtab = s.st.symrefs
	if err != nil {
		return err
	}
	// record the current prefilter state
	s.prefilter = true
	s.recent = append(s.recent[:0], rec.order...)
	return nil
}

// krecord is a record snapshot in a ktop heap
type krecord struct {
	order []byte
	data  ion.Datum
}

// kheap is a heap of ion records
type kheap struct {
	heaporder []int          // records[heaporder[...]] is the max-heap ordering
	records   []krecord      // raw record storage
	fields    []SortOrdering // ordering constraint
	limit     int            // target size
}

// insert a set of ordering fields into the heap,
// returning a non-nil pointer to the destination datum
// *if* the entry should be captured, or nil otherwise
func (k *kheap) insert(fields [][]byte) *ion.Datum {
	if len(fields) != len(k.fields) {
		panic("bad # fields")
	}
	flatten := func(dst []byte, src [][]byte) []byte {
		for i := range src {
			dst = append(dst, src[i]...)
		}
		return dst
	}
	if len(k.records) < k.limit {
		n := len(k.records)
		k.records = append(k.records, krecord{
			order: flatten(nil, fields),
		})
		heap.PushSlice(&k.heaporder, n, k.greater)
		return &k.records[n].data
	}
	top := &k.records[k.heaporder[0]]
	topdata := top.order
	for i := range fields {
		size := ion.SizeOf(topdata)
		if k.fields[i].Compare(fields[i], topdata[:size]) < 0 {
			// overwrite
			top.order = flatten(top.order[:0], fields)
			heap.FixSlice(k.heaporder, 0, k.greater)
			return &top.data
		}
		topdata = topdata[size:]
	}
	return nil
}

func (k *kheap) greater(left, right int) bool {
	lr := &k.records[left]
	rr := &k.records[right]
	return k.reccmp(lr, rr) > 0
}

func (k *kheap) reccmp(lr, rr *krecord) int {
	lrdata, rrdata := lr.order, rr.order
	for i := range k.fields {
		ls, rs := ion.SizeOf(lrdata), ion.SizeOf(rrdata)
		cmp := k.fields[i].Compare(lrdata[:ls], rrdata[:rs])
		if cmp != 0 {
			return cmp
		}
		lrdata, rrdata = lrdata[ls:], rrdata[rs:]
	}
	return 0
}

func (k *kheap) merge(from *kheap) {
	for len(from.heaporder) > 0 {
		n := heap.PopSlice(&from.heaporder, from.greater)
		rec := &from.records[n]
		if len(k.heaporder) < k.limit {
			// need more values, so append unconditionally
			n := len(k.records)
			k.records = append(k.records, *rec)
			heap.PushSlice(&k.heaporder, n, k.greater)
		} else if k.reccmp(&k.records[k.heaporder[0]], rec) > 0 {
			// need more records or have a better "worst" record candidate;
			// insert the value:
			k.records[k.heaporder[0]] = *rec
			heap.FixSlice(k.heaporder, 0, k.greater)
		}
	}
}

func (s *sortstateKtop) writeRows(delims []vmref, rp *rowParams) error {
	if len(delims) == 0 {
		return nil
	}
	if s.prefilter {
		var err error
		delims, err = s.bcfilter(delims, rp)
		if err != nil {
			return err
		}
		if len(delims) == 0 {
			return nil
		}
	}

	// locate fields within the src
	fieldsView, err := s.bcfind(delims, rp)
	if err != nil {
		return err
	}
	cols := shrink(s.colbuf, len(s.kheap.fields))
outer:
	for rowID := 0; rowID < len(delims); rowID++ {
		for j := 0; j < len(cols); j++ {
			cols[j] = getdelim(fieldsView, rowID, j, len(cols)).mem()
			if len(cols[j]) == 0 {
				continue outer // MISSING
			}
		}
		datptr := s.kheap.insert(cols)
		if datptr == nil {
			continue
		}
		s.scratch.Reset()
		s.scratch.BeginStruct(-1)
		// TODO: speed up the transcoding process here:
		data := delims[rowID].mem()
		for len(data) > 0 {
			var sym ion.Symbol
			sym, data, _ = ion.ReadLabel(data)
			s.scratch.BeginField(sym)
			size := ion.SizeOf(data)
			s.scratch.UnsafeAppend(data[:size])
			data = data[size:]
		}
		for j := range s.auxsyms {
			mem := rp.auxbound[j][rowID].mem()
			if len(mem) == 0 {
				continue
			}
			s.scratch.BeginField(s.auxsyms[j])
			s.scratch.UnsafeAppend(mem)
		}
		s.scratch.EndStruct()
		dat, _, _ := ion.ReadDatum(&s.st.Symtab, s.scratch.Bytes())
		dat.CloneInto(datptr)
		s.invalidatePrefilter()
	}
	if len(s.kheap.records) == s.kheap.limit {
		// since the heap is full,
		// we can begin trying to prefilter
		// anything that wouldn't be added trivially
		err := s.maybePrefilter()
		if err != nil {
			return fmt.Errorf("ktop: compiling prefilter: %w", err)
		}
	}
	return nil
}

func (s *sortstateKtop) Close() error {
	if s.parentNotified {
		return nil
	}
	s.parentNotified = true

	s.findbc.reset()
	s.filtbc.reset()
	if len(s.kheap.records) == 0 {
		return nil
	}
	s.parent.recordsLock.Lock()
	if len(s.parent.kheap.records) == 0 {
		s.parent.kheap = s.kheap
	} else {
		s.parent.kheap.merge(&s.kheap)
	}
	s.parent.recordsLock.Unlock()
	return nil
}
