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
	"sync/atomic"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/sorting"
)

// SortColumn represents a single entry in the 'ORDER BY' clause:
// "column-name [ASC|DESC] [NULLS FIRST|NULLS LAST]"
type SortColumn struct {
	Node      expr.Node
	Direction sorting.Direction
	Nulls     sorting.NullsOrder
}

// Order implements a QuerySink that applies
// an ordering to its output rows.
type Order struct {
	dst         io.Writer      // final destination
	columns     []SortColumn   // columns to sort
	limit       *sorting.Limit // optional limit parameters
	parallelism int            // number of threads

	// symbol table for the whole input
	symtab *ion.Symtab

	// lock for writing to `symtab`
	symtabLock sync.Mutex

	// collection of all records received in writeRows (for multicolumn sorting)
	records []sorting.IonRecord

	// collection of all records received in writeRows (for single column sorting)
	column  sorting.MixedTypeColumn
	chunkID uint32
	// map chunkID -> collected rows for given chunk
	//
	// Although we assign sequential chunkIDs, chunk processing obviously
	// may be completed in any order. This raw data will be used later
	// by a rows writer.
	rawrecords map[uint32][][]byte

	// collection of k-top rows
	ktop    *sorting.Ktop
	symtabs []ion.Symtab

	// lock for writing to `records`/`rawrecords`/'ktop'
	recordsLock sync.Mutex

	// allocator for bytes (Ion chunks)
	bytesAlloc bytesAllocator

	// allocator for field indices
	indicesAlloc indicesAllocator

	// mutable state shared
	// with sorting threads
	wg sync.WaitGroup

	rp sorting.RuntimeParameters
}

// NewOrder constructs a new Order QuerySink that
// sorts the provided columns (in left-to-right order).
// If limit is non-nil, then the number of rows output
// by the Order will be less than or equal to the limit.
func NewOrder(dst io.Writer, columns []SortColumn, limit *sorting.Limit, parallelism int) *Order {
	s := &Order{
		columns:     columns,
		limit:       limit,
		parallelism: parallelism,
		dst:         dst,
		rp:          sorting.NewRuntimeParameters(parallelism),
	}

	s.rp.UseStdlib = true // see #917

	s.rawrecords = make(map[uint32][][]byte)
	if s.useKtop() {
		s.ktop = s.newKtop()
	}

	s.bytesAlloc.Init(1024 * 1024)
	s.indicesAlloc.Init(len(columns) * 1024)

	return s
}

// setSymbolTable sets symbol table for the sorted data.
//
// It's expected that all input chunks have exactly the same symtab.
// Otherwise we either have to:
// 1. align all symtabs into a common one and recode all records;
// 2. preprend possibliy each record with symtab.
func (s *Order) setSymbolTable(st *symtab) error {
	s.symtabLock.Lock()
	defer s.symtabLock.Unlock()
	if s.symtab == nil {
		s.symtab = new(ion.Symtab)
		st.Symtab.CloneInto(s.symtab)
		return nil
	}
	if !st.Symtab.Equal(s.symtab) {
		return fmt.Errorf("symtab changed, can't sort data")
	}
	return nil
}

func (s *Order) useSingleColumnSorter() bool {
	// TODO: add support for `LIMIT` and `OFFSET`
	if s.limit != nil {
		return false
	}
	return s.rp.UseSingleColumnSorter && len(s.columns) == 1
}

func (s *Order) useKtop() bool {
	if s.limit == nil {
		return false
	}
	return s.limit.Kind == sorting.LimitToHeadRows
}

func (s *Order) newKtop() *sorting.Ktop {
	orders := make([]sorting.Ordering, len(s.columns))
	for i := range s.columns {
		orders[i].Direction = s.columns[i].Direction
		orders[i].Nulls = s.columns[i].Nulls
	}
	return sorting.NewKtop(s.limit.Limit, orders)
}

// Open implements QuerySink.Open
func (s *Order) Open() (io.WriteCloser, error) {
	s.wg.Add(1)

	if s.useKtop() {
		return splitter(&sortstateKtop{parent: s, ktop: s.newKtop()}), nil
	} else if s.useSingleColumnSorter() {
		chunkID := atomic.AddUint32(&s.chunkID, 1) - 1
		recordID := uint64(chunkID) << 32 // start with ID().chunk() = chunkID and ID().row() == 0
		return splitter(&sortstateSingleColumn{parent: s, chunkID: chunkID, recordID: recordID}), nil
	}

	return splitter(&sortstateMulticolumn{parent: s}), nil
}

// Close implements QuerySink.Close
func (s *Order) Close() error {
	// wait for all sorting threads to
	// indicate that they have been closed;
	// after this returns we can access
	// s.sub safely
	s.wg.Wait()

	if !s.useKtop() && s.symtab == nil {
		if len(s.records) == 0 {
			// no data at all
			return nil
		}

		return fmt.Errorf("malformed Ion: there are data to sort, but no symbol table was set")
	}

	if s.useKtop() {
		return s.finalizeKtop()
	} else if s.useSingleColumnSorter() {
		return s.finalizeSingleColumnSorting()
	}

	return s.finalizeMultiColumnSorting()
}

func (s *Order) finalizeMultiColumnSorting() error {
	rowsWriter, err := sorting.NewRowsWriter(s.dst, s.symtab, s.rp.ChunkAlignment)
	if err != nil {
		return err
	}

	directions := make([]sorting.Direction, len(s.columns))
	nullsOrder := make([]sorting.NullsOrder, len(s.columns))
	for i := range s.columns {
		directions[i] = s.columns[i].Direction
		nullsOrder[i] = s.columns[i].Nulls
	}

	err = sorting.ByColumns(s.records, directions, nullsOrder, s.limit, rowsWriter, &s.rp)
	err2 := rowsWriter.Close()
	if err != nil {
		return err
	}

	return err2
}

func (s *Order) finalizeSingleColumnSorting() error {
	rowsWriter, err := sorting.NewRowsWriter(s.dst, s.symtab, s.rp.ChunkAlignment)
	if err != nil {
		return err
	}

	err = sorting.ByColumn(s.rawrecords, &s.column, s.columns[0].Direction, s.columns[0].Nulls,
		s.limit, rowsWriter, &s.rp)

	err2 := rowsWriter.Close()
	if err != nil {
		return err
	}

	return err2
}

func (s *Order) finalizeKtop() error {
	var row []ion.Field
	var str ion.Struct
	var sym ion.Symbol
	var val ion.Datum
	var tmp ion.Buffer
	var globalst ion.Symtab
	var err error
	records := s.ktop.Capture()
	// for each record, re-encode into
	// the new global symbol table and then
	// serialize into the temporary buffer
	for i := range records {
		st := &s.symtabs[records[i].SymtabID]
		row = row[:0]
		contents := records[i].Bytes()
		for len(contents) > 0 {
			sym, contents, err = ion.ReadLabel(contents)
			if err != nil {
				return err
			}
			val, contents, err = ion.ReadDatum(st, contents)
			if err != nil {
				return err
			}
			row = append(row, ion.Field{
				Label: st.Get(sym),
				Value: val,
			})
		}
		str.SetFields(&globalst, row)
		str.Encode(&tmp, &globalst)
	}
	slice := tmp.Size()
	globalst.Marshal(&tmp, true)
	out := make([]byte, tmp.Size())
	pre := copy(out, tmp.Bytes()[slice:])
	copy(out[pre:], tmp.Bytes()[:slice])
	_, err = s.dst.Write(out)
	return err
}

// ----------------------------------------------------------------------

func symbolize(sort *Order, findbc *bytecode, st *symtab, global bool) error {
	var program prog

	program.Begin()
	mem0 := program.InitMem()
	var mem []*value
	for i := range sort.columns {
		val, err := program.compileStore(mem0, sort.columns[i].Node, stackSlotFromIndex(regV, i), true)
		if err != nil {
			return err
		}
		mem = append(mem, val)
	}
	program.Return(program.MergeMem(mem...))
	program.symbolize(st)
	err := program.compile(findbc)
	findbc.symtab = st.symrefs
	if err != nil {
		return fmt.Errorf("sortstate.symbolize(): %w", err)
	}
	if global {
		return sort.setSymbolTable(st)
	}
	return nil
}

func bcfind(sort *Order, findbc *bytecode, delims []vmref) (out []vRegLayout, err error) {
	if findbc.compiled == nil {
		return out, fmt.Errorf("sortstate.bcfind() before symbolize()")
	}

	// FIXME: don't encode knowledge about vectorization width here...
	blockCount := (len(delims) + bcLaneCount - 1) / bcLaneCount
	regCount := blockCount * len(sort.columns)
	minimumVStackSize := findbc.vstacksize + regCount*vRegSize

	findbc.ensureVStackSize(minimumVStackSize)
	findbc.allocStacks()

	if findbc.scratch != nil {
		findbc.scratch = findbc.scratch[:findbc.scratchreserve]
	}

	err = evalfind(findbc, delims, len(sort.columns))
	if err != nil {
		return
	}

	out = vRegLayoutFromVStackCast(&findbc.vstack, regCount)
	return
}

// ----------------------------------------------------------------------

type sortstateMulticolumn struct {
	// the parent context for this sorting operation
	parent *Order

	// bytecode for locating columns
	findbc bytecode
}

func (s *sortstateMulticolumn) symbolize(st *symtab) error {
	return symbolize(s.parent, &s.findbc, st, true)
}

func (s *sortstateMulticolumn) bcfind(delims []vmref) ([]vRegLayout, error) {
	return bcfind(s.parent, &s.findbc, delims)
}

func (s *sortstateMulticolumn) writeRows(delims []vmref) error {
	// Note: we have to copy all input data, as:
	// 1. the 'src' buffer is mutable,
	// 2. the 'delims' array is mutable too.
	//
	// Since copying is unavoidable, we split input into
	// separate rows.
	if len(delims) == 0 {
		return nil
	}

	// locate fields within the src
	fieldsView, err := s.bcfind(delims)
	if err != nil {
		return err
	}

	// make room for the incoming records
	s.parent.recordsLock.Lock()
	if len(s.parent.records)+len(delims) > cap(s.parent.records) {
		newCapacity := 2 * cap(s.parent.records)
		if newCapacity == 0 {
			newCapacity = 1024
		}
		s.parent.records = slices.Grow(s.parent.records, newCapacity)
	}

	// split input data into separate records
	blockID := 0
	columnCount := len(s.parent.columns)

	for rowID := 0; rowID < len(delims); rowID++ {
		laneID := rowID & bcLaneCountMask

		// extract the record data
		d := delims[rowID]
		bytes := d.mem()

		var record sorting.IonRecord

		// calculate space for boxed values
		for columnID := 0; columnID < columnCount; columnID++ {
			fieldSize := fieldsView[blockID+columnID].sizes[laneID]
			record.Boxed += fieldSize
		}

		// allocate memory and copy original row data
		record.Raw = s.parent.bytesAlloc.Allocate(len(bytes) + int(record.Boxed))
		copy(record.Raw[record.Boxed:], bytes)

		// copy the field locations and also the boxed values
		record.FieldDelims = s.parent.indicesAlloc.Allocate(columnCount)

		boxedOffset := uint32(0)
		for columnID := 0; columnID < columnCount; columnID++ {
			fieldOffset := fieldsView[blockID+columnID].offsets[laneID]
			fieldSize := fieldsView[blockID+columnID].sizes[laneID]
			record.FieldDelims[columnID][0] = boxedOffset
			record.FieldDelims[columnID][1] = fieldSize
			copy(record.Raw[boxedOffset:], vmref{fieldOffset, fieldSize}.mem())
			boxedOffset += fieldSize
		}

		s.parent.records = append(s.parent.records, record)

		if laneID == bcLaneCountMask {
			blockID += columnCount
		}
	}

	s.parent.recordsLock.Unlock()

	return nil
}

func (s *sortstateMulticolumn) Close() error {
	s.parent.wg.Done()

	return nil
}

// ----------------------------------------------------------------------

type sortstateSingleColumn struct {
	// the parent context for this sorting operation
	parent *Order

	// bytecode for locating columns
	findbc bytecode

	chunkID   uint32
	recordID  uint64
	records   [][]byte
	subcolumn sorting.MixedTypeColumn
}

func (s *sortstateSingleColumn) symbolize(st *symtab) error {
	return symbolize(s.parent, &s.findbc, st, true)
}

func (s *sortstateSingleColumn) bcfind(delims []vmref) ([]vRegLayout, error) {
	return bcfind(s.parent, &s.findbc, delims)
}

func (s *sortstateSingleColumn) writeRows(delims []vmref) error {
	if len(delims) == 0 {
		return nil
	}

	// grow the list of records
	if len(s.records)+len(delims) > cap(s.records) {
		newCapacity := 2 * cap(s.records)
		if newCapacity == 0 {
			newCapacity = 1024
		}
		tmp := make([][]byte, len(s.records), newCapacity)
		copy(tmp, s.records)
		s.records = tmp
	}

	// locate fields within the src
	fieldsView, err := s.bcfind(delims)
	if err != nil {
		return err
	}

	// split input data into separate records
	blockID := 0
	columnCount := len(s.parent.columns)

	for rowID := 0; rowID < len(delims); rowID++ {
		laneID := rowID & bcLaneCountMask

		// extract the record data
		bytes := delims[rowID].mem()

		// append record
		record := make([]byte, len(bytes))
		//record := s.parent.bytesAlloc.Allocate(len(bytes)) -- slower
		copy(record, bytes)
		s.records = append(s.records, record)

		// get the field value
		item := fieldsView[blockID].item(laneID)
		if item.size() > 0 {
			err := s.subcolumn.Add(s.recordID, item.mem())
			if err != nil {
				return err
			}
		} else {
			s.subcolumn.AddMissing(s.recordID)
		}

		s.recordID += 1

		if laneID == bcLaneCountMask {
			blockID += columnCount
		}
	}

	return nil
}

func (s *sortstateSingleColumn) Close() error {
	s.parent.recordsLock.Lock()

	// merge column with the global one
	s.parent.column.Append(&s.subcolumn)

	// and move the collected records
	s.parent.rawrecords[s.chunkID] = s.records
	s.parent.recordsLock.Unlock()

	s.parent.wg.Done()
	return nil
}

// ----------------------------------------------------------------------

type sortstateKtop struct {
	// the parent context for this sorting operation
	parent *Order

	// bytecode for locating columns
	findbc bytecode

	// local k-top rows
	ktop *sorting.Ktop

	// a buffer to keep the scratch + record data of the current record in `writeRows`
	buffer []byte

	// list of all captured symbol tables
	symtabs []ion.Symtab
	// # of captures of symtabs[len(symtabs)-1]
	captures int

	// a buffer to keep the fields for the current record in `writeRows`
	fields [][2]uint32

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
	s.recent = nil
	s.filtbc.reset()
	s.filtprog = prog{}
}

func (s *sortstateKtop) symbolize(st *symtab) error {
	if s.captures > 0 || len(s.symtabs) == 0 {
		s.symtabs = append(s.symtabs, ion.Symtab{})
	}

	if s.prefilter && s.filtprog.IsStale(st) {
		s.invalidatePrefilter()
	}

	// copy the source symbol table
	// so that we can still use it after
	// it has been updated
	st.Symtab.CloneInto(&s.symtabs[len(s.symtabs)-1])
	return symbolize(s.parent, &s.findbc, st, false)
}

func (s *sortstateKtop) bcfind(delims []vmref) ([]vRegLayout, error) {
	return bcfind(s.parent, &s.findbc, delims)
}

func (s *sortstateKtop) bcfilter(delims []vmref) ([]vmref, error) {
	n := evalfilterbc(&s.filtbc, delims)
	if s.filtbc.err != 0 {
		return nil, fmt.Errorf("ktop prefilter: %w", s.filtbc.err)
	}
	return delims[:n], nil
}

// peek at the "greatest" retained element
// and compile a filter that drops everything
// that is larger/smaller than this element
func (s *sortstateKtop) maybePrefilter() error {
	if s.prefilter {
		// already enabled
		return nil
	}
	if len(s.parent.columns) > 1 {
		// TODO: implement for multiple columns
		return nil
	}
	rec := s.ktop.Greatest()
	if rec == nil {
		return nil
	}
	f := rec.UnsafeField(0)
	if bytes.Equal(f, s.recent) {
		return nil // up-to-date
	}
	t := ion.TypeOf(f)
	p := &s.filtprog
	p.Begin()
	var (
		imm       *value
		unmatched func(*value) *value
		cmp       func(*value, *value) *value
	)
	switch t {
	case ion.FloatType:
		fp, _, err := ion.ReadFloat64(f)
		if err != nil {
			return err
		}
		imm = p.Constant(fp)
		unmatched = p.notNumber
		cmp = p.Less
	case ion.UintType:
		u, _, err := ion.ReadUint(f)
		if err != nil {
			return err
		}
		imm = p.Constant(u)
		unmatched = p.notNumber
		cmp = p.Less
	case ion.IntType:
		i, _, err := ion.ReadInt(f)
		if err != nil {
			return err
		}
		imm = p.Constant(i)
		unmatched = p.notNumber
		cmp = p.Less
	case ion.TimestampType:
		d, _, err := ion.ReadTime(f)
		if err != nil {
			return err
		}
		imm = p.Constant(d)
		unmatched = p.notTime
		cmp = p.compileTimeOrdered
	default:
		// TODO: support string comparison
		return nil
	}
	v, err := compile(p, s.parent.columns[0].Node)
	if err != nil {
		return err
	}
	var keep *value
	switch s.parent.columns[0].Direction {
	case sorting.Ascending:
		keep = cmp(v, imm)
	case sorting.Descending:
		keep = cmp(imm, v)
	default:
		return fmt.Errorf("unrecognized sort direction %d", s.parent.columns[0].Direction)
	}
	keep = p.Or(keep, unmatched(v))
	p.Return(keep)
	p.symbolize(&s.symtabs[len(s.symtabs)-1])
	err = p.compile(&s.filtbc)
	if err != nil {
		return err
	}
	// record the current prefilter state
	s.prefilter = true
	s.recent = f
	return nil
}

func (s *sortstateKtop) writeRows(delims []vmref) error {
	if len(delims) == 0 {
		return nil
	}
	if s.prefilter {
		var err error
		delims, err = s.bcfilter(delims)
		if err != nil {
			return err
		}
		if len(delims) == 0 {
			return nil
		}
	}

	// locate fields within the src
	fieldsView, err := s.bcfind(delims)
	if err != nil {
		return err
	}

	if s.buffer == nil {
		s.buffer = make([]byte, 16*1024)                    // this may grow
		s.fields = make([][2]uint32, len(s.parent.columns)) // this is constant
	}

	// split input data into separate records
	columnCount := len(s.parent.columns)

	var record sorting.IonRecord
	record.SymtabID = len(s.symtabs) - 1

outer:
	for rowID := 0; rowID < len(delims); rowID++ {
		// extract the record data
		bytes := delims[rowID].mem()

		// calculate space for boxed values
		record.Boxed = 0
		for columnID := 0; columnID < columnCount; columnID++ {
			record.Boxed += getdelim(fieldsView, rowID, columnID, columnCount)[1]
		}

		// grow byte buffer when necessary
		bufsize := int(record.Boxed) + len(bytes)
		if bufsize > cap(s.buffer) {
			s.buffer = make([]byte, bufsize)
		}

		s.buffer = s.buffer[:bufsize]

		// copy original row data
		copy(s.buffer[record.Boxed:], bytes)

		// copy field delimiters and boxed values (if any)
		boxedOffset := 0
		for columnID := 0; columnID < columnCount; columnID++ {
			it := getdelim(fieldsView, rowID, columnID, columnCount)
			size := it.size()
			if size == 0 {
				continue outer // MISSING field (cannot sort)
			}
			s.fields[columnID][0] = uint32(boxedOffset)
			s.fields[columnID][1] = uint32(size)
			copy(s.buffer[boxedOffset:], it.mem())
			boxedOffset += size
		}
		record.Raw = s.buffer
		record.FieldDelims = s.fields

		// when record is added, its data is being copied,
		// and there will be a new item we need to prefilter against
		if s.ktop.Add(&record) {
			s.captures++
			s.invalidatePrefilter()
		}
	}
	if s.ktop.Full() {
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
	s.parent.recordsLock.Lock()
	base := len(s.parent.symtabs)
	recs := s.ktop.Records()
	for i := range recs {
		recs[i].SymtabID += base
	}
	s.parent.symtabs = append(s.parent.symtabs, s.symtabs...)
	s.parent.ktop.Merge(s.ktop)
	s.parent.recordsLock.Unlock()
	s.parent.wg.Done()

	return nil
}

// ----------------------------------------------------------------------

// bytesAllocator allocates smaller chunks of arbitrary byte
// from a pre-allocated block of memory.
//
// It purpose is to reduce GC overhead, while we know that
// that all allocated bytes has to be freed at the same time
// and will not escape the lifetime of sorting routine.
type bytesAllocator struct {
	blockSize int
	blocks    [][]byte
	lock      sync.Mutex
}

func (a *bytesAllocator) Init(blockSize int) {
	a.blockSize = blockSize
	a.blocks = make([][]byte, 1)
	a.blocks[0] = make([]byte, a.blockSize)
}

func (a *bytesAllocator) Allocate(size int) []byte {
	a.lock.Lock()
	defer a.lock.Unlock()

	last := &a.blocks[len(a.blocks)-1]
	if len(*last) >= size {
		bytes := (*last)[:size:size]
		*last = (*last)[size:]

		return bytes
	}

	if size <= a.blockSize {
		block := make([]byte, a.blockSize)
		bytes := block[:size:size]
		a.blocks = append(a.blocks, block[size:])

		return bytes
	} else {
		return make([]byte, size)
	}
}

// indicesAllocator allocates smaller array for indices
// from a pre-allocated array of uint32.
//
// See comment for bytesAllocator
type indicesAllocator struct {
	blockSize int
	blocks    [][][2]uint32
	lock      sync.Mutex
}

func (a *indicesAllocator) Init(blockSize int) {
	a.blockSize = blockSize
	a.blocks = make([][][2]uint32, 1)
	a.blocks[0] = make([][2]uint32, a.blockSize)
}

func (a *indicesAllocator) Allocate(size int) [][2]uint32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	last := &a.blocks[len(a.blocks)-1]
	if len(*last) >= size {
		indices := (*last)[:size:size]
		*last = (*last)[size:]

		return indices
	}

	if size <= a.blockSize {
		block := make([][2]uint32, a.blockSize)
		indices := block[:size:size]
		a.blocks = append(a.blocks, block[size:])

		return indices
	} else {
		return make([][2]uint32, size)
	}
}
