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
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/utf8"

	"golang.org/x/exp/slices"
)

// NewSystemDatashape constucts a QuerySink implementing
// the `SYSTEM_DATASHAPE(*)` aggregation
func NewSystemDatashape(dst QuerySink) QuerySink {
	return &systemDatashape{dst: dst}
}

// NewSystemDatashapeMerge constructs a QuerySink that merges
// the results of multiple `SYSTEM_DATASHAPE(*)` aggregations
func NewSystemDatashapeMerge(dst QuerySink) QuerySink {
	return &systemDatashapeMerge{dst: dst}
}

// systemDatashapeMaxRows is the maximum number of fields returned by datashape view
const systemDatashapeMaxRows = 10_000

const (
	totalField      = "total"
	errorField      = "error"
	fieldsField     = "fields"
	nullField       = "null"
	boolField       = "bool"
	intField        = "int"
	floatField      = "float"
	decimalField    = "decimal"
	timestampField  = "timestamp"
	stringField     = "string"
	listField       = "list"
	structField     = "struct"
	sexpField       = "sexp"
	clobField       = "clob"
	blobField       = "blob"
	annotationField = "annotation"
	listItemsField  = "$items"

	stringMinLengthField = "string-min-length"
	stringMaxLengthField = "string-max-length"
	intMinValueField     = "int-min-value"
	intMaxValueField     = "int-max-value"
	floatMinValueField   = "float-min-value"
	floatMaxValueField   = "float-max-value"
)

// systemDatashape is the main QuerySink that collects
// the data shape of the whole dataset.
type systemDatashape struct {
	dst       QuerySink
	datashape *queryDatashape
	mutex     sync.Mutex
}

func (s *systemDatashape) Open() (io.WriteCloser, error) {
	return splitter(&systemDatashapeTable{
		parent:    s,
		datashape: newQueryDatashape(),
	}), nil
}

func (s *systemDatashape) Close() error {
	var st ion.Symtab
	var data ion.Buffer
	var buf ion.Buffer

	s.datashape.writeIon(&buf, &st)

	st.Marshal(&data, true)
	data.UnsafeAppend(buf.Bytes())

	return writeIon(&data, s.dst)
}

func writeIon(b *ion.Buffer, dst QuerySink) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	_, err = w.Write(b.Bytes())
	if err != nil {
		w.Close()
		return err
	}
	err = w.Close()
	err2 := dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

// ----------------------------------------

// systemDatashapeTable is a stream that collects partial shape
// for Ion data it consumes. On close, it updates the
// global shape
type systemDatashapeTable struct {
	parent    *systemDatashape
	datashape *queryDatashape
	symtab    *symtab
	symlength []int64 // cache for utf8.RuneCount(symtab.Lookup(id)); see symbolLength and symbolize
}

var (
	_ rowConsumer = &systemDatashapeTable{}
)

// implementation of rowConsumer.symbolize
func (s *systemDatashapeTable) symbolize(st *symtab, aux *auxbindings) error {
	s.symtab = st

	// reset cache for symbols length
	n := st.MaxID()
	s.symlength = slices.Grow(s.symlength, n)
	s.symlength = s.symlength[:n]
	for i := range s.symlength {
		s.symlength[i] = -1 // mark as not-filled
	}
	return nil
}

// implementation of rowConsumer.next
func (s *systemDatashapeTable) next() rowConsumer {
	return nil
}

// implementation of rowConsumer.writeRows
func (s *systemDatashapeTable) writeRows(delims []vmref, params *rowParams) error {
	s.datashape.total += int64(len(delims))
	for i := range delims {
		record := delims[i].mem()
		err := s.processRecord(s.datashape.root, record)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *systemDatashapeTable) Close() error {
	s.parent.mutex.Lock()
	defer s.parent.mutex.Unlock()

	if s.parent.datashape == nil {
		s.parent.datashape = newQueryDatashape()
	}

	s.parent.datashape.merge(s.datashape)
	return nil
}

// symbolLength returns the rune count of a string associated with the symbol id.
// Returns 0 if the symbol is not in the symtab.
func (s *systemDatashapeTable) symbolLength(id ion.Symbol) int64 {
	if int(id) >= len(s.symlength) {
		return 0
	}

	n := int(id)
	k := s.symlength[n]
	if k >= 0 {
		return k
	}

	str, ok := s.symtab.Lookup(id)
	if !ok {
		k = 0
	} else {
		b := []byte(str)
		k = int64(utf8.ValidStringLength(b))
	}

	s.symlength[n] = k
	return k
}

func (s *systemDatashapeTable) processRecord(node *datashapeNode, record []byte) error {
	_, err := ion.UnpackStructBody(&s.symtab.Symtab, record, func(field string, val []byte) error {
		child, ok := node.child(field)
		if !ok {
			return nil
		}

		return s.processValue(child, val)
	})

	return err
}

func (s *systemDatashapeTable) processList(node *datashapeNode, body []byte) error {
	_, err := ion.UnpackList(body, func(val []byte) error {
		return s.processValue(node, val)
	})

	return err
}

func (s *systemDatashapeTable) processStruct(node *datashapeNode, body []byte) error {
	_, err := ion.UnpackStruct(&s.symtab.Symtab, body, func(field string, val []byte) error {
		child, ok := node.child(field)
		if !ok {
			return nil
		}

		return s.processValue(child, val)
	})

	return err
}

func (s *systemDatashapeTable) processValue(node *datashapeNode, val []byte) error {
	typ := ion.TypeOf(val)
	node.update(typ)

	updateStringRanges := func(str []byte) {
		// Note: utf8.RuneCount is expansive, we first compare raw bytes length
		n := len(str)
		changed := false
		if n > node.stats.maxBytesLen {
			node.stats.maxBytesLen = n
			changed = true
		}
		if n < node.stats.minBytesLen {
			node.stats.minBytesLen = n
			changed = true
		}

		if changed {
			l := int64(utf8.ValidStringLength(str))
			node.stats.rangeStringLen.update(l)
		}
	}

	switch typ {
	case ion.StructType:
		err := s.processStruct(node, val)
		if err != nil {
			return err
		}

	case ion.ListType:
		child, ok := node.child(listItemsField)
		if !ok {
			return nil
		}
		err := s.processList(child, val)
		if err != nil {
			return err
		}

	case ion.StringType:
		s, _ := ion.Contents(val)
		updateStringRanges(s)

	case ion.SymbolType:
		id, _, err := ion.ReadSymbol(val)
		if err != nil {
			return nil
		}

		node.stats.rangeStringLen.update(s.symbolLength(id))

	case ion.UintType, ion.IntType:
		n, _, err := ion.ReadInt(val)
		if err == nil {
			node.stats.rangeInt64.update(n)
		}

	case ion.FloatType:
		n, _, err := ion.ReadFloat64(val)
		if err == nil {
			node.stats.rangeFloat64.update(n)
		}
	}

	return nil
}

// ----------------------------------------

// queryDatashape is a collection of object paths
// associated with Ion types histogram
type queryDatashape struct {
	total    int64 // The number of rows read
	capacity int64 // The maximum number of entries

	root *datashapeNode // The root node of trie
}

func newQueryDatashape() *queryDatashape {
	ds := &queryDatashape{}
	ds.capacity = systemDatashapeMaxRows
	ds.root, _ = newDatashapeNode(&ds.capacity)

	return ds
}

func (qs *queryDatashape) merge(qs2 *queryDatashape) {
	qs.total += qs2.total

	// merge the tries
	type stackItem struct {
		src *datashapeNode
		dst *datashapeNode
	}

	stack := []stackItem{stackItem{
		src: qs2.root,
		dst: qs.root,
	}}

	for len(stack) > 0 {
		n := len(stack)
		top := stack[n-1]
		stack = stack[:n-1]

		top.dst.stats.merge(&top.src.stats)

		for srcField, srcChild := range top.src.next {
			dstField, ok := top.dst.child(srcField)
			if !ok {
				return
			}

			stack = append(stack, stackItem{
				src: srcChild,
				dst: dstField,
			})
		}
	}
}

func (qs *queryDatashape) writeIon(buf *ion.Buffer, st *ion.Symtab) {
	buf.BeginStruct(-1)
	{
		if qs.capacity <= 0 {
			buf.BeginField(st.Intern(errorField))
			buf.WriteString(fmt.Sprintf("the total number of fields execeeded limit %d", systemDatashapeMaxRows))
		}
		buf.BeginField(st.Intern(totalField))
		buf.WriteInt(qs.total)
		buf.BeginField(st.Intern(fieldsField))
		buf.BeginStruct(-1)

		tmp := make(map[string]*ionStatistics)
		paths := make([]string, 0, systemDatashapeMaxRows-qs.capacity)
		visitDatashapeTrie(qs.root, func(path string, stats *ionStatistics) {
			tmp[path] = stats
			paths = append(paths, path)
		})

		sort.Strings(paths)

		for _, path := range paths {
			buf.BeginField(st.Intern(path))
			tmp[path].writeIon(buf, st)
		}
		buf.EndStruct()
	}
	buf.EndStruct()
}

// datashapeNode is a trie node that represents ion statistics
// for given element of path.
type datashapeNode struct {
	stats    ionStatistics             // Ion statistics for each node
	next     map[string]*datashapeNode // Next nodes, addressed by a path's part
	capacity *int64                    // Total trie capacity
}

func newDatashapeNode(capacity *int64) (*datashapeNode, bool) {
	*capacity -= 1

	node := &datashapeNode{
		next:     make(map[string]*datashapeNode),
		capacity: capacity,
	}

	node.stats.init()

	return node, *capacity >= 0
}

func (n *datashapeNode) update(typ ion.Type) {
	n.stats.count[int(typ)] += 1
}

func (n *datashapeNode) child(field string) (*datashapeNode, bool) {
	c, ok := n.next[field]
	if ok {
		return c, true
	}

	c, ok = newDatashapeNode(n.capacity)
	if ok {
		n.next[field] = c
	}

	return c, ok
}

func visitDatashapeTrie(root *datashapeNode, fn func(path string, stats *ionStatistics)) {
	type stackItem struct {
		node     *datashapeNode
		fullpath string
	}

	stack := []stackItem{stackItem{
		node:     root,
		fullpath: "",
	}}
	for len(stack) > 0 {
		n := len(stack)
		top := stack[n-1]
		stack = stack[:n-1]

		if len(top.fullpath) > 0 {
			fn(top.fullpath, &top.node.stats)
		}

		for field, child := range top.node.next {
			fullpath := field
			if len(top.fullpath) > 0 {
				fullpath = top.fullpath + "." + field
			}

			stack = append(stack, stackItem{
				node:     child,
				fullpath: fullpath,
			})
		}
	}
}

type minMaxInt64 struct {
	min, max int64
}

func (m *minMaxInt64) merge(o *minMaxInt64) {
	if o.min < m.min {
		m.min = o.min
	}
	if o.max > m.max {
		m.max = o.max
	}
}

func (m *minMaxInt64) update(v int64) {
	if v < m.min {
		m.min = v
	}
	if v > m.max {
		m.max = v
	}
}

type minMaxFloat64 struct {
	min, max float64
}

func (m *minMaxFloat64) merge(o *minMaxFloat64) {
	if o.min < m.min {
		m.min = o.min
	}

	if o.max > m.max {
		m.max = o.max
	}
}

func (m *minMaxFloat64) update(v float64) {
	if v < m.min {
		m.min = v
	}
	if v > m.max {
		m.max = v
	}
}

// ionTypeCounter maps an Ion type to the count of fields with this type
type ionTypeCounter [16]int64

func (c *ionTypeCounter) hasIntDetails() bool {
	return c[int(ion.IntType)]+c[int(ion.UintType)] > 0
}

func (c *ionTypeCounter) hasFloatDetails() bool {
	return c[int(ion.FloatType)] > 0
}

func (c *ionTypeCounter) hasStringDetails() bool {
	return c[int(ion.StringType)]+c[int(ion.SymbolType)] > 0
}

func (c *ionTypeCounter) merge(o *ionTypeCounter) {
	for i, count := range o {
		c[i] += count
	}
}

func (c *ionTypeCounter) writeIon(buf *ion.Buffer, st *ion.Symtab) {
	writeCounter := func(name string, count int64) {
		if count > 0 {
			buf.BeginField(st.Intern(name))
			buf.WriteInt(count)
		}
	}

	writeCounter(nullField, c[int(ion.NullType)])
	writeCounter(boolField, c[int(ion.BoolType)])
	writeCounter(intField, c[int(ion.IntType)]+c[int(ion.UintType)])
	writeCounter(floatField, c[int(ion.FloatType)])
	writeCounter(decimalField, c[int(ion.DecimalType)])
	writeCounter(timestampField, c[int(ion.TimestampType)])
	writeCounter(stringField, c[int(ion.StringType)]+c[int(ion.SymbolType)])
	writeCounter(listField, c[int(ion.ListType)])
	writeCounter(structField, c[int(ion.StructType)])
	writeCounter(sexpField, c[int(ion.SexpType)])
	writeCounter(clobField, c[int(ion.ClobType)])
	writeCounter(blobField, c[int(ion.BlobType)])
	writeCounter(annotationField, c[int(ion.AnnotationType)])
}

// ion.Type => the count of fields
type ionStatistics struct {
	count          ionTypeCounter
	rangeInt64     minMaxInt64
	rangeFloat64   minMaxFloat64
	rangeStringLen minMaxInt64
	minBytesLen    int
	maxBytesLen    int
}

func (s *ionStatistics) init() {
	s.rangeInt64.min = math.MaxInt64
	s.rangeInt64.max = math.MinInt64
	s.rangeFloat64.min = math.MaxFloat64
	s.rangeFloat64.max = -math.MaxFloat64
	s.rangeStringLen.min = math.MaxInt64
	s.rangeStringLen.max = math.MinInt64
	s.minBytesLen = math.MaxInt64
	s.maxBytesLen = math.MinInt64
}

func (s *ionStatistics) merge(o *ionStatistics) {
	s.count.merge(&o.count)
	s.rangeInt64.merge(&o.rangeInt64)
	s.rangeFloat64.merge(&o.rangeFloat64)
	s.rangeStringLen.merge(&o.rangeStringLen)
}

func (s *ionStatistics) writeIon(buf *ion.Buffer, st *ion.Symtab) {
	writeInt64 := func(name string, v int64) {
		buf.BeginField(st.Intern(name))
		buf.WriteInt(v)
	}

	writeFloat64 := func(name string, v float64) {
		buf.BeginField(st.Intern(name))
		buf.WriteFloat64(v)
	}

	buf.BeginStruct(-1)
	s.count.writeIon(buf, st)
	if s.count.hasIntDetails() {
		writeInt64(intMinValueField, s.rangeInt64.min)
		writeInt64(intMaxValueField, s.rangeInt64.max)
	}
	if s.count.hasFloatDetails() {
		writeFloat64(floatMinValueField, s.rangeFloat64.min)
		writeFloat64(floatMaxValueField, s.rangeFloat64.max)
	}
	if s.count.hasStringDetails() {
		writeInt64(stringMinLengthField, s.rangeStringLen.min)
		writeInt64(stringMaxLengthField, s.rangeStringLen.max)
	}
	buf.EndStruct()
}

// systemDatashapeMerge is a QuerySink that collects
// partial data shapes and merges them into the final
// data shape.
type systemDatashapeMerge struct {
	dst       QuerySink
	datashape *queryDatashapeFinal
	mutex     sync.Mutex
}

func (s *systemDatashapeMerge) Open() (io.WriteCloser, error) {
	return splitter(&systemDatashapeMergeTable{
		parent:    s,
		datashape: newQueryDatashapeMerge(),
	}), nil
}

func (s *systemDatashapeMerge) Close() error {
	var st ion.Symtab
	var data ion.Buffer
	var buf ion.Buffer

	s.datashape.writeIon(&buf, &st)

	st.Marshal(&data, true)
	data.UnsafeAppend(buf.Bytes())

	return writeIon(&data, s.dst)
}

type systemDatashapeMergeTable struct {
	parent    *systemDatashapeMerge
	datashape *queryDatashapeFinal
	symtab    *symtab
}

var (
	_ rowConsumer = &systemDatashapeMergeTable{}
)

// implementation of rowConsumer.symbolize
func (s *systemDatashapeMergeTable) symbolize(st *symtab, aux *auxbindings) error {
	s.symtab = st
	return nil
}

// implementation of rowConsumer.next
func (s *systemDatashapeMergeTable) next() rowConsumer {
	return nil
}

// implementation of rowConsumer.writeRows
func (s *systemDatashapeMergeTable) writeRows(delims []vmref, params *rowParams) error {
	n := len(delims)
	if n != 1 {
		return fmt.Errorf("systemDatashapeMergeTable: expected exactly one input row, got %d", n)
	}

	return s.datashape.unmarshal(&s.symtab.Symtab, delims[0].mem())
}

func (s *systemDatashapeMergeTable) Close() error {
	s.parent.mutex.Lock()
	defer s.parent.mutex.Unlock()

	if s.parent.datashape == nil {
		s.parent.datashape = newQueryDatashapeMerge()
	}

	s.parent.datashape.merge(s.datashape)
	return nil
}

// --------------------------------------------------

func newQueryDatashapeMerge() *queryDatashapeFinal {
	return &queryDatashapeFinal{
		fields: make(map[string]*ionStatistics),
	}
}

type queryDatashapeFinal struct {
	total  int64
	errmsg string
	fields map[string]*ionStatistics
}

func (q *queryDatashapeFinal) unmarshal(st *ion.Symtab, msg []byte) error {
	_, err := ion.UnpackStructBody(st, msg, func(name string, val []byte) error {
		switch name {
		case totalField:
			total, _, err := ion.ReadInt(val)
			if err != nil {
				return err
			}

			q.total = total

		case errorField:
			errmsg, _, err := ion.ReadString(val)
			if err != nil {
				return err
			}

			q.errmsg = errmsg

		case fieldsField:
			_, err := ion.UnpackStruct(st, val, func(name string, msg []byte) error {
				stats := &ionStatistics{}
				q.fields[name] = stats
				return stats.unmarshal(st, msg)
			})

			return err

		default:
			return fmt.Errorf("unknown field %q", name)
		}

		return nil
	})

	return err
}

func (q *queryDatashapeFinal) merge(o *queryDatashapeFinal) {
	q.total += o.total
	if len(q.errmsg) == 0 {
		q.errmsg = o.errmsg
	}

	if len(q.fields) >= systemDatashapeMaxRows {
		return
	}

	for path, other := range o.fields {
		stats, ok := q.fields[path]
		if ok {
			stats.merge(other)
		} else {
			if len(q.fields) >= systemDatashapeMaxRows {
				q.errmsg = fmt.Sprintf("the total number of fields execeeded limit %d", systemDatashapeMaxRows)
				return
			}
			q.fields[path] = other
		}
	}
}

func (q *queryDatashapeFinal) writeIon(buf *ion.Buffer, st *ion.Symtab) {
	buf.BeginStruct(-1)
	{
		if len(q.errmsg) > 0 {
			buf.BeginField(st.Intern(errorField))
			buf.WriteString(q.errmsg)
		}

		buf.BeginField(st.Intern(totalField))
		buf.WriteInt(q.total)
		buf.BeginField(st.Intern(fieldsField))
		buf.BeginStruct(-1)
		paths := make([]string, 0, len(q.fields))
		for path := range q.fields {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			stats := q.fields[path]
			buf.BeginField(st.Intern(path))
			stats.writeIon(buf, st)
		}
		buf.EndStruct()

	}
	buf.EndStruct()
}

func field2iontype(name string) ion.Type {
	switch name {
	case nullField:
		return ion.NullType
	case boolField:
		return ion.BoolType
	case intField:
		return ion.IntType
	case floatField:
		return ion.FloatType
	case decimalField:
		return ion.DecimalType
	case timestampField:
		return ion.TimestampType
	case stringField:
		return ion.StringType
	case listField:
		return ion.ListType
	case structField:
		return ion.StructType
	case sexpField:
		return ion.SexpType
	case clobField:
		return ion.ClobType
	case blobField:
		return ion.BlobType
	case annotationField:
		return ion.AnnotationType

	default:
		return ion.ReservedType
	}
}

func (s *ionStatistics) unmarshal(st *ion.Symtab, msg []byte) error {
	_, err := ion.UnpackStruct(st, msg, func(name string, val []byte) error {
		var err error
		readInt := func() int64 {
			var num int64
			num, _, err = ion.ReadInt(val)
			return num
		}

		readFloat := func() float64 {
			var num float64
			num, _, err = ion.ReadFloat64(val)
			return num
		}

		if t := field2iontype(name); t != ion.ReservedType {
			s.count[int(t)] = readInt()
			return nil
		}

		switch name {
		case stringMinLengthField:
			s.rangeStringLen.min = readInt()
		case stringMaxLengthField:
			s.rangeStringLen.max = readInt()
		case intMinValueField:
			s.rangeInt64.min = readInt()
		case intMaxValueField:
			s.rangeInt64.max = readInt()
		case floatMinValueField:
			s.rangeFloat64.min = readFloat()
		case floatMaxValueField:
			s.rangeFloat64.max = readFloat()

		default:
			err = fmt.Errorf("unknown field %q", name)
		}

		return err
	})

	return err
}
