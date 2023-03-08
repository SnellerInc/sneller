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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/internal/atomicext"
	"github.com/SnellerInc/sneller/ion"
)

//go:noescape
func evalaggregatebc(w *bytecode, delims []vmref, aggregateDataBuffer []byte) int

// AggregateOpFn specifies the aggregate operation and its type.
type AggregateOpFn uint8

const (
	AggregateOpNone AggregateOpFn = iota
	AggregateOpSumF
	AggregateOpAvgF
	AggregateOpMinF
	AggregateOpMaxF
	AggregateOpSumI
	AggregateOpSumC
	AggregateOpAvgI
	AggregateOpMinI
	AggregateOpMaxI
	AggregateOpAndI
	AggregateOpOrI
	AggregateOpXorI
	AggregateOpAndK
	AggregateOpOrK
	AggregateOpMinTS
	AggregateOpMaxTS
	AggregateOpCount
	AggregateOpApproxCountDistinct
	AggregateOpApproxCountDistinctPartial
	AggregateOpApproxCountDistinctMerge
)

func (o AggregateOpFn) String() string {
	switch o {
	case AggregateOpNone:
		return "AggregateOpNone"
	case AggregateOpSumF:
		return "AggregateOpSumF"
	case AggregateOpAvgF:
		return "AggregateOpAvgF"
	case AggregateOpMinF:
		return "AggregateOpMinF"
	case AggregateOpMaxF:
		return "AggregateOpMaxF"
	case AggregateOpSumI:
		return "AggregateOpSumI"
	case AggregateOpSumC:
		return "AggregateOpSumC"
	case AggregateOpAvgI:
		return "AggregateOpAvgI"
	case AggregateOpMinI:
		return "AggregateOpMinI"
	case AggregateOpMaxI:
		return "AggregateOpMaxI"
	case AggregateOpAndI:
		return "AggregateOpAndI"
	case AggregateOpOrI:
		return "AggregateOpOrI"
	case AggregateOpXorI:
		return "AggregateOpXorI"
	case AggregateOpAndK:
		return "AggregateOpAndK"
	case AggregateOpOrK:
		return "AggregateOpOrK"
	case AggregateOpMinTS:
		return "AggregateOpMinTS"
	case AggregateOpMaxTS:
		return "AggregateOpMaxTS"
	case AggregateOpCount:
		return "AggregateOpCount"
	case AggregateOpApproxCountDistinct:
		return "AggregateOpApproxCountDistinct"
	case AggregateOpApproxCountDistinctPartial:
		return "AggregateOpApproxCountDistinctPartial"
	case AggregateOpApproxCountDistinctMerge:
		return "AggregateOpApproxCountDistinctMerge"
	default:
		return fmt.Sprintf("<AggregateOpFn=%d>", int(o))
	}
}

// AggregateOp describes aggregate operation
type AggregateOp struct {
	fn AggregateOpFn

	// precision for AggregateOpApproxCountDistinct, AggregateOpApproxCountDistinctPartial
	// and AggregateOpApproxCountDistinctMerge
	precision uint8
}

type aggregateOpInfo struct {
	isAtomic   bool         // merging of two buckets can happen atomically
	isFloat    bool         // floating point aggregation (sum/avg/min/max)
	initUInt64 uint64       // initializes the first 8 bytes to this value (the rest is zero initialized)
	initFunc   func([]byte) // custom initialization of the buffer, initUInt64 will not be used if not nil

	// The finalize function is called prior writing out data. Its purpose is to
	// combine multi-word intermediate result into simple pair of scalar value (int64/float64)
	// and count (uint64).
	finalizeFunc func([]byte)
}

var aggregateOpInfoTable = [...]aggregateOpInfo{
	AggregateOpNone: {isAtomic: false, isFloat: false, initUInt64: 0},

	AggregateOpSumF: {isAtomic: false, isFloat: true,
		initFunc: neumaierSummationInit, finalizeFunc: neumaierSummationFinalize},
	AggregateOpAvgF: {isAtomic: false, isFloat: true,
		initFunc: neumaierSummationInit, finalizeFunc: neumaierSummationFinalize},
	AggregateOpMinF:  {isAtomic: true, isFloat: true, initUInt64: math.Float64bits(math.Inf(1))},
	AggregateOpMaxF:  {isAtomic: true, isFloat: true, initUInt64: math.Float64bits(math.Inf(-1))},
	AggregateOpSumI:  {isAtomic: true, isFloat: false, initUInt64: 0},
	AggregateOpSumC:  {isAtomic: true, isFloat: false, initUInt64: 0},
	AggregateOpAvgI:  {isAtomic: true, isFloat: false, initUInt64: 0},
	AggregateOpMinI:  {isAtomic: true, isFloat: false, initUInt64: 0x7FFFFFFFFFFFFFFF},
	AggregateOpMaxI:  {isAtomic: true, isFloat: false, initUInt64: 0x8000000000000000},
	AggregateOpAndI:  {isAtomic: true, isFloat: false, initUInt64: 0xFFFFFFFFFFFFFFFF},
	AggregateOpOrI:   {isAtomic: true, isFloat: false, initUInt64: 0x0000000000000000},
	AggregateOpXorI:  {isAtomic: true, isFloat: false, initUInt64: 0x0000000000000000},
	AggregateOpAndK:  {isAtomic: true, isFloat: false, initUInt64: 0x0000000000000001},
	AggregateOpOrK:   {isAtomic: true, isFloat: false, initUInt64: 0x0000000000000000},
	AggregateOpMinTS: {isAtomic: true, isFloat: false, initUInt64: 0x7FFFFFFFFFFFFFFF},
	AggregateOpMaxTS: {isAtomic: true, isFloat: false, initUInt64: 0x8000000000000000},
	AggregateOpCount: {isAtomic: true, isFloat: false, initUInt64: 0},

	AggregateOpApproxCountDistinct:        {isAtomic: false, initFunc: aggApproxCountDistinctInit},
	AggregateOpApproxCountDistinctPartial: {isAtomic: false, initFunc: aggApproxCountDistinctInit},
	AggregateOpApproxCountDistinctMerge:   {isAtomic: false, initFunc: aggApproxCountDistinctInit},
}

func (a *AggregateOp) dataSize() int {
	switch a.fn {
	case AggregateOpNone:
		return 8

	case AggregateOpSumF, AggregateOpAvgF:
		return aggregateOpSumFDataSize
	case AggregateOpMinF:
		return 16
	case AggregateOpMaxF:
		return 16

	case AggregateOpSumI:
		return 16
	case AggregateOpSumC:
		return 16
	case AggregateOpAvgI:
		return 16
	case AggregateOpMinI:
		return 16
	case AggregateOpMaxI:
		return 16
	case AggregateOpAndI:
		return 16
	case AggregateOpOrI:
		return 16
	case AggregateOpXorI:
		return 16

	case AggregateOpAndK:
		return 16
	case AggregateOpOrK:
		return 16

	case AggregateOpMinTS:
		return 16
	case AggregateOpMaxTS:
		return 16

	case AggregateOpCount:
		return 8

	case AggregateOpApproxCountDistinct, AggregateOpApproxCountDistinctPartial, AggregateOpApproxCountDistinctMerge:
		return 1 << a.precision
	}

	return 0
}

func initAggregateValues(data []byte, aggregateOps []AggregateOp) {
	offset := int(0)
	for i := range aggregateOps {
		// First value is initialized to `initUInt64`.
		op := &aggregateOps[i]
		info := &aggregateOpInfoTable[op.fn]
		dataSize := op.dataSize()

		if info.initFunc != nil {
			info.initFunc(data[offset : offset+dataSize])
		} else {
			binary.LittleEndian.PutUint64(data[offset:], info.initUInt64)
		}

		// All succeeding values were already zero initialized.
		offset += dataSize
	}
}

func mergeAggregatedValues(dst, src []byte, aggregateOps []AggregateOp) {
	for i := range aggregateOps {
		switch aggregateOps[i].fn {
		case AggregateOpSumF, AggregateOpAvgF:
			neumaierSummationMerge(dst, src)
			dst = dst[aggregateOpSumFDataSize:]
			src = src[aggregateOpSumFDataSize:]

		case AggregateOpMinF:
			bufferMinFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMaxF:
			bufferMaxFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpSumI:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpSumC:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpAvgI:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMinI, AggregateOpMinTS:
			bufferMinInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMaxI, AggregateOpMaxTS:
			bufferMaxInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpAndI, AggregateOpAndK:
			bufferAndInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpOrI, AggregateOpOrK:
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpXorI:
			bufferXorInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpCount:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpApproxCountDistinct, AggregateOpApproxCountDistinctPartial, AggregateOpApproxCountDistinctMerge:
			n := aggregateOps[i].dataSize()
			aggApproxCountDistinctUpdateBuckets(n, dst, src)
			dst = dst[n:]
			src = src[n:]

		default:
			panic(fmt.Sprintf("unsupported operation %s", aggregateOps[i].fn))
		}
	}
}

func mergeAggregatedValuesAtomically(dst, src []byte, aggregateOps []AggregateOp) {
	for i := range aggregateOps {
		switch aggregateOps[i].fn {

		case AggregateOpMinF:
			atomicext.MinFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMaxF:
			atomicext.MaxFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpSumI, AggregateOpAvgI, AggregateOpSumC:
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMinI, AggregateOpMinTS:
			atomicext.MinInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpMaxI, AggregateOpMaxTS:
			atomicext.MaxInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpAndI, AggregateOpAndK:
			atomicext.AndInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpOrI, AggregateOpOrK:
			atomicext.OrInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpXorI:
			atomicext.XorInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateOpCount:
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		default:
			panic(fmt.Sprintf("unsupported aggregate operation %s", aggregateOps[i].fn))
		}
	}
}

// writeAggregatedValue writes the final result of the Aggregation to the ion.Buffer
func writeAggregatedValue(b *ion.Buffer, data []byte, op AggregateOp) int {
	switch op.fn {
	case AggregateOpSumF, AggregateOpAvgF:
		count := getuint64(data, 1)
		if count == 0 {
			b.WriteNull()
		} else {
			sum := getfloat64(data, 0)
			if op.fn == AggregateOpAvgF {
				b.WriteCanonicalFloat(sum / float64(count))
			} else {
				b.WriteCanonicalFloat(sum)
			}
		}

		return op.dataSize()
	case AggregateOpMinF, AggregateOpMaxF:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteCanonicalFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)))
		}
		return 16
	case AggregateOpSumI, AggregateOpMinI, AggregateOpMaxI, AggregateOpAndI, AggregateOpOrI, AggregateOpXorI:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteInt(int64(binary.LittleEndian.Uint64(data)))
		}
		return 16
	case AggregateOpAndK, AggregateOpOrK:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			val := false
			if binary.LittleEndian.Uint64(data) != 0 {
				val = true
			}
			b.WriteBool(val)
		}
		return 16
	case AggregateOpSumC:
		count := int64(binary.LittleEndian.Uint64(data))
		b.WriteInt(count)
		return 16
	case AggregateOpAvgI:
		count := binary.LittleEndian.Uint64(data[8:])
		if count == 0 {
			b.WriteNull()
		} else {
			b.WriteInt(int64(binary.LittleEndian.Uint64(data)) / int64(count))
		}
		return 16
	case AggregateOpMinTS, AggregateOpMaxTS:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteTime(date.UnixMicro(int64(binary.LittleEndian.Uint64(data))))
		}
		return 16
	case AggregateOpCount:
		count := binary.LittleEndian.Uint64(data)
		b.WriteUint(count)
		return 8

	case AggregateOpApproxCountDistinct, AggregateOpApproxCountDistinctMerge:
		n := op.dataSize()
		count := aggApproxCountDistinctHLL(data[:n])
		b.WriteUint(count)
		return n

	case AggregateOpApproxCountDistinctPartial:
		n := op.dataSize()
		b.WriteBlob(data[:n])
		return n

	default:
		panic(fmt.Sprintf("Invalid aggregate op: %v", op.fn))
	}
}

// Aggregate is a QuerySink implementation
// that computes simple aggregations that do not use groups.
type Aggregate struct {
	prog *prog
	bind Aggregation
	rest QuerySink

	// AggregateOp for each aggregated value.
	//
	// This member has multiple purposes:
	//   - The length of the array describes how many aggregated fields are projected.
	//     (it should never be zero)
	//   - It specifies how each aggregated data item should be initialized.
	//   - It specifies the kinds of each aggregated field, which also specifies its data
	//     type. This is required to merge partially aggregated data with another data,
	//     possibly final.
	//   - Aggregate kind can be used to calculate the final size of the aggregation buffer.
	aggregateOps []AggregateOp

	// Initial values that the Aggregate will be used in every AggregateLocal instance.
	// These must be set accordingly to the aggregation operator. For example min operator
	// should start with INF and max operator with -INF.
	initialData []byte

	// Aggregated values (results from executing queries, even in parallel)
	AggregatedData []byte

	// Lock used only when there are aggregate that cannot use
	// atomic updates
	lock sync.Mutex
}

// canMergeAtomically returns whether it's possible to use atomic
// operations to merge two buckets sharing the same hash value.
//
// It's not possible when an aggregate uses more than 8 bytes.
func (q *Aggregate) canMergeAtomically() bool {
	for i := range q.aggregateOps {
		if !aggregateOpInfoTable[q.aggregateOps[i].fn].isAtomic {
			return false
		}
	}

	return true
}

type aggregateLocal struct {
	parent      *Aggregate
	prog        prog
	bc          bytecode
	rowCount    uint64
	partialData []byte
}

// AggBinding is a binding
// of a single aggregate expression
// to a result
type AggBinding struct {
	Expr   *expr.Aggregate
	Result string
}

func (a *AggBinding) String() string {
	return expr.ToString(a.Expr) + " AS " + expr.QuoteID(a.Result)
}

func (a AggBinding) Equals(x AggBinding) bool {
	return a.Result == x.Result && a.Expr.Equals(x.Expr)
}

// Aggregation is a list of aggregate bindings
type Aggregation []AggBinding

func (a Aggregation) String() string {
	var out strings.Builder
	for i := range a {
		if i != 0 {
			out.WriteString(", ")
		}
		out.WriteString(a[i].String())
	}
	return out.String()
}

func (a Aggregation) Equals(x Aggregation) bool {
	return slices.EqualFunc(a, x, AggBinding.Equals)
}

func (q *Aggregate) Open() (io.WriteCloser, error) {
	aggregateDataSize := len(q.initialData)
	partialData := make([]byte, aggregateDataSize)
	copy(partialData, q.initialData)

	return splitter(&aggregateLocal{
		parent:      q,
		rowCount:    0,
		partialData: partialData,
	}), nil
}

// Close flushes the result of the
// aggregation into the next QuerySink
func (q *Aggregate) Close() error {
	defer q.prog.reset()
	var b ion.Buffer
	var st ion.Symtab

	for i := range q.bind {
		st.Intern(q.bind[i].Result)
	}

	data := q.AggregatedData

	st.Marshal(&b, true)
	b.BeginStruct(-1)
	for i := range q.aggregateOps {
		sym := st.Intern(q.bind[i].Result)
		b.BeginField(sym)
		fn := q.aggregateOps[i].fn
		if finalize := aggregateOpInfoTable[fn].finalizeFunc; finalize != nil {
			finalize(data)
		}
		consumed := writeAggregatedValue(&b, data, q.aggregateOps[i])
		data = data[consumed:]
	}
	b.EndStruct()

	// now that we have the whole buffer,
	// write it to the output
	// (just open one stream; the output is small)
	w, err := q.rest.Open()
	if err != nil {
		return err
	}
	_, err = w.Write(b.Bytes())
	err2 := w.Close()
	err3 := q.rest.Close()

	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	return err
}

func (p *aggregateLocal) symbolize(st *symtab, aux *auxbindings) error {
	return recompile(st, p.parent.prog, &p.prog, &p.bc, aux, "aggregateLocal")
}

func (p *aggregateLocal) writeRows(delims []vmref, rp *rowParams) error {
	if p.bc.compiled == nil {
		panic("WriteRows() called before Symbolize()")
	}

	p.bc.prepare(rp)

	rowsCount := evalaggregatebc(&p.bc, delims, p.partialData)
	if p.bc.err != 0 {
		return bytecodeerror("aggregate", &p.bc)
	}
	p.rowCount += uint64(rowsCount)
	return nil
}

func (p *aggregateLocal) EndSegment() {
	p.bc.dropScratch() // restored in recompile()
}

func (p *aggregateLocal) next() rowConsumer {
	return nil
}

func (p *aggregateLocal) Close() error {
	if p.parent.canMergeAtomically() {
		mergeAggregatedValuesAtomically(p.parent.AggregatedData, p.partialData, p.parent.aggregateOps)
	} else {
		p.parent.lock.Lock()
		mergeAggregatedValues(p.parent.AggregatedData, p.partialData, p.parent.aggregateOps)
		p.parent.lock.Unlock()
	}

	p.partialData = nil
	p.bc.reset()
	return nil
}

// NewAggregate constructs an aggregation QuerySink.
func NewAggregate(bind Aggregation, rest QuerySink) (*Aggregate, error) {
	q := &Aggregate{
		rest: rest,
		bind: bind,
	}

	err := q.compileAggregate(bind)
	if err != nil {
		return nil, err
	}
	aggregatedData := make([]byte, len(q.initialData))
	copy(aggregatedData, q.initialData)
	q.AggregatedData = aggregatedData

	return q, nil
}

func (q *Aggregate) compileAggregate(agg Aggregation) error {
	q.prog = new(prog)
	p := q.prog
	p.begin()

	mem := make([]*value, len(agg))
	ops := make([]AggregateOp, len(agg))
	offset := aggregateslot(0)

	for i := range agg {
		var filter *value
		if filterExpr := agg[i].Expr.Filter; filterExpr != nil {
			var err error
			// Note: duplicated filter expression will be removed during CSE
			filter, err = p.compileAsBool(filterExpr)
			if err != nil {
				return err
			}
		}

		switch op := agg[i].Expr.Op; op {
		case expr.OpCount:
			// COUNT(...) is the only aggregate op that doesn't accept numbers;
			// additionally, it accepts '*', which has a special meaning in this context.
			if _, ok := agg[i].Expr.Inner.(expr.Star); ok {
				mem[i] = p.aggregateCount(p.validLanes(), filter, offset)
			} else {
				v, err := compile(p, agg[i].Expr.Inner)
				if err != nil {
					return err
				}
				mem[i] = p.aggregateCount(v, filter, offset)
			}
			ops[i].fn = AggregateOpCount

		case expr.OpApproxCountDistinct,
			expr.OpApproxCountDistinctPartial,
			expr.OpApproxCountDistinctMerge:

			v, err := compile(p, agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}

			ops[i].precision = agg[i].Expr.Precision
			switch op {
			case expr.OpApproxCountDistinct:
				mem[i] = p.aggregateApproxCountDistinct(v, filter, offset, agg[i].Expr.Precision)
				ops[i].fn = AggregateOpApproxCountDistinct

			case expr.OpApproxCountDistinctPartial:
				mem[i] = p.aggregateApproxCountDistinctPartial(v, filter, offset, agg[i].Expr.Precision)
				ops[i].fn = AggregateOpApproxCountDistinctPartial

			case expr.OpApproxCountDistinctMerge:
				mem[i] = p.aggregateApproxCountDistinctMerge(v, offset, agg[i].Expr.Precision)
				ops[i].fn = AggregateOpApproxCountDistinctMerge
			}

		case expr.OpBoolAnd, expr.OpBoolOr:
			argv, err := p.compileAsBool(agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			switch op {
			case expr.OpBoolAnd:
				mem[i] = p.aggregateBoolAnd(argv, filter, offset)
				ops[i].fn = AggregateOpAndK
			case expr.OpBoolOr:
				mem[i] = p.aggregateBoolOr(argv, filter, offset)
				ops[i].fn = AggregateOpOrK
			}

		default:
			argv, err := p.compileAsNumber(agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			var fp bool
			switch op {
			case expr.OpSum:
				mem[i], fp = p.aggregateSum(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpSumF
				} else {
					ops[i].fn = AggregateOpSumI
				}
			case expr.OpSumInt:
				mem[i] = p.aggregateSumInt(argv, filter, offset)
				ops[i].fn = AggregateOpSumI
			case expr.OpSumCount:
				mem[i] = p.aggregateSumInt(argv, filter, offset)
				ops[i].fn = AggregateOpSumC
			case expr.OpAvg:
				mem[i], fp = p.aggregateAvg(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpAvgF
				} else {
					ops[i].fn = AggregateOpAvgI
				}
			case expr.OpMin:
				mem[i], fp = p.aggregateMin(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpMinF
				} else {
					ops[i].fn = AggregateOpMinI
				}
			case expr.OpMax:
				mem[i], fp = p.aggregateMax(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpMaxF
				} else {
					ops[i].fn = AggregateOpMaxI
				}
			case expr.OpBitAnd:
				mem[i] = p.aggregateAnd(argv, filter, offset)
				ops[i].fn = AggregateOpAndI
			case expr.OpBitOr:
				mem[i] = p.aggregateOr(argv, filter, offset)
				ops[i].fn = AggregateOpOrI
			case expr.OpBitXor:
				mem[i] = p.aggregateXor(argv, filter, offset)
				ops[i].fn = AggregateOpXorI
			case expr.OpEarliest:
				mem[i] = p.aggregateEarliest(argv, filter, offset)
				ops[i].fn = AggregateOpMinTS
			case expr.OpLatest:
				mem[i] = p.aggregateLatest(argv, filter, offset)
				ops[i].fn = AggregateOpMaxTS
			default:
				return fmt.Errorf("unsupported aggregate operation: %s", &agg[i])
			}
		}

		// We compile all of the aggregate ops as order-independent so that
		// they can potentially be computed in the order in which the fields
		// are present in the input row rather than the order in which the
		// query presents them.
		offset += aggregateslot(ops[i].dataSize())
	}

	aggregateDataSize := offset
	initialData := make([]byte, aggregateDataSize)
	initAggregateValues(initialData, ops)

	q.aggregateOps = ops
	q.initialData = initialData

	p.returnValue(p.mergeMem(mem...))
	return nil
}
