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
)

// AggregateOp describes aggregate operation
type AggregateOp struct {
	fn        AggregateOpFn
	precision uint8 // precision for AggregateOpApproxCountDistinct
}

func (a *AggregateOp) dataSize() int {
	switch a.fn {
	case AggregateOpNone:
		return 8

	case AggregateOpSumF:
		return 16
	case AggregateOpAvgF:
		return 16
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

	case AggregateOpApproxCountDistinct:
		return 1 << a.precision
	}

	return 0
}

type aggregateOpInfo struct {
	isFloat      bool
	firstValue   uint64
	fnFirstValue func([]byte)
}

var aggregateOpInfoTable = [...]aggregateOpInfo{
	AggregateOpNone: {isFloat: false, firstValue: 0},

	AggregateOpSumF: {isFloat: true, firstValue: 0},
	AggregateOpAvgF: {isFloat: true, firstValue: 0},
	AggregateOpMinF: {isFloat: true, firstValue: math.Float64bits(math.Inf(1))},
	AggregateOpMaxF: {isFloat: true, firstValue: math.Float64bits(math.Inf(-1))},

	AggregateOpSumI: {isFloat: false, firstValue: 0},
	AggregateOpSumC: {isFloat: false, firstValue: 0},
	AggregateOpAvgI: {isFloat: false, firstValue: 0},
	AggregateOpMinI: {isFloat: false, firstValue: 0x7FFFFFFFFFFFFFFF},
	AggregateOpMaxI: {isFloat: false, firstValue: 0x8000000000000000},
	AggregateOpAndI: {isFloat: false, firstValue: 0xFFFFFFFFFFFFFFFF},
	AggregateOpOrI:  {isFloat: false, firstValue: 0x0000000000000000},
	AggregateOpXorI: {isFloat: false, firstValue: 0x0000000000000000},

	AggregateOpAndK: {isFloat: false, firstValue: 0x0000000000000001},
	AggregateOpOrK:  {isFloat: false, firstValue: 0x0000000000000000},

	AggregateOpMinTS: {isFloat: false, firstValue: 0x7FFFFFFFFFFFFFFF},
	AggregateOpMaxTS: {isFloat: false, firstValue: 0x8000000000000000},

	AggregateOpCount: {isFloat: false, firstValue: 0},

	AggregateOpApproxCountDistinct: {fnFirstValue: aggApproxCountDistinctInit},
}

func initAggregateValues(data []byte, aggregateOps []AggregateOp) {
	offset := int(0)
	for i := range aggregateOps {
		// First value is initialized to `firstValue`.
		op := &aggregateOps[i]
		info := &aggregateOpInfoTable[op.fn]
		dataSize := op.dataSize()
		if info.fnFirstValue != nil {
			info.fnFirstValue(data[offset : offset+dataSize])
		} else {
			binary.LittleEndian.PutUint64(data[offset:], info.firstValue)
		}

		// All succeeding values were already zero initialized.
		offset += dataSize
	}
}

func mergeAggregatedValues(dst, src []byte, aggregateOps []AggregateOp) {
	for i := range aggregateOps {
		switch aggregateOps[i].fn {
		case AggregateOpSumF:
			bufferAddFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateOpAvgF:
			bufferAddFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

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

		case AggregateOpApproxCountDistinct:
			n := aggregateOps[i].dataSize()
			aggApproxCountDistinctUpdateBuckets(n, dst, src)
			dst = dst[n:]
			src = src[n:]
		}
	}
}

func mergeAggregatedValuesAtomically(dst, src []byte, aggregateOps []AggregateOp) {
	for i := range aggregateOps {
		switch aggregateOps[i].fn {
		case AggregateOpSumF, AggregateOpAvgF:
			atomicext.AddFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

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

		case AggregateOpApproxCountDistinct:
			// Note: realies on the implicit lock, not a real atomic op
			n := aggregateOps[i].dataSize()
			aggApproxCountDistinctUpdateBuckets(n, dst, src)
			dst = dst[n:]
			src = src[n:]
		}
	}
}

func writeAggregatedValue(b *ion.Buffer, data []byte, op AggregateOp) int {
	switch op.fn {
	case AggregateOpSumF, AggregateOpMinF, AggregateOpMaxF:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteCanonicalFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)))
		}
		return 16
	case AggregateOpAvgF:
		count := binary.LittleEndian.Uint64(data[8:])
		if count == 0 {
			b.WriteNull()
		} else {
			b.WriteCanonicalFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)) / float64(count))
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

	case AggregateOpApproxCountDistinct:
		n := op.dataSize()
		count := aggApproxCountDistinctHLL(data[:n])
		b.WriteUint(count)
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

	// Lock used only when there are APPROX_COUNT_DISTINCT aggregate(s)
	// This kind of aggregate uses more complex state update.
	lock sync.Mutex
}

// requiresLock returns if the Aggregate contain any APPROX_COUNT_DISTINCT function.
//
// Unlike other aggregate functions that can update the global state using
// atomic operations, APPROX_COUNT_DISTINCT cannot do it and this is
// why we need a regular lock in such cases.
func (q *Aggregate) requiresLock() bool {
	for i := range q.aggregateOps {
		if q.aggregateOps[i].fn == AggregateOpApproxCountDistinct {
			return true
		}
	}

	return false
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
	var b ion.Buffer
	var st ion.Symtab

	for i := range q.bind {
		st.Intern(q.bind[i].Result)
	}

	data := q.AggregatedData
	offset := int(0)

	st.Marshal(&b, true)
	b.BeginStruct(-1)
	for i := range q.aggregateOps {
		sym := st.Intern(q.bind[i].Result)
		b.BeginField(sym)
		offset += writeAggregatedValue(&b, data[offset:], q.aggregateOps[i])
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
	return recompile(st, p.parent.prog, &p.prog, &p.bc, aux)
}

func (p *aggregateLocal) writeRows(delims []vmref, rp *rowParams) error {
	if p.bc.compiled == nil {
		panic("bytecode WriteRows() before Symbolize()")
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
	lock := p.parent.requiresLock()
	if lock {
		p.parent.lock.Lock()
	}
	mergeAggregatedValuesAtomically(p.parent.AggregatedData, p.partialData, p.parent.aggregateOps)
	if lock {
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
	p.Begin()

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

		op := agg[i].Expr.Op

		// COUNT(...) is the only aggregate op that doesn't accept numbers;
		// additionally, it accepts '*', which has a special meaning in this context.
		if op == expr.OpCount {
			if _, ok := agg[i].Expr.Inner.(expr.Star); ok {
				mem[i] = p.AggregateCount(p.ValidLanes(), filter, offset)
			} else {
				v, err := compile(p, agg[i].Expr.Inner)
				if err != nil {
					return err
				}
				mem[i] = p.AggregateCount(v, filter, offset)
			}
			ops[i].fn = AggregateOpCount
		} else if op == expr.OpApproxCountDistinct {
			v, err := compile(p, agg[i].Expr.Inner)
			if err != nil {
				return err
			}
			mem[i] = p.AggregateApproxCountDistinct(v, filter, offset, agg[i].Expr.Precision)
			ops[i].fn = AggregateOpApproxCountDistinct
			ops[i].precision = agg[i].Expr.Precision
		} else if op.IsBoolOp() {
			argv, err := p.compileAsBool(agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			switch op {
			case expr.OpBoolAnd:
				mem[i] = p.AggregateBoolAnd(argv, filter, offset)
				ops[i].fn = AggregateOpAndK
			case expr.OpBoolOr:
				mem[i] = p.AggregateBoolOr(argv, filter, offset)
				ops[i].fn = AggregateOpOrK
			default:
				return fmt.Errorf("unsupported aggregate operation: %s", &agg[i])
			}
		} else {
			argv, err := p.compileAsNumber(agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			var fp bool
			switch op {
			case expr.OpSum:
				mem[i], fp = p.AggregateSum(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpSumF
				} else {
					ops[i].fn = AggregateOpSumI
				}
			case expr.OpSumInt:
				mem[i] = p.AggregateSumInt(argv, filter, offset)
				ops[i].fn = AggregateOpSumI
			case expr.OpSumCount:
				mem[i] = p.AggregateSumInt(argv, filter, offset)
				ops[i].fn = AggregateOpSumC
			case expr.OpAvg:
				mem[i], fp = p.AggregateAvg(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpAvgF
				} else {
					ops[i].fn = AggregateOpAvgI
				}
			case expr.OpMin:
				mem[i], fp = p.AggregateMin(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpMinF
				} else {
					ops[i].fn = AggregateOpMinI
				}
			case expr.OpMax:
				mem[i], fp = p.AggregateMax(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpMaxF
				} else {
					ops[i].fn = AggregateOpMaxI
				}
			case expr.OpBitAnd:
				mem[i] = p.AggregateAnd(argv, filter, offset)
				ops[i].fn = AggregateOpAndI
			case expr.OpBitOr:
				mem[i] = p.AggregateOr(argv, filter, offset)
				ops[i].fn = AggregateOpOrI
			case expr.OpBitXor:
				mem[i] = p.AggregateXor(argv, filter, offset)
				ops[i].fn = AggregateOpXorI
			case expr.OpEarliest:
				mem[i] = p.AggregateEarliest(argv, filter, offset)
				ops[i].fn = AggregateOpMinTS
			case expr.OpLatest:
				mem[i] = p.AggregateLatest(argv, filter, offset)
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

	p.Return(p.MergeMem(mem...))
	return nil
}
