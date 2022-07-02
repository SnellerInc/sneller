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
	"sync/atomic"
	"unsafe"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/internal/atomicext"
	"github.com/SnellerInc/sneller/ion"
)

//go:noescape
func evalaggregatebc(w *bytecode, delims []vmref, aggregateDataBuffer []byte) int

// AggregateKind Specifies the aggregate operation and its type.
type AggregateKind uint8

const (
	AggregateKindNone AggregateKind = iota
	AggregateKindSumF
	AggregateKindAvgF
	AggregateKindMinF
	AggregateKindMaxF
	AggregateKindSumI
	AggregateKindSumC
	AggregateKindAvgI
	AggregateKindMinI
	AggregateKindMaxI
	AggregateKindAndI
	AggregateKindOrI
	AggregateKindXorI
	AggregateKindAndK
	AggregateKindOrK
	AggregateKindMinTS
	AggregateKindMaxTS
	AggregateKindCount
)

type aggregateKindInfo struct {
	isFloat    bool
	dataSize   uint8
	firstValue uint64
}

var aggregateKindInfoTable = [...]aggregateKindInfo{
	AggregateKindNone: {isFloat: false, dataSize: 8, firstValue: 0},

	AggregateKindSumF: {isFloat: true, dataSize: 16, firstValue: 0},
	AggregateKindAvgF: {isFloat: true, dataSize: 16, firstValue: 0},
	AggregateKindMinF: {isFloat: true, dataSize: 16, firstValue: math.Float64bits(math.Inf(1))},
	AggregateKindMaxF: {isFloat: true, dataSize: 16, firstValue: math.Float64bits(math.Inf(-1))},

	AggregateKindSumI: {isFloat: false, dataSize: 16, firstValue: 0},
	AggregateKindSumC: {isFloat: false, dataSize: 16, firstValue: 0},
	AggregateKindAvgI: {isFloat: false, dataSize: 16, firstValue: 0},
	AggregateKindMinI: {isFloat: false, dataSize: 16, firstValue: 0x7FFFFFFFFFFFFFFF},
	AggregateKindMaxI: {isFloat: false, dataSize: 16, firstValue: 0x8000000000000000},
	AggregateKindAndI: {isFloat: false, dataSize: 16, firstValue: 0xFFFFFFFFFFFFFFFF},
	AggregateKindOrI:  {isFloat: false, dataSize: 16, firstValue: 0x0000000000000000},
	AggregateKindXorI: {isFloat: false, dataSize: 16, firstValue: 0x0000000000000000},

	AggregateKindAndK: {isFloat: false, dataSize: 16, firstValue: 0x0000000000000001},
	AggregateKindOrK:  {isFloat: false, dataSize: 16, firstValue: 0x0000000000000000},

	AggregateKindMinTS: {isFloat: false, dataSize: 16, firstValue: 0x7FFFFFFFFFFFFFFF},
	AggregateKindMaxTS: {isFloat: false, dataSize: 16, firstValue: 0x8000000000000000},

	AggregateKindCount: {isFloat: false, dataSize: 8, firstValue: 0},
}

func initAggregateValues(data []byte, aggregateKinds []AggregateKind) {
	offset := int(0)
	for i := range aggregateKinds {
		// First value is initialized to `firstValue`.
		kind := aggregateKinds[i]
		binary.LittleEndian.PutUint64(data[offset:], aggregateKindInfoTable[kind].firstValue)

		// All succeeding values were already zero initialized.
		offset += int(aggregateKindInfoTable[kind].dataSize)
	}
}

func mergeAggregatedValues(dst, src []byte, aggregateKinds []AggregateKind) {
	for i := range aggregateKinds {
		switch aggregateKinds[i] {
		case AggregateKindSumF:
			bufferAddFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindAvgF:
			bufferAddFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMinF:
			bufferMinFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMaxF:
			bufferMaxFloat64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindSumI:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindSumC:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindAvgI:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMinI, AggregateKindMinTS:
			bufferMinInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMaxI, AggregateKindMaxTS:
			bufferMaxInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindAndI, AggregateKindAndK:
			bufferAndInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindOrI, AggregateKindOrK:
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindXorI:
			bufferXorInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
			bufferOrInt64(dst, src)
			dst = dst[8:]
			src = src[8:]

		case AggregateKindCount:
			bufferAddInt64(dst, src)
			dst = dst[8:]
			src = src[8:]
		}
	}
}

func mergeAggregatedValuesAtomically(dst, src []byte, aggregateKinds []AggregateKind) {
	for i := range aggregateKinds {
		switch aggregateKinds[i] {
		case AggregateKindSumF, AggregateKindAvgF:
			atomicext.AddFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMinF:
			atomicext.MinFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMaxF:
			atomicext.MaxFloat64((*float64)(unsafe.Pointer(&dst[0])), math.Float64frombits(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindSumI, AggregateKindAvgI, AggregateKindSumC:
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMinI, AggregateKindMinTS:
			atomicext.MinInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindMaxI, AggregateKindMaxTS:
			atomicext.MaxInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindAndI, AggregateKindAndK:
			atomicext.AndInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindOrI, AggregateKindOrK:
			atomicext.OrInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindXorI:
			atomicext.XorInt64((*int64)(unsafe.Pointer(&dst[0])), int64(binary.LittleEndian.Uint64(src)))
			dst = dst[8:]
			src = src[8:]
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]

		case AggregateKindCount:
			atomic.AddUint64((*uint64)(unsafe.Pointer(&dst[0])), binary.LittleEndian.Uint64(src))
			dst = dst[8:]
			src = src[8:]
		}
	}
}

func writeAggregatedValue(b *ion.Buffer, data []byte, kind AggregateKind) int {
	switch kind {
	case AggregateKindSumF, AggregateKindMinF, AggregateKindMaxF:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteCanonicalFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)))
		}
		return 16
	case AggregateKindAvgF:
		count := binary.LittleEndian.Uint64(data[8:])
		if count == 0 {
			b.WriteNull()
		} else {
			b.WriteCanonicalFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)) / float64(count))
		}
		return 16
	case AggregateKindSumI, AggregateKindMinI, AggregateKindMaxI, AggregateKindAndI, AggregateKindOrI, AggregateKindXorI:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteInt(int64(binary.LittleEndian.Uint64(data)))
		}
		return 16
	case AggregateKindAndK, AggregateKindOrK:
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
	case AggregateKindSumC:
		count := int64(binary.LittleEndian.Uint64(data))
		b.WriteInt(count)
		return 16
	case AggregateKindAvgI:
		count := binary.LittleEndian.Uint64(data[8:])
		if count == 0 {
			b.WriteNull()
		} else {
			b.WriteInt(int64(binary.LittleEndian.Uint64(data)) / int64(count))
		}
		return 16
	case AggregateKindMinTS, AggregateKindMaxTS:
		mark := binary.LittleEndian.Uint64(data[8:])
		if mark == 0 {
			b.WriteNull()
		} else {
			b.WriteTime(date.UnixMicro(int64(binary.LittleEndian.Uint64(data))))
		}
		return 16
	case AggregateKindCount:
		count := binary.LittleEndian.Uint64(data)
		b.WriteUint(count)
		return 8
	default:
		panic(fmt.Sprintf("Invalid aggregate kind: %v", kind))
	}
}

// Aggregate is a QuerySink implementation
// that computes simple aggregations that do not use groups.
type Aggregate struct {
	prog *prog
	bind Aggregation
	rest QuerySink

	// AggregateKind for each aggregated value.
	//
	// This member has multiple purposes:
	//   - The length of the array describes how many aggregated fields are projected.
	//     (it should never be zero)
	//   - It specifies how each aggregated data item should be initialized.
	//   - It specifies the kinds of each aggregated field, which also specifies its data
	//     type. This is required to merge partially aggregated data with another data,
	//     possibly final.
	//   - Aggregate kind can be used to calculate the final size of the aggregation buffer.
	aggregateKinds []AggregateKind

	// Initial values that the Aggregate will be used in every AggregateLocal instance.
	// These must be set accordingly to the aggregation operator. For example min operator
	// should start with INF and max operator with -INF.
	initialData []byte

	// Aggregated values (results from executing queries, even in parallel)
	AggregatedData []byte
}

type aggregateLocal struct {
	parent      *Aggregate
	prog        prog
	bc          bytecode
	dst         rowConsumer
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

func (q *Aggregate) Open() (io.WriteCloser, error) {
	r, err := q.rest.Open()
	if err != nil {
		return nil, err
	}

	aggregateDataSize := len(q.initialData)
	partialData := make([]byte, aggregateDataSize)
	copy(partialData, q.initialData)

	return splitter(&aggregateLocal{
		parent:      q,
		dst:         asRowConsumer(r),
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
	for i := range q.aggregateKinds {
		sym := st.Intern(q.bind[i].Result)
		b.BeginField(sym)
		offset += writeAggregatedValue(&b, data[offset:], q.aggregateKinds[i])
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

func (p *aggregateLocal) symbolize(st *symtab) error {
	err := recompile(st, p.parent.prog, &p.prog, &p.bc)
	if err != nil {
		return err
	}
	return p.dst.symbolize(st)
}

func (p *aggregateLocal) writeRows(delims []vmref) error {
	if p.bc.compiled == nil {
		panic("bytecode WriteRows() before Symbolize()")
	}
	rowsCount := evalaggregatebc(&p.bc, delims, p.partialData)
	p.rowCount += uint64(rowsCount)
	return nil
}

func (p *aggregateLocal) Close() error {
	mergeAggregatedValuesAtomically(p.parent.AggregatedData, p.partialData, p.parent.aggregateKinds)
	p.partialData = nil
	p.bc.reset()
	return p.dst.Close()
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
	kinds := make([]AggregateKind, len(agg))
	offset := 0

	for i := range agg {
		op := agg[i].Expr.Op

		// COUNT(...) is the only aggregate op that doesn't accept numbers;
		// additionally, it accepts '*', which has a special meaning in this context.
		if op == expr.OpCount {
			if _, ok := agg[i].Expr.Inner.(expr.Star); ok {
				mem[i] = p.AggregateCount(p.ValidLanes(), offset)
			} else {
				v, err := compile(p, agg[i].Expr.Inner)
				if err != nil {
					return err
				}
				mem[i] = p.AggregateCount(v, offset)
			}
			kinds[i] = AggregateKindCount
		} else if op.IsBoolOp() {
			argv, err := p.compileAsBool(agg[i].Expr.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			switch op {
			case expr.OpBoolAnd:
				mem[i] = p.AggregateBoolAnd(argv, offset)
				kinds[i] = AggregateKindAndK
			case expr.OpBoolOr:
				mem[i] = p.AggregateBoolOr(argv, offset)
				kinds[i] = AggregateKindOrK
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
				mem[i], fp = p.AggregateSum(argv, offset)
				if fp {
					kinds[i] = AggregateKindSumF
				} else {
					kinds[i] = AggregateKindSumI
				}
			case expr.OpSumInt:
				mem[i] = p.AggregateSumInt(argv, offset)
				kinds[i] = AggregateKindSumI
			case expr.OpSumCount:
				mem[i] = p.AggregateSumInt(argv, offset)
				kinds[i] = AggregateKindSumC
			case expr.OpAvg:
				mem[i], fp = p.AggregateAvg(argv, offset)
				if fp {
					kinds[i] = AggregateKindAvgF
				} else {
					kinds[i] = AggregateKindAvgI
				}
			case expr.OpMin:
				mem[i], fp = p.AggregateMin(argv, offset)
				if fp {
					kinds[i] = AggregateKindMinF
				} else {
					kinds[i] = AggregateKindMinI
				}
			case expr.OpMax:
				mem[i], fp = p.AggregateMax(argv, offset)
				if fp {
					kinds[i] = AggregateKindMaxF
				} else {
					kinds[i] = AggregateKindMaxI
				}
			case expr.OpBitAnd:
				mem[i] = p.AggregateAnd(argv, offset)
				kinds[i] = AggregateKindAndI
			case expr.OpBitOr:
				mem[i] = p.AggregateOr(argv, offset)
				kinds[i] = AggregateKindOrI
			case expr.OpBitXor:
				mem[i] = p.AggregateXor(argv, offset)
				kinds[i] = AggregateKindXorI
			case expr.OpEarliest:
				mem[i] = p.AggregateEarliest(argv, offset)
				kinds[i] = AggregateKindMinTS
			case expr.OpLatest:
				mem[i] = p.AggregateLatest(argv, offset)
				kinds[i] = AggregateKindMaxTS
			default:
				return fmt.Errorf("unsupported aggregate operation: %s", &agg[i])
			}
		}

		// We compile all of the aggregate ops as order-independent so that
		// they can potentially be computed in the order in which the fields
		// are present in the input row rather than the order in which the
		// query presents them.
		offset += int(aggregateKindInfoTable[kinds[i]].dataSize)
	}

	aggregateDataSize := offset
	initialData := make([]byte, aggregateDataSize)
	initAggregateValues(initialData, kinds)

	q.aggregateKinds = kinds
	q.initialData = initialData

	p.Return(p.MergeMem(mem...))
	return nil
}
