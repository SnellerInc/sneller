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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

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
	AggregateOpTDigest
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
	case AggregateOpTDigest:
		return "AggregateOpTDigest"
	default:
		return fmt.Sprintf("<AggregateOpFn=%d>", int(o))
	}
}

// aggregateOpMergeBufferRowsCount is the maximum number of rows that
// are passed to an aggregation, when the aggregation has functions
// that use non-trivial internal state (see expr.AggregateRole)
const aggregateOpMergeBufferRowsCount = 16

// aggregateOpMergeBufferSize is an extra buffer added for aggregates
// that does aggregation based on the internal state, not actual values.
const aggregateOpMergeBufferSize = aggregateOpMergeBufferRowsCount * aggregateOpMergeBufferItemSize

// aggregateOpMergeBufferItemSize is the size of a single item in the
// extra buffer. Refer to the aggregateLocal.writeRows and
// aggtable.writeRows implementations for the item's interpretation.
const aggregateOpMergeBufferItemSize = 3 * 4

// AggregateOp describes aggregate operation
type AggregateOp struct {
	fn AggregateOpFn

	role expr.AggregateRole

	// precision for AggregateOpApproxCountDistinct, AggregateOpApproxCountDistinctPartial
	// and AggregateOpApproxCountDistinctMerge
	precision uint8

	// misc used by AggregateOpTDigest to contain the percentile values p
	misc float32
}

// The operation needs to pass its whole internal state to the master
// machine (the buffer is used to perform actual aggregation).
func (a AggregateOp) mergestate() bool {
	return a.role == expr.AggregateRoleMerge
}

func (a AggregateOp) savestate() bool {
	return a.role == expr.AggregateRolePartial
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

	AggregateOpTDigest:             {isAtomic: false, initFunc: tDigestInit},
	AggregateOpApproxCountDistinct: {isAtomic: false, initFunc: aggApproxCountDistinctInit},
}

func (a *AggregateOp) dataSize() int {
	switch a.fn {
	case AggregateOpNone:
		return 8

	case AggregateOpSumF, AggregateOpAvgF:
		return aggregateOpSumFDataSize
	case AggregateOpTDigest:
		return tDigestDataSize
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

func initAggregateValues(data []byte, aggregateOps []AggregateOp) {
	for i := range aggregateOps {
		// First value is initialized to `initUInt64`.
		op := &aggregateOps[i]
		info := &aggregateOpInfoTable[op.fn]
		dataSize := op.dataSize()
		if op.mergestate() {
			data = data[aggregateOpMergeBufferSize:]
		}

		if info.initFunc != nil {
			info.initFunc(data[:dataSize])
		} else {
			binary.LittleEndian.PutUint64(data, info.initUInt64)
		}

		// All succeeding values were already zero initialized.
		data = data[dataSize:]
	}
}

func mergeAggregateBuffers(dst, src []byte, op AggregateOp) bool {
	switch op.fn {
	case AggregateOpApproxCountDistinct:
		n := op.dataSize()
		aggApproxCountDistinctUpdateBuckets(n, dst, src)
		return true

	case AggregateOpSumF:
		neumaierSummationMerge(dst, src)
		return true

	case AggregateOpTDigest:
		tDigestMerge(dst, src)
		return true
	}

	return false
}

func mergeAggregatedValues(dst, src []byte, aggregateOps []AggregateOp) {
	for i, op := range aggregateOps {
		if op.mergestate() {
			dst = dst[aggregateOpMergeBufferSize:]
			src = src[aggregateOpMergeBufferSize:]
		}
		switch op.fn {
		case AggregateOpSumF, AggregateOpAvgF:
			neumaierSummationMerge(dst, src)
			dst = dst[aggregateOpSumFDataSize:]
			src = src[aggregateOpSumFDataSize:]

		case AggregateOpTDigest:
			tDigestMerge(dst, src)
			dst = dst[tDigestDataSize:]
			src = src[tDigestDataSize:]

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
	if op.savestate() {
		d := op.dataSize()
		b.WriteBlob(data[:d])
		return d
	}

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

	case AggregateOpApproxCountDistinct:
		n := op.dataSize()
		count := aggApproxCountDistinctHLL(data[:n])
		b.WriteUint(count)
		return n

	case AggregateOpTDigest:
		percentiles, err := calcPercentiles(data[:tDigestDataSize], []float32{op.misc})
		if err != nil {
			panic(err)
		}
		b.WriteCanonicalFloat(float64(percentiles[0]))
		return tDigestDataSize

	default:
		panic(fmt.Sprintf("Invalid aggregate op: %v", op.fn))
	}
}

// Aggregate is a QuerySink implementation
// that computes simple aggregations that do not use groups.
type Aggregate struct {
	prog      *prog
	bind      Aggregation
	rest      QuerySink
	rowcount  int64
	skipEmpty bool

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
	mergestate  bool
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
	return splitter(&aggregateLocal{
		parent:      q,
		rowCount:    0,
		partialData: slices.Clone(q.initialData),
		mergestate:  mergestate(q.aggregateOps),
	}), nil
}

// Close flushes the result of the
// aggregation into the next QuerySink
func (q *Aggregate) Close() error {
	defer q.prog.reset()
	if q.skipEmpty && q.rowcount == 0 {
		return flushEmpty(q.rest)
	}

	var b ion.Buffer
	var st ion.Symtab

	for i := range q.bind {
		st.Intern(q.bind[i].Result)
	}

	data := q.AggregatedData

	st.Marshal(&b, true)
	b.BeginStruct(-1)
	for i, op := range q.aggregateOps {
		sym := st.Intern(q.bind[i].Result)
		b.BeginField(sym)
		if op.mergestate() {
			data = data[aggregateOpMergeBufferSize:]
		}
		if finalize := aggregateOpInfoTable[op.fn].finalizeFunc; finalize != nil && !op.savestate() {
			finalize(data)
		}
		consumed := writeAggregatedValue(&b, data, op)
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

	if p.mergestate {
		n := len(delims)
		var chunk []vmref
		for n > 0 {
			if n > aggregateOpMergeBufferRowsCount {
				chunk = delims[:aggregateOpMergeBufferRowsCount]
				delims = delims[aggregateOpMergeBufferRowsCount:]
			} else {
				chunk = delims
				delims = delims[:0]
			}
			n = len(delims)

			var rowsCount int
			if globalOptimizationLevel >= OptimizationLevelAVX512V1 {
				rowsCount = evalaggregatebc(&p.bc, chunk, p.partialData)
			} else {
				rowsCount = evalaggregatego(&p.bc, chunk, p.partialData)
			}
			if p.bc.err != 0 {
				return bytecodeerror("aggregate", &p.bc)
			}
			p.rowCount += uint64(rowsCount)

			dst := p.partialData
			for i := range p.parent.aggregateOps {
				op := p.parent.aggregateOps[i]
				n := op.dataSize()
				if op.mergestate() {
					positions := dst[:aggregateOpMergeBufferSize]
					dst = dst[aggregateOpMergeBufferSize:]
					for i := range chunk {
						offset := binary.LittleEndian.Uint32(positions[8*i:])
						size := binary.LittleEndian.Uint32(positions[8*i+64:])
						v := vmref{offset, size}
						if !mergeAggregateBuffers(dst, v.mem(), op) {
							panic(fmt.Sprintf("aggregate %s expected to merge its buffer", op.fn))
						}
					}
				}
				dst = dst[n:]
			}
		}
	} else {
		rowsCount := evalaggregatebc(&p.bc, delims, p.partialData)
		if p.bc.err != 0 {
			return bytecodeerror("aggregate", &p.bc)
		}
		p.rowCount += uint64(rowsCount)
	}

	return nil
}

func (p *aggregateLocal) EndSegment() {
	p.bc.dropScratch() // restored in recompile()
}

func (p *aggregateLocal) next() rowConsumer {
	return nil
}

func (p *aggregateLocal) Close() error {
	atomic.AddInt64(&p.parent.rowcount, int64(p.rowCount))
	p.rowCount = 0
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

	q.AggregatedData = slices.Clone(q.initialData)
	return q, nil
}

// SetSkipEmpty configures whether or not the Aggregate
// flushes any data to its output [QuerySink] when [Close]
// is called if zero rows have been written.
// The default behavior is to flush the "zero value" of the rows (typically [NULL]).
func (q *Aggregate) SetSkipEmpty(skip bool) {
	q.skipEmpty = skip
}

func (q *Aggregate) compileAggregate(aggregates Aggregation) error {
	q.prog = new(prog)
	p := q.prog
	p.begin()

	mem := make([]*value, len(aggregates))
	ops := make([]AggregateOp, len(aggregates))
	offset := aggregateslot(0)

	for i := range aggregates {
		agg := aggregates[i].Expr
		var filter *value
		if filterExpr := agg.Filter; filterExpr != nil {
			var err error
			// Note: duplicated filter expression will be removed during CSE
			filter, err = p.compileAsBool(filterExpr)
			if err != nil {
				return err
			}
		}

		switch op := agg.Op; op {
		case expr.OpCount:
			// COUNT(...) is the only aggregate op that doesn't accept numbers;
			// additionally, it accepts '*', which has a special meaning in this context.
			if _, ok := agg.Inner.(expr.Star); ok {
				mem[i] = p.aggregateCount(p.validLanes(), filter, offset)
			} else {
				v, err := compile(p, agg.Inner)
				if err != nil {
					return err
				}
				mem[i] = p.aggregateCount(v, filter, offset)
			}
			ops[i].fn = AggregateOpCount

		case expr.OpApproxCountDistinct:
			v, err := compile(p, agg.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg.Inner, err)
			}

			ops[i].fn = AggregateOpApproxCountDistinct
			ops[i].precision = agg.Precision
			ops[i].role = agg.Role
			switch agg.Role {
			case expr.AggregateRoleFinal, expr.AggregateRolePartial:
				mem[i] = p.aggregateApproxCountDistinct(v, filter, offset, agg.Precision)

			case expr.AggregateRoleMerge:
				mem[i] = p.aggregateMergeState(v, offset)
			}

		case expr.OpBoolAnd, expr.OpBoolOr:
			argv, err := compile(p, agg.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg.Inner, err)
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
			argv, err := p.compileAsNumber(agg.Inner)
			if err != nil {
				return fmt.Errorf("don't know how to aggregate %q: %w", agg.Inner, err)
			}
			var fp bool
			switch op {
			case expr.OpSum:
				mem[i], fp = p.aggregateSum(argv, filter, offset)
				if fp {
					ops[i].fn = AggregateOpSumF
					ops[i].role = agg.Role
					if agg.Role == expr.AggregateRoleMerge {
						mem[i] = p.aggregateMergeState(argv, offset)
					}
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
			case expr.OpApproxMedian:
				ops[i].fn = AggregateOpTDigest
				ops[i].misc = .5
				ops[i].role = agg.Role
				if agg.Role == expr.AggregateRoleMerge {
					mem[i] = p.aggregateMergeState(argv, offset)
				} else {
					mem[i] = p.aggregateTDigest(argv, filter, offset)
				}
			case expr.OpApproxPercentile:
				ops[i].fn = AggregateOpTDigest
				ops[i].misc = agg.Misc
				ops[i].role = agg.Role
				if agg.Role == expr.AggregateRoleMerge {
					mem[i] = p.aggregateMergeState(argv, offset)
				} else {
					mem[i] = p.aggregateTDigest(argv, filter, offset)
				}
			default:
				return fmt.Errorf("unsupported aggregate operation: %s", agg.Op)
			}
		}

		if err := mem[i].geterror(); err != nil {
			return err
		}

		// We compile all of the aggregate ops as order-independent so that
		// they can potentially be computed in the order in which the fields
		// are present in the input row rather than the order in which the
		// query presents them.
		offset += aggregateslot(ops[i].dataSize())
		if ops[i].mergestate() {
			offset += aggregateOpMergeBufferSize
		}
	}

	aggregateDataSize := offset
	initialData := make([]byte, aggregateDataSize)
	initAggregateValues(initialData, ops)

	q.aggregateOps = ops
	q.initialData = initialData

	p.returnValue(p.mergeMem(mem...))
	return nil
}

// mergestate returns true if any aggregate needs state merge
func mergestate(ops []AggregateOp) bool {
	for i := range ops {
		if ops[i].mergestate() {
			return true
		}
	}

	return false
}
