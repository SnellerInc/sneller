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
	"sync"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

const (
	aggregateTagSize int = 8
)

type HashAggregate struct {
	children  int64
	rowcount  int64 // updated on aggtable.Close
	prog      prog
	agg       Aggregation
	by        Selection
	dst       QuerySink
	skipEmpty bool

	aggregateOps []AggregateOp
	initialData  []byte

	lock  sync.Mutex
	final *aggtable
	limit int

	// ordering functions;
	// applied in order to determine
	// the total ordering
	order []aggOrderFn

	windows []window
}

type aggOrderFn func(*aggtable, int, int) int

type window struct {
	// order computes the partitions *plus*
	// the ORDER BY clause for the window
	order []aggOrderFn
	// order[:partitions] is the ordering
	// that produces the individual partitions;
	// this may be zero if no PARTITION BY was supplied
	partitions int
	fn         windowFunc
	final      []uint // actual final results
	result     string
}

// run computes the results of applying the window function
// and sets w.final
func (w *window) run(agt *aggtable) {
	ret := make([]uint, len(agt.pairs))

	// pairs[order[0..i]] will order the pairs by this window's partitions + order
	order := make([]int, len(agt.pairs))
	for i := range order {
		order[i] = i
	}

	fullorder := w.order
	partorder := w.order[:w.partitions]
	partcmp := func(i, j int) int {
		for k := range partorder {
			dir := partorder[k](agt, i, j)
			if dir != 0 {
				return dir
			}
		}
		return 0
	}
	cmp := func(i, j int) int {
		for k := range fullorder {
			dir := fullorder[k](agt, i, j)
			if dir != 0 {
				return dir
			}
		}
		return 0
	}
	slices.SortFunc(order, func(i, j int) bool {
		dir := cmp(i, j)
		return dir < 0
	})
	// walk pairs in order
	repeat := false
	for i := range order {
		repeat = i > 0 && cmp(order[i-1], order[i]) == 0
		if i == 0 || partcmp(order[i-1], order[i]) != 0 {
			w.fn.reset()
			repeat = false
		}
		val := w.fn.next(repeat)
		ret[order[i]] = val
	}
	w.final = ret
}

// Limit sets the maximum number of output rows.
// Limit <= 0 means there is no limit.
func (h *HashAggregate) Limit(n int) {
	h.limit = n
}

func (h *HashAggregate) groupFn(n int, ordering SortOrdering) aggOrderFn {
	return func(agt *aggtable, i, j int) int {
		leftmem := agt.repridx(&agt.pairs[i], n)
		rightmem := agt.repridx(&agt.pairs[j], n)
		return ordering.Compare(leftmem, rightmem)
	}
}

func (h *HashAggregate) aggFn(n int, ordering SortOrdering) aggOrderFn {
	return func(agt *aggtable, i, j int) int {
		op := h.aggregateOps[n]
		lmem := agt.valueof(&agt.pairs[i])
		rmem := agt.valueof(&agt.pairs[j])
		dir := aggcmp(op.fn, lmem, rmem)
		if ordering.Direction == SortDescending {
			return -dir
		}
		return dir
	}
}

func (h *HashAggregate) windowOrder(n int, ordering SortOrdering) aggOrderFn {
	return func(agt *aggtable, i, j int) int {
		return int(h.windows[n].final[i]) - int(h.windows[n].final[j])
	}
}

func (h *HashAggregate) OrderByGroup(n int, ordering SortOrdering) error {
	if n < 0 || n >= len(h.by) {
		return fmt.Errorf("group %d doesn't exist", n)
	}
	h.order = append(h.order, h.groupFn(n, ordering))
	return nil
}

func (h *HashAggregate) OrderByAggregate(n int, ordering SortOrdering) error {
	if n < 0 || n >= len(h.agg) {
		return fmt.Errorf("aggregate %d doesn't exist", n)
	}
	o := SortOrdering{
		Direction:  ordering.Direction,
		NullsOrder: SortNullsFirst,
	}
	h.order = append(h.order, h.aggFn(n, o))
	return nil
}

func (h *HashAggregate) OrderByWindow(n int, ordering SortOrdering) error {
	if n < 0 || n >= len(h.windows) {
		return fmt.Errorf("window %d doesn't exist", n)
	}
	h.order = append(h.order, h.windowOrder(n, ordering))
	return nil
}

// SetSkipEmpty configures whether or not the HashAggregate
// flushes any data to its output [QuerySink] when [Close]
// is called if zero rows have been written.
// The default behavior is to flush the "zero value" of the rows (typically [NULL]).
func (h *HashAggregate) SetSkipEmpty(skip bool) {
	h.skipEmpty = skip
}

func NewHashAggregate(agg, windows Aggregation, by Selection, dst QuerySink) (*HashAggregate, error) {
	if len(by) == 0 {
		return nil, fmt.Errorf("cannot aggregate an empty selection")
	}
	if len(agg) == 0 {
		return nil, fmt.Errorf("zero aggregations...?")
	}
	h := &HashAggregate{dst: dst, agg: agg, by: by}
	err := h.compileWindows(windows, by)
	if err != nil {
		return nil, err
	}

	prog := &h.prog
	prog.begin()

	colmem := make([]*value, len(by))

	var allColumnsMask *value
	var allColumnsHash *value

	for i, column := range by {
		field := column.Expr

		col, err := prog.serialized(field)
		if err != nil {
			return nil, err
		}
		// we always want to hash the *unsymbolized* value
		col = prog.unsymbolized(col)

		if allColumnsHash == nil {
			allColumnsHash = prog.hash(col)
		} else {
			allColumnsHash = prog.hashplus(allColumnsHash, col)
		}

		if allColumnsMask == nil {
			allColumnsMask = prog.mask(col)
		} else {
			allColumnsMask = prog.and(allColumnsMask, prog.mask(col))
		}

		mem, err := prog.store(prog.initMem(), col, stackSlotFromIndex(regV, i))
		if err != nil {
			return nil, err
		}

		colmem[i] = mem
	}

	mem := prog.mergeMem(colmem...)
	out := make([]*value, len(h.agg))
	ops := make([]AggregateOp, len(h.agg))
	bucket := prog.aggbucket(mem, allColumnsHash, allColumnsMask)
	offset := aggregateslot(0)

	for i := range h.agg {
		var filter *value
		if filterExpr := h.agg[i].Expr.Filter; filterExpr != nil {
			var err error
			// Note: duplicated filter expression will be removed during CSE
			filter, err = prog.compileAsBool(filterExpr)
			if err != nil {
				return nil, err
			}
		}

		mask := allColumnsMask
		if filter != nil {
			mask = prog.and(mask, filter)
		}

		a := h.agg[i].Expr
		switch op := a.Op; op {
		case expr.OpCount:
			// COUNT(...) is the only aggregate op that doesn't accept numbers;
			// additionally, it accepts '*', which has a special meaning in this context.
			if _, ok := h.agg[i].Expr.Inner.(expr.Star); ok {
				// `COUNT(*) GROUP BY X` is equivalent to `COUNT(X) GROUP BY X`
			} else {
				k, err := compile(prog, h.agg[i].Expr.Inner)
				if err != nil {
					return nil, err
				}
				mask = prog.and(prog.mask(k), mask)
			}

			out[i] = prog.aggregateSlotCount(mem, bucket, mask, offset)
			ops[i].fn = AggregateOpCount

		case expr.OpApproxCountDistinct:
			argv, err := compile(prog, a.Inner)
			if err != nil {
				return nil, fmt.Errorf("cannot compile %q: %w", a.Inner, err)
			}
			precision := a.Precision

			ops[i].precision = precision
			ops[i].fn = AggregateOpApproxCountDistinct
			ops[i].role = a.Role
			switch a.Role {
			case expr.AggregateRoleFinal, expr.AggregateRolePartial:
				out[i] = prog.aggregateSlotApproxCountDistinct(mem, bucket, argv, mask, offset, precision)
			case expr.AggregateRoleMerge:
				out[i] = prog.aggregateSlotMergeState(bucket, argv, mask, offset+aggregateslot(ops[i].dataSize()))
			}

		case expr.OpBoolAnd, expr.OpBoolOr:
			argv, err := prog.compileAsBool(h.agg[i].Expr.Inner)
			if err != nil {
				return nil, fmt.Errorf("don't know how to aggregate %q: %w", h.agg[i].Expr.Inner, err)
			}
			switch op {
			case expr.OpBoolAnd:
				out[i] = prog.aggregateSlotBoolAnd(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpAndK
			case expr.OpBoolOr:
				out[i] = prog.aggregateSlotBoolOr(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpOrK
			default:
				return nil, fmt.Errorf("unsupported aggregate operation: %s", &h.agg[i])
			}

		default:
			argv, err := prog.compileAsNumber(h.agg[i].Expr.Inner)
			if err != nil {
				return nil, fmt.Errorf("don't know how to aggregate %q: %w", h.agg[i].Expr.Inner, err)
			}
			var fp bool
			switch op {
			case expr.OpAvg:
				out[i], fp = prog.aggregateSlotAvg(mem, bucket, argv, mask, offset)
				if fp {
					ops[i].fn = AggregateOpAvgF
				} else {
					ops[i].fn = AggregateOpAvgI
				}
			case expr.OpSum:
				out[i], fp = prog.aggregateSlotSum(mem, bucket, argv, mask, offset)
				if fp {
					ops[i].fn = AggregateOpSumF
					ops[i].role = a.Role
					if a.Role == expr.AggregateRoleMerge {
						out[i] = prog.aggregateSlotMergeState(bucket, argv, mask, offset+aggregateslot(ops[i].dataSize()))
					}
				} else {
					ops[i].fn = AggregateOpSumI
				}
			case expr.OpSumInt:
				out[i] = prog.aggregateSlotSumInt(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpSumI
			case expr.OpSumCount:
				out[i] = prog.aggregateSlotSumInt(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpSumC
			case expr.OpMin:
				out[i], fp = prog.aggregateSlotMin(mem, bucket, argv, mask, offset)
				if fp {
					ops[i].fn = AggregateOpMinF
				} else {
					ops[i].fn = AggregateOpMinI
				}
			case expr.OpMax:
				out[i], fp = prog.aggregateSlotMax(mem, bucket, argv, mask, offset)
				if fp {
					ops[i].fn = AggregateOpMaxF
				} else {
					ops[i].fn = AggregateOpMaxI
				}
			case expr.OpBitAnd:
				out[i] = prog.aggregateSlotAnd(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpAndI
			case expr.OpBitOr:
				out[i] = prog.aggregateSlotOr(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpOrI
			case expr.OpBitXor:
				out[i] = prog.aggregateSlotXor(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpXorI
			case expr.OpEarliest:
				out[i] = prog.aggregateSlotEarliest(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpMinTS
			case expr.OpLatest:
				out[i] = prog.aggregateSlotLatest(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpMaxTS
			default:
				return nil, fmt.Errorf("unsupported aggregate operation: %s", &h.agg[i])
			}
		}

		if err := out[i].geterror(); err != nil {
			return nil, err
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

	initialData := make([]byte, offset)
	initAggregateValues(initialData, ops)

	h.aggregateOps = ops
	h.initialData = initialData

	prog.returnValue(prog.mergeMem(out...))
	return h, nil
}

func (h *HashAggregate) Open() (io.WriteCloser, error) {
	at := &aggtable{
		parent:       h,
		tree:         newRadixTree(len(h.initialData)),
		aggregateOps: h.aggregateOps,
		mergestate:   mergestate(h.aggregateOps),
	}

	atomic.AddInt64(&h.children, 1)
	return splitter(at), nil
}

func (h *HashAggregate) sort() []int {
	ret := make([]int, len(h.final.pairs))
	for i := range ret {
		ret[i] = i
	}
	if h.order == nil {
		return ret
	}
	slices.SortFunc(ret, func(i, j int) bool {
		for k := range h.order {
			dir := h.order[k](h.final, i, j)
			if dir < 0 {
				return true
			}
			if dir > 0 {
				return false
			}
			// dir == 0 -> equal, continue
		}
		return false
	})
	return ret
}

func (h *HashAggregate) Close() error {
	defer h.prog.reset()
	c := atomic.LoadInt64(&h.children)
	if c != 0 {
		return fmt.Errorf("HashAggregate.Close(): have %d children outstanding", c)
	}
	if h.final == nil {
		return fmt.Errorf("HashAggregate.final == nil, didn't compute any aggregates?")
	}
	if h.skipEmpty && h.rowcount == 0 {
		return flushEmpty(h.dst)
	}

	var outst ion.Symtab
	var outbuf ion.Buffer

	var aggsyms []ion.Symbol
	var bysyms []ion.Symbol
	var windowsyms []ion.Symbol

	for i := range h.by {
		bysyms = append(bysyms, outst.Intern(h.by[i].Result()))
	}
	for i := range h.agg {
		aggsyms = append(aggsyms, outst.Intern(h.agg[i].Result))
	}
	for i := range h.windows {
		windowsyms = append(windowsyms, outst.Intern(h.windows[i].result))
	}
	outst.Marshal(&outbuf, true)

	hasfinalize := false
	for i := range h.final.pairs {
		p := &h.final.pairs[i]
		valmem := h.final.valueof(p)
		offset := 0
		for j := range h.aggregateOps {
			op := h.aggregateOps[j]
			if finalize := aggregateOpInfoTable[op.fn].finalizeFunc; finalize != nil && !op.savestate() {
				buf := valmem[offset:]
				finalize(buf)
				hasfinalize = true
			}
			offset += op.dataSize()
			if op.mergestate() {
				offset += aggregateOpMergeBufferSize
			}
		}

		if !hasfinalize {
			break // no finalize found in the first iteration, exit early
		}
	}

	// compute final window results
	for i := range h.windows {
		h.windows[i].run(h.final)
	}
	// compute ORDER BY + LIMIT
	order := h.sort()
	if h.limit > 0 && len(order) > h.limit {
		order = order[:h.limit]
	}

	// turn the i'th 'agg' output
	// into an offset
	offset := make([]int, len(h.aggregateOps))
	off := 0
	for i, op := range h.aggregateOps {
		offset[i] = off
		if op.mergestate() {
			off += aggregateOpMergeBufferSize
		}
		off += op.dataSize()
	}

	for _, n := range order {
		p := &h.final.pairs[n]
		outbuf.BeginStruct(-1)
		valmem := h.final.valueof(p)
		for j, sym := range bysyms {
			outbuf.BeginField(sym)
			outbuf.UnsafeAppend(h.final.repridx(p, j))
		}
		for j, sym := range aggsyms {
			outbuf.BeginField(sym)
			writeAggregatedValue(&outbuf, valmem[offset[j]:], h.aggregateOps[j])
		}
		for j, sym := range windowsyms {
			outbuf.BeginField(sym)
			outbuf.WriteUint(uint64(h.windows[j].final[n]))
		}
		outbuf.EndStruct()
	}

	h.final = nil
	// finally, write the output...
	dst, err := h.dst.Open()
	if err != nil {
		return err
	}
	// NOTE: we are triggering a vm copy here;
	// we're doing this deliberately because
	// typically the result is small (so, cheap)
	// or the result is large in which case
	// the RowSplitter will take care to split
	// it up into small pieces before copying
	_, err = dst.Write(outbuf.Bytes())
	if err != nil {
		dst.Close()
		return err
	}

	// close the threading context
	// *and* the destination query sink
	err = dst.Close()
	err2 := h.dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

// open a stream on dst, write 0 rows into it, and then close it
func flushEmpty(dst QuerySink) error {
	var b ion.Buffer
	var st ion.Symtab
	st.Marshal(&b, true)
	w, err := dst.Open()
	if err != nil {
		return err
	}
	// these things need to happen
	// in this order regardless of errors:
	_, err = w.Write(b.Bytes())
	err1 := w.Close()
	err2 := dst.Close()

	if err == nil {
		err = err1
	}
	if err == nil {
		err = err2
	}
	return err
}
