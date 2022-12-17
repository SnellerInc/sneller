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
	"github.com/SnellerInc/sneller/sorting"
)

const (
	aggregateTagSize int = 8
)

type HashAggregate struct {
	children int64
	prog     prog
	agg      Aggregation
	by       Selection
	dst      QuerySink

	aggregateOps []AggregateOp
	initialData  []byte

	pos2id []int

	lock  sync.Mutex
	final *aggtable
	limit int

	// ordering functions;
	// applied in order to determine
	// the total ordering
	order []func(*aggtable, hpair, hpair) int
}

// Limit sets the maximum number of output rows.
// Limit <= 0 means there is no limit.
func (h *HashAggregate) Limit(n int) {
	h.limit = n
}

func (h *HashAggregate) OrderByGroup(n int, desc bool, nullslast bool) error {
	if n < 0 || n >= len(h.by) {
		return fmt.Errorf("group %d doesn't exist", n)
	}
	o := sorting.Ordering{}
	o.Direction = sorting.Ascending
	o.Nulls = sorting.NullsFirst
	if desc {
		o.Direction = sorting.Descending
	}
	if nullslast {
		o.Nulls = sorting.NullsLast
	}
	h.order = append(h.order, func(agt *aggtable, left, right hpair) int {
		leftmem := agt.repridx(&left, n)
		rightmem := agt.repridx(&right, n)
		return o.Compare(leftmem, rightmem)
	})
	return nil
}

func (h *HashAggregate) OrderByAggregate(n int, desc bool) error {
	if n >= len(h.agg) {
		return fmt.Errorf("aggregate %d doesn't exist", n)
	}
	aggregateFn := h.aggregateOps[n].fn
	h.order = append(h.order, func(agt *aggtable, left, right hpair) int {
		lmem := agt.valueof(&left)
		rmem := agt.valueof(&right)
		dir := aggcmp(aggregateFn, lmem, rmem)
		if desc {
			return -dir
		}
		return dir
	})
	return nil
}

func NewHashAggregate(agg Aggregation, by Selection, dst QuerySink) (*HashAggregate, error) {
	if len(by) == 0 {
		return nil, fmt.Errorf("cannot aggregate an empty selection")
	}

	if len(agg) == 0 {
		return nil, fmt.Errorf("zero aggregations...?")
	}

	// compute the final positions of each
	// of the projected outputs by sorting
	// them by minimum symbol ID
	//
	// pos[0:len(by)] is the final position of each 'by',
	// and pos[len(by):] is the final position of each 'agg'
	pos2id := make([]int, len(by)+len(agg))
	for i := range pos2id {
		pos2id[i] = i
	}
	posResult := func(i int) string {
		if i >= len(by) {
			return agg[i-len(by)].Result
		}
		return by[i].Result()
	}
	// shuffle results around so that the
	// output symbol table can always be ordered
	slices.SortStableFunc(pos2id, func(i, j int) bool {
		return ion.MinimumID(posResult(i)) < ion.MinimumID(posResult(j))
	})

	h := &HashAggregate{agg: agg, by: by, dst: dst, pos2id: pos2id}

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
	out := make([]*value, len(agg))
	ops := make([]AggregateOp, len(agg))
	bucket := prog.aggbucket(mem, allColumnsHash, allColumnsMask)
	offset := aggregateslot(0)

	for i := range agg {
		var filter *value
		if filterExpr := agg[i].Expr.Filter; filterExpr != nil {
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

		switch op := agg[i].Expr.Op; op {
		case expr.OpCount:
			// COUNT(...) is the only aggregate op that doesn't accept numbers;
			// additionally, it accepts '*', which has a special meaning in this context.
			if _, ok := agg[i].Expr.Inner.(expr.Star); ok {
				// `COUNT(*) GROUP BY X` is equivalent to `COUNT(X) GROUP BY X`
			} else {
				k, err := compile(prog, agg[i].Expr.Inner)
				if err != nil {
					return nil, err
				}
				mask = prog.and(prog.mask(k), mask)
			}

			out[i] = prog.aggregateSlotCount(mem, bucket, mask, offset)
			ops[i].fn = AggregateOpCount

		case expr.OpApproxCountDistinct,
			expr.OpApproxCountDistinctPartial,
			expr.OpApproxCountDistinctMerge:

			argv, err := compile(prog, agg[i].Expr.Inner)
			if err != nil {
				return nil, fmt.Errorf("cannot compile %q: %w", agg[i].Expr.Inner, err)
			}
			precision := agg[i].Expr.Precision

			ops[i].precision = precision
			switch op {
			case expr.OpApproxCountDistinct:
				out[i] = prog.aggregateSlotApproxCountDistinct(mem, bucket, argv, mask, offset, precision)
				ops[i].fn = AggregateOpApproxCountDistinct

			case expr.OpApproxCountDistinctPartial:
				out[i] = prog.aggregateSlotApproxCountDistinctPartial(mem, bucket, argv, mask, offset, precision)
				ops[i].fn = AggregateOpApproxCountDistinctPartial
			case expr.OpApproxCountDistinctMerge:
				out[i] = prog.aggregateSlotApproxCountDistinctMerge(mem, bucket, argv, mask, offset, precision)
				ops[i].fn = AggregateOpApproxCountDistinctMerge
			}

		case expr.OpBoolAnd, expr.OpBoolOr:
			argv, err := prog.compileAsBool(agg[i].Expr.Inner)
			if err != nil {
				return nil, fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
			}
			switch op {
			case expr.OpBoolAnd:
				out[i] = prog.aggregateSlotBoolAnd(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpAndK
			case expr.OpBoolOr:
				out[i] = prog.aggregateSlotBoolOr(mem, bucket, argv, mask, offset)
				ops[i].fn = AggregateOpOrK
			default:
				return nil, fmt.Errorf("unsupported aggregate operation: %s", &agg[i])
			}

		default:
			argv, err := prog.compileAsNumber(agg[i].Expr.Inner)
			if err != nil {
				return nil, fmt.Errorf("don't know how to aggregate %q: %w", agg[i].Expr.Inner, err)
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
				return nil, fmt.Errorf("unsupported aggregate operation: %s", &agg[i])
			}
		}

		// We compile all of the aggregate ops as order-independent so that
		// they can potentially be computed in the order in which the fields
		// are present in the input row rather than the order in which the
		// query presents them.
		offset += aggregateslot(ops[i].dataSize())
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
	}

	atomic.AddInt64(&h.children, 1)
	return splitter(at), nil
}

func (h *HashAggregate) sort(pairs []hpair) {
	if h.order == nil {
		return
	}
	slices.SortFunc(pairs, func(left, right hpair) bool {
		for k := range h.order {
			dir := h.order[k](h.final, left, right)
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
}

func (h *HashAggregate) Close() error {
	c := atomic.LoadInt64(&h.children)
	if c != 0 {
		return fmt.Errorf("HashAggregate.Close(): have %d children outstanding", c)
	}
	if h.final == nil {
		return fmt.Errorf("HashAggregate.final == nil, didn't compute any aggregates?")
	}

	var outst ion.Symtab
	var outbuf ion.Buffer

	var aggsyms []ion.Symbol
	var bysyms []ion.Symbol

	for i := range h.by {
		bysyms = append(bysyms, outst.Intern(h.by[i].Result()))
	}
	for i := range h.agg {
		aggsyms = append(aggsyms, outst.Intern(h.agg[i].Result))
	}

	outst.Marshal(&outbuf, true)

	// perform ORDER BY and LIMIT steps
	pairs := h.final.pairs
	h.sort(pairs)
	if h.limit > 0 && len(pairs) > h.limit {
		pairs = pairs[:h.limit]
	}

	// for each of the pairs,
	// emit the records;
	// we take special care to
	// emit the fields in an order that
	// guarantees that the symbol IDs are sorted
	aggregateOps := h.aggregateOps

	// turn the i'th 'agg' output
	// into an offset
	offset := func(i int) int {
		off := 0
		for _, kind := range h.aggregateOps[:i] {
			off += kind.dataSize()
		}
		return off
	}

	for i := range pairs {
		outbuf.BeginStruct(-1)
		valmem := h.final.valueof(&pairs[i])
		prevsym := ion.Symbol(0)
		for _, pos := range h.pos2id {
			if pos < len(h.by) {
				sym := bysyms[pos]
				if sym < prevsym {
					panic("symbols out-of-order")
				}
				prevsym = sym
				outbuf.BeginField(sym)
				outval := h.final.repridx(&pairs[i], pos)
				outbuf.UnsafeAppend(outval)
			} else {
				pos -= len(bysyms)
				sym := aggsyms[pos]
				if sym < prevsym {
					panic("symbols out-of-order")
				}
				prevsym = sym
				outbuf.BeginField(aggsyms[pos])
				writeAggregatedValue(&outbuf, valmem[offset(pos):], aggregateOps[pos])
			}
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
