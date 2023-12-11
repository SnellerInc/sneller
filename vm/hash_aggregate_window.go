// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package vm

import (
	"fmt"

	"github.com/SnellerInc/sneller/expr"
)

type windowFunc interface {
	next(repeat bool) uint
	reset()
}

var defaultSortOrdering = SortOrdering{
	Direction:  SortAscending,
	NullsOrder: SortNullsFirst,
}

// set h.agg and h.by to agg and by, respectively,
// but filter out window functions
func (h *HashAggregate) compileWindows(windowed Aggregation, by Selection) error {
	if len(windowed) == 0 {
		return nil
	}
	pickGroup := func(e expr.Node) (int, bool) {
		for i := range by {
			if e == expr.Ident(by[i].Result()) ||
				by[i].Expr.Equals(e) {
				return i, true
			}
		}
		return -1, false
	}
	pickOrder := func(e expr.Node, ordering SortOrdering) (aggOrderFn, error) {
		for i := range h.agg {
			if e == expr.Ident(h.agg[i].Result) ||
				h.agg[i].Expr.Equals(e) {
				return h.aggFn(i, ordering), nil
			}
		}
		if grp, ok := pickGroup(e); ok {
			return h.groupFn(grp, ordering), nil
		}
		return nil, fmt.Errorf("unexpected expression %s in window function", expr.ToString(e))
	}

	for i := range windowed {
		var order []aggOrderFn
		wind := windowed[i].Expr.Over
		if wind == nil {
			return fmt.Errorf("%s missing OVER", expr.ToString(windowed[i].Expr))
		}
		wfn, ok := getWindowFunc(windowed[i].Expr.Op)
		if !ok {
			return fmt.Errorf("no support for window function %s", expr.ToString(windowed[i].Expr))
		}
		for j := range wind.PartitionBy {
			fn, err := pickOrder(wind.PartitionBy[j], defaultSortOrdering)
			if err != nil {
				return err
			}
			order = append(order, fn)
		}
		for j := range wind.OrderBy {
			o := SortOrdering{
				Direction:  SortAscending,
				NullsOrder: SortNullsFirst,
			}
			if wind.OrderBy[j].Desc {
				o.Direction = SortDescending
			}
			if wind.OrderBy[j].NullsLast {
				o.NullsOrder = SortNullsLast
			}
			fn, err := pickOrder(wind.OrderBy[j].Column, o)
			if err != nil {
				return err
			}
			order = append(order, fn)
		}
		h.windows = append(h.windows, window{
			order:      order,
			result:     windowed[i].Result,
			fn:         wfn,
			partitions: len(wind.PartitionBy),
		})
	}
	return nil
}

type rowNumber struct {
	num uint
}

func (r *rowNumber) reset() { r.num = 0 }
func (r *rowNumber) next(_ bool) uint {
	r.num++
	return r.num // 1-based
}

type rank struct {
	num, skip uint
}

func (r *rank) reset() { r.num = 0; r.skip = 0 }
func (r *rank) next(repeat bool) uint {
	if repeat {
		r.skip++
		return r.num
	}
	r.num += r.skip + 1
	r.skip = 0
	return r.num
}

type denseRank struct {
	num uint
}

func (d *denseRank) reset() { d.num = 0 }
func (d *denseRank) next(repeat bool) uint {
	if repeat {
		return d.num
	}
	d.num++
	return d.num
}

func getWindowFunc(op expr.AggregateOp) (windowFunc, bool) {
	switch op {
	case expr.OpRowNumber:
		return &rowNumber{}, true
	case expr.OpRank:
		return &rank{}, true
	case expr.OpDenseRank:
		return &denseRank{}, true
	default:
		return nil, false
	}
}
