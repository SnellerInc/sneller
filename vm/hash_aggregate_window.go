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

	"github.com/SnellerInc/sneller/expr"
)

type windowFunc interface {
	next(repeat bool) uint
	reset()
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
	pickOrder := func(e expr.Node, desc, nullslast bool) (aggOrderFn, error) {
		for i := range h.agg {
			if e == expr.Ident(h.agg[i].Result) ||
				h.agg[i].Expr.Equals(e) {
				return h.aggFn(i, desc, nullslast), nil
			}
		}
		if grp, ok := pickGroup(e); ok {
			return h.groupFn(grp, desc, nullslast), nil
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
			fn, err := pickOrder(wind.PartitionBy[j], false, false)
			if err != nil {
				return err
			}
			order = append(order, fn)
		}
		for j := range wind.OrderBy {
			desc := wind.OrderBy[j].Desc
			nullslast := wind.OrderBy[j].NullsLast
			fn, err := pickOrder(wind.OrderBy[j].Column, desc, nullslast)
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
