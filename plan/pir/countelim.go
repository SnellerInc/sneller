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

package pir

import (
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/vm"
)

// walk a set of expressions and see if we can
// match the index of a single aggregate which is
// a COUNT(DISTINCT ...) aggregate
//
// returns (index, true) if there is exactly one match,
// or (-1, false) if there are no matches, more than one match,
// or there are other aggregate expressions present
func singleCountDistinct(agg vm.Aggregation) (int, bool) {
	idx := -1
	for i := range agg {
		if agg[i].Expr.Op != expr.OpCountDistinct || idx != -1 {
			return -1, false
		}
		idx = i
	}
	return idx, idx >= 0
}

// convert SELECT COUNT(DISTINCT x), y...
// into SELECT COUNT(x), y... FROM (SELECT DISTINCT x, y...)
// since we do not natively support COUNT DISTINCT
func countdistinct2count(b *Trace) {
	for s := b.top; s != nil; s = s.parent() {
		a, ok := s.(*Aggregate)
		if !ok {
			continue
		}
		idx, ok := singleCountDistinct(a.Agg)
		if !ok {
			continue
		}
		// rewrite CountDistinct -> Count
		cd := a.Agg[idx].Expr
		cd.Op = expr.OpCount
		distinct := &Distinct{
			Columns: []expr.Node{cd.Inner},
		}
		// make the other columns distinct as well
		for i := range a.GroupBy {
			distinct.Columns = append(distinct.Columns, a.GroupBy[i].Expr)
		}
		// splice in new Distinct node
		distinct.setparent(s.parent())
		a.setparent(distinct)
	}
}
