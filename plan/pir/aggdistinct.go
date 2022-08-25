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
	"fmt"

	"github.com/SnellerInc/sneller/expr"
)

// aggdistinct is expr.Rewriter replaces aggregates having DISTINCT clause with subqueries.
type aggdistinct struct {
	sel *expr.Select
}

func (a *aggdistinct) Rewrite(e expr.Node) expr.Node {
	agg, ok := e.(*expr.Aggregate)
	if ok && agg.IsDistinct() {
		subquery := &expr.Select{
			From:    a.sel.From,
			Where:   a.sel.Where,
			Columns: []expr.Binding{expr.Bind(e, "")},
		}

		return expr.Copy(subquery)
	}

	return e
}

func (a *aggdistinct) Walk(e expr.Node) expr.Rewriter {
	// do not alter subqueries
	if _, ok := e.(*expr.Select); ok {
		return nil
	}

	return a
}

// aggdistinctpromote replaces aggregates having the DISTINCT clasue with subqueries
//
// This transformation is done only if a query has aggregates with and without
// DISTINCT clause.
func aggdistinctpromote(s *expr.Select) error {
	if !hasMixedDistinctAndRegularAggregates(s.Columns) {
		return nil
	}

	if s.GroupBy != nil {
		return fmt.Errorf("cannot use a distinct aggregate with GROUP BY")
	}

	rw := &aggdistinct{sel: s}
	for i := range s.Columns {
		s.Columns[i].Expr = expr.Rewrite(rw, s.Columns[i].Expr)
	}

	return nil
}

func hasMixedDistinctAndRegularAggregates(columns []expr.Binding) bool {
	hasDistinct := false
	hasAggregate := false

	visit := visitfn(func(e expr.Node) bool {
		agg, ok := e.(*expr.Aggregate)
		if ok {
			if agg.IsDistinct() {
				hasDistinct = true
			} else {
				hasAggregate = true
			}
			return false
		}
		return true
	})

	for i := range columns {
		expr.Walk(visit, columns[i].Expr)

		if hasAggregate && hasDistinct {
			return true
		}
	}

	return false
}
