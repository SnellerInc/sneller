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

	visit := expr.WalkFunc(func(e expr.Node) bool {
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
