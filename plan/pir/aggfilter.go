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

// aggfiltersimplify alters aggregate filters.
// Returns aggregate and projections if the query was modified.
func aggfiltersimplify(b *Trace) *aggFilterQuery {
	var q aggFilterQuery

	for s := b.top; s != nil; s = s.parent() {
		ok := aggFilterMatchQuery(s, &q)
		if ok {
			break
		}
	}

	if !q.valid() {
		return nil
	}

	if q.agg.GroupBy != nil {
		return nil
	}

	k := countFilteredAggregates(q.agg)
	if k == 0 {
		return nil
	}

	// case 1: remove all filters if possible
	if k == len(q.agg.Agg) {
		aggFilter := aggregatesCommonFilter(q.agg)
		if aggFilter != nil {
			if q.filter != nil {
				q.filter.Where = expr.And(q.filter.Where, aggFilter)
			} else {
				q.filter = &Filter{Where: aggFilter}
				q.agg.setparent(q.filter)
				q.filter.setparent(q.table)
			}

			// drop all filters
			for i := range q.agg.Agg {
				q.agg.Agg[i].Expr.Filter = nil
			}
			return &q
		}
	}

	// case 2: try to remove redundant filters
	modified := false
	if k > 0 && q.filter != nil {
		conj := conjunctions(q.filter.Where, []expr.Node{})
		// Note: unfortunately, O(n^2) complexity
		for i := range q.agg.Agg {
			if exprExists(q.agg.Agg[i].Expr.Filter, conj) {
				q.agg.Agg[i].Expr.Filter = nil
				modified = true
			}
		}
	}

	if modified {
		return &q
	}

	return nil
}

type aggFilterQuery struct {
	bind   *Bind
	agg    *Aggregate
	filter *Filter
	table  *IterTable
}

func (a *aggFilterQuery) valid() bool {
	return a.agg != nil && a.table != nil
}

// aggFilterMatchQuery matches sequence "Bind?, Aggregate, Filter?, IterTable".
func aggFilterMatchQuery(top Step, query *aggFilterQuery) bool {
	step := top

	bind, ok := step.(*Bind)
	if ok {
		query.bind = bind
		step = step.parent()
	}

	agg, ok := step.(*Aggregate)
	if !ok {
		return false
	}

	query.agg = agg
	step = step.parent()

	filter, ok := step.(*Filter)
	if ok {
		query.filter = filter
		step = step.parent()
	}

	table, ok := step.(*IterTable)
	if !ok {
		return false
	}

	query.table = table
	return true
}

// aggremoveduplicates finds duplicated aggregates and removes them,
// adjusting the binding at the same time.
func aggremoveduplicates(b *Trace, agg *Aggregate, bind *Bind) {
	var uniqagg vm.Aggregation

	findExisting := func(agg vm.AggBinding) *vm.AggBinding {
		for i := range uniqagg {
			if expr.Equivalent(uniqagg[i].Expr, agg.Expr) {
				return &uniqagg[i]
			}
		}

		return nil
	}

	duplicates := make(map[string]string)

	// build a list of unique aggregates
	for i := range agg.Agg {
		existing := findExisting(agg.Agg[i])
		if existing != nil {
			duplicates[agg.Agg[i].Result] = existing.Result
		} else {
			uniqagg = append(uniqagg, agg.Agg[i])
		}
	}

	if len(duplicates) == 0 {
		return
	}

	// replace source paths in projection
	for i := range bind.bind {
		path, ok := bind.bind[i].Expr.(expr.Ident)
		if !ok {
			continue
		}
		source, exists := duplicates[string(path)]
		if exists {
			bind.bind[i].Expr = expr.Ident(source)
		}
	}

	// and set the new set of aggregates
	agg.Agg = uniqagg
}

// aggfilter alters a query with aggregate filters.
//
// It does two optimizations:
// 1) when all aggregates have the same condition, the condition
// is moved to the 'WHERE' part of query; otherwise 2) when aggregate condition
// equals to the 'WHERE' part of query, it is removed from that aggregate.
//
// After that, it removes duplicated aggregates.
func aggfilter(b *Trace) {
	query := aggfiltersimplify(b)
	if query != nil {
		aggremoveduplicates(b, query.agg, query.bind)
	}
}

// countFilteredAggregates returns the number of aggregates with a filter expressions.
func countFilteredAggregates(agg *Aggregate) int {
	n := 0

	for i := range agg.Agg {
		a := agg.Agg[i].Expr
		if a.Filter != nil && a.Over == nil {
			n += 1
		}
	}

	return n
}

// aggregatesCommonFilter finds the common filter condition used for all aggregate expressions.
func aggregatesCommonFilter(agg *Aggregate) expr.Node {
	var common expr.Node
	// We expect all aggregates do not have a window expression and they have a filter
	for i := range agg.Agg {
		a := agg.Agg[i].Expr
		if a.Over != nil {
			return nil
		}
		if a.Filter == nil {
			return nil
		}

		if common == nil {
			common = a.Filter
		} else if !common.Equals(a.Filter) {
			return nil
		}
	}

	return common
}

func exprExists(e expr.Node, lst []expr.Node) bool {
	for i := range lst {
		if expr.Equivalent(lst[i], e) {
			return true
		}
	}

	return false
}
