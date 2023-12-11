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
