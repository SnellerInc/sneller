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
)

func isSubset(a, b []expr.Node) bool {
	find := func(x expr.Node) bool {
		for _, y := range b {
			if x.Equals(y) {
				return true
			}
		}
		return false
	}
	for i := range a {
		if !find(a[i]) {
			return false
		}
	}
	return true
}

// AGGREGATE ... GROUP BY cols... -> FILTER DISTINCT cols... is redundant;
// we can eliminate the FILTER DISTINCT step
//
// similarly,
//
//	FILTER cols ... -> FILTER cols ...
//
// is redundant as well; we can take the smaller set
func distinctelim(b *Trace) {
	var prev Step
outer:
	for s := b.top; s != nil; s = s.parent() {
		d, ok := s.(*Distinct)
		if !ok {
			prev = s
			continue
		}
		par := d.parent()

		// if we have BIND -> DISTINCT, then rearrange:
		if sel, ok := par.(*Bind); ok {
			flattenIntoExprs(sel.bind, d.Columns)
			d.setparent(sel.parent())
			sel.setparent(d)
			if prev == nil {
				b.top = sel
			} else {
				prev.setparent(sel)
			}
			prev = sel
			par = d.parent()
		}

		// if we have DISTINCT -> DISTINCT,
		// we can sometimes eliminate one or the other
		if d2, ok := par.(*Distinct); ok {
			if isSubset(d.Columns, d2.Columns) {
				// remove d2; it is redundant
				d.setparent(d2.parent())
				par = d.parent()
			} else if isSubset(d2.Columns, d.Columns) {
				// remove d; it is redundant
				if prev == nil {
					b.top = d2
				} else {
					prev.setparent(d2)
				}
				prev = d2
				continue
			}
		}

		agg, ok := par.(*Aggregate)
		if !ok || len(agg.GroupBy) > len(d.Columns) {
			prev = s
			continue
		}
		// we can remove this Distinct
		// iff its columns are a subset of (or equal to)
		// the GROUP BY columns, which are already
		// guaranteed to be distinct
		groups := make(map[string]struct{})
		for i := range agg.GroupBy {
			groups[agg.GroupBy[i].Result()] = struct{}{}
		}
		for i := range d.Columns {
			id, ok := d.Columns[i].(expr.Ident)
			if !ok {
				prev = s
				continue outer
			}
			_, ok = groups[string(id)]
			if !ok {
				prev = s
				continue outer
			}
		}
		// split out filter step
		if prev == nil {
			b.top = agg
		} else {
			prev.setparent(agg)
		}
		prev = agg
	}
}
