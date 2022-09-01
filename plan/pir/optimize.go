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
)

// for the final projection specifically,
// we *must* have explicit output names
func freezefinal(b *Trace) {
	for i := range b.final {
		b.final[i].As(b.final[i].Result())
	}
}

func simplify(b *Trace) {
	reg := expr.Simplifier(b)
	log := expr.LogicSimplifier(b)

	fn := func(e expr.Node, logic bool) expr.Node {
		e = expr.Rewrite(reg, e)
		if logic {
			return expr.Rewrite(log, e)
		}
		return e
	}
	for s := b.top; s != nil; s = s.parent() {
		s.rewrite(fn)
	}
}

func (b *Trace) optimize() {
	// pre-passes to make optimization easier:
	freezefinal(b) // explicitly choose final output names

	// actual optimization passes:
	simplify(b)
	aggelim(b) // substitute constants for aggregates if possible
	aggfilter(b)
	orderelim(b)
	projectpushdown(b)     // merge adjacent projections
	liftprojectagg(b)      // eliminate a trivial projection after an aggregate
	countdistinct2count(b) // turn count(distinct x) -> count(x) from (select distinct ...)
	filterelim(b)          // eliminate WHERE TRUE
	filterpushdown(b)      // merge adjacent filters
	projectpushdown(b)     // merge adjacent projections
	projectelim(b)         // drop un-used bindings
	limitpushdown(b)       // push down LIMIT
	flatten(b)             // eliminate left-to-right bindings
	mergereplacements(b)   // eliminate common sub-traces
	simplify(b)            // final simplification pass

	// TODO:
	//  - push down DISTINCT when it occurs
	//  after a simple projection (but not extended projection)
	//
}
