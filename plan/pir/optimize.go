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

// for the final projection specifically,
// we *must* have explicit output names
func freezefinal(b *Trace) {
	for i := range b.final {
		b.final[i].As(b.final[i].Result())
	}
}

func simplify(b *Trace) {
	hint := &stepHint{}
	reg := expr.Simplifier(hint)
	log := expr.LogicSimplifier(hint)
	fn := func(e expr.Node, logic bool) expr.Node {
		e = expr.Rewrite(reg, e)
		if logic {
			return expr.Rewrite(log, e)
		}
		return e
	}
	for s := b.top; s != nil; s = s.parent() {
		hint.parent = s.parent()
		s.rewrite(fn)
	}
}

func subflatten(b *Trace) {
	var prev Step
	for s := b.top; s != nil; s = s.parent() {
		if pt, ok := s.(*pseudoTable); ok {
			if prev == nil {
				b.top = pt.parent()
			} else {
				prev.setparent(pt.parent())
			}
			continue
		}
		prev = s
	}
}

func (b *Trace) optimize() error {
	// pre-passes to make optimization easier:
	freezefinal(b) // explicitly choose final output names

	subflatten(b) // remove pseudo-tables

	// actual optimization passes:
	simplify(b)
	aggelim(b) // substitute constants for aggregates if possible
	aggfilter(b)
	orderelim(b)
	projectpushdown(b) // merge adjacent projections
	liftprojectagg(b)  // eliminate a trivial projection after an aggregate
	distinctelim(b)
	countdistinct2count(b) // turn count(distinct x) -> count(x) from (select distinct ...)
	strengthReduce(b)      // strength-reduce kernels, replacing generic subtraces with their case-specific optimized variants
	filterelim(b)          // eliminate WHERE TRUE
	filterpushdown(b)      // merge adjacent filters
	limitpushdown(b)       // push down LIMIT
	err := joinelim(b)     // turn EquiJoin into a correlated sub-query + projection
	if err != nil {
		return err
	}
	projectelim(b)     // drop un-used bindings
	projectpushdown(b) // merge adjacent projections
	simplify(b)        // final simplification pass
	if err := postcheck(b); err != nil {
		return err
	}
	partition(b)
	mergereplacements(b) // eliminate common sub-traces

	// TODO:
	//  - push down DISTINCT when it occurs
	//  after a simple projection (but not extended projection)
	//
	return nil
}
