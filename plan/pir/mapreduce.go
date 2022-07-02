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

// Split splits a query plan into a mapping
// step and a reduction step. The argument
// to split is destructively edited to become
// the mapping step, and the returned step
// is the new reduction step. If the query
// can't be split (either because it is not
// profitable or not possible), the returned
// Builder is the same as the argument.
//
// The mapping step of the query plan can
// be performed in parallel (on different:w
// machines, etc.), and the rows output
// by the mapping step can be unioned
// and then passed to the reduction step,
// which produces the final query results.
//
// NOTE: Split destructively edits the
// Builder query provided to it, so the
// scope information yielded by b.Scope()
// will no longer be accurate.
// Also, Split will panic if it is called
// on a query that has already been split.
func Split(b *Trace) (*Trace, error) {
	// a no-output plan is equivalent to itself
	if _, ok := b.top.(NoOutput); ok {
		return b, nil
	}
	reduce := &Trace{}
	reduce.Inputs, b.Inputs = b.Inputs, nil
	_, err := splitOne(b.top, b, reduce)
	if err != nil {
		b.Inputs = reduce.Inputs
		return nil, err
	}
	for i := range reduce.Inputs {
		in, err := Split(reduce.Inputs[i])
		if err != nil {
			b.Inputs = reduce.Inputs
			return nil, err
		}
		reduce.Inputs[i] = in
	}
	return reduce, nil
}

func fusesLimit(s Step) bool {
	if _, ok := s.(*Order); ok {
		return true
	}
	_, ok := s.(*Distinct)
	return ok
}

func splitOne(s Step, mapping, reduce *Trace) (bool, error) {
	par := s.parent()
	if par == nil {
		if _, ok := s.(DummyOutput); ok {
			// never split; push everything
			// straight to reduction
			reduce.top = s
			return false, nil
		}
		// must just be IterTable;
		// this can always be split and
		// assigned to the mapping step
		reduce.beginUnionMap(mapping, s.(*IterTable))
		return true, nil
	}
	split, err := splitOne(par, mapping, reduce)
	if err != nil {
		return split, err
	}
	// if we are already in the reduction step,
	// just push the result there straight away
	if !split {
		// if we have ORDER+LIMIT N, we can clone
		// those nodes into the mapping step so
		// that only the top/bottom N results
		// are sent to the final ordering operation
		// from each of the mapping steps
		if lim, ok := s.(*Limit); ok && fusesLimit(reduce.top) {
			if _, ok := reduce.top.parent().(*UnionMap); ok {
				if ord, ok := reduce.top.(*Order); ok {
					nord := ord.clone()
					nord.setparent(mapping.top)
					mapping.top = nord
				}
				lo := lim.clone()
				// if we saw 'LIMIT x OFFSET y',
				// then turn this into 'LIMIT x+y'
				// in just the mapping step
				if lim.Offset != 0 {
					lo.Count += lim.Offset
					lo.Offset = 0
				}
				lo.setparent(mapping.top)
				mapping.top = lo
			}
		}
		s.setparent(reduce.top)
		reduce.top = s
		return false, nil
	}

	// based on the type of the next step,
	// determine if we should transition to
	// reduction
	switch n := s.(type) {
	case *Limit:
		// clone LIMIT so that we do it in both places
		mapping.top = n
		l2 := n.clone()
		l2.setparent(reduce.top)
		reduce.top = l2
		return false, nil
	case *Distinct:
		// similar to Limit, clone the op
		// and perform it in both places
		mapping.top = n
		// FIXME: DISTINCT is almost always
		// followed by a projection operation,
		// and really we'd like to push that
		// projection operation into the
		// mapping step as well!
		d2 := n.clone()
		d2.setparent(reduce.top)
		reduce.top = d2
		// no longer in mapping step
		return false, nil
	case *Order:
		mapping.top = par
		n.setparent(reduce.top)
		reduce.top = n
		return false, nil
	case *Aggregate:
		return false, reduceAggregate(n, mapping, reduce)
	case *OutputIndex:
		mapping.top = par
		n.setparent(reduce.top)
		reduce.top = n
		// no longer in mapping step
		return false, nil
	default:
		// we are trivially still in the mapping step
		return true, nil
	}
}

// numberOrMissing takes the expression e
// and produces an expression that evaluates
// to MISSING if e is non-numeric
func numberOrMissing(e expr.Node) expr.Node {
	return expr.Add(e, expr.Integer(0))
}

// take an aggregate expression and re-write it
// so that the output bindings are sufficient
// for the reduction step to produce the correct
// final output
//
//  for example,
//    AVG(x) AS avg
//      -> map:    SUM(x) AS s, COUNT(CAST(x AS FLOAT)) AS c
//      -> reduce: (SUM(s) / SUM_INT(c)) AS avg
//    COUNT(x) AS count -> map: (COUNT(x) AS c)
//      -> map:    COUNT(x) AS c
//      -> reduce: SUM_INT(c) AS count
func reduceAggregate(a *Aggregate, mapping, reduce *Trace) error {
	// transform AVG into two aggregations
	orig := len(a.Agg)
	var bind *Bind
	for i := range a.Agg[:orig] {
		if a.Agg[i].Expr.Op != expr.OpAvg {
			// if we need a final projection,
			// everything that isn't AVG just
			// gets an identity output binding
			if bind != nil {
				bind.bind = append(bind.bind, expr.Bind(expr.Identifier(a.Agg[i].Result), a.Agg[i].Result))
			}
			continue
		}
		a.Agg[i].Expr.Op = expr.OpSum
		count := gensym(1, i)
		a.Agg = append(a.Agg, vm.AggBinding{Expr: expr.Count(numberOrMissing(a.Agg[i].Expr.Inner)), Result: count})
		if bind == nil {
			bind = &Bind{}
			for j := range a.Agg[:i] {
				bind.bind = append(bind.bind, expr.Bind(expr.Identifier(a.Agg[j].Result), a.Agg[j].Result))
			}
		}
		// insert 'sumvar / countvar AS sumvar' into output projection
		sumid := expr.Identifier(a.Agg[i].Result)
		countid := expr.Identifier(count)
		bind.bind = append(bind.bind, expr.Bind(expr.Div(sumid, countid), a.Agg[i].Result))
	}
	if bind != nil {
		for i := range a.GroupBy {
			name := a.GroupBy[i].Result()
			id := expr.Identifier(name)
			bind.bind = append(bind.bind, expr.Bind(id, name))
		}
	}

	// first, compute the set of output columns
	var out vm.Aggregation
	for i := range a.Agg {
		age := a.Agg[i].Expr
		result := a.Agg[i].Result
		// rename the outputs of the mapping-step aggregates;
		// we will re-map them to their original outputs
		gen := gensym(0, i)
		a.Agg[i].Result = gen
		innerref := expr.Identifier(gen)
		switch age.Op {
		default:
			// should have already been compiled away
			return errorf(age, "cannot split %s", expr.ToString(age))
		case expr.OpCount:
			// convert to SUM_COUNT(COUNT(x))
			out = append(out, vm.AggBinding{Expr: expr.SumCount(innerref), Result: result})
		case expr.OpSum, expr.OpMin, expr.OpMax, expr.OpSumInt, expr.OpSumCount, expr.OpBitAnd, expr.OpBitOr, expr.OpBitXor, expr.OpBoolAnd, expr.OpBoolOr, expr.OpEarliest, expr.OpLatest:
			// these are all distributive
			out = append(out, vm.AggBinding{Expr: &expr.Aggregate{Op: age.Op, Inner: innerref}, Result: result})
		}
	}
	// the mapping step terminates here
	// FIXME: update final outputs for mapping
	mapping.top = a
	// FIXME: re-write reduce.Scope() so that
	// bindings that used to originate from
	// the old aggregate expression now point
	// to the new reduction aggregate step
	red := &Aggregate{
		Agg: out,
	}
	if a.GroupBy != nil {
		// insert the set of identity bindings
		// for all of the GROUP BY columns
		group := make([]expr.Binding, len(a.GroupBy))
		for i := range a.GroupBy {
			name := a.GroupBy[i].Result()
			group[i] = expr.Bind(expr.Identifier(name), name)
		}
		red.GroupBy = group
	}
	red.setparent(reduce.top)
	reduce.top = red
	if bind != nil {
		bind.setparent(reduce.top)
		reduce.top = bind
	}
	return nil
}
