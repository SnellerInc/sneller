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
// be performed in parallel (on different
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
	reduce := &Trace{finalTypes: b.FinalTypes()}
	reduce.Replacements, b.Replacements = b.Replacements, nil
	_, err := splitOne(b.top, b, reduce)
	if err != nil {
		b.Replacements = reduce.Replacements
		return nil, err
	}
	for i := range reduce.Replacements {
		in, err := Split(reduce.Replacements[i])
		if err != nil {
			b.Replacements = reduce.Replacements
			return nil, err
		}
		reduce.Replacements[i] = in
	}
	postoptimize(reduce)
	return reduce, nil
}

// NoSplit optimizes a trace assuming
// it won't ever be passed to Split.
func NoSplit(t *Trace) *Trace {
	postoptimize(t)
	return t
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
	case *UnpivotAtDistinct:
		mapping.top = n
		cols := []expr.Node{&expr.Path{First: *n.Ast.At}}
		dis := Distinct{Columns: cols}
		dis.setparent(reduce.top)
		reduce.top = &dis
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
//	for example,
//	  AVG(x) AS avg
//	    -> map:    SUM(x) AS s, COUNT(CAST(x AS FLOAT)) AS c
//	    -> reduce: (SUM(s) / SUM_INT(c)) AS avg
//	  COUNT(x) AS count -> map: (COUNT(x) AS c)
//	    -> map:    COUNT(x) AS c
//	    -> reduce: SUM_INT(c) AS count
func reduceAggregate(a *Aggregate, mapping, reduce *Trace) error {

	needsFinalProjection := false
	for i := range a.Agg {
		switch a.Agg[i].Expr.Op {
		case expr.OpApproxCountDistinct:
			// All APPROX_COUNT_DISTINCT becomes its partial version.
			//
			// Note: Technically, the partial version does exactly what
			//       the sole version does. However, the sole version returns
			//       an int, while partial one, the auxiliary buffer
			//       which is meant to be merged in the final step.
			a.Agg[i].Expr.Op = expr.OpApproxCountDistinctPartial

		case expr.OpAvg:
			// If there is AVG aggregate, we need to introduce
			// extra binding and projection to properly gather
			// the partial results.
			needsFinalProjection = true
		}
	}

	isIntCast := func(e expr.Node) bool {
		c, ok := e.(*expr.Cast)
		return ok && c.To == expr.IntegerType
	}

	var bind *Bind
	if needsFinalProjection {
		orig := len(a.Agg)
		bind = &Bind{}
		bind.bind = make([]expr.Binding, 0, len(a.Agg)+len(a.GroupBy))
		for i := range a.Agg[:orig] {
			switch a.Agg[i].Expr.Op {
			default:
				// if we need a final projection,
				// everything that isn't AVG just
				// gets an identity output binding
				bind.bind = append(bind.bind, expr.Identity(a.Agg[i].Result))
			case expr.OpAvg:
				// transform AVG into two aggregations
				a.Agg[i].Expr.Op = expr.OpSum

				inner := a.Agg[i].Expr.Inner
				cast := func(e expr.Node) expr.Node { return e }
				// if we have AVG(CAST(x AS INTEGER)),
				// then we need all the operations to be integer ops
				if isIntCast(inner) {
					a.Agg[i].Expr.Op = expr.OpSumInt
					cast = func(e expr.Node) expr.Node {
						return &expr.Cast{From: e, To: expr.IntegerType}
					}
				}
				count := gensym(1, i)
				countagg := expr.Count(numberOrMissing(a.Agg[i].Expr.Inner))
				if filter := a.Agg[i].Expr.Filter; filter != nil {
					countagg.Filter = expr.Copy(filter)
				}
				a.Agg = append(a.Agg, vm.AggBinding{Expr: countagg, Result: count})

				// insert
				// CASE count IS NOT NULL THEN sum / count ELSE NULL
				sumid := expr.Identifier(a.Agg[i].Result)
				countid := expr.Identifier(count)
				result := &expr.Case{
					Limbs: []expr.CaseLimb{{
						When: expr.Compare(expr.Equals, countid, expr.Integer(0)),
						Then: expr.Null{},
					}},
					Else: expr.Div(cast(sumid), cast(countid)),
				}
				bind.bind = append(bind.bind, expr.Bind(result, a.Agg[i].Result))
			}
		}

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
		gen := gensym(2, i)
		a.Agg[i].Result = gen
		innerref := expr.Identifier(gen)
		var newagg *expr.Aggregate
		switch age.Op {
		case expr.OpCount:
			// convert to SUM_COUNT(COUNT(x))
			newagg = expr.SumCount(innerref)
		case expr.OpMin, expr.OpMax:
			// mostly trivial, but be sure to force integer calculations here:
			newagg = &expr.Aggregate{Op: age.Op, Inner: innerref}
			if isIntCast(age.Inner) {
				newagg.Inner = &expr.Cast{From: newagg.Inner, To: expr.IntegerType}
			}
		case expr.OpSum, expr.OpSumInt, expr.OpSumCount,
			expr.OpBitAnd, expr.OpBitOr, expr.OpBitXor, expr.OpBoolAnd, expr.OpBoolOr,
			expr.OpEarliest, expr.OpLatest:
			// these are all distributive
			newagg = &expr.Aggregate{Op: age.Op, Inner: innerref}
		case expr.OpApproxCountDistinctPartial:
			newagg = &expr.Aggregate{
				Op:        expr.OpApproxCountDistinctMerge,
				Precision: age.Precision,
				Inner:     innerref}
		case expr.OpSystemDatashape:
			newagg = &expr.Aggregate{
				Op:    expr.OpSystemDatashapeMerge,
				Inner: innerref}
		}

		if newagg == nil {
			// should have already been compiled away
			return errorf(age, "cannot split aggregate %s", expr.ToString(age))
		}

		out = append(out, vm.AggBinding{Expr: newagg, Result: result})
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
			group[i] = expr.Identity(name)
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

// try to push o past steps that
// do not permute the order of outputs
func pushOrder(o *Order, s Step) Step {
	switch s := s.(type) {
	case *Filter:
		// no bindings to adjust;
		// we can do this trivially
		if next := pushOrder(o, s.parent()); next != nil {
			return next
		}
		return s
	case *Bind:
		// adjust bindings to reflect the
		// state before rather than after
		// the projection:
		bf := bindflattener{from: s.bind}
		for i := range o.Columns {
			o.Columns[i].Column = expr.Rewrite(&bf, o.Columns[i].Column)
		}
		if next := pushOrder(o, s.parent()); next != nil {
			return next
		}
		return s
	default:
		return nil
	}
}

// run optimizations on a trace that
// has been frozen for splitting
func postoptimize(t *Trace) {
	// steps that follow an aggregation
	// operation are single-stream; in those
	// cases we can move around ORDER BY operations
	// to try to make them more efficient
	if agg := findAggregate(t); agg != nil {
		pushReduceOrder(t, agg)
	}
}

func findAggregate(t *Trace) Step {
	for x := t.top; x != nil; x = x.parent() {
		if _, ok := x.(*Aggregate); ok {
			return x
		}
	}
	return nil
}

// we know that reduction steps are "single-threaded,"
// so we can shift an ORDER BY past the operations that
// do not permute the row order
func pushReduceOrder(t *Trace, upto Step) {
	var parent Step
	for x := t.top; x != nil && x != upto; x = x.parent() {
		ord, ok := x.(*Order)
		if !ok {
			parent = x
			continue
		}
		next := ord.parent()
		newparent := pushOrder(ord, next)
		if newparent != nil {
			// newparent -> ord -> newparent.parent()
			ord.setparent(newparent.parent())
			newparent.setparent(ord)
			if parent == nil {
				t.top = next
			} else {
				parent.setparent(next)
			}
			// let's assume only 1 ORDER BY;
			// let's also check to see if moving
			// the ORDER BY around exposed some simple
			// optimizations w.r.t. BIND and LIMIT
			limitpushdown(t)
			projectpushdown(t)
			return
		}
		parent = x
	}
}
