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
	"slices"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/vm"
)

type visitor func(e expr.Node) expr.Visitor

func (v visitor) Visit(e expr.Node) expr.Visitor {
	return v(e)
}

// agglifter is a rewriter
// that does not proceed past
// aggregate expressions
type agglifter struct {
	rewrite  func(e expr.Node, windowok bool) expr.Node
	windowok bool
}

func (a *agglifter) Rewrite(e expr.Node) expr.Node {
	return a.rewrite(e, a.windowok)
}

func (a *agglifter) Walk(e expr.Node) expr.Rewriter {
	switch e.(type) {
	case *expr.Aggregate, *expr.Select:
		return nil

	}
	return a
}

func matchAny[T any](lst []T, proc func(*T) bool) bool {
	for i := range lst {
		if proc(&lst[i]) {
			return true
		}
	}
	return false
}

// broadly speaking we accept aggregates
// like COUNT(...) in any expression position,
// but in practice they cannot be nested, so
// let's search for those cases in advance
// so that we can provide a more helpful error
func rejectNestedAggregates(columns []expr.Binding, order []expr.Order, fn func(*expr.Aggregate)) error {
	var err error

	// we have two AST visitors, and we switch
	// from the outer to the inner visitor upon
	// encountering an aggregate expression
	// (and the inner visitor errors when it finds
	// inner aggregates)
	var walkouter, walkinner, walkwindow visitor
	walkouter = func(e expr.Node) expr.Visitor {
		agg, ok := e.(*expr.Aggregate)
		if ok {
			fn(agg)
			if agg.Over != nil {
				return walkwindow
			}
			return walkinner
		}
		return walkouter
	}
	walkinner = func(e expr.Node) expr.Visitor {
		if err != nil {
			return nil
		}
		agg, ok := e.(*expr.Aggregate)
		if ok {
			err = errorf(agg, "cannot handle nested aggregate %s", expr.ToString(agg))
			return nil
		}

		_, ok = e.(*expr.Select)
		if ok {
			// do not check sub-queries
			return walkouter
		}

		return walkinner
	}
	// window functions often *do* have embedded aggregates;
	// don't worry about them
	walkwindow = func(e expr.Node) expr.Visitor {
		if err != nil {
			return nil
		}
		agg, ok := e.(*expr.Aggregate)
		if ok && agg.Over != nil {
			err = errorf(agg, "cannot handle nested aggregate %s", expr.ToString(agg))
			return nil
		}
		_, ok = e.(*expr.Select)
		if ok {
			return walkouter
		}
		return walkinner
	}

	for i := range columns {
		expr.Walk(walkouter, columns[i].Expr)
		if err != nil {
			return err
		}
	}
	for i := range order {
		expr.Walk(walkouter, order[i].Column)
		if err != nil {
			return err
		}
	}
	return nil
}

type flattenerItem struct {
	node expr.Binding
	id   int // ordinal position within binding
}

type flattener struct {
	bindings     map[string]flattenerItem
	id           int
	replacements int
}

func newFlattener(size int) *flattener {
	return &flattener{bindings: make(map[string]flattenerItem, size)}
}

// add adds a new binding to the current set
func (f *flattener) add(b expr.Binding) {
	f.bindings[b.Result()] = flattenerItem{
		node: b,
		id:   f.id,
	}
	f.id += 1
}

// hasduplicates returns if there are any duplicated aliases
func (f *flattener) hasduplicates() bool {
	return f.id != len(f.bindings)
}

// final returns new bindings without duplicates
func (f *flattener) final() []expr.Binding {
	type item struct {
		node expr.Binding
		id   int
	}

	tmp := make([]item, len(f.bindings))
	i := 0
	for _, v := range f.bindings {
		tmp[i] = item(v)
		i += 1
	}

	slices.SortFunc(tmp, func(a, b item) int {
		return a.id - b.id
	})

	bindings := make([]expr.Binding, len(tmp))
	for i := range tmp {
		bindings[i] = tmp[i].node
	}

	return bindings
}

func (f *flattener) Rewrite(e expr.Node) expr.Node {
	id, ok := e.(expr.Ident)
	if !ok {
		return e
	}
	result, ok2 := f.bindings[string(id)]
	if !ok2 {
		return e
	}

	f.replacements += 1
	return expr.Copy(result.node.Expr)
}

func (f *flattener) Walk(e expr.Node) expr.Rewriter {
	if _, ok := e.(*expr.Select); ok {
		// don't walk into sub-queries;
		// otherwise we will end up breaking
		// subquery correlation detection
		return nil
	}
	return f
}

// flattenBind takes a set of bindings like
//
//	3 as x, x+1 as y, y+1 as z
//
// and rewrites them to
//
//	3 as x, 3+1 as y, 3+1+1 as z
//
// so that there are no references to previous
// columns in the final projection
//
// Also removes the duplicated bindings, for instance:
//
//	x as a, y as a, z as a
//
// becomes
//
//	z as a
func flattenBind(columns []expr.Binding) []expr.Binding {
	if len(columns) <= 1 {
		return columns
	}
	f := newFlattener(len(columns))
	f.add(columns[0])
	// walk the expressions in order and replace
	// any references to previous columns with
	// the actual expression on the left-hand-side
	for i := 1; i < len(columns); i++ {
		f.replacements = 0
		columns[i].Expr = expr.Rewrite(f, columns[i].Expr)
		if f.replacements > 0 {
			// simplify only modified expressions
			columns[i].Expr = expr.Simplify(columns[i].Expr, expr.NoHint)
		}

		f.add(columns[i])
	}

	if f.hasduplicates() {
		return f.final()
	}

	return columns
}

func flattenIntoExprs(x []expr.Binding, y []expr.Node) {
	flattenIntoFunc(x, len(y), func(j int) *expr.Node {
		return &y[j]
	})
}

func flattenIntoOrder(x []expr.Binding, y []expr.Order) {
	flattenIntoFunc(x, len(y), func(j int) *expr.Node {
		return &y[j].Column
	})
}

func flattenOne(x []expr.Binding, e expr.Node) expr.Node {
	f := newFlattener(len(x))
	for i := range x {
		f.add(x[i])
	}
	return expr.Rewrite(f, e)
}

func flattenIntoFunc(x []expr.Binding, n int, item func(int) *expr.Node) {
	f := newFlattener(len(x))
	for i := range x {
		f.add(x[i])
	}

	for j := 0; j < n; j++ {
		e := item(j)
		*e = expr.Rewrite(f, *e)
	}
}

// splitAggregate generates the aggregation (and HAVING) step(s)
// and rewrites the order and distinct expressions to use
// the bindings produced by the aggregation step
func (b *Trace) splitAggregate(order []expr.Order, distinct []expr.Node, columns, groups []expr.Binding, having expr.Node) error {
	hasaggregate := false
	iterall := false // an aggregate needs all columns
	err := rejectNestedAggregates(columns, order, func(agg *expr.Aggregate) {
		hasaggregate = true
		if agg.Op == expr.OpSystemDatashape {
			iterall = true
		}
	})
	if err != nil {
		return err
	}
	if anyHasAggregate(groups) {
		return fmt.Errorf("GROUP BY cannot contain aggregates")
	}
	if !hasaggregate {
		flattenIntoExprs(groups, distinct)
		err = b.DistinctFromBindings(groups)
		if err != nil {
			return err
		}
		return b.Bind(groups)
	}

	if iterall {
		b.top.get("*")
	}

	var aggcols vm.Aggregation
	symno := 0

	rewriteAggregate := func(age *expr.Aggregate, allowOver bool) expr.Node {
		if !allowOver && age.Over != nil && !age.Op.WindowOnly() {
			err = errorf(age, "window function in illegal position")
			return age
		}
		// see if this is a duplicate aggregate expression;
		// if it is, simply return another path pointing to it
		for i := range aggcols {
			if expr.Equivalent(aggcols[i].Expr, age) {
				return expr.Ident(aggcols[i].Result)
			}
		}
		// introduce a new intermediate binding
		// that produces the aggregate result
		gen := gensym(0, symno)
		symno++
		p := expr.Ident(gen)
		aggcols = append(aggcols, vm.AggBinding{Expr: age, Result: gen})
		return p
	}

	// in SELECT, take every aggregate or
	// grouping column reference and lift it out
	// into a previous aggregation step
	rewrite := func(e expr.Node, windowok bool) expr.Node {
		if age, ok := e.(*expr.Aggregate); ok {
			return rewriteAggregate(age, windowok)
		}
		// if this expression matches a grouping expression,
		// then set the output of the grouping expression
		// to the temporary value for 'bind'
		for i := range groups {
			if expr.Equivalent(e, groups[i].Expr) {
				if !groups[i].Explicit() {
					gen := gensym(0, symno)
					symno++
					groups[i].As(gen)
				}
				return expr.Ident(groups[i].Result())
			}
		}
		return e
	}
	rw := &agglifter{rewrite: rewrite, windowok: false}
	if having != nil {
		// flatten any SELECT bindings into the HAVING clause
		// so that it can operate with the same set of bindings
		// as we had before actually computing the final projection
		having = flattenOne(columns, having)
		// note: performing the same rewrite
		// for HAVING as we do for projection means
		// that HAVING can actually introduce new aggregate
		// bindings... I suppose that's sometimes useful?
		having = expr.Rewrite(rw, having)
		if err != nil {
			return err
		}
	}
	// keep a copy of the original projection
	// in case we have to create phantom outputs
	// during aggregation that need to be eliminated
	for i := range columns {
		res := columns[i].Result()
		columns[i].Expr = expr.Rewrite(rw, columns[i].Expr)
		columns[i].As(res)
		if err != nil {
			return err
		}
	}
	rw.windowok = true
	// ORDER BY and DISTINCT ON have the same binding semantics:
	for i := range order {
		order[i].Column = expr.Rewrite(rw, order[i].Column)
	}
	for i := range distinct {
		distinct[i] = expr.Rewrite(rw, distinct[i])
	}
	// now we can push these to the builder
	// in the correct order of evaluation
	err = b.Aggregate(aggcols, groups)
	if err != nil {
		return err
	}
	if having != nil {
		err = b.Where(having)
	}
	return err
}

// aggelim replaces aggregate expressions that can be
// satisfied using index metadata with constants.
func aggelim(b *Trace) {
	var a *Aggregate
	var tbl *IterTable
	var child Step
	found := false
	for s := b.top; s != nil; s = s.parent() {
		var ok bool
		a, ok = s.(*Aggregate)
		if !ok {
			child = s
			continue
		}
		tbl, ok = s.parent().(*IterTable)
		if !ok {
			child = s
			continue
		}
		found = true
		break
	}
	if !found || a.GroupBy != nil || tbl.Index == nil {
		return
	}
	// attempt to substitute aggregate expressions
	// with constants using the index
	var subs []expr.Node
	for i := range a.Agg {
		c := agg2const(tbl, a.Agg[i].Expr)
		if c == nil {
			continue
		}
		if subs == nil {
			subs = make([]expr.Node, len(a.Agg))
		}
		subs[i] = c
	}
	if subs == nil {
		return
	}
	// create new bindings for the constant values,
	// eliminate the replaced aggregates, and
	// adjust the scope
	bi := &Bind{}
	bi.bind = make([]expr.Binding, len(subs))
	aggs := a.Agg[:0]
	for i := range a.Agg {
		if subs[i] == nil {
			aggs = append(aggs, a.Agg[i])
			id := expr.Identifier(a.Agg[i].Result)
			bi.bind[i] = expr.Bind(id, a.Agg[i].Result)
			continue
		}
		for j := range b.final {
			if b.final[j].Expr == a.Agg[i].Expr {
				b.final[j].Expr = subs[i]
			}
		}
		bi.bind[i] = expr.Bind(subs[i], a.Agg[i].Result)
	}
	// if all aggregates were substituted, we can
	// eliminate the table and aggregate entirely
	if len(aggs) == 0 {
		bi.setparent(DummyOutput{})
	} else {
		bi.setparent(a)
		a.Agg = aggs
	}
	if child == nil {
		b.top = bi
	} else {
		child.setparent(bi)
	}
}

func agg2const(tbl *IterTable, agg *expr.Aggregate) expr.Constant {
	if agg.Inner == nil {
		return nil
	}
	p, ok := expr.FlatPath(agg.Inner)
	if !ok {
		return nil
	}
	switch agg.Op {
	case expr.OpEarliest:
		min, _, ok := tbl.timeRange(p)
		if ok {
			return &expr.Timestamp{Value: min}
		}
	case expr.OpLatest:
		_, max, ok := tbl.timeRange(p)
		if ok {
			return &expr.Timestamp{Value: max}
		}
	}
	return nil
}

// an ORDER BY that occurs immediately
// following an operation that must return one row
func orderelim(b *Trace) {
	if b.Class() > SizeOne {
		return
	}
	var prev Step
	for s := b.top; s != nil; s = s.parent() {
		// if we encounter an op with fixed cardinality <= 1,
		// then we are no longer able to optimize away the ORDER BY
		if l, ok := s.(*Limit); ok && l.Count <= 1 {
			break
		}
		if agg, ok := s.(*Aggregate); ok && len(agg.GroupBy) == 0 {
			break
		}
		ord, ok := s.(*Order)
		if !ok {
			prev = s
			continue
		}
		// this ORDER BY step doesn't do
		// anything because it only orders 1 row...
		if prev == nil {
			b.top = s.parent()
		} else {
			prev.setparent(ord.parent())
		}
	}
}
