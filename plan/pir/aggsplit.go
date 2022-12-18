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

	"golang.org/x/exp/slices"
)

type visitor func(e expr.Node) expr.Visitor

func (v visitor) Visit(e expr.Node) expr.Visitor {
	return v(e)
}

// agglifter is a rewriter
// that does not proceed past
// aggregate expressions
type agglifter struct {
	rewrite func(expr.Node) expr.Node
}

func (a *agglifter) Rewrite(e expr.Node) expr.Node {
	return a.rewrite(e)
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
	var walkouter, walkinner visitor
	walkouter = func(e expr.Node) expr.Visitor {
		agg, ok := e.(*expr.Aggregate)
		if ok {
			fn(agg)
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
	bindings map[string]flattenerItem
	id       int
}

func newFlattener() *flattener {
	return &flattener{bindings: make(map[string]flattenerItem)}
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

	slices.SortFunc(tmp, func(a, b item) bool {
		return a.id < b.id
	})

	bindings := make([]expr.Binding, len(tmp))
	for i := range tmp {
		bindings[i] = tmp[i].node
	}

	return bindings
}

func (f *flattener) Rewrite(e expr.Node) expr.Node {
	p, ok := e.(*expr.Path)
	if !ok {
		return e
	}

	result, ok2 := f.bindings[p.First]
	if !ok2 {
		return e
	}

	cp := expr.Copy(result.node.Expr)
	if p.Rest == nil {
		return cp
	}
	return joinpath(cp, p.Rest)
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
	f := newFlattener()
	f.add(columns[0])
	// walk the expressions in order and replace
	// any references to previous columns with
	// the actual expression on the left-hand-side
	for i := 1; i < len(columns); i++ {
		columns[i].Expr = expr.Rewrite(f, columns[i].Expr)
		f.add(columns[i])
	}

	if f.hasduplicates() {
		return f.final()
	}

	return columns
}

func flattenInto(x, y []expr.Binding) {
	flattenIntoFunc(x, len(y), func(j int) *expr.Node {
		return &y[j].Expr
	})
}

func flattenIntoFunc(x []expr.Binding, n int, item func(int) *expr.Node) {
	f := newFlattener()
	for i := range x {
		f.add(x[i])
	}

	for j := 0; j < n; j++ {
		e := item(j)
		*e = expr.Rewrite(f, *e)
	}
}

func (b *Trace) splitAggregate(order []expr.Order, columns, groups []expr.Binding, having expr.Node) error {
	_, err := b.splitAggregateWithAuxiliary(order, nil, columns, groups, having)
	return err
}

// Note: `extra` and `columns` are bindings required by the next step,
//
//	function returns transformed `columns` to be used later by that
//	step. Introduced to handle DISTINCT ON + GROUP BY combining;
//	in the remaining cases, `auxiliary` is nil and the returned `columns `
//	are ignored.
func (b *Trace) splitAggregateWithAuxiliary(order []expr.Order, extra, columns, groups []expr.Binding, having expr.Node) ([]expr.Binding, error) {
	hasaggregate := false
	iterall := false // an aggregate needs all columns
	err := rejectNestedAggregates(columns, order, func(agg *expr.Aggregate) {
		hasaggregate = true
		if agg.Op == expr.OpSystemDatashape {
			iterall = true
		}
	})
	if err != nil {
		return nil, err
	}
	if !hasaggregate {
		// this is actually a DISTINCT
		// written in a funny way:
		err = b.DistinctFromBindings(groups)
		if err != nil {
			return nil, err
		}
		// if there are any bindings in groups,
		// make sure they are reflected in columns
		flattenInto(groups, columns)
		flattenInto(groups, extra)
		err = b.Bind(extra, columns)
		if err != nil {
			return nil, err
		}
		if order == nil {
			return columns, nil
		}
		return columns, b.Order(order)
	}

	if iterall {
		b.top.get("*")
	}

	var aggcols vm.Aggregation
	var aggbindings []expr.Binding // auxiliary bindings for aggregates used only in the ORDER BY clause
	symno := 0

	rewriteAggregate := func(age *expr.Aggregate, allowOver, addAggBinding bool) expr.Node {
		if !allowOver && age.Over != nil {
			err = errorf(age, "window function in illegal position")
			return age
		}
		// see if this is a duplicate aggregate expression;
		// if it is, simply return another path pointing to it
		for i := range aggcols {
			if expr.Equivalent(aggcols[i].Expr, age) {
				p := &expr.Path{First: aggcols[i].Result}
				return p
			}
		}
		// introduce a new intermediate binding
		// that produces the aggregate result
		gen := gensym(0, symno)
		symno++
		p := &expr.Path{First: gen}
		aggcols = append(aggcols, vm.AggBinding{Expr: age, Result: gen})

		if addAggBinding {
			// add identity binding to first bind
			aggbindings = append(aggbindings, expr.Bind(expr.Identifier(gen), gen))
		}
		return p
	}

	// in SELECT, take every aggregate or
	// grouping column reference and lift it out
	// into a previous aggregation step
	rewrite := func(e expr.Node) expr.Node {
		if age, ok := e.(*expr.Aggregate); ok {
			const allowOver = false
			const addAggBinding = false
			return rewriteAggregate(age, allowOver, addAggBinding)
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
				return &expr.Path{First: groups[i].Result()}
			}
		}
		return e
	}
	rw := &agglifter{rewrite: rewrite}
	if having != nil {
		// note: performing the same rewrite
		// for HAVING as we do for projection means
		// that HAVING can actually introduce new aggregate
		// bindings... I suppose that's sometimes useful?
		having = expr.Rewrite(rw, having)
		if err != nil {
			return nil, err
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
			return nil, err
		}
	}
	for i := range extra {
		res := extra[i].Result()
		extra[i].Expr = expr.Rewrite(rw, extra[i].Expr)
		extra[i].As(res)
		if err != nil {
			return nil, err
		}
	}
	// add new aggregations as necessary if
	// they appear in ORDER BY
	rewrite = func(e expr.Node) expr.Node {
		if age, ok := e.(*expr.Aggregate); ok {
			const allowOver = true
			const addAggBinding = true
			return rewriteAggregate(age, allowOver, addAggBinding)
		}
		return e
	}
	rw.rewrite = rewrite
	for i := range order {
		order[i].Column = expr.Rewrite(rw, order[i].Column)
	}
	// now we can push these to the builder
	// in the correct order of evaluation
	err = b.Aggregate(aggcols, groups)
	if err != nil {
		return nil, err
	}
	if having != nil {
		err = b.Where(having)
		if err != nil {
			return nil, err
		}
	}
	err = b.Bind(extra, columns, aggbindings)
	if err != nil {
		return nil, err
	}
	if order != nil {
		err = b.Order(order)
		if err != nil {
			return nil, err
		}
		if len(aggbindings) > 0 {
			// trim out auxiliary aggregate bindings for ORDER BY
			err = b.Bind(identityBindings(extra), identityBindings(columns))
			if err != nil {
				return nil, err
			}
		}
	}

	return columns, nil
}

// identityBindings builds an identity projection, like: x AS x, y AS y, etc.
func identityBindings(columns []expr.Binding) []expr.Binding {
	b := make([]expr.Binding, len(columns))
	for i := range b {
		name := columns[i].Result()
		b[i] = expr.Bind(expr.Identifier(name), name)
	}

	return b
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
	p, ok := agg.Inner.(*expr.Path)
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
