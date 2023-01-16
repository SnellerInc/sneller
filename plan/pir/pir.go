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
	"bytes"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// table is the base type for operations
// that iterate something that produces rows
//
// tables have a built-in Filter so that references
// to stored tables that use the same filter condition
// can be easily compared for caching purposes
// (in other words, the same table with the same filter
// and referenced columns is the same "view," so we can
// cache that view rather than the whole table if it
// ends up getting touched multiple times)
type table struct {
	Filter     expr.Node
	Bind       string
	star       bool // this table is referenced via '*'
	haveParent bool // free variable references are allowed
}

func (t *table) equals(x *table) bool {
	return t == x ||
		expr.Equal(t.Filter, x.Filter) &&
			t.Bind == x.Bind &&
			t.star == x.star &&
			t.haveParent == x.haveParent
}

func (t *table) walk(v expr.Visitor) {
	if t.Filter != nil {
		expr.Walk(v, t.Filter)
	}
}

// strip a path that has been determined
// to resolve to a reference to this table
func (i *IterTable) strip(path []string) ([]string, error) {
	if i.Bind == "" {
		i.free[path[0]] = struct{}{}
		return path, nil
	} else if path[0] != i.Bind {
		if !i.haveParent {
			return nil, errorf(expr.MakePath(path), "reference to undefined variable %q", path[0])
		}
		i.free[path[0]] = struct{}{}
		// this is *definitely* a free variable
		return path, nil
	}
	if len(path) == 1 {
		return nil, errorf(expr.MakePath(path), "cannot reference raw table")
	}
	i.definite[path[1]] = struct{}{}
	return path[1:], nil
}

type IterTable struct {
	table
	// free is the set of free variables
	// within this trace that ostensibly
	// reference this table; they may actually
	// be correlated with a parent trace!
	free, definite map[string]struct{}

	Table       *expr.Table
	Schema      expr.Hint
	Index       Index
	Partitioned bool
}

func (i *IterTable) equals(x Step) bool {
	i2, ok := x.(*IterTable)
	return ok && (i == i2 || i.table.equals(&i2.table) &&
		maps.Equal(i.free, i2.free) &&
		i.Table.Equals(i2.Table) &&
		i.Schema == i2.Schema && // necessary?
		i.Index == i2.Index && // necessary?
		i.Partitioned == i2.Partitioned)
}

func (i *IterTable) rewrite(rw func(expr.Node, bool) expr.Node) {
	i.Table = rw(i.Table, false).(*expr.Table)
	if i.Filter != nil {
		i.Filter = rw(i.Filter, true)
	}
}

func (i *IterTable) timeRange(path []string) (min, max date.Time, ok bool) {
	if i.Index == nil {
		return date.Time{}, date.Time{}, false
	}
	return i.Index.TimeRange(path)
}

// Wildcard returns true if the table
// is referenced by the '*' operator
// (in other words, if all column bindings
// in the table are live at some point in the program)
func (i *IterTable) Wildcard() bool {
	return i.star
}

// Fields returns the fields belonging to
// the table that are actually referenced.
// Note that zero fields are returned if
// *either* the table is referenced with *
// or if no fields are actually referenced.
// Use IterTable.Wildcard to determine if the
// table is referenced with *
func (i *IterTable) Fields() []string {
	if i.star {
		return nil
	}
	all := append(maps.Keys(i.free), maps.Keys(i.definite)...)
	slices.Sort(all)
	return slices.Compact(all)
}

func (i *IterTable) get(x string) (Step, expr.Node) {
	if x == "*" {
		i.table.star = true
		return i, nil
	}
	result := i.Table.Result()
	if result != "" && result == x {
		return i, i.Table
	}
	return i, nil
}

// trim keeps only the fields listed on `used` list
func (i *IterTable) trim(used map[string]struct{}) {
	// 1. extend the used fields with ones from the Filter node
	if i.Filter != nil {
		collect := func(e expr.Node) {
			p, ok := e.(expr.Ident)
			if ok {
				used[string(p)] = struct{}{}
			}
		}
		expr.Walk(walkfn(collect), i.Filter)
	}

	// 2. filter out unused fields
	pred := func(s string, _ struct{}) bool {
		_, ok := used[s]
		return !ok
	}
	maps.DeleteFunc(i.definite, pred)
	maps.DeleteFunc(i.free, pred)
}

func (i *IterTable) describe(dst io.Writer) {
	prefix := "ITERATE"
	if i.Partitioned {
		prefix = "ITERATE PART"
	}
	fields := "*"
	if !i.star {
		fields = formatFields(i.Fields())
	}
	if i.Filter == nil {
		fmt.Fprintf(dst, "%s %s FIELDS %s\n", prefix, expr.ToString(i.Table), fields)
	} else {
		fmt.Fprintf(dst, "%s %s FIELDS %s WHERE %s\n", prefix, expr.ToString(i.Table), fields, expr.ToString(i.Filter))
	}
}

func (i *IterTable) parent() Step     { return nil }
func (i *IterTable) setparent(s Step) { panic("IterTable cannot set parent") }

type IterValue struct {
	parented
	Value  expr.Node // the expression to be iterated
	Result string    // the binding produced by iteration
}

func (i *IterValue) walk(v expr.Visitor) {
	expr.Walk(v, i.Value)
}

func (i *IterValue) equals(x Step) bool {
	i2, ok := x.(*IterValue)
	return ok && (i == i2 ||
		(expr.Equal(i.Value, i2.Value) && i.Result == i2.Result))
}

func (i *IterValue) describe(dst io.Writer) {
	fmt.Fprintf(dst, "ITERATE FIELD %s AS %s\n", expr.ToString(i.Value), i.Result)
}

func (i *IterValue) rewrite(rw func(expr.Node, bool) expr.Node) {
	i.Value = rw(i.Value, false)
}

type EquiFilter struct {
	Outer, Inner expr.Node
}

type EquiJoin struct {
	parented

	// build a SELECT for the rhs of the join
	// and delay evaluating it until we've done
	// some optimizations; we start with just
	//
	//   SELECT * FROM table x
	//
	// and then add predicate and dereference pushdown info
	// as it becomes availble
	built *expr.Select

	env Env

	// key is the computed inner key expression,
	// and value is the outer variable compared against it
	key, value expr.Node
}

func (e *EquiJoin) get(x string) (Step, expr.Node) {
	// explicit reference to a result of the join:
	if x == e.built.From.(*expr.Table).Result() {
		return e, e.built.From.(*expr.Table).Expr
	}
	return e.parent().get(x)
}

// push one part of a filter expression
func (e *EquiJoin) filterOne(node expr.Node, s *Trace) bool {
	self := e.built.From.(*expr.Table).Result()
	// base case: doesn't reference the join
	if doesNotReference(node, self) {
		push(&Filter{Where: node}, e.parent(), s)
		return true
	}
	// another base case: *only* references the join
	if onlyReferences(node, self) {
		// easy: just push this into the inner WHERE
		if e.built.Where == nil {
			e.built.Where = node
		} else {
			e.built.Where = expr.And(e.built.Where, node)
		}
		return true
	}
	return false
}

func (e *EquiJoin) describe(w io.Writer) {
	fmt.Fprintf(w, "EQUIJOIN ON %s = %s FROM %s\n",
		expr.ToString(e.key), expr.ToString(e.value), expr.ToString(e.built))
}

func (e *EquiJoin) equals(s Step) bool {
	e2, ok := s.(*EquiJoin)
	if !ok {
		return false
	}
	return e.built.Equals(e2.built) &&
		e.key.Equals(e2.key) &&
		e.value.Equals(e2.value)
}

func (e *EquiJoin) rewrite(rw func(expr.Node, bool) expr.Node) {
	// we're only rewriting the inner expressions;
	// the outer expressions are computed against
	// the sub-query and should *not* be rewritten
	// (they aren't part of this trace!)
	e.value = rw(e.value, false)
}

func (e *EquiJoin) walk(v expr.Visitor) {
	expr.Walk(v, e.value)
}

// pseudoTable exists as a shim during construction
// in order to allow the syntax
//
//	FROM (SELECT ...) x
//
// In the example above, we'd use name = "x" to ensure
// that references to e.g. x.foo are correctly stripped
// of the leading "x" and resolved against the preceding trace steps
type pseudoTable struct {
	parented
	noexprs
	name string
}

func (pt *pseudoTable) describe(w io.Writer) {
	fmt.Fprintf(w, "PSEUDOTABLE %s\n", pt.name)
}

// this is the reason pseudoTable exists:
// we know that a sub-SELECT has been bound
// to a variable, so we can strip the leading
// variable reference here to make sure it is
// resolved correctly
func (pt *pseudoTable) strip(p []string) ([]string, error) {
	if len(p) > 1 && p[0] == pt.name {
		return p[1:], nil
	}
	return p, nil
}

func (pt *pseudoTable) get(x string) (Step, expr.Node) {
	if x == pt.name {
		return pt, nil
	}
	return pt.parent().get(x)
}

func (pt *pseudoTable) equals(s Step) bool {
	p2, ok := s.(*pseudoTable)
	return ok && pt.name == p2.name
}

type parented struct {
	par Step
}

func (p *parented) parent() Step     { return p.par }
func (p *parented) setparent(s Step) { p.par = s }

// default behavior for get() for parented nodes
func (p *parented) get(x string) (Step, expr.Node) {
	return p.par.get(x)
}

type binds struct {
	bind []expr.Binding
}

func (b *binds) walk(v expr.Visitor) {
	for i := range b.bind {
		expr.Walk(v, b.bind[i].Expr)
	}
}

func (b *binds) equal(b2 *binds) bool {
	return slices.EqualFunc(b.bind, b2.bind, expr.Binding.Equals)
}

type noexprs struct{}

func (n *noexprs) walk(_ expr.Visitor)                     {}
func (n *noexprs) rewrite(func(expr.Node, bool) expr.Node) {}

func (i *IterValue) get(x string) (Step, expr.Node) {
	if x == i.Result {
		return i, i.Value
	}
	return i.par.get(x)
}

type Step interface {
	parent() Step
	setparent(Step)
	get(string) (Step, expr.Node)
	describe(dst io.Writer)
	rewrite(func(expr.Node, bool) expr.Node)
	walk(expr.Visitor)
	equals(Step) bool
}

// Input returns the input to a Step
func Input(s Step) Step {
	return s.parent()
}

// UnionMap represents a terminal
// query Step that unions the results
// of parallel invocations of the Child
// subquery operation.
type UnionMap struct {
	// Inner is the table that needs
	// to be partitioned into the child
	// subquery.
	Inner *IterTable
	// Child is the sub-query that is
	// applied to the partitioned table.
	Child *Trace
	noexprs
}

func (u *UnionMap) parent() Step   { return nil }
func (u *UnionMap) setparent(Step) { panic("cannot UnionMap.setparent()") }

func (u *UnionMap) equals(x Step) bool {
	u2, ok := x.(*UnionMap)
	return ok && (u == u2 || u.Inner.equals(u2.Inner) &&
		u.Child.Equals(u2.Child))
}

func (u *UnionMap) get(x string) (Step, expr.Node) {
	results := u.Child.FinalBindings()
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].Result() == x {
			return u, results[i].Expr
		}
	}
	return nil, nil
}

func (u *UnionMap) describe(dst io.Writer) {
	var buf bytes.Buffer
	u.Child.Describe(&buf)
	inner := buf.Bytes()
	if inner[len(inner)-1] == '\n' {
		inner = inner[:len(inner)-1]
	}
	inner = bytes.ReplaceAll(inner, []byte{'\n'}, []byte{'\n', '\t'})
	io.WriteString(dst, "UNION MAP ")
	io.WriteString(dst, expr.ToString(u.Inner.Table))
	io.WriteString(dst, " (\n\t")
	dst.Write(inner)
	io.WriteString(dst, ")\n")
}

type Filter struct {
	parented
	Where expr.Node
}

func (f *Filter) equals(x Step) bool {
	f2, ok := x.(*Filter)
	return ok && (f == f2 || expr.Equal(f.Where, f2.Where))
}

func (f *Filter) rewrite(rw func(expr.Node, bool) expr.Node) {
	f.Where = rw(f.Where, true)
}

func (f *Filter) walk(v expr.Visitor) {
	expr.Walk(v, f.Where)
}

func (f *Filter) describe(dst io.Writer) {
	fmt.Fprintf(dst, "FILTER %s\n", expr.ToString(f.Where))
}

type Distinct struct {
	parented
	Columns []expr.Node
}

func formatFields(fields []string) string {
	return "[" + strings.Join(fields, ", ") + "]"
}

func toStrings(in []expr.Node) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = expr.ToString(in[i])
	}
	return out
}

func (d *Distinct) equals(x Step) bool {
	d2, ok := x.(*Distinct)
	return ok && (d == d2 ||
		slices.EqualFunc(d.Columns, d2.Columns, expr.Node.Equals))
}

func (d *Distinct) describe(dst io.Writer) {
	fmt.Fprintf(dst, "FILTER DISTINCT %s\n", formatFields(toStrings(d.Columns)))
}

func (d *Distinct) rewrite(rw func(expr.Node, bool) expr.Node) {
	for i := range d.Columns {
		d.Columns[i] = rw(d.Columns[i], false)
	}
}

func (d *Distinct) walk(v expr.Visitor) {
	for i := range d.Columns {
		expr.Walk(v, d.Columns[i])
	}
}

func (d *Distinct) clone() *Distinct {
	return &Distinct{Columns: d.Columns}
}

func (b *Bind) Bindings() []expr.Binding {
	return b.bind
}

// Bind is a collection of transformations
// that produce a set of output bindings from
// the current binding environment
type Bind struct {
	parented
	binds
	complete bool
	star     bool // referenced by '*'
}

func (b *Bind) equals(x Step) bool {
	b2, ok := x.(*Bind)
	return ok && (b == b2 || b.binds.equal(&b2.binds) &&
		b.complete == b2.complete &&
		b.star == b2.star)
}

func (b *Bind) rewrite(rw func(expr.Node, bool) expr.Node) {
	for i := range b.bind {
		b.bind[i].Expr = rw(b.bind[i].Expr, false)
	}
}

func (b *Bind) describe(dst io.Writer) {
	io.WriteString(dst, "PROJECT ")
	for i := range b.binds.bind {
		if i != 0 {
			io.WriteString(dst, ", ")
		}
		io.WriteString(dst, expr.ToString(&b.binds.bind[i]))
	}
	io.WriteString(dst, "\n")
}

func (b *Bind) get(x string) (Step, expr.Node) {
	if x == "*" {
		b.star = true
		return b, nil
	}
	for i := len(b.bind) - 1; i >= 0; i-- {
		if b.bind[i].Result() == x {
			return b, b.bind[i].Expr
		}
	}
	if !b.complete {
		return b.parent().get(x)
	}
	return nil, nil
}

// Aggregate is an aggregation operation
// that produces a new set of bindings
type Aggregate struct {
	parented
	Agg vm.Aggregation
	// GroupBy is nil for normal
	// aggregations, or non-nil
	// when the aggregation is formed
	// on multiple columns
	//
	// note that the groups form part
	// of the binding set
	GroupBy []expr.Binding

	complete bool
}

func (a *Aggregate) equals(x Step) bool {
	a2, ok := x.(*Aggregate)
	return ok && (a == a2 || a.Agg.Equals(a2.Agg) &&
		slices.EqualFunc(a.GroupBy, a2.GroupBy, expr.Binding.Equals) &&
		a.complete == a2.complete)
}

func (a *Aggregate) describe(dst io.Writer) {
	if a.GroupBy == nil {
		fmt.Fprintf(dst, "AGGREGATE %s\n", a.Agg)
	} else {
		fmt.Fprintf(dst, "AGGREGATE %s BY %s\n", a.Agg, vm.Selection(a.GroupBy))
	}
}

func (a *Aggregate) rewrite(rw func(expr.Node, bool) expr.Node) {
	for i := range a.Agg {
		a.Agg[i].Expr = rw(a.Agg[i].Expr, false).(*expr.Aggregate)
	}
	for i := range a.GroupBy {
		a.GroupBy[i].Expr = rw(a.GroupBy[i].Expr, false)
	}
}

func (a *Aggregate) walk(v expr.Visitor) {
	for i := range a.Agg {
		expr.Walk(v, a.Agg[i].Expr)
	}
	for i := range a.GroupBy {
		expr.Walk(v, a.GroupBy[i].Expr)
	}
}

func (a *Aggregate) get(x string) (Step, expr.Node) {
	for i := len(a.Agg) - 1; i >= 0; i-- {
		if a.Agg[i].Result == x {
			return a, a.Agg[i].Expr
		}
	}
	for i := len(a.GroupBy) - 1; i >= 0; i-- {
		if a.GroupBy[i].Result() == x {
			return a, a.GroupBy[i].Expr
		}
	}
	if !a.complete {
		return a.parent().get(x)
	}
	// aggregates do not preserve the input binding set
	return nil, nil
}

type Order struct {
	parented
	Columns []expr.Order
}

func (o *Order) equals(x Step) bool {
	o2, ok := x.(*Order)
	return ok && (o == o2 ||
		slices.EqualFunc(o.Columns, o2.Columns, expr.Order.Equals))
}

func (o *Order) clone() *Order {
	return &Order{Columns: o.Columns}
}

func (o *Order) describe(dst io.Writer) {
	io.WriteString(dst, "ORDER BY ")
	for i := range o.Columns {
		if i != 0 {
			io.WriteString(dst, ", ")
		}
		io.WriteString(dst, expr.ToString(&o.Columns[i]))
	}
	io.WriteString(dst, "\n")
}

func (o *Order) rewrite(rw func(expr.Node, bool) expr.Node) {
	for i := range o.Columns {
		o.Columns[i].Column = rw(o.Columns[i].Column, false)
	}
}

func (o *Order) walk(v expr.Visitor) {
	for i := range o.Columns {
		expr.Walk(v, o.Columns[i].Column)
	}
}

type Limit struct {
	parented
	noexprs
	Count  int64
	Offset int64
}

func (l *Limit) equals(x Step) bool {
	l2, ok := x.(*Limit)
	return ok && (l == l2 || l.Count == l2.Count &&
		l.Offset == l2.Offset)
}

func (l *Limit) describe(dst io.Writer) {
	if l.Offset == 0 {
		fmt.Fprintf(dst, "LIMIT %d\n", l.Count)
		return
	}
	fmt.Fprintf(dst, "LIMIT %d OFFSET %d\n", l.Count, l.Offset)
}

// OutputPart writes output rows into
// a single part and returns a row like
//
//	{"part": "path/to/packed-XXXXX.ion.zst"}
type OutputPart struct {
	Basename string
	parented
	noexprs
}

func (o *OutputPart) equals(x Step) bool {
	o2, ok := x.(*OutputPart)
	return ok && (o == o2 || o.Basename == o2.Basename)
}

func (o *OutputPart) describe(dst io.Writer) {
	fmt.Fprintf(dst, "OUTPUT PART %s\n", o.Basename)
}

func (o *OutputPart) get(x string) (Step, expr.Node) {
	if x == "part" {
		return o, nil
	}
	// NOTE: this would be problematic
	// if Output* nodes were inserted
	// before optimization, as the input
	// fields wouldn't be marked as live
	return nil, nil
}

// OutputIndex is a step that takes the "part" field
// of incoming rows and constructs an index out of them,
// returning a single row like
//
//	{"table": "db.table-XXXXXX"}
type OutputIndex struct {
	Table    expr.Node
	Basename string
	parented
	noexprs
}

func (o *OutputIndex) equals(x Step) bool {
	o2, ok := x.(*OutputIndex)
	return ok && (o == o2 || o.Table.Equals(o2.Table) &&
		o.Basename == o2.Basename)
}

func (o *OutputIndex) describe(dst io.Writer) {
	fmt.Fprintf(dst, "OUTPUT INDEX %s AT %s\n", expr.ToString(o.Table), o.Basename)
}

func (o *OutputIndex) get(x string) (Step, expr.Node) {
	if x == "table_name" {
		// the String("") here is just to provide a type hint
		return o, expr.String("")
	}
	// see comment in OutputPart.get
	return nil, nil
}

// NoOutput is a dummy input of 0 rows.
type NoOutput struct{}

func (n NoOutput) equals(x Step) bool {
	_, ok := x.(NoOutput)
	return ok
}

func (n NoOutput) describe(dst io.Writer) {
	io.WriteString(dst, "NO OUTPUT\n")
}

func (n NoOutput) rewrite(func(expr.Node, bool) expr.Node) {}

func (n NoOutput) get(x string) (Step, expr.Node) { return nil, nil }

func (n NoOutput) parent() Step { return nil }

func (n NoOutput) setparent(Step) { panic("NoOutput.setparent") }

func (n NoOutput) walk(expr.Visitor) {}

// DummyOutput is a dummy input of one record, {}
type DummyOutput struct{}

func (d DummyOutput) equals(x Step) bool {
	_, ok := x.(DummyOutput)
	return ok
}

func (d DummyOutput) rewrite(func(expr.Node, bool) expr.Node) {}
func (d DummyOutput) walk(expr.Visitor)                       {}
func (d DummyOutput) get(x string) (Step, expr.Node)          { return nil, nil }
func (d DummyOutput) parent() Step                            { return nil }
func (d DummyOutput) setparent(Step)                          { panic("DummyOutput.setparent") }

func (d DummyOutput) describe(dst io.Writer) {
	io.WriteString(dst, "[{}]\n")
}

func (l *Limit) clone() *Limit {
	return &Limit{Count: l.Count, Offset: l.Offset}
}

// Trace is a linear sequence
// of physical plan operators.
// Traces are arranged in a tree,
// where each Trace depends on the
// execution of zero or more children
// (see Inputs).
type Trace struct {
	// If this trace is not a root trace,
	// then Parent will be a trace that
	// has this trace as one of its inputs.
	Parent *Trace
	// Replacements are traces that produce
	// results that are necessary in order
	// to execute this trace.
	// The results of input traces
	// are available to this trace
	// through the SCALAR_REPLACEMENT(index)
	// and IN_REPLACEMENT(index) expressions.
	// The traces in Input may be executed
	// in any order.
	Replacements []*Trace

	prcache *pathRewriter

	top Step
	cur Step

	// final is the most recent
	// complete set of bindings
	// produced by an expression
	final      []expr.Binding
	finalTypes []expr.TypeSet
}

// Equals returns true if b and x would produce the same
// rows and thus can be substituted for one another.
func (b *Trace) Equals(x *Trace) bool {
	return b == x || stepsEqual(b.top, x.top) &&
		slices.EqualFunc(b.final, x.final, expr.Binding.Equals) &&
		slices.EqualFunc(b.Replacements, x.Replacements, (*Trace).Equals)
}

func stepsEqual(a, b Step) bool {
	for {
		if a == nil || b == nil {
			return a == nil && b == nil
		}
		if !a.equals(b) {
			return false
		}
		a, b = a.parent(), b.parent()
	}
}

func (b *Trace) Begin(f *expr.Table, e Env) error {
	it := &IterTable{Table: f}
	it.definite = make(map[string]struct{})
	it.free = make(map[string]struct{})
	it.haveParent = b.Parent != nil
	if f.Explicit() {
		it.Bind = f.Result()
	}
	if e != nil {
		it.Schema = e.Schema(f.Expr)
		idx, err := e.Index(f.Expr)
		if err != nil {
			return err
		}
		it.Index = idx
	}
	b.top = it
	return nil
}

func (b *Trace) beginUnionMap(src *Trace, table *IterTable) {
	// we know that the result of a
	// parallelized query ought to be
	// the same as a non-parallelized query,
	// so we can populate the binding information
	// immediately
	infinal := src.FinalBindings()
	final := make([]expr.Binding, len(infinal))
	copy(final, infinal)
	table.Partitioned = true
	b.top = &UnionMap{Child: src, Inner: table}
	b.final = final
}

func (b *Trace) push() error {
	b.cur.setparent(b.top)
	b.top, b.cur = b.cur, nil
	return nil
}

func (b *Trace) rewriter() *pathRewriter {
	r := b.prcache
	b.prcache = nil
	if r == nil {
		r = &pathRewriter{}
	}
	r.err = r.err[:0]
	r.cur = b.cur
	return r
}

func (b *Trace) pathwalk(e expr.Node) (expr.Node, error) {
	pr := b.rewriter()
	ret := expr.Rewrite(pr, e)
	b.prcache = pr
	return ret, pr.combine()
}

// Where pushes a filter to the expression stack
func (b *Trace) Where(e expr.Node) error {
	f := &Filter{Where: e}
	f.setparent(b.top)
	b.cur = f
	e, err := b.pathwalk(e)
	if err != nil {
		return err
	}
	f.Where = e
	if err := check(b.top, e); err != nil {
		return err
	}
	if err := checkNoAggregateInCondition(e, "WHERE"); err != nil {
		return err
	}
	return b.push()
}

// Iterate pushes an implicit iteration to the stack
func (b *Trace) Iterate(bind *expr.Binding) error {
	iv := &IterValue{Value: bind.Expr}
	iv.Result = bind.Result()
	// walk with the current scope
	// set to the parent scope; we don't
	// introduce any bindings here that
	// are visible within this node itself
	b.cur = b.top
	val, err := b.pathwalk(iv.Value)
	if err != nil {
		return err
	}
	iv.Value = val
	b.cur = iv
	b.final = append(b.final, *bind)
	return b.push()
}

// Distinct takes a sets of bindings
// and produces only distinct sets of output tuples
// of the given variable bindings
func (b *Trace) Distinct(exprs []expr.Node) error {
	b.cur = b.top
	di := &Distinct{}
	for i := range exprs {
		if exp, err := b.pathwalk(exprs[i]); err != nil {
			return err
		} else {
			di.Columns = append(di.Columns, exp)
		}
	}

	if err := b.checkExpressions(exprs); err != nil {
		return err
	}

	b.cur = di
	return b.push()
}

func (b *Trace) DistinctFromBindings(bind []expr.Binding) error {
	exprs := make([]expr.Node, len(bind))
	for i := range bind {
		exprs[i] = bind[i].Expr
	}

	return b.Distinct(exprs)
}

func (b *Trace) BindStar() error {
	b.top.get("*")
	return nil
}

// Bind pushes a set of variable bindings to the stack
func (b *Trace) Bind(bindings ...[]expr.Binding) error {
	bi := &Bind{complete: false}
	bi.setparent(b.top)
	b.cur = bi

	// walk for each binding introduced,
	// then add it to the current binding set
	for _, bind := range bindings {
		for i := range bind {
			if exp, err := b.pathwalk(bind[i].Expr); err != nil {
				return err
			} else {
				bind[i].Expr = exp
				bi.bind = append(bi.bind, bind[i])
			}
		}
	}
	for i := range bi.bind {
		if err := check(b.top, bi.bind[i].Expr); err != nil {
			return err
		}
	}
	// clobber the current binding set
	bi.complete = true
	b.final = bi.bind
	return b.push()
}

// Aggregate pushes an aggregation to the stack
func (b *Trace) Aggregate(agg vm.Aggregation, groups []expr.Binding) error {
	ag := &Aggregate{}
	ag.setparent(b.top)
	ag.complete = false
	b.cur = ag
	var bind []expr.Binding
	for i := range groups {
		exp, err := b.pathwalk(groups[i].Expr)
		if err != nil {
			return err
		}
		groups[i].Expr = exp
		ag.GroupBy = append(ag.GroupBy, groups[i])
		bind = append(bind, groups[i])
	}
	for i := range agg {
		exp, err := b.pathwalk(agg[i].Expr)
		if err != nil {
			return err
		}
		agg[i].Expr = exp.(*expr.Aggregate)
		ag.Agg = append(ag.Agg, agg[i])
		bind = append(bind, expr.Bind(agg[i].Expr, agg[i].Result))
	}

	isExisting := func(e expr.Node) bool {
		for i := range bind {
			if e == expr.Ident(bind[i].Result()) ||
				bind[i].Expr.Equals(e) {
				return true
			}
		}
		return false
	}

	for i := range agg {
		if err := check(b.top, ag.Agg[i].Expr); err != nil {
			return err
		}
		// implementation restriction:
		// force PARTITION BY / ORDER BY cols inside OVER
		// to match an existing binding outside the aggregate
		//
		// (we can relax this constraint later with some additional pain)
		if wind := ag.Agg[i].Expr.Over; wind != nil {
			if len(groups) == 0 {
				return fmt.Errorf("window function disallowed without GROUP BY: %s", expr.ToString(ag.Agg[i].Expr))
			}
			for j := range wind.PartitionBy {
				if !isExisting(wind.PartitionBy[j]) {
					return fmt.Errorf("PARTITION BY %s is not bound outside the window", expr.ToString(wind.PartitionBy[j]))
				}
			}
			for j := range wind.OrderBy {
				if !isExisting(wind.OrderBy[j].Column) {
					return fmt.Errorf("ORDER BY %s in window is not also bound outside the window", expr.ToString(wind.OrderBy[j].Column))
				}
			}
		}
	}
	ag.complete = true
	b.final = bind
	return b.push()
}

// Order pushes an ordering to the stack
func (b *Trace) Order(cols []expr.Order) error {
	// ... now the variable references should be correct
	b.cur = b.top
	for i := range cols {
		if col, err := b.pathwalk(cols[i].Column); err != nil {
			return err
		} else {
			cols[i].Column = col
		}
	}
	b.cur = &Order{Columns: cols}
	return b.push()
}

// LimitOffset pushes a limit operation to the stack
func (b *Trace) LimitOffset(limit, offset int64) error {
	l := &Limit{Count: limit, Offset: offset}
	// no walking here because
	// Limit doesnt include any
	// meaningful expressions
	b.cur = l
	return b.push()
}

func (b *Trace) innerJoin(bind *expr.Binding, on expr.Node, env Env) error {
	cmp, ok := on.(*expr.OnEquals)
	if !ok {
		return fmt.Errorf("ON must be an equality condition; have %s", expr.ToString(on))
	}
	self := bind.Result()
	var key, value expr.Node
	if onlyReferences(cmp.Left, self) && doesNotReference(cmp.Right, self) {
		key = cmp.Left
		value = cmp.Right
	} else if onlyReferences(cmp.Right, self) && doesNotReference(cmp.Left, self) {
		key = cmp.Right
		value = cmp.Left
	} else {
		return fmt.Errorf("cannot disambiguate JOIN ... ON condition")
	}
	eq := &EquiJoin{
		built: &expr.Select{
			Columns: []expr.Binding{expr.Bind(key, "$__key")},
			From:    &expr.Table{Binding: *bind},
		},
		env: env,
		key: key,
	}
	eq.setparent(b.top)
	b.cur = eq
	// check the inner condition (the one that references the outer table(s))
	// against the existing trace
	value, err := b.pathwalk(value)
	if err != nil {
		return err
	}
	eq.value = value
	if err := check(b.top, value); err != nil {
		return err
	}
	return b.push()
}

// Into handles the INTO clause by pushing
// the appropriate OutputIndex and OutputPart nodes.
func (b *Trace) Into(table expr.Node, basepath string) {
	op := &OutputPart{Basename: basepath}
	op.setparent(b.top)
	oi := &OutputIndex{
		Table:    table,
		Basename: basepath,
	}
	oi.setparent(op)
	b.top = oi
	result := expr.String(path.Base(basepath))
	final := expr.Bind(result, "table_name")
	b.final = []expr.Binding{final}
}

// FinalBindings returns the set of output bindings,
// or none if they could not be computed
func (b *Trace) FinalBindings() []expr.Binding {
	return b.final
}

// FinalTypes returns the computed ion type sets
// of the output bindings. Each element of the
// returned slice corresponds with the same index
// in the return value of Builder.FinalBindings
//
// Note that the return value may be nil if the
// query does not produce a know (finite) result-set
func (b *Trace) FinalTypes() []expr.TypeSet {
	if b.finalTypes != nil {
		return b.finalTypes
	}
	hint := &stepHint{b.top}
	out := make([]expr.TypeSet, len(b.final))
	for i := range b.final {
		out[i] = expr.TypeOf(expr.Identifier(b.final[i].Result()), hint)
	}
	b.finalTypes = out
	return out
}

// Final returns the final step of the query.
// The caller can use Input to walk the inputs
// to each step up to the first input step.
func (b *Trace) Final() Step {
	return b.top
}

// String implements fmt.Stringer.
func (b *Trace) String() string {
	if b == nil {
		return "<nil>"
	}
	var sb strings.Builder
	b.Describe(&sb)
	return sb.String()
}

// Describe writes a plain-text representation
// of b to dst. The output of this representation
// is purely for diagnostic purposes; it cannot
// be deserialized back into a trace.
func (b *Trace) Describe(dst io.Writer) {
	var tmp bytes.Buffer
	for i := range b.Replacements {
		io.WriteString(dst, "WITH (\n\t")
		tmp.Reset()
		b.Replacements[i].Describe(&tmp)
		inner := bytes.ReplaceAll(tmp.Bytes(), []byte{'\n'}, []byte{'\n', '\t'})
		inner = inner[:len(inner)-1] // chomp \t on last entry
		dst.Write(inner)
		fmt.Fprintf(dst, ") AS REPLACEMENT(%d)\n", i)
	}
	var describe func(s Step)
	describe = func(s Step) {
		if p := s.parent(); p != nil {
			describe(p)
		}
		s.describe(dst)
	}
	describe(b.top)
}

// conjunctions returns the list of top-level
// conjunctions from a logical expression
// by appending the results to 'lst'
//
// this is used for predicate pushdown so that
//
//	<a> AND <b> AND <c>
//
// can be split and evaluated as early as possible
// in the query-processing pipeline
func conjunctions(e expr.Node, lst []expr.Node) []expr.Node {
	a, ok := e.(*expr.Logical)
	if !ok || a.Op != expr.OpAnd {
		return append(lst, e)
	}
	return conjunctions(a.Left, conjunctions(a.Right, lst))
}

// conjoinAll does the inverse of conjunctions,
// returning the given expressions joined with AND.
//
// NOTE: conjunctions(x AND y AND z) returns [z, x, y],
// so conjoinAll(x, y, z) returns "z AND y AND x".
func conjoinAll(x []expr.Node, scope *Trace, whence Step) expr.Node {
	var node expr.Node
	for i := range x {
		node = conjoin(x[i], node, scope, whence)
	}
	if node != nil {
		node = expr.SimplifyLogic(node, &stepHint{whence.parent()})
	}
	return node
}

func (b *Trace) Rewrite(rw expr.Rewriter) {
	inner := func(e expr.Node, _ bool) expr.Node {
		return expr.Rewrite(rw, e)
	}
	for cur := b.top; cur != nil; cur = cur.parent() {
		cur.rewrite(inner)
	}
}

// Unpivot represents the UNPIVOT expr AS as AT at statement
type Unpivot struct {
	parented
	Ast *expr.Unpivot // The AST node this node was constructed from
	noexprs
}

func (u *Unpivot) get(x string) (Step, expr.Node) {
	if (u.Ast.As != nil) && (*u.Ast.As == x) {
		return u, u.Ast
	} else if (u.Ast.At != nil) && (*u.Ast.At == x) {
		return u, u.Ast
	}
	// Binding cannot be resolved
	return nil, nil
}

func (u *Unpivot) describe(dst io.Writer) {
	io.WriteString(dst, "UNPIVOT")
	if u.Ast.As != nil {
		fmt.Fprintf(dst, " AS %s", *u.Ast.As)
	}
	if u.Ast.At != nil {
		fmt.Fprintf(dst, " AT %s", *u.Ast.At)
	}
	io.WriteString(dst, "\n")
}

func (u *Unpivot) equals(brhs Step) bool {
	lhs := u
	rhs, ok := brhs.(*Unpivot)
	if !ok {
		return false
	}
	return lhs.Ast.Equals(rhs.Ast)
}

// UnpivotAtDistinct represents the UNPIVOT expr AT x GROUP BY x statement
type UnpivotAtDistinct struct {
	parented
	Ast *expr.Unpivot // The AST node this node was constructed from
	noexprs
}

func (u *UnpivotAtDistinct) get(x string) (Step, expr.Node) {
	if *u.Ast.At == x {
		return u, u.Ast
	}
	// Binding cannot be resolved
	return nil, nil
}

func (u *UnpivotAtDistinct) describe(dst io.Writer) {
	fmt.Fprintf(dst, "UNPIVOT_AT_DISTINCT %s\n", *u.Ast.At)
}

func (u *UnpivotAtDistinct) rewrite(rw func(expr.Node, bool) expr.Node) {}

func (u *UnpivotAtDistinct) equals(brhs Step) bool {
	lhs := u
	rhs, ok := brhs.(*UnpivotAtDistinct)
	if !ok {
		return false
	}
	return *lhs.Ast.At == *rhs.Ast.At
}
