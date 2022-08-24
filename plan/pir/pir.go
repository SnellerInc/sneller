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
	refs []*expr.Path
	// free variable references
	outer      []string
	Filter     expr.Node
	Bind       string
	star       bool // this table is referenced via '*'
	haveParent bool // free variable references are allowed
}

func (t *table) equals(x *table) bool {
	peq := (*expr.Path).EqualsPath
	return t == x || slices.EqualFunc(t.refs, x.refs, peq) &&
		slices.Equal(t.outer, x.outer) &&
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
func (t *table) strip(p *expr.Path) error {
	if t.Bind == "" {
		return nil
	} else if p.First != t.Bind {
		if !t.haveParent || p.Rest != nil {
			return errorf(p, "reference to undefined variable %q", p)
		}
		// this is *definitely* a free variable
		t.outer = append(t.outer, p.First)
		return nil
	}
	d, ok := p.Rest.(*expr.Dot)
	if !ok {
		return errorf(p, "cannot compute %s on table %s", p.Rest, t.Bind)
	}
	p.First = d.Field
	p.Rest = d.Rest
	t.refs = append(t.refs, p)
	return nil
}

type IterTable struct {
	table
	// free is the set of free variables
	// within this trace that ostensibly
	// reference this table; they may actually
	// be correlated with a parent trace!
	free     []string
	definite []string

	Table       *expr.Table
	Schema      expr.Hint
	Index       Index
	Partitioned bool
}

func (i *IterTable) equals(x Step) bool {
	i2, ok := x.(*IterTable)
	return ok && (i == i2 || i.table.equals(&i2.table) &&
		slices.Equal(i.free, i2.free) &&
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

func (i *IterTable) timeRange(p *expr.Path) (min, max date.Time, ok bool) {
	if i.Index == nil {
		return date.Time{}, date.Time{}, false
	}
	return i.Index.TimeRange(p)
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
// Use IterTable.Wilcard to determine if the
// table is referenced with *
func (i *IterTable) Fields() []string {
	if i.star {
		return nil
	}
	all := append(i.free[:len(i.free):len(i.free)], i.definite...)
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
	i.free = append(i.free, x)
	return i, nil
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
	table
	Value expr.Node // the expression to be iterated

	liveat     []expr.Binding
	liveacross []expr.Binding
}

func (i *IterValue) walk(v expr.Visitor) {
	expr.Walk(v, i.Value)
	i.table.walk(v)
}

func bindstr(bind []expr.Binding) string {
	var out strings.Builder
	for i := range bind {
		if i != 0 {
			out.WriteString(", ")
		}
		out.WriteString(expr.ToString(&bind[i]))
	}
	return out.String()
}

func (i *IterValue) equals(x Step) bool {
	i2, ok := x.(*IterValue)
	return ok && (i == i2 || i.table.equals(&i2.table) &&
		expr.Equal(i.Value, i2.Value) &&
		slices.EqualFunc(i.liveat, i2.liveat, expr.Binding.Equals) &&
		slices.EqualFunc(i.liveacross, i2.liveacross, expr.Binding.Equals))
}

func (i *IterValue) describe(dst io.Writer) {
	if i.Filter == nil {
		fmt.Fprintf(dst, "ITERATE FIELD %s (ref: [%s], live: [%s])\n", expr.ToString(i.Value), bindstr(i.liveat), bindstr(i.liveacross))
	} else {
		fmt.Fprintf(dst, "ITERATE FIELD %s WHERE %s (ref: [%v], live: [%v])\n", expr.ToString(i.Value), expr.ToString(i.Filter), bindstr(i.liveat), bindstr(i.liveacross))
	}
}

func (i *IterValue) rewrite(rw func(expr.Node, bool) expr.Node) {
	i.Value = rw(i.Value, false)
	if i.Filter != nil {
		i.Filter = rw(i.Filter, true)
	}
}

// Wildcard returns whether the value is referenced
// via the '*' operator
// (see also: IterTable.Wildcard)
func (i *IterValue) Wildcard() bool {
	return i.star
}

// References returns the references to this value
func (i *IterValue) References() []*expr.Path {
	return i.refs
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
	if x == "*" {
		i.table.star = true
		// don't return; the '*'
		// captures the upstream values
		// as well...
	} else if x == i.Bind {
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
	inner = bytes.Replace(inner, []byte{'\n'}, []byte{'\n', '\t'}, -1)
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
	Table    *expr.Path
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
		return o, nil
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

func (o NoOutput) walk(expr.Visitor) {}

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

	top   Step
	cur   Step
	scope map[*expr.Path]scopeinfo
	err   []error

	// final is the most recent
	// complete set of bindings
	// produced by an expression
	final []expr.Binding
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
	b.scope = src.scope
	table.Partitioned = true
	b.top = &UnionMap{Child: src, Inner: table}
	b.final = final
}

func (b *Trace) push() error {
	if b.err != nil {
		return b.combine()
	}
	b.cur.setparent(b.top)
	b.top, b.cur = b.cur, nil
	return nil
}

// Where pushes a filter to the expression stack
func (b *Trace) Where(e expr.Node) error {
	f := &Filter{Where: e}
	f.setparent(b.top)
	b.cur = f
	expr.Walk(b, e)
	if err := b.Check(e); err != nil {
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
	iv.Bind = bind.Result()
	// walk with the current scope
	// set to the parent scope; we don't
	// introduce any bindings here that
	// are visible within this node itself
	b.cur = b.top
	expr.Walk(b, iv.Value)
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
		expr.Walk(b, exprs[i])
		if b.err != nil {
			return b.combine()
		}
		di.Columns = append(di.Columns, exprs[i])
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
			expr.Walk(b, bind[i].Expr)
			if b.err != nil {
				return b.combine()
			}
			bi.bind = append(bi.bind, bind[i])
		}
	}
	for i := range bi.bind {
		if err := b.Check(bi.bind[i].Expr); err != nil {
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
		expr.Walk(b, groups[i].Expr)
		if b.err != nil {
			return b.combine()
		}
		ag.GroupBy = append(ag.GroupBy, groups[i])
		bind = append(bind, groups[i])
	}
	for i := range agg {
		expr.Walk(b, agg[i].Expr)
		ag.Agg = append(ag.Agg, agg[i])
		bind = append(bind, expr.Bind(agg[i].Expr, agg[i].Result))
	}
	for i := range agg {
		if err := b.Check(ag.Agg[i].Expr); err != nil {
			return err
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
		expr.Walk(b, cols[i].Column)
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

// Into handles the INTO clause by pushing
// the appropriate OutputIndex and OutputPart nodes.
func (b *Trace) Into(table *expr.Path, basepath string) {
	op := &OutputPart{Basename: basepath}
	op.setparent(b.top)
	b.add(expr.Identifier("part"), op, nil)
	oi := &OutputIndex{
		Table:    table,
		Basename: basepath,
	}
	oi.setparent(op)
	b.top = oi
	tblname := expr.Identifier("table_name")
	result := expr.String(path.Base(basepath))
	b.add(tblname, oi, result)
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
	out := make([]expr.TypeSet, len(b.final))
	for i := range b.final {
		out[i] = b.TypeOf(b.final[i].Expr)
	}
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
		inner := bytes.Replace(tmp.Bytes(), []byte{'\n'}, []byte{'\n', '\t'}, -1)
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
func conjoinAll(x []expr.Node, scope *Trace) expr.Node {
	var node expr.Node
	for i := range x {
		node = conjoin(x[i], node, scope)
	}
	if node != nil {
		node = expr.SimplifyLogic(node, scope)
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
