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
	"io"
	"path"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
)

// CompileError is an error associated
// with compiling a particular expression.
type CompileError struct {
	In  expr.Node
	Err string
}

// Error implements error
func (c *CompileError) Error() string { return c.Err }

// WriteTo implements io.WriterTo
//
// WriteTo writes a plaintext representation
// of the error to dst, including the expression
// associated with the error.
func (c *CompileError) WriteTo(dst io.Writer) (int64, error) {
	if c.In == nil {
		i, err := fmt.Fprintf(dst, "%s\n", c.Err)
		return int64(i), err
	}
	i, err := fmt.Fprintf(dst, "in expression:\n\t%s\n%s\n", expr.ToString(c.In), c.Err)
	return int64(i), err
}

func errorf(e expr.Node, f string, args ...interface{}) error {
	return &CompileError{
		In:  e,
		Err: fmt.Sprintf(f, args...),
	}
}

func (b *Trace) walkFrom(f expr.From, e Env) error {
	if f == nil {
		b.top = DummyOutput{}
		return nil
	}
	switch f := f.(type) {
	case *expr.Join:
		return b.walkFromJoin(f, e)
	case *expr.Table:
		return b.walkFromTable(f, e)
	default:
		return errorf(f, "unexpected expression %q", f)
	}
}

func (b *Trace) walkFromTable(f *expr.Table, e Env) error {
	switch s := f.Expr.(type) {
	case *expr.Select:
		err := b.walkSelect(s, e)
		if err != nil {
			return err
		}
		// make sure we strip psuedo-table bindings correctly
		if f.Binding.Explicit() {
			pt := &pseudoTable{name: f.Binding.Result()}
			pt.setparent(b.top)
			b.top = pt
		}
		return nil
	case *expr.Unpivot:
		return b.buildUnpivot(s, e)
	default:
		return b.Begin(f, e)
	}
}

func (b *Trace) walkFromJoin(f *expr.Join, e Env) error {
	err := b.walkFrom(f.Left, e)
	if err != nil {
		return err
	}
	switch f.Kind {
	case expr.CrossJoin:
		// FIXME: if the rhs expression is a SELECT,
		// then this is almost certainly a correlated
		// sub-query ...
		return b.Iterate(&f.Right)
	case expr.InnerJoin:
		return b.innerJoin(&f.Right, f.On, e)
	default:
		return errorf(f, "join %q not yet supported", f.Kind)
	}
}

// walk a list of bindings and determine if
// any of the bindings includes an aggregate
// expression
func anyHasAggregate(lst []expr.Binding) bool {
	return matchAny(lst, func(b *expr.Binding) bool {
		return hasAggregate(b.Expr)
	})
}

func anyOrderHasAggregate(lst []expr.Order) bool {
	return matchAny(lst, func(o *expr.Order) bool {
		return hasAggregate(o.Column)
	})
}

func hasAggregate(e expr.Node) bool {
	found := false
	visit := expr.WalkFunc(func(e expr.Node) bool {
		if found {
			return false
		}
		if _, ok := e.(*expr.Select); ok {
			return false
		}
		if _, ok := e.(*expr.Aggregate); ok {
			found = true
			return false
		}
		return true
	})
	expr.Walk(visit, e)
	return found
}

// Env can be provided in calls to Build to provide
// additional context for plan optimization purposes.
type Env interface {
	// Schema returns type hints associated
	// with a particular table expression.
	// In the event that there is no available
	// type information, Schema may return nil.
	Schema(expr.Node) expr.Hint
	// Index returns the index for the given table
	// expression. This may return (nil, nil) if
	// the index for the table is not available.
	Index(expr.Node) (Index, error)
}

type Index interface {
	// TimeRange returns the inclusive time range
	// for the given path expression across the
	// given table.
	TimeRange(path []string) (min, max date.Time, ok bool)
	// HasPartition returns true if the index
	// can be partitioned on the provided field,
	// or false otherwise.
	HasPartition(field string) bool
}

// Build walks the provided Query
// and lowers it into the optimized query IR.
// If the provided SchemaHint is non-nil,
// then it will be used to provide additional
// type information that can be used to type-check
// and optimize the query.
func Build(q *expr.Query, e Env) (*Trace, error) {
	body := q.Body
	var err error
	if len(q.With) > 0 {
		body, err = replaceTables(body, q.With)
		if err != nil {
			return nil, err
		}
	}
	if sel, ok := body.(*expr.Select); ok {
		t, err := build(nil, sel, e)
		if err != nil {
			return nil, err
		}
		if q.Into != nil {
			// expect db.table
			p, ok := expr.FlatPath(q.Into)
			if !ok || len(p) != 2 {
				return nil, fmt.Errorf("unsupported INTO: %q", expr.ToString(q.Into))
			}
			t.Into(q.Into, path.Join("db", p[0], p[1]))
		}
		return t, nil
	}
	// TODO: body can be UNION ALL, UNION, etc.
	return nil, errorf(body, "cannot pir.Build %T", body)
}

func build(parent *Trace, s *expr.Select, e Env) (*Trace, error) {
	b := &Trace{Parent: parent}
	s = expr.Simplify(s, expr.NoHint).(*expr.Select)
	err := expr.Check(s)
	if err != nil {
		return nil, err
	}
	err = b.walkSelect(s, e)
	if err != nil {
		return nil, err
	}
	err = b.optimize()
	return b, err
}

type tableReplacer struct {
	with []expr.CTE
	err  error
}

func (t *tableReplacer) Rewrite(e expr.Node) expr.Node {
	var bind *expr.Binding
	switch e := e.(type) {
	case *expr.Table:
		bind = &e.Binding
	case *expr.Join:
		// we'll walk e.Left later
		bind = &e.Right
	default:
		return e
	}
	switch v := bind.Expr.(type) {
	case expr.Ident:
		if cte := t.cloneCTE(v, bind); cte != nil {
			bind.Expr = cte
		}
	case *expr.Unpivot:
		if id, ok := v.TupleRef.(expr.Ident); ok {
			if cte := t.cloneCTE(id, bind); cte != nil {
				v.TupleRef = cte
			}
		}
		v.TupleRef = t.Rewrite(v.TupleRef)
	case *expr.Appended:
		for i := range v.Values {
			id, ok := v.Values[i].(expr.Ident)
			if ok {
				if cte := t.cloneCTE(id, bind); cte != nil {
					v.Values[i] = cte
				}
			}
			v.Values[i] = t.Rewrite(v.Values[i])
		}
	}
	return e
}

// cloneCTE finds CTE by name and returns its copy
func (t *tableReplacer) cloneCTE(id expr.Ident, bind *expr.Binding) expr.Node {
	with := t.with
	// search for a matching binding in
	// binding order:
	for i := len(with) - 1; i >= 0; i-- {
		if with[i].Table == string(id) {
			return expr.Copy(with[i].As)
		}

		// see FIXME in Walk;
		// for now we refuse bindings for tables
		// that can conflict with one another
		if bind.Result() == t.with[i].Table {
			t.err = errorf(bind.Expr, "table binding %q shadows CTE binding %q", bind.Result(), t.with[i].Table)
			break
		}
	}

	return nil
}

func (t *tableReplacer) Walk(e expr.Node) expr.Rewriter {
	if t.err != nil {
		return nil
	}
	// FIXME: a JOIN (implicit or otherwise)
	// can clobber a CTE binding; we should not
	// perform CTE replacement when the AS part
	// of a JOIN clobbers a clause in t!
	return t
}

func replaceTables(body expr.Node, with []expr.CTE) (expr.Node, error) {
	// first, replace bindings
	// within each CTE:
	rp := &tableReplacer{}
	for i := 1; i < len(with); i++ {
		rp.with = with[:i]
		with[i].As = expr.Rewrite(rp, with[i].As).(*expr.Select)
		if rp.err != nil {
			return nil, rp.err
		}
	}
	// then, write out the CTE bindings
	// into the query:
	rp.with = with
	ret := expr.Rewrite(rp, body)
	return ret, rp.err
}

// assign automatic result names if they
// are not present; otherwise we won't
// know what to project
func pickOutputs(s *expr.Select) {
	auto := make(map[string]struct{})
	used := func(x string) bool {
		_, ok := auto[x]
		return ok
	}
	use := func(x string) {
		auto[x] = struct{}{}
	}
	for i := range s.Columns {
		if s.Columns[i].Explicit() {
			use(s.Columns[i].Result())
			continue
		}
		// do not *implicitly* assign the same
		// result name more than once;
		// if we see that, then append _%d until
		// we find something unique
		res := s.Columns[i].Result()
		for res == "" || used(res) {
			res += fmt.Sprintf("_%d", i+1)
		}
		use(res)
		s.Columns[i].As(res)
	}
}

// adjust ORDER BY and DISTINCT ON(...) so that it can be computed
// before the final SELECT binding step
func normalizeOrderBy(s *expr.Select) {
	if len(s.OrderBy) == 0 {
		return
	}
	flattenIntoOrder(s.Columns, s.OrderBy)
	flattenIntoExprs(s.Columns, s.DistinctExpr)
}

type hoistwalk struct {
	parent *Trace
	in     []*Trace
	err    error
	env    Env
}

func (h *hoistwalk) Walk(e expr.Node) expr.Rewriter {
	if h.err != nil {
		return nil
	}
	// don't walk SELECT in the FROM position;
	// we handle that during ordinary walking
	if _, ok := e.(*expr.Table); ok {
		return nil
	}
	if b, ok := e.(*expr.Builtin); ok && b.Func == expr.InSubquery {
		return nil
	}
	if _, ok := e.(*expr.Select); ok {
		return nil
	}
	return h
}

var (
	scalarkind expr.Node = expr.String("scalar")
	structkind expr.Node = expr.String("struct")
	listkind   expr.Node = expr.String("list")
)

// when interpreted as a HASH_REPLACEMENT() result,
// does the set of output bindings given by lst
// never produce a MISSING result?
func replacementNeverMissing(t *Trace, lst []expr.Binding, except string) bool {
	if len(lst) > 2 {
		return true
	}
	b := &lst[0]
	if b.Result() == except {
		b = &lst[1]
	}
	return expr.TypeOf(b.Expr, &stepHint{t.top.parent()})&expr.MissingType == 0
}

// strip all the final bindings except for one
func stripFinal(t *Trace, except string) bool {
	b, ok := t.top.(*Bind)
	if !ok {
		return false
	}
	old := b.bind
	keep := -1
	for i := range old {
		if old[i].Result() == except {
			keep = i
			break
		}
	}
	if keep == -1 {
		return false
	}
	b.bind = []expr.Binding{old[keep]}
	return true
}

func (h *hoistwalk) Rewrite(e expr.Node) expr.Node {
	// if we have
	//   HASH_REPLACEMENT(id, kind, label, var) IS NOT MISSING
	// and the replacement var is never MISSING,
	// then this equivalent to a semi-join:
	//   IN_REPLACEMENT(var, id)
	if is, ok := e.(*expr.IsKey); ok && (is.Key == expr.IsMissing || is.Key == expr.IsNotMissing) {
		if b, ok := is.Expr.(*expr.Builtin); ok && b.Func == expr.HashReplacement {
			rep := h.in[int(b.Args[0].(expr.Integer))]
			label := string(b.Args[2].(expr.String))
			corrv := b.Args[3]
			if replacementNeverMissing(rep, rep.FinalBindings(), label) &&
				stripFinal(rep, label) {
				ret := (expr.Node)(&expr.Builtin{
					Func: expr.InReplacement,
					Args: []expr.Node{corrv, b.Args[0]},
				})
				if is.Key == expr.IsMissing {
					ret = &expr.Not{Expr: ret}
				}
				return ret
			}
		}
		return e
	}

	if b, ok := e.(*expr.Builtin); ok {
		switch b.Func {
		case expr.InSubquery:
			return h.rewriteInSubquery(b)
		case expr.ScalarReplacement, expr.ListReplacement, expr.StructReplacement:
			return b
		default:
			// every other builtin ought to take a scalar
			for i := range b.Args {
				b.Args[i] = h.rewriteScalarArg(b.Args[i])
			}
		}
		return b
	}
	if c, ok := e.(*expr.Comparison); ok {
		c.Left = h.rewriteScalarArg(c.Left)
		c.Right = h.rewriteScalarArg(c.Right)
		return c
	}
	if a, ok := e.(*expr.Arithmetic); ok {
		a.Left = h.rewriteScalarArg(a.Left)
		a.Right = h.rewriteScalarArg(a.Right)
		return a
	}
	if is, ok := e.(*expr.IsKey); ok {
		is.Expr = h.rewriteScalarArg(is.Expr)
		return is
	}
	s, ok := e.(*expr.Select)
	if !ok {
		return e
	}
	t, err := build(h.parent, s, h.env)
	if err != nil {
		h.err = err
		return e
	}
	scalar := len(t.FinalBindings()) == 1
	class := t.Class()
	if class == SizeZero {
		return expr.Missing{}
	}
	index := expr.Integer(len(h.in))
	label, corrv, corrbind, err := t.decorrelate()
	if err != nil {
		h.err = err
		return e
	}

	if corrv != nil {
		var err error
		visit := func(e expr.Node) bool {
			sel, ok := e.(*expr.Select)
			if ok && sel == s {
				err = fmt.Errorf("decorrelation error: subquery %q is self-referenced as %q in the outer query",
					expr.ToString(sel), corrbind)
				return false
			}

			return true
		}

		expr.Walk(expr.WalkFunc(visit), corrv)

		if err != nil {
			h.err = err
			return e
		}
	}

	switch class {
	case SizeOne:
		h.in = append(h.in, t)
		if corrv != nil {
			kind := structkind
			if scalar {
				kind = scalarkind
			}
			return expr.Call(expr.HashReplacement, index, kind, label, corrv)
		}
		if scalar {
			return expr.Call(expr.ScalarReplacement, index)
		}
		return expr.Call(expr.StructReplacement, index)
	case SizeExactSmall, SizeColumnCardinality:
		h.in = append(h.in, t)
		if corrv != nil {
			return expr.Call(expr.HashReplacement, index, listkind, label, corrv)
		}
		return expr.Call(expr.ListReplacement, index)
	default:
		h.err = errorf(s, "cardinality of sub-query is too large; use LIMIT")
		return s
	}
}

func (h *hoistwalk) rewriteInSubquery(b *expr.Builtin) expr.Node {
	// TODO: push down a DISTINCT,
	// since the IN expression
	// is equivalent regardless of
	// how many times the same result
	// appears in the output
	t, err := build(h.parent, b.Args[1].(*expr.Select), h.env)
	if err != nil {
		h.err = err
		return b
	}
	if cols := len(t.FinalBindings()); cols != 1 {
		h.err = errorf(b.Args[1].(*expr.Select), "IN sub-query should have 1 column; have %d", cols)
		return b
	}
	index := len(h.in)
	switch t.Class() {
	case SizeZero:
		return expr.Bool(false)
	case SizeOne:
		h.in = append(h.in, t)
		repl := expr.Call(expr.ScalarReplacement, expr.Integer(index))
		return expr.Compare(expr.Equals, b.Args[0], repl)
	case SizeExactSmall, SizeColumnCardinality:
		h.in = append(h.in, t)
		return expr.Call(expr.InReplacement, b.Args[0], expr.Integer(index))
	default:
		h.err = errorf(b.Args[1].(*expr.Select), "sub-query cardinality too large: %s", expr.ToString(b.Args[1]))
		return b
	}
}

// an SFW expression on either side of a comparison
// or arithmetic operation must be coerced to a scalar:
func (h *hoistwalk) rewriteScalarArg(e expr.Node) expr.Node {
	s, ok := e.(*expr.Select)
	if !ok {
		return e
	}
	index := len(h.in)
	t, err := build(h.parent, s, h.env)
	if err != nil {
		h.err = err
		return e
	}
	if cols := len(t.FinalBindings()); cols != 1 {
		h.err = errorf(s, "cannot coerce sub-query with %d columns into a scalar", cols)
		return e
	}
	switch t.Class() {
	case SizeZero:
		// NOTE: NULL is the obvious SQL answer,
		// but doesn't MISSING make more sense in
		// the PartiQL context?
		return expr.Null{}
	case SizeOne:
		h.in = append(h.in, t)
		return expr.Call(expr.ScalarReplacement, expr.Integer(index))
	default:
		// For now, require that scalar sub-queries
		// have a known output size of 0 or 1,
		// and make users provide LIMIT 1 if they
		// really mean just the first result
		h.err = errorf(e, "scalar sub-query %q has unbounded results; use LIMIT 1", expr.ToString(s))
		return e
	}
}

// hoist takes subqueries and hoists them
// into b.Inputs
func (b *Trace) hoist(e Env) error {
	hw := &hoistwalk{env: e, parent: b}
	for s := b.top; s != nil; s = s.parent() {
		s.rewrite(func(e expr.Node, _ bool) expr.Node {
			if hw.err != nil {
				return e
			}
			return expr.Rewrite(hw, e)
		})
		if hw.err != nil {
			return hw.err
		}
	}
	b.Replacements = append(b.Replacements, hw.in...)
	return nil
}

type windowHoist struct {
	outer *expr.Select
	trace *Trace
	env   Env
	err   error
}

func (w *windowHoist) Walk(e expr.Node) expr.Rewriter {
	if w.err != nil {
		return nil
	}
	if _, ok := e.(*expr.Aggregate); ok {
		return nil
	}
	if _, ok := e.(*expr.Select); ok {
		// don't walk sub-queries
		return nil
	}
	return w
}

func hasOnlyOneAggregate(outer *expr.Select) bool {
	var uniq []*expr.Aggregate
	visit := expr.WalkFunc(func(e expr.Node) bool {
		if s, ok := e.(*expr.Select); ok {
			return s == outer
		}
		agg, ok := e.(*expr.Aggregate)
		if !ok {
			return true
		}
		for i := range uniq {
			if uniq[i].Equals(agg) {
				return false
			}
		}
		uniq = append(uniq, agg)
		return false
	})
	expr.Walk(visit, outer)
	return len(uniq) == 1
}

func (w *windowHoist) Rewrite(e expr.Node) expr.Node {
	agg, ok := e.(*expr.Aggregate)
	if !ok {
		return e
	}
	// if we have COUNT(DISTINCT ...) along with
	// other aggregates, we can rewrite it to
	// work more like a window function:
	if agg.Op == expr.OpCountDistinct &&
		len(w.outer.GroupBy) > 0 &&
		!hasOnlyOneAggregate(w.outer) {
		agg.Over = &expr.Window{
			PartitionBy: expr.BindingValues(w.outer.GroupBy),
		}
	}
	if agg.Over == nil {
		return e
	}
	if agg.Op.WindowOnly() {
		// handled natively by the core
		return e
	}
	if len(agg.Over.PartitionBy) == 0 {
		w.err = fmt.Errorf("PARTITION BY has 0 partition elements")
		return e
	}

	partitions := agg.Over.PartitionBy
	self := copyForWindow(w.outer)
	if agg.Op == expr.OpCountDistinct {
		self.GroupBy = append(self.GroupBy, expr.Bind(agg.Inner, "$__distinct"))
		agg.Op = expr.OpCount
		agg.Inner = expr.Star{}
	}
	// outerkey is the lookup expression to yield in the outer query
	outerkey := partitions[0]
	if len(partitions) > 1 {
		outerkey = expr.Call(expr.MakeList, partitions...)
	}
	outerkey = expr.Copy(outerkey)
	// if there is an existing GROUP BY,
	// then the PARTITION BY should match
	// one of those bindings (otherwise it
	// is referencing an unbound variable);
	// wrap the sub-query so that we perform
	// a second grouping based on the first one,
	// which we can perform simply as a DISTINCT:
	if len(self.GroupBy) > 0 {
		group := self.GroupBy
		for i := range group {
			for j := range partitions {
				if expr.Equivalent(group[i].Expr, partitions[j]) {
					// since we want to reference this expression
					// by name, we need to generate a temporary for it
					// if it doesn't have one already
					if !group[i].Explicit() {
						group[i].As(gensym(3, i))
					}
					partitions[j] = expr.Identifier(group[i].Result())
					break
				}
			}
		}
		self.GroupBy = nil
		self.Columns = group
		self.Distinct = true
		newt := &expr.Select{
			From: &expr.Table{Binding: expr.Bind(self, "")},
		}
		self = newt
	}
	for i := range partitions {
		self.GroupBy = append(self.GroupBy, expr.Bind(partitions[i], ""))
	}
	// selfkey is the expression to produce for the sub-query
	// that is hash-matched against outerkey
	selfkey := partitions[0]
	if len(partitions) > 1 {
		selfkey = expr.Call(expr.MakeList, partitions...)
	}
	self.Columns = []expr.Binding{
		expr.Bind(agg, "$__val"),
		expr.Bind(selfkey, "$__key"),
	}
	agg.Over = nil
	// we want HASH_LOOKUP(<partition_expr>, ...)
	// to replace the aggregate so that
	//   SUM(x) OVER (PARTITION BY y)
	// is turned into
	//   HASH_LOOKUP($__key, (SELECT SUM(x) AS $__val, y AS $__key FROM ... GROUP BY y), default)
	def := (expr.Node)(expr.Null{})
	if agg.Op == expr.OpCount {
		def = expr.Integer(0)
	}
	ret := expr.Call(expr.HashReplacement,
		expr.Integer(len(w.trace.Replacements)),
		scalarkind,
		expr.String("$__key"),
		outerkey, def)
	// FIXME: this doesn't do anything yet
	// because we don't have true window functions;
	// other aggregates (COUNT, SUM, etc.) are insensitive
	// to the input order:
	// self.OrderBy = partition.OrderBy

	t, err := build(w.trace, self, w.env)
	if err != nil {
		w.err = err
		return e
	}
	w.trace.Replacements = append(w.trace.Replacements, t)
	return ret
}

// copyForWindow performs a deep copy of the
// portions of a SELECT that are relevant to
// a window rewrite as a correlated sub-query
// (i.e. everything that happens before SELECT)
func copyForWindow(s *expr.Select) *expr.Select {
	alt := &expr.Select{
		GroupBy: s.GroupBy,
		Where:   s.Where,
		From:    s.From,
	}
	return expr.Copy(alt).(*expr.Select)
}

func (b *Trace) hoistWindows(s *expr.Select, e Env) error {
	rw := &windowHoist{
		trace: b,
		outer: s,
		env:   e,
	}
	for i := range s.Columns {
		s.Columns[i].Expr = expr.Rewrite(rw, s.Columns[i].Expr)
		if rw.err != nil {
			return rw.err
		}
	}
	return nil
}

func (b *Trace) walkSelect(s *expr.Select, e Env) error {
	// perform normalizations
	pickOutputs(s)
	selectall := isselectall(s)
	s.Columns = flattenBind(s.Columns)
	err := b.hoistWindows(s, e)
	if err != nil {
		return err
	}
	normalizeOrderBy(s)
	err = aggdistinctpromote(s)
	if err != nil {
		return err
	}

	// Walk in binding order:
	// FROM -> WHERE -> (SELECT / GROUP BY / ORDER BY)
	err = b.walkFrom(s.From, e)
	if err != nil {
		return err
	}

	if s.Where != nil {
		err = b.Where(s.Where)
		if err != nil {
			return err
		}
	}

	// if we are doing aggregation anywhere, then split it:
	if s.Having != nil || s.GroupBy != nil || anyHasAggregate(s.Columns) || anyOrderHasAggregate(s.OrderBy) {
		// s.OrderBy and s.Columns are rewritten to reference
		// the generated aggregate expression
		// (and also HAVING is taken care of)
		err = b.splitAggregate(s.OrderBy, s.DistinctExpr, s.Columns, s.GroupBy, s.Having)
		if err != nil {
			return err
		}
	}

	// per the postgresql docs, DISTINCT ON(...) is interpreted
	// with the same rules as ORDER BY
	if len(s.DistinctExpr) > 0 {
		err = b.Distinct(s.DistinctExpr)
		if err != nil {
			return err
		}
	} else if s.Distinct {
		if selectall {
			return fmt.Errorf("DISTINCT * not supported")
		}
		err = b.DistinctFromBindings(s.Columns)
		if err != nil {
			return err
		}
	}
	// we're cheating and doing ORDER BY before SELECT
	// because we've normalized them w.r.t. incoming bindings;
	// this allows ORDER BY to reference columns that do not
	// make it to the final SELECT list
	if s.OrderBy != nil {
		err = b.Order(s.OrderBy)
		if err != nil {
			return err
		}
	}
	// we can evaluate LIMIT before SELECT
	if s.Limit != nil {
		offset := int64(0)
		if s.Offset != nil {
			offset = int64(*s.Offset)
		}
		limit := int64(*s.Limit)
		err = b.LimitOffset(limit, offset)
		if err != nil {
			return err
		}
	}
	if !selectall {
		err = b.Bind(s.Columns)
		if err != nil {
			return err
		}
	} else {
		b.BindStar()
	}
	return b.hoist(e)
}

// isselectall checks if there's only a single '*' in select
func isselectall(s *expr.Select) bool {
	return len(s.Columns) == 1 && s.Columns[0].Expr == (expr.Star{})
}

func (b *Trace) buildUnpivot(u *expr.Unpivot, e Env) error {
	// Validation
	if (u.As != nil) && (u.At != nil) && (*u.As == *u.At) {
		return fmt.Errorf("the AS and AT UNPIVOT labels must not be the same '%s'", *u.As)
	}
	if (u.As == nil) && (u.At == nil) {
		return fmt.Errorf("the AS and AT UNPIVOT labels must not be empty simultaneously")
	}
	// Emission
	switch ref := u.TupleRef.(type) {
	case *expr.Table: // UNPIVOT table
		if err := b.walkFromTable(ref, e); err != nil {
			return err
		}
		b.top.get("*")

	case *expr.Builtin: // UNPIVOT {...}
		// CAUTION: when MAKE_STRUCT is implemented, the output might be expr.Table as well.
		if ref.Func != expr.MakeStruct {
			return fmt.Errorf("UNPIVOT expects a path or an explicit structure, but '%s' is provided", ref)
		}
		return fmt.Errorf("buildUnpivot *expr.Builtin.MakeStruct has not been implemented yet")
	default:
		return fmt.Errorf("UNPIVOT expects a path or an explicit structure, but '%s' is provided", ref)
	}

	unp := &Unpivot{Ast: u}
	unp.setparent(b.top)
	b.top = unp
	return nil
}
