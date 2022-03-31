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
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func (b *Trace) walkFrom(f expr.From, e Env) error {
	if f == nil {
		b.top = DummyOutput{}
		return nil
	}
	switch f := f.(type) {
	default:
		return fmt.Errorf("unexpected expression %q", f)
	case *expr.Join:
		if f.Kind != expr.CrossJoin {
			return fmt.Errorf("join %q not yet supported", f.Kind)
		}
		err := b.walkFrom(f.Left, e)
		if err != nil {
			return err
		}
		// FIXME: if the rhs expression is a SELECT,
		// then this is almost certainly a correlated
		// sub-query ...
		return b.Iterate(&f.Right)
	case *expr.Table:
		if s, ok := f.Expr.(*expr.Select); ok {
			b.walkSelect(s, e)
			// TODO: if any subsequent expressions
			// refer to a binding created by
			//   FROM (SELECT ...) AS x,
			// we should strip 'x.' from those
			// bindings...
		} else {
			b.Begin(f, e)
		}
	}
	return nil
}

// walk a list of bindings and determine if
// any of the bindings includes an aggregate
// expression
func anyHasAggregate(lst []expr.Binding) bool {
	for i := range lst {
		if hasAggregate(lst[i].Expr) {
			return true
		}
	}
	return false
}

func hasAggregate(e expr.Node) bool {
	found := false
	visit := visitfn(func(e expr.Node) bool {
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

// Env is a subset of plan.Env.
type Env interface {
	Schema(table *expr.Table) expr.Hint
}

// TimeRanger is an interface that may optionally be
// implemented by Env to allow sparse indexing
// information to be used during planning.
type TimeRanger interface {
	// TimeRange returns the inclusive time range
	// for the given path expression across the
	// given table.
	TimeRange(*expr.Table, *expr.Path) (min, max time.Time, ok bool)
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
		return build(nil, sel, e)
	}
	// TODO: body can be UNION ALL, UNION, etc.
	return nil, fmt.Errorf("cannot pir.Build %T", q.Body)
}

func build(parent *Trace, s *expr.Select, e Env) (*Trace, error) {
	b := &Trace{Parent: parent}
	s = expr.Simplify(s, b).(*expr.Select)
	err := b.walkSelect(s, e)
	if err != nil {
		return nil, err
	}
	b.optimize()
	return b, nil
}

type tableReplacer struct {
	with []expr.CTE
	err  error
}

func exprcopy(e expr.Node) (expr.Node, error) {
	var dst ion.Buffer
	var st ion.Symtab
	e.Encode(&dst, &st)
	ret, _, err := expr.Decode(&st, dst.Bytes())
	return ret, err
}

func (t *tableReplacer) Rewrite(e expr.Node) expr.Node {
	tbl, ok := e.(*expr.Table)
	if !ok {
		return e
	}
	with := t.with
	// search for a matching binding in
	// binding order:
	for i := len(with) - 1; i >= 0; i-- {
		if expr.IsIdentifier(tbl.Expr, with[i].Table) {
			cop, err := exprcopy(with[i].As)
			if err != nil && t.err == nil {
				t.err = err
			}
			tbl.Expr = cop
			return e
		}
		// see FIXME in Walk;
		// for now we refuse bindings for tables
		// that can conflict with one another
		if tbl.Result() == with[i].Table {
			t.err = fmt.Errorf("table binding %q shadows CTE binding %q", tbl.Result(), with[i].Table)
		}
	}
	return e
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
	for i := range s.Columns {
		if s.Columns[i].Explicit() {
			continue
		}
		// in the absence of a natural output name,
		// produce an ordinal column name
		if res := s.Columns[i].Result(); res == "" {
			s.Columns[i].As(fmt.Sprintf("_%d", i+1))
		}
	}
}

// if OrderBy uses a top-level expression
// in SELECT, replace the ORDER BY expression
// with the result value
//
// according to the PartiQL spec, these expressions
// have to be syntatically identical, so we ought
// to be able to match them just with expr.Equivalent()
func normalizeOrderBy(s *expr.Select) {
	for i := range s.OrderBy {
		for j := range s.Columns {
			if expr.Equivalent(s.OrderBy[i].Column, s.Columns[j].Expr) {
				s.OrderBy[i].Column = &expr.Path{First: s.Columns[j].Result()}
				break
			}
		}
	}
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

func (h *hoistwalk) Rewrite(e expr.Node) expr.Node {
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
	label, corrv, err := t.decorrelate()
	if err != nil {
		h.err = err
		return e
	}
	switch class {
	case SizeOne:
		h.in = append(h.in, t)
		if corrv != nil {
			kind := structkind
			if scalar {
				kind = scalarkind
			}
			return expr.Call("HASH_REPLACEMENT", index, kind, label, corrv)
		}
		if scalar {
			return expr.Call("SCALAR_REPLACEMENT", index)
		}
		return expr.Call("STRUCT_REPLACEMENT", index)
	case SizeExactSmall, SizeColumnCardinality:
		h.in = append(h.in, t)
		if corrv != nil {
			return expr.Call("HASH_REPLACEMENT", index, listkind, label, corrv)
		}
		return expr.Call("LIST_REPLACEMENT", index)
	default:
		h.err = fmt.Errorf("cardinality of sub-query is too large; use LIMIT")
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
		h.err = fmt.Errorf("IN sub-query should have 1 column; have %d", cols)
		return b
	}
	index := len(h.in)
	switch t.Class() {
	case SizeZero:
		return expr.Bool(false)
	case SizeOne:
		h.in = append(h.in, t)
		repl := expr.Call("SCALAR_REPLACEMENT", expr.Integer(index))
		return expr.Compare(expr.Equals, b.Args[0], repl)
	case SizeExactSmall, SizeColumnCardinality:
		h.in = append(h.in, t)
		return expr.Call("IN_REPLACEMENT", b.Args[0], expr.Integer(index))
	default:
		h.err = fmt.Errorf("sub-query cardinality too large: %s", b.Args[1])
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
		return nil
	}
	if cols := len(t.FinalBindings()); cols != 1 {
		h.err = fmt.Errorf("cannot coerce sub-query with %d columns into a scalar", cols)
		return nil
	}
	switch t.Class() {
	case SizeZero:
		// NOTE: NULL is the obvious SQL answer,
		// but doesn't MISSING make more sense in
		// the PartiQL context?
		return expr.Null{}
	case SizeOne:
		h.in = append(h.in, t)
		return expr.Call("SCALAR_REPLACEMENT", expr.Integer(index))
	default:
		// For now, require that scalar sub-queries
		// have a known output size of 0 or 1,
		// and make users provide LIMIT 1 if they
		// really mean just the first result
		h.err = fmt.Errorf("scalar sub-query %q has unbounded results; use LIMIT 1", expr.ToString(s))
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
	b.Inputs = append(b.Inputs, hw.in...)
	return nil
}

func (b *Trace) walkSelect(s *expr.Select, e Env) error {
	// Walk in binding order:
	// FROM -> WHERE -> (SELECT / GROUP BY / ORDER BY)
	pickOutputs(s)
	normalizeOrderBy(s)

	err := b.walkFrom(s.From, e)
	if err != nil {
		return err
	}

	if s.Where != nil {
		err = b.Where(s.Where)
		if err != nil {
			return err
		}
	}

	// walk SELECT + GROUP BY + HAVING
	if s.Having != nil || s.GroupBy != nil || anyHasAggregate(s.Columns) {
		if s.Distinct && s.GroupBy != nil {
			return fmt.Errorf("mixed hash aggregate and DISTINCT not supported")
			// if we have DISTINCT but no group by,
			// just ignore it; we are only producing
			// one output row anyway...
		}
		err = b.splitAggregate(s.Columns, s.GroupBy, s.Having)
	} else {
		if len(s.Columns) == 1 && s.Columns[0].Expr == (expr.Star{}) {
			err = b.BindStar()
		} else {
			if s.Distinct {
				err = b.Distinct(s.Columns)
				if err != nil {
					return err
				}
			}
			err = b.Bind(s.Columns)
		}
	}
	if err != nil {
		return err
	}

	if s.OrderBy != nil {
		err = b.Order(s.OrderBy)
		if err != nil {
			return err
		}
	}

	// finally, LIMIT
	if s.Limit != nil {
		offset := int64(0)
		if s.Offset != nil {
			offset = int64(*s.Offset)
			if offset < 0 {
				return fmt.Errorf("negative offset %d not supported", offset)
			}
		}
		limit := int64(*s.Limit)
		if limit < 0 {
			return fmt.Errorf("negative limit %d not supported", limit)
		}
		err = b.LimitOffset(int64(*s.Limit), offset)
		if err != nil {
			return err
		}
	}

	return b.hoist(e)
}
