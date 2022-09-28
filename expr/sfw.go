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

package expr

import (
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
)

type Binding struct {
	Expr Node
	// this is set/computed lazily
	as       string
	explicit bool
}

// Bind creates a binding from an expression
// and an output binding name
func Bind(e Node, as string) Binding {
	return Binding{Expr: e, as: as, explicit: as != ""}
}

// Identity creates an identity binding from a simple identifier into itself.
func Identity(s string) Binding {
	return Bind(Identifier(s), s)
}

// As sets the binding result of b
// to x. If x is the empty string,
// then the binding is reset to
// the default value for this expression.
func (b *Binding) As(x string) {
	b.as = x
	b.explicit = x != ""
}

// Explicit returns whether the variable binding
// is explicit, or whether the output variable is
// determined implicitly due to the form on the left-hand-side
func (b *Binding) Explicit() bool {
	return b.explicit
}

// Result returns the name of
// the result that the binding outputs.
//
// Note that Result is "" for expressions
// that do not have an obvious automatic name
// and have not had a name explicitly added
// via Binding.As.
func (b *Binding) Result() string {
	if b.as != "" {
		return b.as
	}
	b.as = b.result()
	return b.as
}

func (b *Binding) result() string {
	switch e := b.Expr.(type) {
	case *Path:
		return e.Binding()
	case *Aggregate:
		return e.Op.defaultResult()
	default:
		return ""
	}
}

func (b *Binding) text(dst *strings.Builder, redact bool) {
	b.Expr.text(dst, redact)
	if b.explicit {
		dst.WriteString(" AS ")
		dst.WriteString(QuoteID(b.Result()))
	}
}

func (b Binding) Equals(o Binding) bool {
	return b.Result() == o.Result() && b.Expr.Equals(o.Expr)
}

// BindingValues collects all of bind[*].Expr
// and returns them as a slice.
func BindingValues(bind []Binding) []Node {
	out := make([]Node, len(bind))
	for i := range bind {
		out[i] = bind[i].Expr
	}
	return out
}

type JoinKind int

const (
	NoJoin JoinKind = iota
	InnerJoin
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

func (j JoinKind) String() string {
	switch j {
	default:
		return ""
	case InnerJoin:
		return "JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullJoin:
		return "FULL JOIN"
	case CrossJoin:
		return "CROSS JOIN"
	}
}

// From represents a FROM clause
type From interface {
	// Tables returns the list of
	// table bindings created in
	// the FROM clause
	Tables() []Binding
	Node
}

// Opaque is an opaque object that
// can be serialized and deserialized
type Opaque interface {
	// TypeName should return a unique
	// name for this type. It is used
	// in order to determine how to
	// decode the object (see AddOpaqueDecoder)
	TypeName() string
	// Encode should encode the object to dst,
	// adding symbols as appropriate to st
	Encode(dst *ion.Buffer, st *ion.Symtab)
}

// Table is an implementation of From
// that simply binds a top-level table
// as a bag of values
type Table struct {
	Binding
}

func (t *Table) Tables() []Binding {
	return []Binding{t.Binding}
}

func walkbind(v Visitor, b *Binding) {
	Walk(v, b.Expr)
}

func rewritebind(r Rewriter, b *Binding) Binding {
	b.Expr = Rewrite(r, b.Expr)
	return *b
}

func (t *Table) walk(v Visitor) {
	walkbind(v, &t.Binding)
}

// OnEquals represents a single
//
//	<left> = <right>
//
// statement inside an ON clause
type OnEquals struct {
	Left, Right Node
}

func (o *OnEquals) text(dst *strings.Builder, redact bool) {
	o.Left.text(dst, redact)
	dst.WriteString(" = ")
	o.Right.text(dst, redact)
}

func (o *OnEquals) Equals(x Node) bool {
	xo, ok := x.(*OnEquals)
	return ok && o.Left.Equals(xo.Left) && o.Right.Equals(xo.Right)
}

func (o *OnEquals) walk(v Visitor) {
	Walk(v, o.Left)
	Walk(v, o.Right)
}

func (o *OnEquals) rewrite(r Rewriter) Node {
	o.Left = Rewrite(r, o.Left)
	o.Right = Rewrite(r, o.Right)
	return o
}

func (o *OnEquals) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype(dst, st, "on")
	dst.BeginField(st.Intern("left"))
	o.Left.Encode(dst, st)
	dst.BeginField(st.Intern("right"))
	o.Right.Encode(dst, st)
	dst.EndStruct()
}

func (o *OnEquals) setfield(name string, st *ion.Symtab, body []byte) error {
	var err error
	switch name {
	case "left":
		o.Left, _, err = Decode(st, body)
	case "right":
		o.Right, _, err = Decode(st, body)

	}
	return err
}

// Join is an implementation of From
// that joins a table and a subsequent From clause
type Join struct {
	Kind  JoinKind
	On    Node
	Left  From    // left table expression; can be another join
	Right Binding // right binding
}

func (j *Join) Tables() []Binding {
	return append(j.Left.Tables(), j.Right)
}

func (j *Join) walk(v Visitor) {
	if j.On != nil {
		Walk(v, j.On)
	}
	Walk(v, j.Left)
	walkbind(v, &j.Right)
}

func (j *Join) rewrite(r Rewriter) Node {
	j.On = Rewrite(r, j.On)
	j.Left = Rewrite(r, j.Left).(From)
	j.Right = rewritebind(r, &j.Right)
	return j
}

func (j *Join) Equals(x Node) bool {
	xj, ok := x.(*Join)
	if !ok || xj.Kind != j.Kind {
		return false
	}
	if (j.On == nil) != (xj.On == nil) {
		return false
	}
	if j.On != nil && !j.On.Equals(xj.On) {
		return false
	}
	if !j.Left.Equals(xj.Left) || !j.Right.Expr.Equals(xj.Right.Expr) {
		return false
	}
	return j.Right.Result() == xj.Right.Result()
}

func (j *Join) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype(dst, st, "join")
	dst.BeginField(st.Intern("kind"))
	dst.WriteUint(uint64(j.Kind))
	if j.On != nil {
		dst.BeginField(st.Intern("on"))
		j.On.Encode(dst, st)
	}
	if j.Left != nil {
		dst.BeginField(st.Intern("left"))
		j.Left.Encode(dst, st)
	}
	dst.BeginField(st.Intern("right"))
	j.Right.Expr.Encode(dst, st)
	dst.BeginField(st.Intern("bind"))
	dst.WriteString(j.Right.Result())
	dst.EndStruct()
}

func (j *Join) setfield(name string, st *ion.Symtab, body []byte) error {
	var err error
	switch name {
	case "kind":
		u, _, err := ion.ReadUint(body)
		if err != nil {
			return err
		}
		j.Kind = JoinKind(u)
	case "on":
		j.On, _, err = Decode(st, body)
		return err
	case "left":
		e, _, err := Decode(st, body)
		if err != nil {
			return err
		}
		f, ok := e.(From)
		if !ok {
			return fmt.Errorf("expression %q invalid in FROM position", e)
		}
		j.Left = f
		return nil
	case "right":
		var err error
		j.Right.Expr, _, err = Decode(st, body)
		return err
	case "bind":
		str, _, err := ion.ReadString(body)
		if err != nil {
			return err
		}
		j.Right.As(str)
		return nil
	}
	return nil
}

func (j *Join) text(out *strings.Builder, redact bool) {
	j.Left.text(out, redact)
	out.WriteString(" ")
	out.WriteString(j.Kind.String())
	out.WriteString(" ")
	j.Right.text(out, redact)
	if j.On != nil {
		out.WriteString(" ON ")
		j.On.text(out, redact)
	}
}

type Order struct {
	Column          Node
	Desc, NullsLast bool
}

func (o *Order) text(dst *strings.Builder, redact bool) {
	o.Column.text(dst, redact)
	if o.Desc {
		dst.WriteString(" DESC")
	} else {
		dst.WriteString(" ASC")
	}
	if o.NullsLast {
		dst.WriteString(" NULLS LAST")
	} else {
		dst.WriteString(" NULLS FIRST")
	}
}

func (o Order) Equals(x Order) bool {
	if o.Desc != x.Desc {
		return false
	}
	if o.NullsLast != x.NullsLast {
		return false
	}

	return o.Column.Equals(x.Column)
}

type Select struct {
	// DISTINCT presence
	Distinct bool
	// DISTINCT ON (expressions)
	DistinctExpr []Node
	// List of output columns
	Columns []Binding
	// FROM clause
	From From
	// WHERE clause
	Where Node
	// GROUP BY clauses, or nil
	GroupBy []Binding
	// HAVING clause, or nil
	Having Node
	// ORDER BY clauses, or nil
	OrderBy []Order
	// When OrderBy is non-nil,
	// indicates the presence of PRESERVE
	Preserve bool
	// LIMIT <Integer> when non-nil
	Limit *Integer
	// OFFSET <Integer> when non-nil
	Offset *Integer
}

func (s *Select) walk(v Visitor) {
	// walking happens in PartiQL binding order:
	// from -> where -> groupby -> select -> orderby -> limit
	if s.From != nil {
		Walk(v, s.From)
	}
	if s.Where != nil {
		Walk(v, s.Where)
	}
	if s.Having != nil {
		Walk(v, s.Having)
	}
	for i := range s.GroupBy {
		walkbind(v, &s.GroupBy[i])
	}
	for i := range s.Columns {
		walkbind(v, &s.Columns[i])
	}
	for i := range s.OrderBy {
		Walk(v, s.OrderBy[i].Column)
	}
	for i := range s.DistinctExpr {
		Walk(v, s.DistinctExpr[i])
	}
	if s.Limit != nil {
		Walk(v, *s.Limit)
	}
	if s.Offset != nil {
		Walk(v, *s.Offset)
	}
}

// Tables implements From.Tables
func (s *Select) Tables() []Binding {
	if s.From == nil {
		return nil
	}
	return s.From.Tables()
}

func (s *Select) rewrite(r Rewriter) Node {
	// FROM gets rewritten first so that
	// any variable bindings get introduced
	// in the right order
	if s.From != nil {
		s.From = Rewrite(r, s.From).(From)
	}
	// ... and then bindings introduced by
	// the SELECT clause
	for i := range s.Columns {
		s.Columns[i] = rewritebind(r, &s.Columns[i])
	}
	s.Where = Rewrite(r, s.Where)
	s.Having = Rewrite(r, s.Having)
	for i := range s.GroupBy {
		s.GroupBy[i] = rewritebind(r, &s.GroupBy[i])
	}
	for i := range s.OrderBy {
		s.OrderBy[i].Column = Rewrite(r, s.OrderBy[i].Column)
	}
	for i := range s.DistinctExpr {
		s.DistinctExpr[i] = Rewrite(r, s.DistinctExpr[i])
	}
	return s
}

func (s *Select) Equals(x Node) bool {
	xs, ok := x.(*Select)
	if !ok ||
		(s.From == nil) != (xs.From == nil) ||
		(s.Where == nil) != (xs.Where == nil) ||
		(s.Having == nil) != (xs.Having == nil) ||
		(s.Limit == nil) != (xs.Limit == nil) ||
		(s.Offset == nil) != (xs.Offset == nil) ||
		(s.Distinct != xs.Distinct) {
		return false
	}
	if s.From != nil && !s.From.Equals(xs.From) {
		return false
	}
	if !slices.EqualFunc(s.Columns, xs.Columns, func(x, y Binding) bool { return x.Equals(y) }) {
		return false
	}
	if s.Where != nil && !s.Where.Equals(xs.Where) {
		return false
	}
	if s.Having != nil && !s.Having.Equals(xs.Having) {
		return false
	}
	if !slices.EqualFunc(s.OrderBy, xs.OrderBy, Order.Equals) {
		return false
	}
	if !slices.EqualFunc(s.DistinctExpr, xs.DistinctExpr, Node.Equals) {
		return false
	}
	return true
}

func EncodeBindings(bind []Binding, dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginList(-1)
	for i := range bind {
		dst.BeginStruct(-1)
		dst.BeginField(st.Intern("expr"))
		bind[i].Expr.Encode(dst, st)
		if bind[i].Explicit() {
			dst.BeginField(st.Intern("bind"))
			dst.WriteString(bind[i].Result())
		}
		dst.EndStruct()
	}
	dst.EndList()
}

func addfield(dst *ion.Buffer, st *ion.Symtab, name string, node Node) {
	if node != nil {
		dst.BeginField(st.Intern(name))
		node.Encode(dst, st)
	}
}

func EncodeOrder(ord []Order, dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginList(-1)
	for i := range ord {
		dst.BeginStruct(-1)
		settype(dst, st, "ord")
		dst.BeginField(st.Intern("col"))
		ord[i].Column.Encode(dst, st)
		if ord[i].Desc {
			dst.BeginField(st.Intern("desc"))
			dst.WriteBool(true)
		}
		if ord[i].NullsLast {
			dst.BeginField(st.Intern("nulls_last"))
			dst.WriteBool(true)
		}
		dst.EndStruct()
	}
	dst.EndList()
}

func (s *Select) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype(dst, st, "select")
	dst.BeginField(st.Intern("cols"))
	EncodeBindings(s.Columns, dst, st)
	addfield(dst, st, "from", s.From)
	addfield(dst, st, "where", s.Where)
	addfield(dst, st, "having", s.Having)
	if len(s.GroupBy) > 0 {
		dst.BeginField(st.Intern("group_by"))
		EncodeBindings(s.GroupBy, dst, st)
	}
	if len(s.OrderBy) > 0 {
		dst.BeginField(st.Intern("order_by"))
		EncodeOrder(s.OrderBy, dst, st)
	}
	if s.Distinct {
		dst.BeginField(st.Intern("distinct"))
		dst.WriteBool(true)
	} else if s.DistinctExpr != nil {
		dst.BeginField(st.Intern("distinct_expr"))
		dst.BeginList(len(s.DistinctExpr))
		for i := range s.DistinctExpr {
			s.DistinctExpr[i].Encode(dst, st)
		}
		dst.EndList()
	}
	if s.Limit != nil {
		dst.BeginField(st.Intern("limit"))
		dst.WriteInt(int64(*s.Limit))
	}
	if s.Offset != nil {
		dst.BeginField(st.Intern("offset"))
		dst.WriteInt(int64(*s.Offset))
	}
	dst.EndStruct()
}

func fmtbinding(lst []Binding, dst *strings.Builder, redact bool) {
	for i := range lst {
		lst[i].text(dst, redact)
		if i != len(lst)-1 {
			dst.WriteString(", ")
		}
	}
}

func (s *Select) text(out *strings.Builder, redact bool) {
	out.WriteByte('(')
	s.write(out, redact, nil)
	out.WriteByte(')')
}

// Text is like ToString(s), but it
// does not insert parentheses around
// the query.
func (s *Select) Text() string {
	var out strings.Builder
	s.write(&out, false, nil)
	return out.String()
}

func (s *Select) write(out *strings.Builder, redact bool, into Node) {
	out.WriteString("SELECT ")
	if s.Distinct {
		out.WriteString("DISTINCT ")
	} else if s.DistinctExpr != nil {
		out.WriteString("DISTINCT ON (")
		for i := range s.DistinctExpr {
			if i > 0 {
				out.WriteString(", ")
			}

			s.DistinctExpr[i].text(out, redact)
		}
		out.WriteString(") ")
	}
	fmtbinding(s.Columns, out, redact)
	if into != nil {
		out.WriteString(" INTO ")
		into.text(out, redact)
	}
	if s.From != nil {
		out.WriteString(" FROM ")
		s.From.text(out, redact)
	}
	if s.Where != nil {
		out.WriteString(" WHERE ")
		s.Where.text(out, redact)
	}
	if s.GroupBy != nil {
		out.WriteString(" GROUP BY ")
		for i := range s.GroupBy {
			s.GroupBy[i].text(out, redact)
			if i != len(s.GroupBy)-1 {
				out.WriteString(", ")
			}
		}
	}
	if s.Having != nil {
		out.WriteString(" HAVING ")
		s.Having.text(out, redact)
	}
	if s.OrderBy != nil {
		out.WriteString(" ORDER BY ")
		for i := range s.OrderBy {
			s.OrderBy[i].text(out, redact)
			if i != len(s.OrderBy)-1 {
				out.WriteString(", ")
			}
		}
		if s.Preserve {
			out.WriteString(" PRESERVE")
		}
	}
	if s.Limit != nil {
		out.WriteString(" LIMIT ")
		fmt.Fprintf(out, "%d", int64(*s.Limit))
	}
	if s.Offset != nil {
		out.WriteString(" OFFSET ")
		fmt.Fprintf(out, "%d", int64(*s.Offset))
	}
}

// HasDistinct returns if select implements 'SELECT DISTINCT ... FROM ...'
// or 'SELECT DISTINCT ON (...) ... FROM ...'
func (s *Select) HasDistinct() bool {
	return s.Distinct || s.DistinctExpr != nil
}

func decodeOrder(st *ion.Symtab, body []byte) ([]Order, error) {
	var out []Order
	_, err := ion.UnpackList(body, func(body []byte) error {
		var o Order
		_, err := ion.UnpackStruct(st, body, func(name string, inner []byte) error {
			var err error
			switch name {
			case "col":
				o.Column, _, err = Decode(st, inner)
			case "desc":
				o.Desc, _, err = ion.ReadBool(inner)
			case "nulls_last":
				o.NullsLast, _, err = ion.ReadBool(inner)
			}
			if err != nil {
				return fmt.Errorf("decoding field %s: %w", name, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if o.Column == nil {
			return fmt.Errorf("order element missing 'column' field")
		}
		out = append(out, o)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func DecodeBindings(st *ion.Symtab, body []byte) ([]Binding, error) {
	b, err := decodeBindings(st, body)
	if err != nil {
		err = fmt.Errorf("expr.DecodeBindings: %w", err)
	}
	return b, err
}

func decodeBindings(st *ion.Symtab, body []byte) ([]Binding, error) {
	var out []Binding
	_, err := ion.UnpackList(body, func(body []byte) error {
		var b Binding
		_, err := ion.UnpackStruct(st, body, func(name string, inner []byte) error {
			switch name {
			case "expr":
				exp, _, err := Decode(st, inner)
				if err != nil {
					return err
				}
				b.Expr = exp
			case "bind":
				str, _, err := ion.ReadString(inner)
				if err != nil {
					return err
				}
				b.As(str)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if b.Expr == nil {
			return fmt.Errorf("binding element missing 'expr' field")
		}
		out = append(out, b)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func decodeDistinctExpr(st *ion.Symtab, body []byte) ([]Node, error) {
	var out []Node
	_, err := ion.UnpackList(body, func(body []byte) error {
		n, _, err := Decode(st, body)
		if err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})

	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Select) setfield(name string, st *ion.Symtab, body []byte) error {
	var err error
	switch name {
	case "cols":
		s.Columns, err = decodeBindings(st, body)
	case "from":
		var e Node
		e, _, err = Decode(st, body)
		if err != nil {
			return err
		}
		f, ok := e.(From)
		if !ok {
			return fmt.Errorf("%q not valid in FROM position", e)
		}
		s.From = f
	case "where":
		s.Where, _, err = Decode(st, body)
	case "having":
		s.Having, _, err = Decode(st, body)
	case "group_by":
		s.GroupBy, err = decodeBindings(st, body)
	case "order_by":
		s.OrderBy, err = decodeOrder(st, body)
	case "distinct":
		s.Distinct, _, err = ion.ReadBool(body)
	case "distinct_expr":
		s.DistinctExpr, err = decodeDistinctExpr(st, body)
	case "limit":
		var i int64
		i, _, err = ion.ReadInt(body)
		if err != nil {
			return err
		}
		lim := Integer(i)
		s.Limit = &lim
	case "offset":
		var i int64
		i, _, err = ion.ReadInt(body)
		if err != nil {
			return err
		}
		off := Integer(i)
		s.Offset = &off
	}
	return err
}
