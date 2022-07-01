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

	"github.com/SnellerInc/sneller/ion"
)

// TypeError is the error type returned
// from Check when an expression is ill-typed.
type TypeError struct {
	At  Node
	Msg string
}

// SyntaxError is the error type
// returned from Check when an
// expression has illegal syntax.
type SyntaxError struct {
	At  Node
	Msg string
}

// Error implements error
func (t *TypeError) Error() string {
	return fmt.Sprintf("%q is ill-typed: %s", ToString(t.At), t.Msg)
}

func (s *SyntaxError) Error() string {
	if s.At != nil {
		return fmt.Sprintf("%q %s", ToString(s.At), s.Msg)
	}
	return s.Msg
}

func errat(err error, whence Node) {
	switch e := err.(type) {
	case *TypeError:
		e.At = whence
	case *SyntaxError:
		e.At = whence
	}
}

func errtype(e Node, msg string) *TypeError {
	return &TypeError{At: e, Msg: msg}
}

func errsyntax(msg string) *SyntaxError {
	return &SyntaxError{Msg: msg}
}

// Hint is an argument that can be
// supplied to type-checking operations
// to refine the type of nodes that have
// types that would otherwise be unknown
// to the query planner.
type Hint interface {
	TypeOf(e Node) TypeSet
}

// HintFn is a function that implements Hint
type HintFn func(Node) TypeSet

func (h HintFn) TypeOf(e Node) TypeSet {
	return h(e)
}

// NoHint is the empty Hint
func NoHint(Node) TypeSet {
	return AnyType
}

type checker interface {
	check(Hint) error
}

type checkwalk struct {
	errors  []error
	hint    Hint
	inTable bool
	tdepth  int
}

func (c *checkwalk) errorf(f string, args ...interface{}) {
	c.errors = append(c.errors, errsyntaxf(f, args...))
}

type checktable struct {
	parent *checkwalk
}

func (c *checktable) errorf(f string, args ...interface{}) {
	c.parent.errorf(f, args...)
}

func (c *checktable) Visit(n Node) Visitor {
	if n == nil {
		return nil
	}
	// TODO: allow list literals in table position
	switch t := n.(type) {
	case *Builtin:
		if !t.isTable() {
			c.errorf("cannot use %s in table position", ToString(n))
		}
		return c.parent
	case *Path:
		// ok
		return nil
	case *Select:
		// ok
		return c.parent
	case String:
		// FIXME: allowed for now, but really shouldn't be...
		return nil
	case *Appended:
		return c.parent
	default:
		c.errorf("cannot use %s in table position", ToString(n))
		return nil
	}
}

func (c *checkwalk) Visit(n Node) Visitor {
	if n == nil {
		return nil
	}
	ce, ok := n.(checker)
	if ok {
		err := ce.check(c.hint)
		if err != nil {
			c.errors = append(c.errors, err)
			return nil
		}
	}
	if _, ok := n.(*Appended); ok {
		c.errorf("cannot use %s in non-table position", ToString(n))
		return nil
	}
	if b, ok := n.(*Builtin); ok {
		if b.isTable() {
			c.errors = append(c.errors, errsyntaxf("cannot use %s in non-table position", ToString(n)))
			return nil
		}
	}
	if _, ok := n.(*Table); ok {
		return &checktable{parent: c}
	}
	return c
}

func combine(err []error) error {
	if len(err) == 1 {
		return err[0]
	}
	return fmt.Errorf("%w and %d other errors", err[0], len(err)-1)
}

// Check walks the AST given by n
// and performs rudimentary sanity-checking
// on all of the values in the tree.
func Check(n Node) error {
	return CheckHint(n, HintFn(NoHint))
}

// CheckHint performs the same sanity-checking
// as Check, except that it uses additional type-hint
// information.
func CheckHint(n Node, h Hint) error {
	c := &checkwalk{hint: h}
	Walk(c, n)
	if c.inTable || c.tdepth > 0 {
		return fmt.Errorf("expr.Check: unexpected table depth %d", c.tdepth)
	}
	if c.errors == nil {
		return nil
	}
	return combine(c.errors)
}

func (n *Not) check(h Hint) error {
	if !TypeOf(n.Expr, h).Logical() {
		return errtype(n, "can't compute NOT of non-logical expression")
	}
	return nil
}

// logical operations need boolean-typed args
func (l *Logical) check(h Hint) error {
	if !TypeOf(l.Left, h).Logical() {
		return errtype(l, "left-hand-side not a logical expression")
	}
	if !TypeOf(l.Right, h).Logical() {
		return errtype(l, "right-hand-side not a logical expression")
	}
	return nil
}

func (c *Comparison) check(h Hint) error {
	if c.Op == Like || c.Op == Ilike {
		_, ok := c.Right.(String)
		if !ok {
			return errsyntax("LIKE requires a literal string on the right-hand-side")
		}
		if t := TypeOf(c.Left, h); t&StringType == 0 {
			return errtype(c, "lhs of LIKE/ILIKE is never a string")
		}
		return nil
	}
	oktypes := AnyType &^ MissingType
	if c.Op.ordinal() {
		oktypes = NumericType | TimeType // only types supported for ordinal comparison for now
	}
	lt := TypeOf(c.Left, h) & oktypes
	rt := TypeOf(c.Right, h) & oktypes
	if lt&rt == 0 {
		return errtype(c, "lhs and rhs of comparison are never comparable")
	}
	return nil
}

// numeric returns whether or not
// a node yields a numeric result
func numeric(n Node, h Hint) bool {
	return TypeOf(n, h)&NumericType != 0
}

func (u *UnaryArith) check(h Hint) error {
	if !numeric(u.Child, h) {
		return errtype(u, "argument is not numeric")
	}
	return nil
}

func (a *Arithmetic) check(h Hint) error {
	if a.Op == DivOp {
		n, ok := a.Right.(number)
		if ok {
			r := n.rat()
			if r.IsInt() && r.Num().IsInt64() && r.Num().Int64() == 0 {
				return errtype(a, "division by zero")
			}
		}
	}
	if !numeric(a.Left, h) || (a.Right != nil && !numeric(a.Right, h)) {
		return errtype(a, "arguments are not numeric")
	}
	return nil
}

func (c *Case) check(h Hint) error {
	for i := range c.Limbs {
		if !TypeOf(c.Limbs[i].When, h).Contains(ion.BoolType) {
			return errtype(c.Limbs[i].When, "not a valid WHEN clause; doesn't evaluate to a boolean")
		}
	}
	return nil
}

func (c *Cast) check(h Hint) error {
	ft := TypeOf(c.From, h)
	switch c.To {
	case SymbolType, DecimalType:
		return errsyntaxf("unsupported cast %q", c)
	case StringType:
		if ft&(StringType|IntegerType) == 0 {
			return errtype(c, "unsupported cast will never succeed")
		}
	case StructType, ListType, TimeType:
		// for each of these types, we only support
		// no-op casting, so if we can determine statically
		// that we will be doing a meaningful cast, then return
		// an error rather than silently converting to MISSING...
		if ft&c.To == 0 {
			return errtype(c, "unsupported cast will never succeed")
		}
	}
	return nil
}

type visitfn func(Node) Visitor

func (v visitfn) Visit(e Node) Visitor {
	return v(e)
}

func containsAggregate(e Node) (bool, error) {
	var fn visitfn
	seen := false
	var err error
	fn = func(e Node) Visitor {
		if a, ok := e.(*Aggregate); ok {
			seen = true
			sub, suberr := containsAggregate(a.Inner)
			if suberr != nil {
				err = suberr
			} else if sub {
				err = errsyntaxf("%s contains an aggregate-within-aggregate", ToString(a))
			}
			return nil
		}
		return fn
	}
	Walk(fn, e)
	return seen, err
}
