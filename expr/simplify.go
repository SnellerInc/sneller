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
	"math/big"
	"strings"

	"github.com/SnellerInc/sneller/ion"
)

// TypeSet is a set of ion types;
// type #15 is the MISSING type
//
// Value expression nodes can
// produce their TypeSet (see TypeOf),
// which lets the AST checker perform
// some rudimentary type-checking to
// determine if the semantics of
// the operations are incompatible.
type TypeSet uint16

const (
	// AnyType is the TypeSet that
	// contains all types.
	AnyType     TypeSet = 0xffff
	MissingType TypeSet = (1 << 15)
	BoolType    TypeSet = (1 << ion.BoolType)
	// LogicalType is the return type
	// of logical operations
	LogicalType TypeSet = (1 << ion.BoolType) | MissingType
	// UnsignedType is the return type
	// of operations that produce only
	// unsigned integers
	UnsignedType TypeSet = (1 << ion.UintType)
	// IntegerType is the return type
	// of operations that produce only
	// signed and unsigned integers
	IntegerType TypeSet = UnsignedType | (1 << ion.IntType)
	FloatType   TypeSet = (1 << ion.FloatType)
	// NumericType is the return type
	// of number operations
	NumericType TypeSet = IntegerType | (1 << ion.FloatType)
	StringType  TypeSet = (1 << ion.StringType)
	TimeType    TypeSet = (1 << ion.TimestampType)
	ListType    TypeSet = (1 << ion.ListType)
	StructType  TypeSet = (1 << ion.StructType)
	DecimalType TypeSet = (1 << ion.DecimalType)
	SymbolType  TypeSet = (1 << ion.SymbolType)
	NullType    TypeSet = (1 << ion.NullType)
)

// Only returns whether or not t
// contains only the types in set.
// (In other words, Only computes
// whether or not the intersection
// of t and set is equal to t.)
func (t TypeSet) Only(set TypeSet) bool {
	return (t &^ set) == 0
}

func (t TypeSet) AnyOf(set TypeSet) bool {
	return (t & set) != 0
}

func (t TypeSet) String() string {
	var str strings.Builder
	first := true
	for i := 0; i < 15; i++ {
		if t&(1<<i) != 0 {
			if !first {
				str.WriteString("|")
			}
			str.WriteString(ion.Type(i).String())
			first = false
		}
	}
	if t&MissingType != 0 {
		if !first {
			str.WriteString("|")
		}
		str.WriteString("MISSING")
	}
	return str.String()
}

// Comparable returns whether or not
// two values can be compared against
// one another under ordinary typing rules
func (t TypeSet) Comparable(other TypeSet) bool {
	// we don't care about possible MISSING
	// values; if there are no concrete overlaps,
	// then the result is deterministically MISSING
	return (t&other)&^MissingType != 0
}

// Contains returns whether or not a TypeSet
// contains a particular ion type
func (t TypeSet) Contains(it ion.Type) bool {
	return (t & (1 << TypeSet(it))) != 0
}

// MaybeMissing returns whether or not the
// type set includes the MISSING value
func (t TypeSet) MaybeMissing() bool {
	return (t >> 15) != 0
}

// Logical returns whether or not the
// type set includes the boolean type
// (in other words, whether it is sensible
// to use this type in a logical expression)
func (t TypeSet) Logical() bool {
	return t.Contains(ion.BoolType)
}

func oneType(v ion.Type) TypeSet {
	return 1 << uint16(v)
}

func types(args ...ion.Type) TypeSet {
	out := TypeSet(0)
	for i := range args {
		out |= oneType(args[i])
	}
	return out
}

func typesOrMissing(args ...ion.Type) TypeSet {
	return types(args...) | 1<<15
}

type simplifier interface {
	simplify(Hint) Node
}

type maybetrue interface {
	isTrue(Hint) Node
}

type number interface {
	rat() *big.Rat
}

// Simplifier returns a Rewriter
// that performs bottom-up simplification
// of expressions using the given Hint
func Simplifier(h Hint) Rewriter {
	return simplerw{h}
}

// LogicSimplifier returns a Rewriter
// that performs bottom-up simplification
// of logical expressions using the given Hint
func LogicSimplifier(h Hint) Rewriter {
	return logicrw{h}
}

// Simplify attempts to perform some
// algebraic simplifications of 'n'
// and returns the simplified node.
// If no simplification can be performed,
// 'n' itself is returned.
func Simplify(n Node, h Hint) Node {
	return Rewrite(Simplifier(h), n)
}

// rewriter that applies simplifications
type simplerw struct {
	Hint
}

func (s simplerw) Rewrite(n Node) Node {
	if rs, ok := n.(simplifier); ok {
		n = rs.simplify(s.Hint)
	}
	return n
}

func (s simplerw) Walk(n Node) Rewriter { return s }

type logicrw struct {
	Hint
}

func (l logicrw) Rewrite(n Node) Node {
	if rs, ok := n.(simplifier); ok {
		n = rs.simplify(l.Hint)
	}
	if mb, ok := n.(maybetrue); ok {
		n = mb.isTrue(l.Hint)
	}
	return n
}

func (l logicrw) Walk(n Node) Rewriter { return l }

// SimplifyLogic is similar to Simplify,
// except that it performs additional simplifications
// assuming that the result of the expression is implicitly
// tested against 'IS TRUE'.
// (In other words, this simplifier performs optimizations
// that are only legal inside a WHERE clause.)
func SimplifyLogic(n Node, h Hint) Node {
	return Rewrite(logicrw{h}, n)
}

func not(x Node, h Hint) Node {
	if i, ok := x.(logical); ok {
		if nv := i.invert(); nv != nil {
			return nv
		}
	}
	return (&Not{Expr: x}).simplify(h)
}

func null(x Node, h Hint) bool {
	_, ok := x.(Null)
	if ok {
		return true
	}
	return TypeOf(x, h) == NullType
}

func (c *Comparison) isTrue(h Hint) Node {
	if miss(c.Left, h) || miss(c.Right, h) || null(c.Left, h) || null(c.Right, h) {
		return Bool(false)
	}

	// when we are comparing boolean-typed values
	// AND we are in logical context, we can simply
	// perform XOR or XNOR of the results
	lrt := TypeOf(c.Left, nil) & TypeOf(c.Right, nil)
	if lrt == oneType(ion.BoolType) || lrt == typesOrMissing(ion.BoolType) {
		switch c.Op {
		case Equals:
			return Xnor(c.Left, c.Right)
		case NotEquals:
			return Xor(c.Left, c.Right)
		default:
			return Bool(false)
		}
	}
	return c
}

// isTrue returns a more aggressively optimized
// version of the logical expression assuming that
// we will treat MISSING as FALSE for logical purposes
func (l *Logical) isTrue(h Hint) Node {
	if miss(l.Left, h) || miss(l.Right, h) || null(l.Left, h) || null(l.Right, h) {
		return Bool(false)
	}
	if Equivalent(l.Left, l.Right) {
		switch l.Op {
		case OpXor:
			return Bool(false)
		case OpXnor:
			// true when not missing
			return SimplifyLogic(Is(l.Left, IsNotMissing), h)
		}
	}
	return l
}

func (l *Logical) simplify(h Hint) Node {
	if Equivalent(l.Left, l.Right) {
		switch l.Op {
		case OpAnd, OpOr:
			// A OR/AND A -> A
			return l.Left
		}
	}
	left := l.Left
	right := l.Right
	// canonicalize ordering so
	// a constant boolean will appear
	// on the lhs if it is present
	if _, ok := l.Right.(Bool); ok {
		left, right = right, left
	}
	if b, ok := left.(Bool); ok {
		switch l.Op {
		case OpAnd:
			// true AND expr -> expr
			if b {
				return right
			}
			// false AND expr -> false
			return b
		case OpOr:
			// true OR expr -> true
			if b {
				return b
			}
			// false OR expr -> expr
			return right
		case OpXor:
			// true XOR expr -> NOT expr
			if b {
				return not(right, h)
			}
			// false XOR expr -> expr
			return right
		case OpXnor:
			// true XNOR expr -> expr
			if b {
				return right
			}
			// false XNOR expr -> NOT expr
			return not(right, h)
		}
	}
	l.Left = left
	l.Right = right
	return l
}

func constcmp(op CmpOp, left, right *big.Rat) Bool {
	switch op {
	case Greater:
		return Bool(left.Cmp(right) > 0)
	case GreaterEquals:
		return Bool(left.Cmp(right) >= 0)
	case Less:
		return Bool(left.Cmp(right) < 0)
	case LessEquals:
		return Bool(left.Cmp(right) <= 0)
	case Equals:
		return Bool(left.Cmp(right) == 0)
	case NotEquals:
		return Bool(left.Cmp(right) != 0)
	default:
		panic("???")
	}
}

type logical interface {
	invert() Node
}

func immediate(x Node) bool {
	_, ok := x.(Constant)
	return ok
}

// canonicalize the comparison expression
// by rotating the immediate argument to
// the right-hand-side, if it is present
func (c *Comparison) canonical() *Comparison {
	if !immediate(c.Left) || immediate(c.Right) {
		return c
	}
	c.Op = c.Op.Flip()
	c.Left, c.Right = c.Right, c.Left
	return c
}

func miss(e Node, h Hint) bool {
	_, ok := e.(Missing)
	return ok || TypeOf(e, h) == MissingType
}

// isSubstringSearchPattern determines whether the provided str contains a valid substring search pattern %pattern% used by LIKE
func isSubstringSearchPattern(str string) (term string, ok bool) {
	const wildcard = "%"
	if strings.HasPrefix(str, wildcard) && strings.HasSuffix(str, wildcard) {
		keyword := strings.TrimPrefix(strings.TrimSuffix(str, wildcard), wildcard)
		if !strings.ContainsAny(keyword, "_") {
			return keyword, true
		}
	}
	return "", false
}

// isRegularString determines whether the provided str is a normal string that should not be used by LIKE but by EQUALS
func isRegularString(str string) bool {
	return !strings.ContainsAny(str, "%_")
}

// cmpCase pushes a comparison into CASE limbs
// so that the result-set of the case can be determined
// statically to be a boolean result (these are cheaper
// for the back-end to handle)
func cmpCase(c *Case, rewrite func(when Node) Node) *Case {
	for i := range c.Limbs {
		c.Limbs[i].Then = rewrite(c.Limbs[i].Then)
	}
	if c.Else == nil {
		c.Else = Missing{}
	} else {
		c.Else = rewrite(c.Else)
	}
	c.Valence = "logical"
	return c
}

func (c *Comparison) simplify(h Hint) Node {
	c.Left = missingUnless(c.Left, h, ^(MissingType | NullType))
	c.Right = missingUnless(c.Right, h, ^(MissingType | NullType))

	// equivalence operations are MISSING
	// unless the types overlap, so push
	// down that hint:
	if c.Op == NotEquals || c.Op == Equals {
		intersect := TypeOf(c.Left, h) & TypeOf(c.Right, h)
		c.Left = missingUnless(c.Left, h, intersect)
		c.Right = missingUnless(c.Right, h, intersect)
	}

	left := c.Left
	right := c.Right
	if miss(left, h) || miss(right, h) {
		return Missing{}
	}

	ln, okl := left.(number)
	rn, okr := right.(number)
	if okr && okl {
		if c.Op == Like || c.Op == Ilike {
			return Missing{}
		}
		return constcmp(c.Op, ln.rat(), rn.rat())
	}

	// if the lhs or rhs is a CASE expression,
	// insert the comparisons into the arms of the case
	// expression directly, provided that the limbs
	// of the case expression are not basic path expressions.
	if cs, ok := left.(*Case); ok && !cs.IsPathLimbs() {
		return cmpCase(cs, func(when Node) Node {
			return Compare(c.Op, when, right).(simplifier).simplify(h)
		}).simplify(h)
	}
	if cs, ok := right.(*Case); ok && !cs.IsPathLimbs() {
		return cmpCase(cs, func(when Node) Node {
			return Compare(c.Op, left, when).(simplifier).simplify(h)
		}).simplify(h)
	}

	switch c.Op {
	case Equals, NotEquals:
		// for equality and inequality,
		// we can constprop the result
		// if we have immediates or identical
		// expressions on both sides

		// <expr> = <expr> -> TRUE or MISSING
		// <expr> <> <expr> -> FALSE or MISSING
		if Equivalent(left, right) {
			// if the expression is never NULL or MISSING,
			// then the result is trivial:
			if TypeOf(left, h)&(MissingType|NullType) == 0 {
				return Bool(c.Op == Equals)
			}
			// CASE WHEN <expr> IS NOT NULL THEN <TRUE/FALSE> ELSE MISSING
			return &Case{
				Limbs: []CaseLimb{{
					When: Is(left, IsNotNull),
					Then: Bool(c.Op == Equals),
				}},
				Else:    Missing{},
				Valence: "logical",
			}
		}
		// we know <left> and <right> are not
		// equal numbers or equivalent expressions,
		// so if they are both immediates they
		// must not be equal
		if immediate(left) && immediate(right) {
			return Bool(c.Op == NotEquals)
		}
		// if this is a boolean comparison,
		// turn it into an Xnor/Xor op
		if TypeOf(left, h).Only(LogicalType) && TypeOf(right, h).Only(LogicalType) {
			if c.Op == Equals {
				return Xnor(left, right)
			}
			return Xor(left, right)
		}

		if lhs, ok := left.(*Builtin); ok {
			switch lhs.Func {
			case Upper:
				if c.Op == Equals {
					rhsStr, ok := right.(String)
					if ok {
						if strings.ToUpper(string(rhsStr)) != string(rhsStr) {
							// (UPPER(x) = "fred") -> FALSE
							return Bool(false)
						}
						// (UPPER(z.name) = "FRED") -> EQUALS_CI(z.name, "FRED")
						return Call("EQUALS_CI", lhs.Args[0], right)
					}
				}
			case Lower:
				if c.Op == Equals {
					rhsStr, ok := right.(String)
					if ok {
						if strings.ToLower(string(rhsStr)) != string(rhsStr) {
							// (LOWER(z.name) = "FRED") -> FALSE
							return Bool(false)
						}
						// (LOWER(z.name) = "fred" -> EQUALS_CI(z.name, "fred")
						return Call("EQUALS_CI", lhs.Args[0], right)
					}
				}
			}
		}
	case Like:
		if lhs, ok := left.(*Builtin); ok {
			switch lhs.Func {
			case Upper:
				rhsStr := string(right.(String))
				if strings.ToUpper(rhsStr) != rhsStr {
					// UPPER(z.name) LIKE "%fred%" -> FALSE
					return Bool(false)
				}
				if rhsStr2, ok2 := isSubstringSearchPattern(rhsStr); ok2 {
					// UPPER(z.name) LIKE "%FRED%" -> CONTAINS_CI(z.name, "FRED")
					return Call("CONTAINS_CI", lhs.Args[0], String(rhsStr2))
				} else if isRegularString(rhsStr) {
					// UPPER(z.name) LIKE "FRED" -> EQUALS_CI(z.name, "FRED")
					return Call("EQUALS_CI", lhs.Args[0], String(rhsStr))
				}
			case Lower:
				rhsStr := string(right.(String))
				if strings.ToLower(rhsStr) != rhsStr {
					// LOWER(z.name) LIKE "%FRED%" -> FALSE
					return Bool(false)
				}
				if rhsStr2, ok2 := isSubstringSearchPattern(rhsStr); ok2 {
					// LOWER(z.name) LIKE "%fred%" -> CONTAINS_CI(z.name, "fred")
					return Call("CONTAINS_CI", lhs.Args[0], String(rhsStr2))
				} else if isRegularString(rhsStr) {
					// LOWER(z.name) LIKE "fred" -> EQUALS_CI(z.name, "fred")
					return Call("EQUALS_CI", lhs.Args[0], String(rhsStr))
				}
			}
		}
	}
	return c.canonical()
}

func constmath(op ArithOp, left, right *big.Rat) Node {
	out := new(big.Rat)
	switch op {
	case AddOp:
		return (*Rational)(out.Add(left, right))
	case SubOp:
		return (*Rational)(out.Sub(left, right))
	case DivOp:
		if right.Sign() == 0 {
			return Missing{}
		}
		return (*Rational)(out.Quo(left, right))
	case MulOp:
		return (*Rational)(out.Mul(left, right))
	case ModOp:
		if right.Sign() == 0 {
			return Missing{}
		}
		return (*Rational)(out.Quo(left, right))

	case BitAndOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()
		return (*Rational)(out.SetInt64(a & b))

	case BitOrOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()
		return (*Rational)(out.SetInt64(a | b))

	case BitXorOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()
		return (*Rational)(out.SetInt64(a ^ b))

	case ShiftLeftLogicalOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()

		if b > 63 {
			return (*Rational)(out.SetInt64(0))
		}
		return (*Rational)(out.SetInt64(a << b))

	case ShiftRightArithmeticOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()

		if b > 63 {
			return (*Rational)(out.SetInt64(a >> 63))
		}
		return (*Rational)(out.SetInt64(a >> b))

	case ShiftRightLogicalOp:
		a := roundBigRat(left, roundTruncOp).Num().Int64()
		b := roundBigRat(right, roundTruncOp).Num().Int64()

		if b > 63 {
			return (*Rational)(out.SetInt64(0))
		}
		return (*Rational)(out.SetInt64(int64(uint64(a) >> b)))

	default:
		panic("???")
	}
}

func (u *UnaryArith) canonical(h Hint) *UnaryArith {
	return u
}

type roundOp uint8

const (
	roundNearestOp roundOp = iota
	roundEvenOp
	roundTruncOp
	roundFloorOp
	roundCeilOp
)

func roundBigRat(value *big.Rat, op roundOp) *big.Rat {
	one := new(big.Int).SetUint64(1)
	denom := value.Denom()

	if denom.Cmp(one) <= 0 {
		return value
	}

	halfDenom := new(big.Int).Div(denom, new(big.Int).SetInt64(2))
	p, q := new(big.Int).DivMod(value.Num(), denom, new(big.Int))

	switch op {
	case roundNearestOp:
		if q.Cmp(halfDenom) >= 0 {
			p.Add(p, one)
		}
	case roundEvenOp:
		cmp := q.Cmp(halfDenom)
		if cmp > 0 {
			p.Add(p, one)
		} else if cmp == 0 {
			odd := new(big.Int).Set(p)
			odd.Abs(odd)
			odd.And(odd, one)
			if odd.Cmp(one) == 0 {
				p.Add(p, one)
			}
		}
	case roundTruncOp:
		if q.Cmp(one) >= 0 && value.Num().Sign() < 0 {
			p.Add(p, one)
		}
	case roundFloorOp:
		// DivMod actually floors `p` so we already have the required number.
	case roundCeilOp:
		if q.Cmp(one) >= 0 {
			p.Add(p, one)
		}
	default:
		panic("invalid rounding operation passed to roundBigRat")
	}

	return new(big.Rat).SetFrac(p, one)
}

func simplifyRoundOp(h Hint, args []Node, op roundOp) Node {
	if len(args) != 1 {
		return nil
	}

	args[0] = missingUnless(args[0], h, NumericType)
	if cn, ok := args[0].(number); ok {
		return (*Rational)(roundBigRat(cn.rat(), op))
	}

	return nil
}

func simplifyAbs(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}

	args[0] = missingUnless(args[0], h, NumericType)
	if cn, ok := args[0].(number); ok {
		return (*Rational)(new(big.Rat).Abs(cn.rat()))
	}

	return nil
}

func simplifySign(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}

	args[0] = missingUnless(args[0], h, NumericType)
	if cn, ok := args[0].(number); ok {
		return (*Rational)(new(big.Rat).SetInt64(int64(cn.rat().Sign())))
	}

	return nil
}

func simplifyRound(h Hint, args []Node) Node     { return simplifyRoundOp(h, args, roundNearestOp) }
func simplifyRoundEven(h Hint, args []Node) Node { return simplifyRoundOp(h, args, roundEvenOp) }
func simplifyTrunc(h Hint, args []Node) Node     { return simplifyRoundOp(h, args, roundTruncOp) }
func simplifyFloor(h Hint, args []Node) Node     { return simplifyRoundOp(h, args, roundFloorOp) }
func simplifyCeil(h Hint, args []Node) Node      { return simplifyRoundOp(h, args, roundCeilOp) }

func (u *UnaryArith) simplify(h Hint) Node {
	u.Child = missingUnless(u.Child, h, NumericType)
	u = u.canonical(h)

	// Arithmetic operation with MISSING is MISSING.
	if miss(u.Child, h) {
		return Missing{}
	}

	if cn, ok := u.Child.(number); ok {
		switch u.Op {
		case NegOp:
			return (*Rational)(new(big.Rat).Neg(cn.rat()))
		}
	}

	return u
}

func (a *Arithmetic) canonical(h Hint) *Arithmetic {
	if a.Right == nil {
		return a
	}
	li := immediate(a.Left)
	ri := immediate(a.Right)
	if ri == li {
		// we're only interested in the case
		// where one side is an immediate and
		// the other isn't
		return a
	}
	if li {
		switch a.Op {
		case AddOp, MulOp:
			// op is commutative, so we can flip easily
			a = &Arithmetic{Op: a.Op, Left: a.Right, Right: a.Left}
		case SubOp, DivOp:
			// we don't have reverse-subtract, or reverse-div,
			// so don't re-canonicalize for now
		default:
		}
	}
	// now we know the rhs is an immediate;
	// check for constant re-association
	if leftarith, ok := a.Left.(*Arithmetic); ok && leftarith.Op == a.Op {
		switch a.Op {
		case SubOp:
			// rotate ((x - 1) - 1) -> (x - (1 + 1))
			leftarith.Op = AddOp
			fallthrough
		case AddOp, MulOp:
			// rotate ((x + 1) + 1) -> (x + (1 + 1))
			// and then simplify the inner expression (again)
			a.Left, a.Right, leftarith.Left = leftarith.Left, leftarith, a.Right
			a.Right = Simplify(a.Right, h)
		default:
			// TODO: other re-associations
		}
	}
	return a
}

func (a *Arithmetic) simplify(h Hint) Node {
	a.Left = missingUnless(a.Left, h, NumericType)
	if a.Right != nil {
		a.Right = missingUnless(a.Right, h, NumericType)
	}

	a = a.canonical(h)
	left := a.Left
	right := a.Right
	// arithmetic with MISSING is MISSING
	if miss(left, h) || (right != nil && miss(right, h)) {
		return Missing{}
	}
	if right != nil {
		ln, okl := left.(number)
		rn, okr := right.(number)
		if okr && okl {
			return constmath(a.Op, ln.rat(), rn.rat())
		}
	}

	return a
}

func (t *Table) rewrite(r Rewriter) Node {
	t.Expr = Rewrite(r, t.Expr)
	return t
}

func (t *Table) Equals(x Node) bool {
	xt, ok := x.(*Table)
	return ok && t.explicit == xt.explicit && t.as == xt.as && t.Expr.Equals(xt.Expr)
}

func (t *Table) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype(dst, st, "table")
	dst.BeginField(st.Intern("expr"))
	t.Expr.Encode(dst, st)
	if t.Explicit() {
		dst.BeginField(st.Intern("bind"))
		dst.WriteString(t.Result())
	}
	dst.EndStruct()
}

func (t *Table) setfield(name string, st *ion.Symtab, body []byte) error {
	var err error
	switch name {
	case "expr":
		t.Expr, _, err = Decode(st, body)
	case "bind":
		str, _, err := ion.ReadString(body)
		if err != nil {
			return fmt.Errorf("reading expr.Table: %w", err)
		}
		t.As(str)
	}
	return err
}

func (j *Join) simplify(h Hint) Node {
	// <a> [LEFT] JOIN <b> ON TRUE -> <a> CROSS JOIN <b>
	if j.On == Bool(true) && (j.Kind == InnerJoin || j.Kind == LeftJoin) {
		j.Kind = CrossJoin
		j.On = nil
		return j
	}
	// <a> LEFT JOIN <b> ON FALSE -> <a>
	if j.On == Bool(false) && j.Kind == LeftJoin {
		return j.Left
	}
	return j
}

func (n *Not) simplify(h Hint) Node {
	n.Expr = missingUnless(n.Expr, h, BoolType)

	l, ok := n.Expr.(logical)
	if ok {
		nv := l.invert()
		if nv != nil {
			return nv
		}
	}
	// !MISSING -> MISSING
	// !NULL -> MISSING
	if miss(n.Expr, h) || null(n.Expr, h) {
		return Missing{}
	}
	return n
}

func (i *IsKey) simplify(h Hint) Node {
	if i.Key == IsTrue {
		i.Expr = SimplifyLogic(i.Expr, h)
	}
	ts := TypeOf(i.Expr, h)

	// if the lhs can never be missing,
	// then IS NOT MISSING is TRUE
	if !ts.MaybeMissing() && i.Key == IsNotMissing {
		return Bool(true)
	}
	// if the lhs is never bool-typed,
	// then IS TRUE / IS FALSE will always be FALSE,
	// and IS NOT TRUE / IS NOT FALSE will always be TRUE
	if !ts.Contains(ion.BoolType) {
		switch i.Key {
		case IsTrue, IsFalse:
			return Bool(false)
		case IsNotTrue, IsNotFalse:
			return Bool(true)
		}
	}

	if miss(i.Expr, h) {
		switch i.Key {
		case IsMissing, IsNotTrue, IsNotFalse:
			// MISSING IS MISSING -> TRUE
			// MISSING IS NOT (TRUE/FALSE) -> TRUE
			return Bool(true)
		default:
			return Bool(false)
		}
	}
	if null(i.Expr, h) {
		switch i.Key {
		case IsNull, IsNotMissing, IsNotFalse, IsNotTrue:
			// NULL IS (NULL | NOT MISSING | NOT FALSE | NOT TRUE)
			// -> TRUE
			return Bool(true)
		default:
			// NULL IS (NOT NULL | MISSING | FALSE | TRUE)
			// -> FALSE
			return Bool(false)
		}
	}
	// now do the reverse of the above:
	// if the argument is known to never be
	// NULL or MISSING, we can eliminate
	// the IS (NOT) NULL|MISSING ops
	etype := TypeOf(i.Expr, h)
	if etype&MissingType == 0 {
		switch i.Key {
		case IsMissing:
			return Bool(false)
		case IsNotMissing:
			return Bool(true)
		}
	}
	if etype&NullType == 0 {
		switch i.Key {
		case IsNull:
			return Bool(false)
		case IsNotNull:
			return Bool(true)
		}
	}

	if b, ok := i.Expr.(Bool); ok {
		// TRUE IS (TRUE / NOT FALSE) -> TRUE
		// FALSE IS (FALSE / NOT TRUE) -> TRUE
		return Bool(b == (i.Key == IsTrue || i.Key == IsNotFalse))
	}

	// non-bool immediate; won't match anything
	// except IS NOT MISSING or IS NOT NULL
	if immediate(i.Expr) {
		return Bool(i.Key == IsNotMissing || i.Key == IsNotNull)
	}

	// push the IS comparison into CASE
	if cs, ok := i.Expr.(*Case); ok {
		return Simplify(cmpCase(cs, func(when Node) Node {
			return Is(when, i.Key)
		}), h)
	}

	// when we evaluate MISSING or NOT MISSING
	// on a compound expression, we can simplify
	// this to a conjunction or disjunction
	// of MISSING / NOT MISSING expressions
	if i.Key == IsMissing || i.Key == IsNotMissing {
		// NOT x IS MISSING -> x IS MISSING, etc.
		if n, ok := i.Expr.(*Not); ok {
			return Simplify(Is(n.Expr, i.Key), h)
		}
	}
	return i
}

func (a *Aggregate) simplify(h Hint) Node {
	switch a.Op {
	case OpMin, OpMax, OpSum, OpAvg:
		a.Inner = missingUnless(a.Inner, h, NumericType)
	}
	// convert SUM(x) where 'x' is always an integer
	// to SUM_INT(x)
	if a.Op == OpSum && TypeOf(a.Inner, h)&^(IntegerType|MissingType) == 0 {
		a.Op = OpSumInt
	}

	if a.Filter != nil {
		iscount := (a.Op == OpCount || a.Op == OpCountDistinct)
		switch v := a.Filter.(type) {
		case Null, Missing:
			if iscount {
				return Integer(0)
			} else {
				return Null{}
			}
		case Bool:
			if v == Bool(true) {
				a.Filter = nil
			} else {
				if iscount {
					return Integer(0)
				} else {
					return Null{}
				}
			}
		}
	}

	return a
}

func (c *Case) filter(fn func(when, then Node) bool) {
	j := 0
	for i := 0; i < len(c.Limbs); i++ {
		if fn(c.Limbs[i].When, c.Limbs[i].Then) {
			c.Limbs[j] = c.Limbs[i]
			j++
		}
	}
	c.Limbs = c.Limbs[:j]
}

func (c *Case) isTrue(h Hint) Node {
	for i := range c.Limbs {
		c.Limbs[i].Then = SimplifyLogic(c.Limbs[i].Then, h)
	}
	if c.Else != nil {
		c.Else = SimplifyLogic(c.Else, h)
	} else {
		c.Else = Bool(false)
	}
	// see if we ended up short-circuiting anything
	return c.simplify(h)
}

// missingUnless simplifies a node
// taking into account that the calling
// expression will be MISSING unless
// the result type of the expression is
// a member of 'want'
func missingUnless(e Node, h Hint, want TypeSet) Node {
	c, ok := e.(*Case)
	if !ok {
		return e
	}
	var match Node
	var matchn int
	for i := range c.Limbs {
		t := TypeOf(c.Limbs[i].Then, h)
		if t&want == 0 {
			c.Limbs[i].Then = Missing{}
		} else {
			match = c.Limbs[i].Then
			matchn++
		}
	}
	elsetype := NullType
	if c.Else != nil {
		elsetype = TypeOf(c.Else, h)
	}
	if elsetype&want == 0 {
		c.Else = Missing{}
	} else {
		match = c.Else
		if match == nil {
			match = Null{}
		}
		matchn++
	}
	// if there is only one non-missing clause,
	// then simply evaluate that clause, since
	// it is the only semantically meaningful one
	if matchn == 1 {
		return match
	}
	return c.simplify(h)
}

func (c *Case) toHashLookup() (*Builtin, bool) {
	if c.Else != nil && c.Else != (Missing{}) {
		return nil, false
	}
	var lookup []Node
	for i := range c.Limbs {
		when := c.Limbs[i].When
		then := c.Limbs[i].Then
		eq, ok := when.(*Comparison)
		if !ok {
			return nil, false
		}
		if eq.Op != Equals {
			return nil, false
		}
		res, ok := then.(Constant)
		if !ok {
			return nil, false
		}
		left, right := eq.Left, eq.Right
		// canonicalization should have put
		// the comparison on the right-hand-side
		if lookup == nil {
			lookup = append(lookup, left)
		} else if !Equivalent(lookup[0], left) {
			return nil, false
		}
		c, ok := right.(Constant)
		if !ok {
			return nil, false
		}
		lookup = append(lookup, c, res)
	}
	return &Builtin{Func: HashLookup, Args: lookup}, true
}

func (c *Case) simplify(h Hint) Node {
	// limb conditions are evaluated in logical context
	for i := range c.Limbs {
		c.Limbs[i].When = SimplifyLogic(c.Limbs[i].When, h)
	}
	// first, strip any trivially false nodes
	c.filter(func(when, then Node) bool {
		b, ok := when.(Bool)
		if ok {
			return bool(b)
		}
		return true
	})
	// if there is a trivially-true limb,
	// set it to the ELSE clause and eliminate
	// the rest of the limbs
	for i := range c.Limbs {
		b, ok := c.Limbs[i].When.(Bool)
		if !ok {
			continue
		}
		if b {
			c.Else = c.Limbs[i].Then
			c.Limbs = c.Limbs[:i]
			break
		}
	}
	// while ELSE is the same as
	// the last condition, eliminate
	// the last condition
	for c.Else != nil && len(c.Limbs) > 0 && c.Limbs[len(c.Limbs)-1].Then.Equals(c.Else) {
		c.Limbs = c.Limbs[:len(c.Limbs)-1]
	}

	// finally, if there are no limbs
	// and we just have an ELSE, return that
	// (otherwise NULL)
	if len(c.Limbs) == 0 {
		if c.Else != nil {
			return c.Else
		}
		return Null{}
	}
	if ret, ok := c.toHashLookup(); ok {
		return ret
	}
	return c
}

// converts returns the set of types
// that can be converted into the given type
//
// NOTE: this will have to change as the VM
// grows more features...
func converts(to TypeSet) TypeSet {
	switch to {
	case MissingType, NullType:
		// we support any->null and any->missing
		return AnyType
	case BoolType:
		// we support int->bool and bool->bool
		return IntegerType | BoolType
	case FloatType, IntegerType:
		// we support conversion to/from
		// floats, ints, and bools (zero = false, otherwise true)
		return FloatType | IntegerType | BoolType
	case StringType:
		// we support int->string
		return IntegerType | StringType
	default:
		// to = to; we support converting
		// any other type to itself
		return to
	}
}

func (c *Cast) simplify(h Hint) Node {
	// discard any part of the input expression
	// that produces a result we cannot cast
	possible := converts(c.To)
	c.From = missingUnless(c.From, h, possible)
	ft := TypeOf(c.From, h)
	// NOTE: this is an intentional deviation
	// from the behavior of some AWS products
	// in order to make the implementation consistent:
	// ill-typed / unsupported conversions yield MISSING,
	// since that is the default behavior for an error in conversion
	if ft&possible == 0 {
		return Missing{}
	}
	if c.To == NullType {
		return Null{}
	}
	if c.To == MissingType {
		return Missing{}
	}
	// if the input type is always
	// the output type (modulo MISSING),
	// then the cast is a no-op
	if (ft &^ MissingType) == c.To {
		return c.From
	}

	// literal FP conversion constprop
	if c.To == FloatType {
		if fn, ok := c.From.(number); ok {
			rat := fn.rat()
			f, _ := rat.Float64()
			return Float(f)
		}
		if b, ok := c.From.(Bool); ok {
			if b {
				return Float(1.0)
			}
			return Float(0.0)
		}
	}

	// literal integer conversion constprop
	if c.To == IntegerType {
		if fn, ok := c.From.(number); ok {
			rat := fn.rat()
			if !rat.IsInt() {
				rat = roundBigRat(rat, roundTruncOp)
			}
			num := rat.Num()
			if num.IsInt64() {
				return Integer(num.Int64())
			}
			return (*Rational)(rat)
		}
		if b, ok := c.From.(Bool); ok {
			if b {
				return Integer(1)
			}
			return Integer(0)
		}
	}

	// literal string conversion constprop
	if c.To == StringType {
		if fn, ok := c.From.(number); ok {
			rat := fn.rat()
			if rat.IsInt() {
				return String(rat.RatString())
			}
		}
	}

	return c
}

func (m *Member) simplify(h Hint) Node {
	// when the first argument and
	// the values do not share overlapping
	// type descriptions, the constant
	// can be removed
	argtype := TypeOf(m.Arg, h)
	for i := 0; i < len(m.Values); i++ {
		vt := TypeOf(m.Values[i], h)
		if !vt.AnyOf(argtype) {
			// remove this entry and swap
			// it for the last one, then shorten the slice
			m.Values[i] = m.Values[len(m.Values)-1]
			m.Values = m.Values[:len(m.Values)-1]
			i--
			continue
		}
	}
	if len(m.Values) == 0 {
		// x IN () -> FALSE
		return Bool(false)
	}
	if len(m.Values) == 1 {
		return Simplify(Compare(Equals, m.Arg, m.Values[0]), h)
	}

	carg, ok := m.Arg.(Constant)
	if !ok {
		return m
	}
	for i := range m.Values {
		if carg.Equals(m.Values[i]) {
			return Bool(true)
		}
	}
	// if we have a Constant and
	// none of the arguments match,
	// this must be a false match
	return Bool(false)
}
