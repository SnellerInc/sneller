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

package vm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"

	"github.com/SnellerInc/sneller/regexp2"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

// compileLogical compiles a logical expression
// into an ssa program
func compileLogical(e expr.Node) (*prog, error) {
	p := new(prog)
	p.Begin()
	v, err := p.compileAsBool(e)
	if err != nil {
		return nil, err
	}
	p.Return(p.RowsMasked(p.ValidLanes(), v))
	return p, nil
}

// ret yields the raw return type of v
func (v *value) ret() ssatype {
	return ssainfo[v.op].rettype
}

// primary yields the primary return type of v
// (if a value returns more than one type,
// primary returns the type minus the mask
// and list components)
func (v *value) primary() ssatype {
	r := v.ret()
	if r == stBool {
		return r
	}
	r &^= stBool | stList
	return r
}

func compile(p *prog, e expr.Node) (*value, error) {
	switch n := e.(type) {
	case expr.Bool:
		return p.Constant(bool(n)), nil
	case expr.String:
		return p.Constant(string(n)), nil
	case expr.Integer:
		return p.Constant(int64(n)), nil
	case expr.Float:
		return p.Constant(float64(n)), nil
	case expr.Null:
		return p.Constant(nil), nil
	case *expr.Struct:
		return p.Constant(n.Datum()), nil
	case *expr.List:
		return p.Constant(n.Datum()), nil
	case *expr.Rational:
		r := (*big.Rat)(n)
		if r.IsInt() {
			num := r.Num()
			if !num.IsInt64() {
				return nil, fmt.Errorf("%s overflows int64", num)
			}
			return p.Constant(num.Int64()), nil
		}
		// ok if we lose some precision here, I guess...
		f, _ := r.Float64()
		return p.Constant(f), nil
	case *expr.Comparison:
		switch n.Op {
		case expr.Equals, expr.NotEquals:
			left, err := compile(p, n.Left)
			if err != nil {
				return nil, err
			}
			right, err := compile(p, n.Right)
			if err != nil {
				return nil, err
			}
			eq := p.Equals(left, right)
			if n.Op == expr.NotEquals {
				// FIXME: this isn't exactly right;
				// we don't want to return TRUE on MISSING...
				eq = p.Not(eq)
			}
			return eq, nil
		case expr.Like, expr.Ilike:
			left, err := p.compileAsString(n.Left)
			if err != nil {
				return nil, err
			}
			s, ok := n.Right.(expr.String)
			if !ok {
				panic("missed bad LIKE in type-checking...")
			}
			caseSensitive := n.Op == expr.Like
			return p.Like(left, string(s), caseSensitive), nil
		case expr.SimilarTo, expr.RegexpMatch, expr.RegexpMatchCi:
			left, err := p.compileAsString(n.Left)
			if err != nil {
				return nil, err
			}
			s, ok := n.Right.(expr.String)
			if !ok {
				return nil, fmt.Errorf("expected string expression")
			}
			//NOTE: We do not implement the escape char from the SQL SIMILAR TO syntax, backslash is the only used escape-char
			regexStr := string(s)
			if err := regexp2.IsSupported(regexStr); err != nil {
				return nil, fmt.Errorf("regex %v is not supported: %v", regexStr, err)
			}
			regexType := regexp2.SimilarTo
			if n.Op == expr.RegexpMatch {
				regexType = regexp2.Regexp
			} else if n.Op == expr.RegexpMatchCi {
				regexType = regexp2.RegexpCi
			}
			regex, err := regexp2.Compile(regexStr, regexType)
			if err != nil {
				return nil, err
			}
			dfaStore, err := regexp2.CompileDFA(regex, regexp2.MaxNodesAutomaton)
			if err != nil {
				return nil, fmt.Errorf("Error: %v; construction of DFA from regex %v failed", err, regex)
			}
			return p.RegexMatch(left, dfaStore)
		}

		left, err := compile(p, n.Left)
		if err != nil {
			return nil, err
		}
		right, err := compile(p, n.Right)
		if err != nil {
			return nil, err
		}
		switch n.Op {
		case expr.Less:
			return p.Less(left, right), nil
		case expr.LessEquals:
			return p.LessEqual(left, right), nil
		case expr.Greater:
			return p.Greater(left, right), nil
		case expr.GreaterEquals:
			return p.GreaterEqual(left, right), nil
		}
		return nil, fmt.Errorf("unhandled comparison expression %q", n)

	case *expr.UnaryArith:
		child, err := p.compileAsNumber(n.Child)
		if err != nil {
			return nil, err
		}

		switch n.Op {
		case expr.NegOp:
			return p.Neg(child), nil
		case expr.BitNotOp:
			return p.BitNot(child), nil
		}

		return nil, fmt.Errorf("unknown arithmetic expression %q", n)

	case *expr.Arithmetic:
		left, err := p.compileAsNumber(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.compileAsNumber(n.Right)
		if err != nil {
			return nil, err
		}
		switch n.Op {
		case expr.AddOp:
			return p.Add(left, right), nil
		case expr.SubOp:
			return p.Sub(left, right), nil
		case expr.MulOp:
			return p.Mul(left, right), nil
		case expr.DivOp:
			return p.Div(left, right), nil
		case expr.ModOp:
			return p.Mod(left, right), nil
		case expr.BitAndOp:
			return p.BitAnd(left, right), nil
		case expr.BitOrOp:
			return p.BitOr(left, right), nil
		case expr.BitXorOp:
			return p.BitXor(left, right), nil
		case expr.ShiftLeftLogicalOp:
			return p.ShiftLeftLogical(left, right), nil
		case expr.ShiftRightArithmeticOp:
			return p.ShiftRightArithmetic(left, right), nil
		case expr.ShiftRightLogicalOp:
			return p.ShiftRightLogical(left, right), nil
		}
		return nil, fmt.Errorf("unrecognized expression %q", n)

	case *expr.Logical:
		left, err := p.compileAsBool(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.compileAsBool(n.Right)
		if err != nil {
			return nil, err
		}
		switch n.Op {
		case expr.OpOr:
			return p.Or(left, right), nil
		case expr.OpAnd:
			return p.And(left, right), nil
		case expr.OpXor:
			// boolean =
			notmiss := p.And(p.notMissing(left), p.notMissing(right))
			return p.And(p.xor(left, right), notmiss), nil
		case expr.OpXnor:
			// boolean <>
			notmiss := p.And(p.notMissing(left), p.notMissing(right))
			return p.And(p.xnor(left, right), notmiss), nil
		}
		return nil, fmt.Errorf("unrecognized expression %q", n)
	case *expr.Not:
		inner, err := p.compileAsBool(n.Expr)
		if err != nil {
			return nil, err
		}
		return p.Not(inner), nil
	case *expr.Path:
		return compilepath(p, p.Dot(n.First, p.ValidLanes()), n.Rest)
	case *expr.IsKey:
		inner, err := compile(p, n.Expr)
		if err != nil {
			return nil, err
		}
		switch n.Key {
		case expr.IsNull:
			return p.isnull(inner), nil
		case expr.IsNotNull:
			return p.isnonnull(inner), nil
		case expr.IsMissing:
			return p.Not(p.notMissing(inner)), nil
		case expr.IsNotMissing:
			return p.notMissing(inner), nil
		case expr.IsTrue:
			return p.IsTrue(inner), nil
		case expr.IsFalse:
			return p.IsFalse(inner), nil
		case expr.IsNotTrue:
			// we always compute IS TRUE,
			// so IS NOT TRUE is the negation of that
			return p.IsNotTrue(inner), nil
		case expr.IsNotFalse:
			// either MISSING or TRUE
			return p.IsNotFalse(inner), nil
		}
		return nil, fmt.Errorf("unhandled IS: %q", n)
	case *expr.Builtin:
		return compilefunc(p, n, n.Args)
	case *expr.Case:
		return compilecase(p, n)
	case *expr.Cast:
		return p.compileCast(n)
	case *expr.Timestamp:
		return p.Constant(n.Value), nil
	case *expr.Member:
		return p.member(n.Arg, n.Values)
	case expr.Missing:
		return p.ssa0(skfalse), nil
	default:
		return nil, fmt.Errorf("unhandled expression %T %q", e, e)
	}
}

func (p *prog) compileAsBool(e expr.Node) (*value, error) {
	// double-check that the result of 'e'
	// is actually a boolean-typed value
	coerce := false
	switch e := e.(type) {
	case expr.Null:
		return p.ssa0(skfalse), nil
	case expr.Missing:
		return p.ssa0(skfalse), nil
	case *expr.Case:
		return p.compileLogicalCase(e)
	case *expr.Logical:
	case *expr.Comparison:
	case *expr.IsKey:
	case expr.Bool:
		if e {
			return p.ValidLanes(), nil
		}
		return p.ssa0(skfalse), nil
	case *expr.Builtin:
	case *expr.Not:
	case *expr.Member:
	case *expr.Path:
		coerce = true
	default:
		return nil, fmt.Errorf("%q is not a logical expression, but has type %T", e, e)
	}
	v, err := compile(p, e)
	if err != nil {
		return nil, err
	}
	// if we encountered a path expression
	// used in boolean context, insert an
	// explicit IS TRUE comparison against
	// the encoded bytes
	if coerce && v.op != skfalse {
		v = p.ssa2(sistrue, v, p.mask(v))
	}
	return v, nil
}

func (p *prog) compileAsNumber(e expr.Node) (*value, error) {
	if c, ok := e.(*expr.Case); ok {
		return p.compileNumericCase(c)
	}
	v, err := compile(p, e)
	if err != nil {
		return nil, err
	}
	// if we have a literal or a path expression,
	// then just return the result as-is and let the
	// conversion happen lazily
	if v.op == sliteral || v.primary() == stValue {
		return v, nil
	}
	if r := v.primary(); r&(stInt|stFloat) == 0 {
		if v.op == sinvalid {
			return nil, fmt.Errorf("compiling %s: %v", e, v.imm)
		}
		return nil, fmt.Errorf("can't compile %s as a number (instr %s)", e, v)
	}
	return v, nil
}

func (p *prog) compileAsTime(e expr.Node) (*value, error) {
	v, err := compile(p, e)
	if err != nil {
		return nil, err
	}
	if v.op == sliteral {
		return v, nil
	}
	if t := v.primary(); t != stValue && t != stTime && t != stTimeInt {
		if v.op == sinvalid {
			return nil, fmt.Errorf("compiling %s: %v", e, v.imm)
		}
		return nil, fmt.Errorf("can't compile %s as a timestamp (instr %s)", e, v)
	}
	return v, nil
}

func (p *prog) compileAsString(e expr.Node) (*value, error) {
	if c, ok := e.(*expr.Case); ok {
		return p.compileStringCase(c)
	}
	v, err := compile(p, e)
	if err != nil {
		return nil, err
	}
	if v.op == sliteral {
		return v, nil
	}
	switch v.primary() {
	case stString:
		return v, nil
	case stValue:
		v = p.unsymbolized(v)
		return p.ssa2(stostr, v, p.mask(v)), nil
	default:
		return nil, fmt.Errorf("cannot compile expression %s as a string", e)
	}
}

// handle FN(args...) expressions
func compilefunc(p *prog, b *expr.Builtin, args []expr.Node) (*value, error) {
	fn := b.Func

	if fn.IsDateAdd() {
		part, _ := fn.TimePart()
		val0, err0 := p.compileAsNumber(args[0])
		if err0 != nil {
			return nil, err0
		}

		val1, err1 := p.compileAsTime(args[1])
		if err1 != nil {
			return nil, err1
		}

		val := p.DateAdd(part, val0, val1)
		return val, nil
	}

	if fn.IsDateDiff() {
		part, _ := fn.TimePart()
		val0, err0 := p.compileAsTime(args[0])
		if err0 != nil {
			return nil, err0
		}

		val1, err1 := p.compileAsTime(args[1])
		if err1 != nil {
			return nil, err1
		}

		val := p.DateDiff(part, val0, val1)
		return val, nil
	}

	if fn.IsDateExtract() {
		part, _ := fn.TimePart()
		val0, err := p.compileAsTime(args[0])
		if err != nil {
			return nil, err
		}
		return p.DateExtract(part, val0), nil
	}

	if fn.IsDateTrunc() {
		part, _ := fn.TimePart()
		val, err := p.compileAsTime(args[0])
		if err != nil {
			return nil, err
		}

		if part == expr.DOW {
			dow, ok := args[1].(expr.Integer)
			if !ok {
				panic("DATE_TRUNC() requires day of week to be a constant")
			}
			return p.DateTruncWeekday(val, expr.Weekday(dow)), nil
		}

		return p.DateTrunc(part, val), nil
	}

	switch fn {
	case expr.BitCount:
		arg, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}
		return p.BitCount(arg), nil

	case expr.Pi:
		if len(args) != 0 {
			return nil, fmt.Errorf("%s cannot have arguments", fn)
		}

		return p.Constant(pi), nil

	case expr.Log:
		count := len(args)

		if count < 1 || count > 2 {
			return nil, fmt.Errorf("%s must have either 1 or 2 arguments", fn)
		}

		n, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		var val *value
		if count == 1 {
			// use LOG base 10 if no base was specified
			val = p.Log10(n)
		} else {
			base := n

			n, err = p.compileAsNumber(args[1])
			if err != nil {
				return nil, err
			}

			// logarithm of any base can be calculated as `log2(n) / log2(base)`
			val = p.Div(p.Log2(n), p.Log2(base))
		}

		return val, nil

	case expr.Degrees, expr.Radians:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s must have at exactly 1 argument", fn)
		}

		arg, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		var c float64
		if fn == expr.Degrees {
			c = radiansToDegrees
		} else {
			c = degreesToRadians
		}

		return p.Mul(arg, p.Constant(c)), nil

	case expr.Abs:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s must have at exactly 1 argument", fn)
		}

		arg, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		return p.Abs(arg), nil

	case expr.Sign:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s must have at exactly 1 argument", fn)
		}

		arg, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		return p.Sign(arg), nil

	case expr.Round, expr.RoundEven, expr.Trunc, expr.Floor, expr.Ceil,
		expr.Sqrt, expr.Cbrt,
		expr.Exp, expr.Exp2, expr.Exp10, expr.ExpM1,
		expr.Ln, expr.Ln1p, expr.Log2, expr.Log10,
		expr.Sin, expr.Cos, expr.Tan,
		expr.Asin, expr.Acos, expr.Atan:

		if len(args) != 1 {
			return nil, fmt.Errorf("%s must have at exactly 1 argument", fn)
		}

		arg, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		var val *value
		switch fn {
		case expr.Round:
			val = p.Round(arg)
		case expr.RoundEven:
			val = p.RoundEven(arg)
		case expr.Trunc:
			val = p.Trunc(arg)
		case expr.Floor:
			val = p.Floor(arg)
		case expr.Ceil:
			val = p.Ceil(arg)
		case expr.Sqrt:
			val = p.Sqrt(arg)
		case expr.Cbrt:
			val = p.Cbrt(arg)
		case expr.Exp:
			val = p.Exp(arg)
		case expr.Exp2:
			val = p.Exp2(arg)
		case expr.Exp10:
			val = p.Exp10(arg)
		case expr.ExpM1:
			val = p.ExpM1(arg)
		case expr.Ln:
			val = p.Ln(arg)
		case expr.Ln1p:
			val = p.Ln1p(arg)
		case expr.Log2:
			val = p.Log2(arg)
		case expr.Log10:
			val = p.Log10(arg)
		case expr.Sin:
			val = p.Sin(arg)
		case expr.Cos:
			val = p.Cos(arg)
		case expr.Tan:
			val = p.Tan(arg)
		case expr.Asin:
			val = p.Asin(arg)
		case expr.Acos:
			val = p.Acos(arg)
		case expr.Atan:
			val = p.Atan(arg)
		}
		return val, nil

	case expr.Hypot, expr.Pow, expr.Atan2:
		count := len(args)

		if count != 2 {
			return nil, fmt.Errorf("%s must have at exactly 2 arguments", fn)
		}

		arg1, err1 := p.compileAsNumber(args[0])
		if err1 != nil {
			return nil, err1
		}
		arg2, err2 := p.compileAsNumber(args[1])
		if err2 != nil {
			return nil, err2
		}

		var val *value
		switch fn {
		case expr.Hypot:
			val = p.Hypot(arg1, arg2)
		case expr.Pow:
			val = p.Pow(arg1, arg2)
		case expr.Atan2:
			val = p.Atan2(arg1, arg2)
		}
		return val, nil

	case expr.Concat:
		sargs := make([]*value, len(args))
		for i := range args {
			sarg, err := p.compileAsString(args[i])
			if err != nil {
				return nil, err
			}
			sargs[i] = sarg
		}
		return p.Concat(sargs...), nil

	case expr.Least, expr.Greatest:
		least := fn == expr.Least
		count := len(args)

		if count < 1 {
			return nil, fmt.Errorf("%s must have at least one argument", fn)
		}

		val, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		for i := 1; i < count; i++ {
			rhs, err := p.compileAsNumber(args[i])
			if err != nil {
				return nil, err
			}

			if least {
				val = p.MinValue(val, rhs)
			} else {
				val = p.MaxValue(val, rhs)
			}
		}

		return val, nil

	case expr.WidthBucket:
		if len(args) != 4 {
			return nil, fmt.Errorf("%s must have exactly 4 arguments", fn)
		}

		val, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		min, err := p.compileAsNumber(args[1])
		if err != nil {
			return nil, err
		}

		max, err := p.compileAsNumber(args[2])
		if err != nil {
			return nil, err
		}

		bucketCount, err := p.compileAsNumber(args[3])
		if err != nil {
			return nil, err
		}

		return p.WidthBucket(val, min, max, bucketCount), nil

	case expr.TimeBucket:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s must have exactly 2 arguments", fn)
		}

		arg, err := p.compileAsTime(args[0])
		if err != nil {
			return nil, err
		}

		interval, err2 := p.compileAsNumber(args[1])
		if err2 != nil {
			return nil, err2
		}

		return p.TimeBucket(arg, interval), nil

	case expr.Trim, expr.Ltrim, expr.Rtrim:
		tt := trimtype(fn)
		if len(args) == 1 { // TRIM(arg) is a regular space (ascii 0x20) trim
			s, err := p.compileAsString(args[0])
			if err != nil {
				return nil, err
			}

			return p.TrimSpace(s, tt), nil
		} else if len(args) == 2 { //TRIM("$$arg", "$") is a char trim
			s, err := p.compileAsString(args[0])
			if err != nil {
				return nil, err
			}
			chars, ok := args[1].(expr.String)
			if !ok {
				return nil, fmt.Errorf("the second argument of %s has to be a literal string; found %s with type %T", fn, args[1], args[1])
			}

			return p.TrimChar(s, string(chars), tt), nil
		} else {
			return nil, fmt.Errorf("%s should have one or two argument, got %v", fn, len(args))
		}

	case expr.Contains, expr.ContainsCI:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s should have 2 arguments, got %v", fn, len(args))
		}
		s, ok := args[1].(expr.String)
		if !ok {
			return nil, fmt.Errorf("the second argument %s should be a literal string; found %s with type %T", fn, args[1], args[1])
		}
		lhs, err := p.compileAsString(args[0])
		if err != nil {
			return nil, err
		}
		return p.Contains(lhs, string(s), fn == expr.Contains), nil

	case expr.EqualsCI:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s should have 2 arguments, got %v", fn, len(args))
		}
		_, ok := args[1].(expr.String)
		if !ok {
			return nil, fmt.Errorf("the second argument %s should be a literal string; found %s with type %T", fn, args[1], args[1])
		}
		lhs, err1 := p.compileAsString(args[0])
		if err1 != nil {
			return nil, err1
		}
		rhs, err2 := p.compileAsString(args[1])
		if err2 != nil {
			return nil, err2
		}
		return p.EqualStr(lhs, rhs, false), nil

	case expr.IsSubnetOf:
		if len(args) != 3 {
			return nil, fmt.Errorf("preprocessing %s went wrong, got %d args expected 3", fn, len(args))
		}
		minStr, ok0 := args[0].(expr.String)
		if !ok0 {
			return nil, fmt.Errorf("preprocessing %s went wrong, first argument should be a literal string; found %s with type %T", fn, args[0], args[0])
		}
		maxStr, ok1 := args[1].(expr.String)
		if !ok1 {
			return nil, fmt.Errorf("preprocessing %s went wrong, second argument should be a literal string; found %s with type %T", fn, args[1], args[1])
		}
		lhs, err := p.compileAsString(args[2])
		if err != nil {
			return nil, err
		}
		// the min/max are byte wise min/max values encoded as a string with dot as a separator.
		min := (*[4]byte)(net.ParseIP(string(minStr)).To4())
		max := (*[4]byte)(net.ParseIP(string(maxStr)).To4())
		return p.IsSubnetOfIP4(lhs, *min, *max), nil

	case expr.CharLength:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s should have 1 argument, got %v", fn, len(args))
		}
		lhs, err1 := p.compileAsString(args[0])
		if err1 != nil {
			return nil, err1
		}
		return p.CharLength(lhs), nil

	case expr.Substring:
		if len(args) != 3 {
			return nil, fmt.Errorf("preprocessing %s went wrong, got %d args expected 3", fn, len(args))
		}
		lhs, err1 := p.compileAsString(args[0])
		if err1 != nil {
			return nil, err1
		}
		substrOffset, err2 := p.compileAsNumber(args[1])
		if err2 != nil {
			return nil, err2
		}
		substrLength, err3 := p.compileAsNumber(args[2])
		if err3 != nil {
			return nil, err3
		}
		return p.Substring(lhs, substrOffset, substrLength), nil

	case expr.SplitPart:
		if len(args) != 3 {
			return nil, fmt.Errorf("preprocessing %s went wrong, got %d args expected 3", fn, len(args))
		}
		lhs, err1 := p.compileAsString(args[0])
		if err1 != nil {
			return nil, err1
		}
		delimiterStr, ok := args[1].(expr.String)
		if !ok {
			return nil, fmt.Errorf("the second argument %s should be a literal string; found %s with type %T", fn, args[1], args[1])
		}
		splitPartIndex, err2 := p.compileAsNumber(args[2])
		if err2 != nil {
			return nil, err2
		}
		return p.SplitPart(lhs, delimiterStr[0], splitPartIndex), nil

	case expr.Unspecified:
		switch b.Name() {
		case "UPVALUE":
			if len(args) != 1 {
				return nil, fmt.Errorf("UPVALUE should have 1 argument")
			}
			n, ok := args[0].(expr.Integer)
			if !ok {
				return nil, fmt.Errorf("first arg to UPVALUE should be an integer; found %s", args[0])
			}
			return p.Upvalue(p.InitMem(), stackSlotFromIndex(regV, int(n))), nil
		default:
			return nil, fmt.Errorf("unhandled builtin %q", b.Name())
		}
	case expr.ToUnixEpoch:
		if len(args) != 1 {
			return nil, fmt.Errorf("TO_UNIX_EPOCH has %d arguments?", len(args))
		}
		arg, err := p.compileAsTime(args[0])
		if err != nil {
			return nil, err
		}
		return p.DateToUnixEpoch(arg), nil
	case expr.ToUnixMicro:
		if len(args) != 1 {
			return nil, fmt.Errorf("TO_UNIX_MICRO has %d arguments?", len(args))
		}
		arg, err := p.compileAsTime(args[0])
		if err != nil {
			return nil, err
		}
		return p.DateToUnixMicro(arg), nil

	case expr.GeoHash, expr.GeoTileES:
		if len(args) != 3 {
			return nil, fmt.Errorf("%s requires 3 arguments, %d given", fn, len(args))
		}

		arg0, err1 := p.compileAsNumber(args[0])
		if err1 != nil {
			return nil, err1
		}

		arg1, err2 := p.compileAsNumber(args[1])
		if err2 != nil {
			return nil, err2
		}

		arg2, err3 := p.compileAsNumber(args[2])
		if err3 != nil {
			return nil, err3
		}

		var val *value
		if fn == expr.GeoHash {
			val = p.GeoHash(arg0, arg1, arg2)
		} else {
			val = p.GeoTileES(arg0, arg1, arg2)
		}
		return val, nil

	case expr.GeoDistance:
		if len(args) != 4 {
			return nil, fmt.Errorf("%s requires 4 arguments, %d given", fn, len(args))
		}

		arg0, err0 := p.compileAsNumber(args[0])
		if err0 != nil {
			return nil, err0
		}

		arg1, err1 := p.compileAsNumber(args[1])
		if err1 != nil {
			return nil, err1
		}

		arg2, err2 := p.compileAsNumber(args[2])
		if err2 != nil {
			return nil, err2
		}

		arg3, err3 := p.compileAsNumber(args[3])
		if err3 != nil {
			return nil, err3
		}

		return p.GeoDistance(arg0, arg1, arg2, arg3), nil

	case expr.GeoTileX, expr.GeoTileY:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s must have exactly 2 arguments", fn)
		}

		arg0, err1 := p.compileAsNumber(args[0])
		if err1 != nil {
			return nil, err1
		}

		arg1, err2 := p.compileAsNumber(args[1])
		if err2 != nil {
			return nil, err2
		}

		var val *value
		if fn == expr.GeoTileX {
			val = p.GeoTileX(arg0, arg1)
		} else {
			val = p.GeoTileY(arg0, arg1)
		}

		return val, nil

	case expr.ObjectSize:
		if len(args) != 1 {
			return nil, fmt.Errorf("SIZE does not accept %d arguments", len(args))
		}

		arg, err := compile(p, args[0])
		if err != nil {
			return nil, err
		}

		return p.ssa2(sobjectsize, arg, p.mask(arg)), nil
	case expr.HashLookup:
		return p.compileHashLookup(b.Args)
	case expr.Lower, expr.Upper:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s does not accept %d arguments", fn, len(args))
		}
		s, err := p.compileAsString(args[0])
		if err != nil {
			return nil, err
		}

		var v *value
		if fn == expr.Lower {
			v = p.Lower(s)
		} else {
			v = p.Upper(s)
		}
		return v, nil

	case expr.MakeList:
		if len(args) == 0 {
			return nil, fmt.Errorf("%s failed to perform constant propagation (empty list must be a constant)", fn)
		}

		items := make([]*value, len(args))
		for i := range args {
			item, err := p.serialized(args[i])
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return p.MakeList(items...), nil

	case expr.MakeStruct:
		if len(args) == 0 {
			return nil, fmt.Errorf("%s failed to perform constant propagation (empty struct must be a constant)", fn)
		}

		if (len(args) & 1) != 0 {
			return nil, fmt.Errorf("%s must have an even number of arguments representing key/value pairs", fn)
		}

		structArgs := make([]*value, 0, len(args)*3+1)
		structArgs = append(structArgs, p.ValidLanes())
		for i := 0; i < len(args); i += 2 {
			key, ok := args[i].(expr.String)
			if !ok {
				return nil, fmt.Errorf("%s key must be a string", fn)
			}

			val, err := p.serialized(args[i+1])
			if err != nil {
				return nil, err
			}
			structArgs = append(structArgs, p.ssa0imm(smakestructkey, string(key)), val, p.mask(val))
		}
		return p.MakeStruct(structArgs), nil
	case expr.TypeBit:
		arg, err := compile(p, args[0])
		if err != nil {
			return nil, err
		}
		if arg.op == skfalse {
			return p.Constant(0), nil
		}
		if arg.op == sliteral {
			return p.Constant(expr.JSONTypeBits(ionType(arg.imm))), nil
		}
		k := p.mask(arg)
		var n uint
		switch arg.primary() {
		case stValue:
			return p.ssa2(stypebits, arg, k), nil
		case stList:
			n = expr.JSONTypeBits(ion.ListType)
		case stInt, stFloat:
			n = expr.JSONTypeBits(ion.FloatType)
		case stTime, stTimeInt:
			n = expr.JSONTypeBits(ion.TimestampType)
		case stString:
			n = expr.JSONTypeBits(ion.StringType)
		}
		v := p.Constant(uint64(n))
		if k.op != sinit {
			v = p.vk(v, k)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unhandled builtin function name %q", fn)
	}
}

func (p *prog) compileHashLookup(lst []expr.Node) (*value, error) {
	var datums []ion.Datum
	if len(lst) <= 1 {
		return nil, fmt.Errorf("HASH_LOOKUP with %d arguments?", len(lst))
	}
	base := lst[0]
	lst = lst[1:]
	for i := range lst {
		c, ok := lst[i].(expr.Constant)
		if !ok {
			return nil, fmt.Errorf("expr %s is not a constant", expr.ToString(lst[i]))
		}
		datums = append(datums, c.Datum())
	}
	return p.hashLookup(base, datums)
}

func (p *prog) toTime(v *value) *value {
	switch v.primary() {
	case stValue:
		return p.ssa2(stotime, v, p.mask(v))
	case stTime:
		return v
	}
	return p.errorf("cannot convert result of %s to time", v)
}

func (p *prog) toTimeInt(v *value) *value {
	if v.op == sliteral {
		ts, ok := v.imm.(date.Time)
		if ok {
			return p.ssa0imm(sbroadcastts, ts.UnixMicro())
		}
	}
	switch v.primary() {
	case stValue:
		v = p.ssa2(stotime, v, p.mask(v))
		fallthrough
	case stTime:
		v = p.ssa2(sunboxtime, v, p.mask(v))
		fallthrough
	case stTimeInt:
		return v
	}
	return p.errorf("cannot convert result of %s to timeInt", v)
}

// produce left < right by forcing everything to unixmicro integers
func (p *prog) timeIntLess(left, right *value) *value {
	left = p.toTimeInt(left)
	right = p.toTimeInt(right)
	return p.ssa3(scmpltts, left, right, p.And(p.mask(left), p.mask(right)))
}

func (p *prog) compileTimeOrdered(left, right *value) *value {
	if left.primary() == stTimeInt ||
		right.primary() == stTimeInt ||
		(left.op != sliteral && right.op != sliteral) {
		return p.timeIntLess(left, right)
	}
	// we only use this fast-path when
	// comparing a raw boxed value against
	// a constant timestamp
	op := sltconsttm
	// move constant to rhs
	if left.op == sliteral {
		if right.op == sliteral {
			// we shouldn't hit this; the simplifier
			// should have handled the constprop
			panic("missed timestamp constprop!")
		}
		op = sgtconsttm
		left, right = right, left
	}
	v := p.toTime(left)
	return p.ssa2imm(op, v, p.mask(v), right.imm)
}

// compile a path expression from its top-level value
func compilepath(p *prog, top *value, rest expr.PathComponent) (*value, error) {
	for r := rest; r != nil; r = r.Next() {
		switch n := r.(type) {
		case *expr.Dot:
			top = p.Dot(n.Field, top)
		case *expr.LiteralIndex:
			top = p.Index(top, n.Field)
		default:
			return nil, fmt.Errorf("unhandled path component %q", r)
		}
	}
	return top, nil
}

func (p *prog) hashLookup(e expr.Node, lookup []ion.Datum) (*value, error) {
	v, err := p.serialized(e)
	if err != nil {
		return nil, err
	}
	h := p.hash(v)
	if len(lookup)&1 != 0 {
		// there is an ELSE value, so we need
		//   ret = hash_lookup(x)?:else
		elseval := p.Constant(lookup[len(lookup)-1])
		fetched := p.ssaimm(shashlookup, lookup[:len(lookup)-1], h, p.mask(h))
		blended := p.ssa3(sblendv, fetched, elseval, p.Not(fetched))
		return p.vk(blended, p.mask(v)), nil
	}
	return p.ssaimm(shashlookup, lookup, h, p.mask(h)), nil
}

func (p *prog) member(e expr.Node, values []expr.Constant) (*value, error) {
	v, err := p.serialized(e)
	if err != nil {
		return nil, err
	}
	h := p.hash(v)
	// we're using ssaimm here because we don't
	// want the CSE code to look at the immediate field
	return p.ssaimm(shashmember, values, h, p.mask(h)), nil
}

func (p *prog) recordDatum(d ion.Datum, st syms) {
	switch d.Type() {
	case ion.StructType:
		d, _ := d.Struct()
		d.Each(func(f ion.Field) bool {
			p.record(st.Get(f.Sym), f.Sym)
			p.recordDatum(f.Value, st)
			return true
		})
	case ion.ListType:
		d, _ := d.List()
		d.Each(func(d ion.Datum) bool {
			p.recordDatum(d, st)
			return true
		})
	case ion.SymbolType:
		d, _ := d.String()
		sym, ok := st.Symbolize(d)
		if !ok {
			sym = ^ion.Symbol(0)
		}
		p.record(d, sym)
	}
}

type hashResult struct {
	tree     *radixTree64
	literals []byte
}

func (p *prog) mkhash(st syms, imm interface{}) *hashResult {
	values := imm.([]ion.Datum)
	var tmp, tmp2 ion.Buffer
	tree := newRadixTree(8)
	var hmem [16]byte
	for len(values) >= 2 {
		ifeq := values[0]
		then := values[1]
		values = values[2:]
		p.recordDatum(ifeq, st)
		p.recordDatum(then, st)

		tmp.Reset()
		ifeq.Encode(&tmp, ionsyms(st))
		chacha8Hash(tmp.Bytes(), hmem[:])
		buf, _ := tree.Insert(binary.LittleEndian.Uint64(hmem[:]))

		// encode reference to scratch buffer
		// (4-byte base + 4-byte offset)
		base := tmp2.Size()
		then.Encode(&tmp2, ionsyms(st))
		binary.LittleEndian.PutUint32(buf, uint32(base))
		size := tmp2.Size() - base
		binary.LittleEndian.PutUint32(buf[4:], uint32(size))
	}
	return &hashResult{tree: tree, literals: tmp2.Bytes()}
}

// hook called on symbolization of hashmember;
// convert []expr.Constant into a hash tree
// using the current input symbol table
func (p *prog) mktree(st syms, imm interface{}) *radixTree64 {
	values := imm.([]expr.Constant)

	var tmp ion.Buffer
	tree := newRadixTree(0)
	var hmem [16]byte
	for i := range values {
		tmp.Reset()
		dat := values[i].Datum()
		dat.Encode(&tmp, ionsyms(st))
		p.recordDatum(dat, st)
		chacha8Hash(tmp.Bytes(), hmem[:])
		tree.insertSlow(binary.LittleEndian.Uint64(hmem[:]))
	}
	return tree
}

// serialized turns an arbitrary expression
// into a boxed value (stValue) so that it
// can be passed to generic operations (store, hash, etc.)
func (p *prog) serialized(e expr.Node) (*value, error) {
	v, err := compile(p, e)
	if err != nil {
		return nil, err
	}
	switch v.primary() {
	case stValue:
		if _, ok := e.(*expr.IsKey); ok {
			return p.ssa2(sboxmask, v, p.ValidLanes()), nil
		}
		// already got one
		return v, nil
	case stBool:
		var nonmissing *value
		if _, ok := e.(*expr.IsKey); ok {
			nonmissing = p.ValidLanes() // all lanes are valid
		} else {
			nonmissing = p.notMissing(v)
		}
		return p.ssa2(sboxmask, v, nonmissing), nil
	case stInt:
		return p.ssa2(sboxint, v, p.mask(v)), nil
	case stFloat:
		return p.ssa2(sboxfloat, v, p.mask(v)), nil
	case stString:
		return p.ssa2(sboxstring, v, p.mask(v)), nil
	case stTimeInt:
		return p.ssa2(sboxts, v, p.mask(v)), nil
	case stList:
		return p.ssa2(sboxlist, v, p.mask(v)), nil
	default:
		if v.op == sinvalid {
			return nil, errors.New(v.imm.(string))
		}
		return nil, fmt.Errorf("don't know how to re-serialize %q", e)
	}
}

// turn an arbitrary expression into a store-able value
func (p *prog) compileStore(mem *value, e expr.Node, slot stackslot, unsymbolize bool) (*value, error) {
	v, err := p.serialized(e)
	if err != nil {
		return nil, err
	}
	if unsymbolize {
		v = p.unsymbolized(v)
	}
	return p.Store(mem, v, slot)
}

// determine if the CASE expression c
// has THEN/ELSE arms that are only
// path expressions, MISSING, or NULL
func compilecase(p *prog, c *expr.Case) (*value, error) {
	if c.Valence == "logical" {
		return p.compileLogicalCase(c)
	}
	return p.compileGenericCase(c)
}

// logical CASE expressions are easier
// than most cases because we don't
// have to track the MISSING-ness of
// each expression directly; we're only
// interested in whether or no the
// result is TRUE
//
// since the expression simplifier tends
// to push comparisons into CASE expressions,
// non-projected CASE expressions often end up
// being logical case expressions, which
// is good for us as we can simplify it to
// some fairly straightforward boolean logic
func (p *prog) compileLogicalCase(c *expr.Case) (*value, error) {
	els := c.Else
	if els == nil {
		els = (expr.Null{})
	}
	final, err := p.compileAsBool(els)
	if err != nil {
		return nil, err
	}
	outk := p.ssa0(skfalse)   // actual output
	merged := p.ssa0(skfalse) // has a previous WHEN already matched?
	for i := range c.Limbs {
		when, err := p.compileAsBool(c.Limbs[i].When)
		if err != nil {
			return nil, err
		}
		if when.op == skfalse {
			continue
		}
		then, err := p.compileAsBool(c.Limbs[i].Then)
		if err != nil {
			return nil, err
		}
		// only merge to result if <WHEN> AND NOT <merged>,
		// then update the set of already-merged lanes
		live := p.nand(merged, when)
		outk = p.Or(outk, p.And(live, then))
		merged = p.Or(merged, when)
	}
	if final.op == skfalse {
		return outk, nil
	}
	outk = p.Or(outk, p.And(final, p.nand(merged, final)))
	return outk, nil
}

func (p *prog) compileNumericCase(c *expr.Case) (*value, error) {
	// special case: if this is a path case,
	// we can just merge the path expression values
	// and perform only one load+convert
	if c.IsPathLimbs() {
		v, err := p.compileGenericCase(c)
		if err != nil {
			return nil, err
		}
		v, k := p.coercefp(v)
		return p.floatk(v, k), nil
	}

	var outnum, outk, merged *value

	// if the ELSE result is actually numeric,
	// produce the initial value and output mask
	// that we merge into
	if c.Else != nil && c.Else != (expr.Missing{}) && c.Else != (expr.Null{}) {
		els, err := p.compileAsNumber(c.Else)
		if err != nil {
			return nil, err
		}
		if els.op == sliteral {
			if !isNumericImmediate(els.imm) {
				return nil, fmt.Errorf("%v of type %T is not float nor integer number", els.imm, els.imm)
			}
			outnum, outk = p.coercefp(els)
			merged = p.ssa0(skfalse)
		} else if els.op != skfalse {
			merged = p.ssa0(skfalse)
			switch els.primary() {
			case stValue, stFloat, stInt:
				outnum, outk = p.coercefp(els)
			default:
				return nil, fmt.Errorf("unexpected ELSE in numeric CASE: %s", c.Else)
			}
		}
	}

	for i := range c.Limbs {
		when, err := p.compileAsBool(c.Limbs[i].When)
		if err != nil {
			return nil, err
		}
		if when.op == skfalse {
			continue
		}
		then, err := p.compileAsNumber(c.Limbs[i].Then)
		if err != nil {
			return nil, err
		}
		if then.op == skfalse {
			merged = p.Or(merged, when)
			continue
		}
		if then.op == sliteral {
			if !isNumericImmediate(then.imm) {
				return nil, fmt.Errorf("%v of type %T is not float nor integer number", then.imm, then.imm)
			}
			then, _ = p.coercefp(then)
		}
		if outnum == nil {
			// first limb; easy case
			merged = when
			switch then.primary() {
			case stValue, stInt, stFloat:
				t, k := p.coercefp(then)
				outnum = t
				outk = p.And(k, when)

			default:
				return nil, fmt.Errorf("cannot convert %s in CASE to float", c.Limbs[i].Then)
			}
			continue
		}
		// the set of lanes moved into
		// the result is (!merged AND WHEN is true)
		shouldmerge := p.nand(merged, when)
		if shouldmerge.op == skfalse {
			continue
		}
		// the set of lanes merged is now
		// merged OR WHEN is true
		merged = p.Or(merged, when)
		switch then.primary() {
		case stInt:
			then, _ = p.coercefp(then)
			fallthrough
		case stFloat:
			outk = p.Or(outk, p.And(p.mask(then), shouldmerge))
			outnum = p.ssa3(sblendfloat, outnum, then, shouldmerge)
		case stValue:
			// we can perform blending by
			// performing in-line conversion
			f, k := p.blendv2fp(outnum, then, shouldmerge)
			outk = p.Or(outk, k)
			outnum = f
		default:
			return nil, fmt.Errorf("cannot convert %s in CASE to float", c.Limbs[i].Then)
		}
	}
	return p.floatk(outnum, outk), nil
}

// a "generic case" is one in which all the
// results of the arms of the case are coerced to stValue,
// which happens if a) they were already all stValue,
// or b) they have incompatible types and the caller needs
// to perform unboxing again to get at the results
func (p *prog) compileGenericCase(c *expr.Case) (*value, error) {
	var v, k, merged *value
	var err error
	if c.Else != nil && c.Else != (expr.Missing{}) {
		v, err = p.serialized(c.Else)
		if err != nil {
			return nil, err
		}
		k = p.mask(v)
		merged = p.ssa0(skfalse)
	}
	for i := range c.Limbs {
		when, err := p.compileAsBool(c.Limbs[i].When)
		if err != nil {
			return nil, err
		}
		if when.op == skfalse {
			continue
		}
		then, err := p.serialized(c.Limbs[i].Then)
		if err != nil {
			return nil, err
		}
		// return type can be that of 'dot'
		// or that of 'split'
		if then.primary() != stValue {
			panic("inside compilePathCase: unexpected return type")
		}
		if v == nil {
			k = p.And(when, p.mask(then))
			v = then
			merged = when
			continue
		}
		shouldmerge := p.nand(merged, when)
		v = p.ssa3(sblendv, v, then, shouldmerge)
		k = p.Or(k, p.And(shouldmerge, p.mask(then)))
		merged = p.Or(merged, when)
	}
	if v == nil {
		return p.ssa0(skfalse), nil
	}
	if v == k {
		return v, nil
	}
	return p.vk(v, k), nil
}

func (p *prog) compileStringCase(c *expr.Case) (*value, error) {
	return nil, fmt.Errorf("string-case unimplemented")
}

func (p *prog) compileCast(c *expr.Cast) (*value, error) {
	from, err := compile(p, c.From)
	if err != nil {
		return nil, err
	}
	switch c.To {
	case expr.MissingType:
		return p.ssa0(skfalse), nil
	case expr.NullType:
		return p.Constant(nil), nil
	case expr.BoolType:
		switch from.primary() {
		case stBool:
			return from, nil
		case stInt:
			// bool(i != 0)
			return p.nand(p.ssa2imm(scmpeqimmi, from, p.mask(from), 0), p.ValidLanes()), nil
		case stFloat:
			// bool(f != 0)
			return p.nand(p.ssa2imm(scmpeqimmf, from, p.mask(from), 0.0), p.ValidLanes()), nil
		case stValue:
			// we can convert booleans and numbers to bools
			iszero := p.ssa2imm(sequalconst, from, p.mask(from), 0)
			isfalse := p.ssa2(sisfalse, from, p.mask(from))
			eqfalse := p.Or(iszero, isfalse)
			oktype := p.checkTag(from, expr.BoolType|expr.NumericType)
			// return (!(b == 0 || b == false) && (b is numeric))
			// TODO: should NaN become false as well?
			ret := p.nand(eqfalse, oktype)
			ret.notMissing = oktype
			return ret, nil
		default:
			// not convertible
			return p.ssa0(skfalse), nil
		}
	case expr.IntegerType:
		switch from.primary() {
		case stBool:
			// true/false/missing -> 1/0/missing
			return p.ssa2(scvtktoi, from, p.notMissing(from)), nil
		case stInt:
			return from, nil
		case stFloat:
			return p.ssa2(sroundi, from, p.mask(from)), nil
		case stValue:
			return p.ssa2(sunboxcvti64, from, p.mask(from)), nil
		default:
			return p.ssa0(skfalse), nil
		}
	case expr.FloatType:
		if from.op == sliteral {
			return from, nil // FIXME: check that from.imm is actually numeric
		}
		switch from.primary() {
		case stBool:
			return p.ssa2(scvtktof, from, p.notMissing(from)), nil
		case stInt:
			return p.ssa2(scvtitof, from, p.mask(from)), nil
		case stFloat:
			return from, nil
		case stValue:
			return p.ssa2(sunboxcvtf64, from, p.mask(from)), nil
		default:
			return p.ssa0(skfalse), nil
		}
	case expr.StringType:
		switch from.primary() {
		case stString:
			return from, nil
		case stInt:
			return p.ssa2(scvti64tostr, from, p.mask(from)), nil
		case stValue:
			// we can encode strings as symbols,
			// so include symbols in the bits we check
			return p.checkTag(from, c.To|expr.SymbolType), nil
		default:
			return p.ssa0(skfalse), nil
		}
	case expr.TimeType:
		switch from.primary() {
		case stTime:
			return from, nil
		case stValue:
			return p.checkTag(from, c.To), nil
		default:
			return p.ssa0(skfalse), nil
		}
	case expr.ListType:
		if from.ret()&stList != 0 {
			return from, nil
		}
		if from.primary() == stValue {
			return p.checkTag(from, c.To), nil
		}
		return p.ssa0(skfalse), nil
	case expr.StructType, expr.DecimalType, expr.SymbolType:
		if from.ret()&stValue != 0 {
			return p.checkTag(from, c.To), nil
		}
		return p.ssa0(skfalse), nil
	default:
		return nil, fmt.Errorf("unsupported cast %q", c)
	}
}

func (p *prog) notNumber(v *value) *value {
	switch v.primary() {
	case stValue:
		// must have type bits corresponding to
		// something other than numeric
		return p.checkTag(v, ^expr.NumericType)
	case stInt, stFloat:
		return p.ssa0(skfalse) // false, definitely a number
	default:
		return p.ValidLanes() // true, never a number
	}
}

func (p *prog) notTime(v *value) *value {
	switch v.primary() {
	case stValue:
		return p.checkTag(v, ^expr.TimeType)
	case stTime, stTimeInt:
		return p.ssa0(skfalse) // false; definitely a timestamp
	default:
		return p.ValidLanes() // true; never a timestamp
	}
}

func (p *prog) checkTag(from *value, typ expr.TypeSet) *value {
	return p.ssa2imm(schecktag, from, p.mask(from), uint16(typ))
}
