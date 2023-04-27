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
	"unicode/utf8"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"

	"golang.org/x/exp/slices"
)

// compileLogical compiles a logical expression
// into an ssa program
func compileLogical(e expr.Node) (*prog, error) {
	p := new(prog)
	p.begin()
	v, err := p.compileAsBool(e)
	if err != nil {
		return nil, err
	}
	p.returnBK(p.validLanes(), v)
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
		return p.constant(bool(n)), nil
	case expr.String:
		return p.constant(string(n)), nil
	case expr.Integer:
		return p.constant(int64(n)), nil
	case expr.Float:
		return p.constant(float64(n)), nil
	case expr.Null:
		return p.constant(nil), nil
	case *expr.Struct:
		return p.constant(n.Datum()), nil
	case *expr.List:
		return p.constant(n.Datum()), nil
	case *expr.Rational:
		r := (*big.Rat)(n)
		if r.IsInt() {
			num := r.Num()
			if !num.IsInt64() {
				return nil, fmt.Errorf("%s overflows int64", num)
			}
			return p.constant(num.Int64()), nil
		}
		// ok if we lose some precision here, I guess...
		f, _ := r.Float64()
		return p.constant(f), nil
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
			eq := p.equals(left, right)
			if n.Op == expr.NotEquals {
				eq = p.not(eq)
			}
			return eq, nil
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
			return p.less(left, right), nil
		case expr.LessEquals:
			return p.lessEqual(left, right), nil
		case expr.Greater:
			return p.greater(left, right), nil
		case expr.GreaterEquals:
			return p.greaterEqual(left, right), nil
		}
		return nil, fmt.Errorf("unhandled comparison expression %q", n)
	case *expr.StringMatch:
		switch n.Op {
		case expr.Like, expr.Ilike:
			left, err := p.compileAsString(n.Expr)
			if err != nil {
				return nil, err
			}
			// NOTE: StringMatch.check checks if n.Escape has valid content
			escRune, _ := utf8.DecodeRuneInString(n.Escape)
			caseSensitive := n.Op == expr.Like
			inner := p.like(left, n.Pattern, escRune, caseSensitive)
			// the bool-typed result is just the opcode mask
			ret := p.ssa1(snotmissing, inner)
			// the missing-ness of the result is the string-ness of the argument
			ret.notMissing = p.mask(left)
			return ret, nil
		case expr.SimilarTo, expr.RegexpMatch, expr.RegexpMatchCi:
			left, err := p.compileAsString(n.Expr)
			if err != nil {
				return nil, err
			}
			// NOTE: We do not implement the escape char from the SQL SIMILAR TO syntax, backslash is the only used escape-char
			regexStr := n.Pattern
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
			return p.regexMatch(left, dfaStore)
		}
		return nil, fmt.Errorf("unimplemented StringMatch operation")
	case *expr.UnaryArith:
		child, err := p.compileAsNumber(n.Child)
		if err != nil {
			return nil, err
		}

		switch n.Op {
		case expr.NegOp:
			return p.neg(child), nil
		case expr.BitNotOp:
			return p.bitNot(child), nil
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
			return p.add(left, right), nil
		case expr.SubOp:
			return p.sub(left, right), nil
		case expr.MulOp:
			return p.mul(left, right), nil
		case expr.DivOp:
			return p.div(left, right), nil
		case expr.ModOp:
			return p.mod(left, right), nil
		case expr.BitAndOp:
			return p.bitAnd(left, right), nil
		case expr.BitOrOp:
			return p.bitOr(left, right), nil
		case expr.BitXorOp:
			return p.bitXor(left, right), nil
		case expr.ShiftLeftLogicalOp:
			return p.shiftLeftLogical(left, right), nil
		case expr.ShiftRightArithmeticOp:
			return p.shiftRightArithmetic(left, right), nil
		case expr.ShiftRightLogicalOp:
			return p.shiftRightLogical(left, right), nil
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
			return p.or(left, right), nil
		case expr.OpAnd:
			return p.and(left, right), nil
		case expr.OpXor:
			// boolean =
			notmiss := p.and(p.notMissing(left), p.notMissing(right))
			return p.and(p.xor(left, right), notmiss), nil
		case expr.OpXnor:
			// boolean <>
			notmiss := p.and(p.notMissing(left), p.notMissing(right))
			return p.and(p.xnor(left, right), notmiss), nil
		}
		return nil, fmt.Errorf("unrecognized expression %q", n)
	case *expr.Not:
		inner, err := p.compileAsBool(n.Expr)
		if err != nil {
			return nil, err
		}
		return p.not(inner), nil
	case *expr.Dot:
		inner, err := compile(p, n.Inner)
		if err != nil {
			return nil, err
		}
		return p.dot(n.Field, inner), nil
	case expr.Ident:
		return p.dot(string(n), p.validLanes()), nil
	case *expr.Index:
		inner, err := compile(p, n.Inner)
		if err != nil {
			return nil, err
		}
		return p.index(inner, n.Offset), nil
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
			return p.not(p.notMissing(inner)), nil
		case expr.IsNotMissing:
			return p.notMissing(inner), nil
		case expr.IsTrue:
			return p.isTrue(inner), nil
		case expr.IsFalse:
			return p.isFalse(inner), nil
		case expr.IsNotTrue:
			// we always compute IS TRUE,
			// so IS NOT TRUE is the negation of that
			return p.isNotTrue(inner), nil
		case expr.IsNotFalse:
			// either MISSING or TRUE
			return p.isNotFalse(inner), nil
		}
		return nil, fmt.Errorf("unhandled IS: %q", n)
	case *expr.Builtin:
		return compilefunc(p, n, n.Args)
	case *expr.Case:
		return compilecase(p, n)
	case *expr.Cast:
		return p.compileCast(n)
	case *expr.Timestamp:
		return p.constant(n.Value), nil
	case *expr.Member:
		return p.member(n.Arg, &n.Set)
	case *expr.Lookup:
		return p.hashLookup(n)
	case expr.Missing:
		return p.missing(), nil
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
		return p.missing(), nil
	case expr.Missing:
		return p.missing(), nil
	case *expr.Case:
		return p.compileLogicalCase(e)
	case *expr.Logical:
	case *expr.Comparison:
	case *expr.StringMatch:
	case *expr.IsKey:
	case expr.Bool:
		if e {
			return p.validLanes(), nil
		}
		return p.missing(), nil
	case *expr.Builtin:
	case *expr.Not:
	case *expr.Member:
	case *expr.Index:
		coerce = true
	case *expr.Dot:
		coerce = true
	case expr.Ident:
		coerce = true
	case *expr.Cast:
		if e.To != expr.BoolType {
			return nil, fmt.Errorf("%q cannot be interpreted as a logical expression; the target type is %s",
				expr.ToString(e), e.TargetTypeName())
		}
	default:
		return nil, fmt.Errorf("%q is not a logical expression, but has type %T", expr.ToString(e), e)
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
	if t := v.primary(); t != stValue && t != stTime {
		if v.op == sinvalid {
			return nil, fmt.Errorf("compiling %s: %v", e, v.imm)
		}
		return nil, fmt.Errorf("can't compile %s as a timestamp (instr %s)", e, v)
	}
	return v, nil
}

func (p *prog) compileAsString(e expr.Node) (*value, error) {
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
	v, err := compilefuncaux(p, b, args)
	if err != nil {
		return nil, fmt.Errorf("compiling %s: %w", b.Func, err)
	}

	return v, nil
}

type compileType int

const (
	compileExpression compileType = iota
	compileNumber
	compileString
	compileTime
	compileBool
	compileValue
	literalString
	constInteger
	omit
)

func compileargs(p *prog, args []expr.Node, types ...compileType) ([]*value, error) {
	if len(types) != len(args) {
		return nil, fmt.Errorf("expects %d arguments, got %d", len(types), len(args))
	}

	compiled := make([]*value, len(types))
	for i, t := range types {
		var err error
		var val *value
		switch t {
		case compileNumber:
			val, err = p.compileAsNumber(args[i])
		case compileString:
			val, err = p.compileAsString(args[i])
		case compileTime:
			val, err = p.compileAsTime(args[i])
		case compileBool:
			val, err = p.compileAsBool(args[i])
		case compileValue:
			val, err = p.serialized(args[i])
		case compileExpression:
			val, err = compile(p, args[i])
		case literalString:
			_, ok := args[i].(expr.String)
			if !ok {
				err = fmt.Errorf("expected literal string, got %T", args[i])
			}
		case constInteger:
			_, ok := args[i].(expr.Integer)
			if !ok {
				err = fmt.Errorf("expected an integer, got %T", args[i])
			}
		case omit:
			// do nothing
		}

		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}

		compiled[i] = val
	}

	return compiled, nil
}

func compilefuncaux(p *prog, b *expr.Builtin, args []expr.Node) (*value, error) {
	fn := b.Func

	if fn.IsDateAdd() {
		part, _ := fn.TimePart()
		val, err := compileargs(p, args, compileNumber, compileTime)
		if err != nil {
			return nil, err
		}

		return p.dateAdd(part, val[0], val[1]), nil
	}

	if fn.IsDateDiff() {
		part, _ := fn.TimePart()
		val, err := compileargs(p, args, compileTime, compileTime)
		if err != nil {
			return nil, err
		}

		return p.dateDiff(part, val[0], val[1]), nil
	}

	if fn.IsDateExtract() {
		part, _ := fn.TimePart()
		val, err := compileargs(p, args, compileTime)
		if err != nil {
			return nil, err
		}
		return p.dateExtract(part, val[0]), nil
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
				return nil, fmt.Errorf("day of week has to be a constant")
			}
			return p.dateTruncWeekday(val, expr.Weekday(dow)), nil
		}

		return p.dateTrunc(part, val), nil
	}

	switch fn {
	case expr.Pi:
		if len(args) != 0 {
			return nil, fmt.Errorf("accepts no arguments")
		}

		return p.constant(pi), nil

	case expr.Log:
		count := len(args)

		if count < 1 || count > 2 {
			return nil, fmt.Errorf("expects either 1 or 2 arguments")
		}

		n, err := p.compileAsNumber(args[0])
		if err != nil {
			return nil, err
		}

		var val *value
		if count == 1 {
			// use LOG base 10 if no base was specified
			val = p.log10(n)
		} else {
			base := n

			n, err = p.compileAsNumber(args[1])
			if err != nil {
				return nil, err
			}

			// logarithm of any base can be calculated as `log2(n) / log2(base)`
			val = p.div(p.log2(n), p.log2(base))
		}

		return val, nil

	case expr.Degrees, expr.Radians:
		val, err := compileargs(p, args, compileNumber)
		if err != nil {
			return nil, err
		}

		var c float64
		if fn == expr.Degrees {
			c = radiansToDegrees
		} else {
			c = degreesToRadians
		}

		return p.mul(val[0], p.constant(c)), nil

	case expr.Round, expr.RoundEven, expr.Trunc, expr.Floor, expr.Ceil,
		expr.Sqrt, expr.Cbrt,
		expr.Exp, expr.Exp2, expr.Exp10, expr.ExpM1,
		expr.Ln, expr.Ln1p, expr.Log2, expr.Log10,
		expr.Sin, expr.Cos, expr.Tan,
		expr.Asin, expr.Acos, expr.Atan, expr.Abs, expr.Sign, expr.BitCount:

		v, err := compileargs(p, args, compileNumber)
		if err != nil {
			return nil, err
		}

		arg := v[0]

		var val *value
		switch fn {
		case expr.Round:
			val = p.round(arg)
		case expr.RoundEven:
			val = p.roundEven(arg)
		case expr.Trunc:
			val = p.trunc(arg)
		case expr.Floor:
			val = p.floor(arg)
		case expr.Ceil:
			val = p.ceil(arg)
		case expr.Sqrt:
			val = p.sqrt(arg)
		case expr.Cbrt:
			val = p.cbrt(arg)
		case expr.Exp:
			val = p.exp(arg)
		case expr.Exp2:
			val = p.exp2(arg)
		case expr.Exp10:
			val = p.exp10(arg)
		case expr.ExpM1:
			val = p.expM1(arg)
		case expr.Ln:
			val = p.ln(arg)
		case expr.Ln1p:
			val = p.ln1p(arg)
		case expr.Log2:
			val = p.log2(arg)
		case expr.Log10:
			val = p.log10(arg)
		case expr.Sin:
			val = p.sin(arg)
		case expr.Cos:
			val = p.cos(arg)
		case expr.Tan:
			val = p.tan(arg)
		case expr.Asin:
			val = p.asin(arg)
		case expr.Acos:
			val = p.acos(arg)
		case expr.Atan:
			val = p.atan(arg)
		case expr.Sign:
			val = p.sign(arg)
		case expr.Abs:
			val = p.abs(arg)
		case expr.BitCount:
			val = p.bitCount(arg)
		}
		return val, nil

	case expr.Hypot, expr.Pow, expr.Atan2:
		v, err := compileargs(p, args, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		arg1 := v[0]
		arg2 := v[1]

		var val *value
		switch fn {
		case expr.Hypot:
			val = p.hypot(arg1, arg2)
		case expr.Pow:
			val = p.pow(arg1, arg2)
		case expr.Atan2:
			val = p.atan2(arg1, arg2)
		}
		return val, nil

	case expr.PowUint:
		v, err := compileargs(p, args, compileNumber, constInteger)
		if err != nil {
			return nil, err
		}

		arg := v[0]
		exp := int64(args[1].(expr.Integer))
		if exp < 0 {
			return nil, fmt.Errorf("exponent must not be less than zero")
		}

		return p.powuint(arg, exp), nil

	case expr.DateBin:
		v, err := compileargs(p, args, constInteger, compileTime, compileTime)
		if err != nil {
			return nil, err
		}

		return p.dateBin(int64(args[0].(expr.Integer)), v[1], v[2]), nil

	case expr.Concat:
		sargs := make([]*value, len(args))
		for i := range args {
			sarg, err := p.compileAsString(args[i])
			if err != nil {
				return nil, err
			}
			sargs[i] = sarg
		}
		return p.concat(sargs...), nil

	case expr.Least, expr.Greatest:
		least := fn == expr.Least
		count := len(args)

		if count < 1 {
			return nil, fmt.Errorf("expects at least one argument")
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
				val = p.minValue(val, rhs)
			} else {
				val = p.maxValue(val, rhs)
			}
		}

		return val, nil

	case expr.WidthBucket:
		v, err := compileargs(p, args, compileNumber, compileNumber, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		val := v[0]
		min := v[1]
		max := v[2]
		bucketCount := v[3]

		return p.widthBucket(val, min, max, bucketCount), nil

	case expr.TimeBucket:
		v, err := compileargs(p, args, compileTime, compileNumber)
		if err != nil {
			return nil, err
		}

		arg := v[0]
		interval := v[1]

		return p.timeBucket(arg, interval), nil

	case expr.Trim, expr.Ltrim, expr.Rtrim:
		tt := trimtype(fn)
		if len(args) == 1 { // TRIM(arg) is a regular space (ascii 0x20) trim
			s, err := p.compileAsString(args[0])
			if err != nil {
				return nil, err
			}

			return p.trimSpace(s, tt), nil
		} else if len(args) == 2 { //TRIM("$$arg", "$") is a char trim
			v, err := compileargs(p, args, compileString, literalString)
			if err != nil {
				return nil, err
			}

			s := v[0]
			chars, _ := args[1].(expr.String)

			return p.trimChar(s, string(chars), tt), nil
		} else {
			return nil, fmt.Errorf("expects one or two arguments")
		}

	case expr.Contains, expr.ContainsCI:
		v, err := compileargs(p, args, compileString, literalString)
		if err != nil {
			return nil, err
		}

		lhs := v[0]
		s := args[1].(expr.String)

		return p.contains(lhs, stringext.Needle(s), fn == expr.Contains), nil

	case expr.EqualsCI:
		v, err := compileargs(p, args, compileString, literalString)
		if err != nil {
			return nil, err
		}

		lhs := v[0]
		s := args[1].(expr.String)

		return p.equalsStr(lhs, stringext.Needle(s), false), nil

	case expr.EqualsFuzzy, expr.EqualsFuzzyUnicode:
		val, err := compileargs(p, args, compileString, literalString, compileNumber)
		if err != nil {
			return nil, err
		}
		ascii := fn == expr.EqualsFuzzy
		return p.equalsFuzzy(val[0], stringext.Needle(args[1].(expr.String)), val[2], ascii), nil

	case expr.ContainsFuzzy, expr.ContainsFuzzyUnicode:
		val, err := compileargs(p, args, compileString, literalString, compileNumber)
		if err != nil {
			return nil, err
		}
		ascii := fn == expr.ContainsFuzzy
		return p.containsFuzzy(val[0], stringext.Needle(args[1].(expr.String)), val[2], ascii), nil

	case expr.IsSubnetOf:
		v, err := compileargs(p, args, literalString, literalString, compileString)
		if err != nil {
			return nil, err
		}

		minStr, _ := args[0].(expr.String)
		maxStr, _ := args[1].(expr.String)
		lhs := v[2]

		// the min/max are byte wise min/max values encoded as a string with dot as a separator.
		min := (*[4]byte)(net.ParseIP(string(minStr)).To4())
		max := (*[4]byte)(net.ParseIP(string(maxStr)).To4())

		return p.isSubnetOfIP4(lhs, *min, *max), nil

	case expr.OctetLength:
		v, err := compileargs(p, args, compileString)
		if err != nil {
			return nil, err
		}
		return p.octetLength(v[0]), nil

	case expr.CharLength:
		v, err := compileargs(p, args, compileString)
		if err != nil {
			return nil, err
		}
		return p.charLength(v[0]), nil

	case expr.Substring:
		val, err := compileargs(p, args, compileString, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		lhs := val[0]
		substrOffset := val[1]
		substrLength := val[2]
		return p.substring(lhs, substrOffset, substrLength), nil

	case expr.SplitPart:
		v, err := compileargs(p, args, compileString, literalString, compileNumber)
		if err != nil {
			return nil, err
		}

		lhs := v[0]
		delimiterStr := args[1].(expr.String)
		splitPartIndex := v[2]

		return p.splitPart(lhs, delimiterStr[0], splitPartIndex), nil

	case expr.Unspecified:
		return nil, fmt.Errorf("unhandled builtin %q", b.Name())

	case expr.ToUnixEpoch:
		v, err := compileargs(p, args, compileTime)
		if err != nil {
			return nil, err
		}

		return p.dateToUnixEpoch(v[0]), nil

	case expr.ToUnixMicro:
		v, err := compileargs(p, args, compileTime)
		if err != nil {
			return nil, err
		}

		return p.dateToUnixMicro(v[0]), nil

	case expr.GeoHash, expr.GeoTileES:
		v, err := compileargs(p, args, compileNumber, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		var val *value
		if fn == expr.GeoHash {
			val = p.geoHash(v[0], v[1], v[2])
		} else {
			val = p.geoTileES(v[0], v[1], v[2])
		}
		return val, nil

	case expr.GeoDistance:
		v, err := compileargs(p, args, compileNumber, compileNumber, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		return p.geoDistance(v[0], v[1], v[2], v[3]), nil

	case expr.GeoTileX, expr.GeoTileY:
		v, err := compileargs(p, args, compileNumber, compileNumber)
		if err != nil {
			return nil, err
		}

		var val *value
		if fn == expr.GeoTileX {
			val = p.geoTileX(v[0], v[1])
		} else {
			val = p.geoTileY(v[0], v[1])
		}

		return val, nil

	case expr.ObjectSize:
		v, err := compileargs(p, args, compileExpression)
		if err != nil {
			return nil, err
		}

		if v[0].primary() == stList {
			return p.arraySize(v[0]), nil
		}

		return p.objectSize(v[0]), nil

	case expr.ArraySize:
		v, err := compileargs(p, args, compileExpression)
		if err != nil {
			return nil, err
		}
		return p.arraySize(v[0]), nil

	case expr.ArrayContains:
		v, err := compileargs(p, args, compileExpression, compileValue)
		if err != nil {
			return nil, err
		}
		return p.arrayContains(v[0], v[1]), nil

	case expr.ArrayPosition:
		v, err := compileargs(p, args, compileExpression, compileValue)
		if err != nil {
			return nil, err
		}
		return p.arrayPosition(v[0], v[1]), nil

	case expr.Lower, expr.Upper:
		vals, err := compileargs(p, args, compileString)
		if err != nil {
			return nil, err
		}

		s := vals[0]

		var v *value
		if fn == expr.Lower {
			v = p.lower(s)
		} else {
			v = p.upper(s)
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
			items[i] = p.unsymbolized(item)
		}
		return p.makeList(items...), nil

	case expr.MakeStruct:
		if len(args) == 0 {
			return nil, fmt.Errorf("%s failed to perform constant propagation (empty struct must be a constant)", fn)
		}

		if (len(args) & 1) != 0 {
			return nil, fmt.Errorf("%s must have an even number of arguments representing key/value pairs", fn)
		}

		structArgs := make([]*value, 0, len(args)*3+1)
		structArgs = append(structArgs, p.validLanes())
		for i := 0; i < len(args); i += 2 {
			key, ok := args[i].(expr.String)
			if !ok {
				return nil, fmt.Errorf("%s key must be a string", fn)
			}

			val, err := p.serialized(args[i+1])
			if err != nil {
				return nil, err
			}
			// unsymbolize values so that they
			// can be used as keys in hashlookup, etc.
			val = p.unsymbolized(val)
			structArgs = append(structArgs, p.ssa0imm(smakestructkey, string(key)), val, p.mask(val))
		}
		return p.makeStruct(structArgs), nil

	case expr.TypeBit:
		arg, err := compile(p, args[0])
		if err != nil {
			return nil, err
		}
		if arg.op == skfalse {
			return p.constant(0), nil
		}
		if arg.op == sliteral {
			return p.constant(expr.JSONTypeBits(ionType(arg.imm))), nil
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
		case stTime:
			n = expr.JSONTypeBits(ion.TimestampType)
		case stString:
			n = expr.JSONTypeBits(ion.StringType)
		}
		v := p.constant(uint64(n))
		if k.op != sinit {
			v = p.makevk(v, k)
		}
		return v, nil

	case expr.AssertIonType:
		arg, err := compile(p, args[0])
		if err != nil {
			return nil, err
		}

		var typeset expr.TypeSet
		for i := 1; i < len(args); i++ {
			typeset |= expr.TypeSet(1 << int(args[i].(expr.Integer)))
		}

		switch arg.primary() {
		case stValue:
			return p.checkTag(arg, typeset), nil

		case stString:
			if typeset.AnyOf(expr.StringType) {
				return arg, nil
			}
			return p.missing(), nil

		case stFloat:
			if typeset.AnyOf(expr.FloatType) {
				return arg, nil
			}
			return p.missing(), nil

		case stInt:
			if typeset.AnyOf(expr.IntegerType) {
				return arg, nil
			}
			return p.missing(), nil

		case stTime:
			if typeset.AnyOf(expr.TimeType) {
				return arg, nil
			}
			return p.missing(), nil
		}

		return nil, fmt.Errorf("cannot handle value of type %q", arg.primary())

	default:
		return nil, fmt.Errorf("unhandled builtin function name %q", fn)
	}
}

type hashImm struct {
	table *expr.Lookup

	// precomputed is non-nil if the set of keys
	// for the hash table does not depend on the symbol table
	// (i.e. a list of strings or integers, which is quite common);
	// positions are the locations in the precomputed radix tree
	// to which the n'th key value is mapped
	//
	// NOTE: since we tend to share one prog for many cores,
	// the tree here has to be cloned to each CPU (and cannot be written to)
	// if the precomputation doesn't also include the values
	precomputed *radixTree64
	positions   []int32
	slab        slab
	complete    bool
}

type hashSetImm struct {
	set         *ion.Bag
	precomputed *radixTree64
}

// note: ion.Bag.Transcoder flattens symbols,
// so a raw symbol value is just hashed as a string
// and thus we can treat those as hash consts as well
func isHashConst(d ion.Datum) bool {
	switch d.Type() {
	case ion.ListType:
		// we used constructed lists for multi-key joins,
		// so it's helpful to recognize ['foo', 'bar'] as constant
		iter, err := d.Iterator()
		if err != nil {
			return false
		}
		for !iter.Done() {
			next, err := iter.Next()
			if err != nil || !isHashConst(next) {
				return false
			}
		}
		return true
	case ion.StructType:
		return false
	default:
		return true
	}
}

// attempt to set h.precomputed and h.positions
func (h *hashImm) precompute(p *prog) {
	tree := newRadixTree(8)
	var positions []int32
	var empty ion.Symtab

	// gather up to 4 values before hashing
	var tmp ion.Buffer
	var endpos [4]uint32
	var pos int

	flush := func(n int) {
		buf := tmp.Bytes()
		// ensure a movq at buf[len(buf)-1] will touch valid memory:
		buf = slices.Grow(buf, 7)
		ret := chacha8x4(&buf[0], endpos)
		for i := 0; i < n; i++ {
			n, _ := tree.insertSlow(binary.LittleEndian.Uint64(ret[i][:]))
			positions = append(positions, n)
		}
		pos = 0
		tmp.Set(buf[:0])
	}

	enc := h.table.Keys.Transcoder(&empty)
	ok := true
	h.table.Keys.Each(func(d ion.Datum) bool {
		if !isHashConst(d) {
			ok = false
			return false
		}
		enc(&tmp, d)
		endpos[pos] = uint32(tmp.Size())
		pos++
		if pos == 4 {
			flush(4)
		}
		return true
	})
	if !ok {
		return
	}
	if pos > 0 {
		valid := pos
		for pos < 4 {
			// duplicate zero-width lanes up to 4
			endpos[pos] = endpos[pos-1]
			pos++
		}
		flush(valid)
	}
	h.precomputed = tree
	h.positions = positions

	// now try const-ifying the values
	enc = h.table.Values.Transcoder(&empty)
	ok = true
	tmp.Reset()
	i := 0
	h.table.Values.Each(func(d ion.Datum) bool {
		if !isHashConst(d) {
			ok = false
			return false
		}
		tmp.Reset()
		enc(&tmp, d)
		buf := h.slab.malloc(tmp.Size())
		copy(buf, tmp.Bytes())
		pos, ok := vmdispl(buf)
		if !ok {
			panic("slab.malloc returned non-vm memory?")
		}
		_, dst := tree.value(positions[i])
		binary.LittleEndian.PutUint32(dst, uint32(pos))
		binary.LittleEndian.PutUint32(dst[4:], uint32(tmp.Size()))
		i++
		return true
	})
	if !ok {
		h.slab.reset()
		return
	}
	h.complete = true
	p.finalize = append(p.finalize, h.slab.reset)
}

func (h *hashSetImm) precompute() {
	values := h.set
	tree := newRadixTree(0)

	// gather up to 4 values before hashing
	var tmp ion.Buffer
	var empty ion.Symtab
	var endpos [4]uint32
	var pos int

	flush := func(n int) {
		buf := tmp.Bytes()
		// ensure a movq at buf[len(buf)-1] will touch valid memory:
		buf = slices.Grow(buf, 7)
		ret := chacha8x4(&buf[0], endpos)
		for i := 0; i < n; i++ {
			tree.insertSlow(binary.LittleEndian.Uint64(ret[i][:]))
		}
		pos = 0
		tmp.Set(buf[:0])
	}

	enc := values.Transcoder(&empty)
	ok := true
	values.Each(func(d ion.Datum) bool {
		if !isHashConst(d) {
			ok = false
			return false
		}
		enc(&tmp, d)
		endpos[pos] = uint32(tmp.Size())
		pos++
		if pos == 4 {
			flush(4)
		}
		return true
	})
	if !ok {
		return
	}
	if pos > 0 {
		valid := pos
		for pos < 4 {
			// duplicate zero-width lanes up to 4
			endpos[pos] = endpos[pos-1]
			pos++
		}
		flush(valid)
	}
	h.precomputed = tree
}

func (p *prog) hashLookup(lookup *expr.Lookup) (*value, error) {
	v, err := p.serialized(lookup.Expr)
	if err != nil {
		return nil, err
	}
	var elseval *value
	if lookup.Else != nil {
		elseval, err = compile(p, lookup.Else)
		if err != nil {
			return nil, err
		}
	}
	imm := &hashImm{
		table: lookup,
	}
	imm.precompute(p)
	h := p.hash(v)
	res := p.ssaimm(shashlookup, imm, h, p.mask(h))
	if elseval != nil {
		// blend in ELSE value for missing lookups
		res = p.ssa4(sblendv, elseval, p.mask(elseval), res, p.mask(res))
	}
	return res, nil
}

func (p *prog) member(e expr.Node, set *ion.Bag) (*value, error) {
	v, err := p.serialized(e)
	if err != nil {
		return nil, err
	}
	imm := &hashSetImm{
		set: set,
	}
	imm.precompute()
	h := p.hash(v)
	// we're using ssaimm here because we don't
	// want the CSE code to look at the immediate field
	return p.ssaimm(shashmember, imm, h, p.mask(h)), nil
}

func (p *prog) mkhash(st *symtab, imm interface{}) *radixTree64 {
	var tmp ion.Buffer
	lut := imm.(*hashImm)
	if lut.complete {
		// super-fast-path: we've already got a hash table
		return lut.precomputed
	}

	p.literals = true // recompile on symbol table changes

	putval := lut.table.Values.Transcoder(&st.Symtab)
	if lut.precomputed != nil {
		// fast (common) path: just insert values
		tree := lut.precomputed.clone()
		i := 0
		lut.table.Values.Each(func(d ion.Datum) bool {
			tmp.Reset()
			putval(&tmp, d)
			data := st.slab.malloc(tmp.Size())
			copy(data, tmp.Bytes())
			pos, ok := vmdispl(data)
			if !ok {
				panic("vm.slab.malloc returned a bad address")
			}
			_, buf := tree.value(lut.positions[i])
			i++
			binary.LittleEndian.PutUint32(buf, uint32(pos))
			binary.LittleEndian.PutUint32(buf[4:], uint32(tmp.Size()))
			return true
		})
		return tree
	}

	putkey := lut.table.Keys.Transcoder(&st.Symtab)
	tree := newRadixTree(8)

	// batches of 4
	var pos int
	var endpos [4]uint32
	var recent [4]ion.Datum

	flush := func(n int) {
		buf := tmp.Bytes()
		buf = slices.Grow(buf, 7)
		ret := chacha8x4(&buf[0], endpos)
		tmp.Set(buf[:0])
		for i := 0; i < n; i++ {
			putval(&tmp, recent[i])
			buf, _ := tree.Insert(binary.LittleEndian.Uint64(ret[i][:]))
			data := st.slab.malloc(tmp.Size())
			copy(data, tmp.Bytes())
			pos, ok := vmdispl(data)
			if !ok {
				panic("vm.slab.malloc returned a bad address")
			}
			binary.LittleEndian.PutUint32(buf, uint32(pos))
			binary.LittleEndian.PutUint32(buf[4:], uint32(tmp.Size()))
			tmp.Reset()
		}
	}

	lut.table.Keys.EachPair(&lut.table.Values, func(k, v ion.Datum) bool {
		putkey(&tmp, k)
		endpos[pos] = uint32(tmp.Size())
		recent[pos] = v
		pos++
		if pos == 4 {
			flush(4)
			pos = 0
		}
		return true
	})
	if pos > 0 {
		valid := pos
		for pos < 4 {
			// duplicate zero-width lanes up to 4
			endpos[pos] = endpos[pos-1]
			pos++
		}
		flush(valid)
	}
	return tree
}

// hook called on symbolization of hashmember
func (p *prog) mktree(st *symtab, imm interface{}) *radixTree64 {
	hset := imm.(*hashSetImm)
	if hset.precomputed != nil {
		return hset.precomputed
	}
	p.literals = true // recompile on symbol table changes

	values := hset.set
	tree := newRadixTree(0)
	// gather up to 4 values before hashing
	var tmp ion.Buffer
	var endpos [4]uint32
	var pos int

	enc := values.Transcoder(&st.Symtab)
	values.Each(func(d ion.Datum) bool {
		enc(&tmp, d)
		endpos[pos] = uint32(tmp.Size())
		pos++
		if pos == 4 {
			buf := tmp.Bytes()
			// ensure a movq at buf[len(buf)-1] will touch valid memory:
			buf = slices.Grow(buf, 7)
			ret := chacha8x4(&buf[0], endpos)
			for i := 0; i < 4; i++ {
				tree.insertSlow(binary.LittleEndian.Uint64(ret[i][:]))
			}
			pos = 0
			tmp.Set(buf[:0])
		}
		return true
	})
	if pos > 0 {
		valid := pos
		for pos < 4 {
			// duplicate zero-width lanes up to 4
			endpos[pos] = endpos[pos-1]
			pos++
		}
		buf := tmp.Bytes()
		buf = slices.Grow(buf, 7)
		ret := chacha8x4(&buf[0], endpos)
		for i := 0; i < valid; i++ {
			tree.insertSlow(binary.LittleEndian.Uint64(ret[i][:]))
		}
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
			return p.ssa2(sboxmask, v, p.validLanes()), nil
		}
		// already got one
		return v, nil
	case stBool:
		var nonmissing *value
		if _, ok := e.(*expr.IsKey); ok {
			nonmissing = p.validLanes() // all lanes are valid
		} else {
			nonmissing = p.notMissing(v)
		}
		return p.ssa2(sboxmask, v, nonmissing), nil
	case stInt:
		return p.ssa2(sboxint, v, p.mask(v)), nil
	case stFloat:
		return p.ssa2(sboxfloat, v, p.mask(v)), nil
	case stString:
		return p.ssa2(sboxstr, v, p.mask(v)), nil
	case stTime:
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
	return p.store(mem, v, slot)
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
		live := p.andn(merged, when)
		outk = p.or(outk, p.and(live, then))
		merged = p.or(merged, when)
	}
	if final.op == skfalse {
		return outk, nil
	}
	outk = p.or(outk, p.and(final, p.andn(merged, final)))
	return outk, nil
}

// a "generic case" is one in which all the
// results of the arms of the case are coerced to stValue,
// which happens if a) they were already all stValue,
// or b) they have incompatible types and the caller needs
// to perform unboxing again to get at the results
func (p *prog) compileGenericCase(c *expr.Case) (*value, error) {
	var outV, outK *value
	var elseV, elseK, matched *value

	if c.Else != nil && c.Else != (expr.Missing{}) {
		els, err := p.serialized(c.Else)
		if err != nil {
			return nil, err
		}

		els = p.unsymbolized(els)
		if els.primary() != stValue {
			panic("unexpected return type in compileGenericCase()")
		}

		if p.mask(els).op != skfalse {
			elseV = els
			elseK = p.mask(els)
		}
	}

	for i := len(c.Limbs) - 1; i >= 0; i-- {
		when, err := p.compileAsBool(c.Limbs[i].When)
		if err != nil {
			return nil, err
		}

		if when.op == skfalse {
			continue
		}

		thenV, err := p.serialized(c.Limbs[i].Then)
		if err != nil {
			return nil, err
		}

		thenV = p.unsymbolized(thenV)
		if thenV.primary() != stValue {
			panic("unexpected return type in compileGenericCase()")
		}

		thenK := p.and(p.mask(thenV), when)
		if thenK.op == skfalse {
			continue
		}

		if elseV != nil {
			if matched != nil {
				matched = p.or(matched, when)
			} else {
				matched = when
			}
		}

		if outV == nil {
			outV = thenV
			outK = thenK
		} else {
			outV = p.ssa4(sblendv, outV, outK, thenV, thenK)
			outK = outV
		}
	}

	// ELSE must be merged at the end as we need all matching lanes for that.
	if elseV != nil {
		if outV != nil {
			outV = p.ssa4(sblendv, outV, outK, elseV, p.andn(matched, elseK))
			outK = outV
		} else {
			outV = elseV
			outK = elseK
		}
	}

	return p.makevk(outV, outK), nil
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
		v, _ = p.coerceF64(v)
		return v, nil
	}

	var outV, outK *value

	if c.Else != nil && c.Else != (expr.Missing{}) && c.Else != (expr.Null{}) {
		els, err := p.compileAsNumber(c.Else)
		if err != nil {
			return nil, err
		}
		if els.op != skfalse {
			if els.op == sliteral {
				if !isNumericImmediate(els.imm) {
					return nil, fmt.Errorf("%v of type %T is not float nor integer number", els.imm, els.imm)
				}
			}
			outV, outK = p.coerceF64(els)
		}
	}

	for i := len(c.Limbs) - 1; i >= 0; i-- {
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

		if then.op == sliteral {
			if !isNumericImmediate(then.imm) {
				return nil, fmt.Errorf("%v of type %T is not float nor integer number", then.imm, then.imm)
			}
		}

		thenV, thenK := p.coerceF64(then)
		thenK = p.and(thenK, when)

		if outV == nil {
			outV, outK = thenV, thenK
		} else {
			outV = p.ssa4(sblendf64, outV, outK, thenV, thenK)
			outK = outV
		}
	}

	return p.floatk(outV, outK), nil
}

func (p *prog) compileCast(c *expr.Cast) (*value, error) {
	from, err := compile(p, c.From)
	if err != nil {
		return nil, err
	}
	switch c.To {
	case expr.MissingType:
		return p.missing(), nil
	case expr.NullType:
		return p.constant(nil), nil
	case expr.BoolType:
		switch from.primary() {
		case stBool:
			return from, nil
		case stInt:
			return p.ssa2(scvti64tok, from, p.mask(from)), nil
		case stFloat:
			return p.ssa2(scvtf64tok, from, p.mask(from)), nil
		case stValue:
			// we can convert booleans and numbers to bools
			iszero := p.ssa2imm(sequalconst, from, p.mask(from), 0)
			isfalse := p.ssa2(sisfalse, from, p.mask(from))
			eqfalse := p.or(iszero, isfalse)
			oktype := p.checkTag(from, expr.BoolType|expr.NumericType)
			// return (!(b == 0 || b == false) && (b is numeric))
			ret := p.andn(eqfalse, oktype)
			ret.notMissing = oktype
			return ret, nil
		default:
			// not convertible
			return p.missing(), nil
		}
	case expr.IntegerType:
		switch from.primary() {
		case stBool:
			// true/false/missing -> 1/0/missing
			return p.ssa2(scvtktoi64, from, p.notMissing(from)), nil
		case stInt:
			return from, nil
		case stFloat:
			return p.ssa2(sroundi, from, p.mask(from)), nil
		case stValue:
			return p.ssa2(sunboxcvti64, from, p.mask(from)), nil
		default:
			return p.missing(), nil
		}
	case expr.FloatType:
		switch from.primary() {
		case stBool:
			return p.ssa2(scvtktof64, from, p.notMissing(from)), nil
		case stInt:
			return p.ssa2(scvti64tof64, from, p.mask(from)), nil
		case stFloat:
			return from, nil
		case stValue:
			return p.ssa2(sunboxcvtf64, from, p.mask(from)), nil
		default:
			return p.missing(), nil
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
			return p.missing(), nil
		}
	case expr.TimeType:
		switch from.primary() {
		case stTime:
			return from, nil
		case stValue:
			return p.checkTag(from, c.To), nil
		default:
			return p.missing(), nil
		}
	case expr.ListType:
		if from.ret()&stList != 0 {
			return from, nil
		}
		if from.primary() == stValue {
			return p.checkTag(from, c.To), nil
		}
		return p.missing(), nil
	case expr.StructType, expr.DecimalType, expr.SymbolType:
		if from.ret()&stValue != 0 {
			return p.checkTag(from, c.To), nil
		}
		return p.missing(), nil
	default:
		return nil, fmt.Errorf("unsupported cast %q", c)
	}
}

func (p *prog) checkTag(from *value, typ expr.TypeSet) *value {
	return p.ssa2imm(schecktag, from, p.mask(from), uint16(typ))
}
