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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/date"
)

func mismatch(want, got int) error {
	return errsyntaxf("got %d args; need %d", got, want)
}

func errtypef(n Node, f string, args ...interface{}) error {
	return &TypeError{
		At:  n,
		Msg: fmt.Sprintf(f, args...),
	}
}

func errsyntaxf(f string, args ...interface{}) error {
	return &SyntaxError{
		Msg: fmt.Sprintf(f, args...),
	}
}

// fixedArgs can be used to specify
// the type arguments for a builtin function
// when the argument length is fixed
func fixedArgs(lst ...TypeSet) func(Hint, []Node) error {
	return func(h Hint, args []Node) error {
		if len(lst) != len(args) {
			return mismatch(len(lst), len(args))
		}
		for i := range args {
			if !TypeOf(args[i], h).AnyOf(lst[i]) {
				return errtypef(args[i], "not compatible with type %s", lst[i])
			}
		}
		return nil
	}
}

func variadicArgs(kind TypeSet) func(Hint, []Node) error {
	return func(h Hint, args []Node) error {
		for i := range args {
			if !TypeOf(args[i], h).AnyOf(kind) {
				return errtypef(args[i], "not compatible with type %s", kind)
			}
		}
		return nil
	}
}

// builtin information; used in the builtin LUT
type binfo struct {
	// check, if non-nil, should examine
	// the arguments and return an error
	// if they are not well-typed
	check func(Hint, []Node) error
	// simplify, if non-nil, should examine
	// the arguments and return a simplified
	// representation of the expression, or otherwise
	// return nil if the expression could not be simplified
	simplify func(Hint, []Node) Node
	// text, if non-nil, provides a custom
	// textual representation of the built-in function
	text func([]Node, *strings.Builder, bool)

	// ret, if non-zero, specifies the return type
	// of the expression
	ret TypeSet

	// if a builtin is private, it cannot
	// be created during parsing; it can
	// only be created by the query planner
	private bool

	// isTable is set if this builtin
	// is expected only in the table position
	isTable bool
}

type BuiltinOp int

const (
	Concat BuiltinOp = iota
	Trim
	Ltrim
	Rtrim
	Upper
	Lower
	Contains
	ContainsCI
	EqualsCI
	CharLength
	IsSubnetOf
	SubString
	SplitPart

	BitCount

	Abs
	Sign

	Round
	RoundEven
	Trunc
	Floor
	Ceil

	Sqrt
	Cbrt
	Exp
	ExpM1
	Exp2
	Exp10
	Hypot
	Ln
	Ln1p
	Log
	Log2
	Log10
	Pow

	Pi
	Degrees
	Radians
	Sin
	Cos
	Tan
	Asin
	Acos
	Atan
	Atan2

	Least
	Greatest
	WidthBucket

	// generic time-ordering routine;
	// the semantics of
	//   BEFORE(x, y, z...)
	// are equivalent to
	//   x < y && y < z && ...
	// at least two arguments must be present
	Before

	DateAddMicrosecond
	DateAddMillisecond
	DateAddSecond
	DateAddMinute
	DateAddHour
	DateAddDay
	DateAddMonth
	DateAddYear

	DateDiffMicrosecond
	DateDiffMillisecond
	DateDiffSecond
	DateDiffMinute
	DateDiffHour
	DateDiffDay
	DateDiffMonth
	DateDiffYear

	DateExtractMicrosecond
	DateExtractMillisecond
	DateExtractSecond
	DateExtractMinute
	DateExtractHour
	DateExtractDay
	DateExtractMonth
	DateExtractYear
	DateToUnixEpoch
	DateToUnixMicro

	DateTruncMicrosecond
	DateTruncMillisecond
	DateTruncSecond
	DateTruncMinute
	DateTruncHour
	DateTruncDay
	DateTruncMonth
	DateTruncYear

	GeoHash
	GeoTileX
	GeoTileY
	GeoTileES
	GeoDistance

	ObjectSize // SIZE(x)

	TableGlob
	TablePattern

	// used by query planner:
	InSubquery        // matches IN (SELECT ...)
	HashLookup        // matches CASE with only literal comparisons
	InReplacement     // IN_REPLACEMENT(x, id)
	HashReplacement   // HASH_REPLACEMENT(id, kind, k, x)
	ScalarReplacement // SCALAR_REPLACEMENT(id)
	StructReplacement // STRUCT_REPLACEMENT(id)
	ListReplacement   // LIST_REPLACEMENT(id)

	TimeBucket

	MakeList   // MAKE_LIST(args...) constructs a list
	MakeStruct // MAKE_STRUCT(field, value, ...) constructs a structure

	Unspecified // catch-all for opaque built-ins
	maxBuiltin
)

var name2Builtin = map[string]BuiltinOp{
	"CONCAT":                   Concat,
	"TRIM":                     Trim,
	"LTRIM":                    Ltrim,
	"RTRIM":                    Rtrim,
	"UPPER":                    Upper,
	"LOWER":                    Lower,
	"CONTAINS":                 Contains,
	"CONTAINS_CI":              ContainsCI,
	"EQUALS_CI":                EqualsCI,
	"CHAR_LENGTH":              CharLength,
	"CHARACTER_LENGTH":         CharLength,
	"IS_SUBNET_OF":             IsSubnetOf,
	"SUBSTRING":                SubString,
	"SPLIT_PART":               SplitPart,
	"BIT_COUNT":                BitCount,
	"ABS":                      Abs,
	"SIGN":                     Sign,
	"ROUND":                    Round,
	"ROUND_EVEN":               RoundEven,
	"TRUNC":                    Trunc,
	"FLOOR":                    Floor,
	"CEIL":                     Ceil,
	"CEILING":                  Ceil,
	"SQRT":                     Sqrt,
	"CBRT":                     Cbrt,
	"EXP":                      Exp,
	"EXPM1":                    ExpM1,
	"EXP2":                     Exp2,
	"EXP10":                    Exp10,
	"HYPOT":                    Hypot,
	"LN":                       Ln,
	"LN1P":                     Ln1p,
	"LOG":                      Log,
	"LOG2":                     Log2,
	"LOG10":                    Log10,
	"POW":                      Pow,
	"POWER":                    Pow,
	"PI":                       Pi,
	"DEGREES":                  Degrees,
	"RADIANS":                  Radians,
	"SIN":                      Sin,
	"COS":                      Cos,
	"TAN":                      Tan,
	"ASIN":                     Asin,
	"ACOS":                     Acos,
	"ATAN":                     Atan,
	"ATAN2":                    Atan2,
	"LEAST":                    Least,
	"GREATEST":                 Greatest,
	"WIDTH_BUCKET":             WidthBucket,
	"BEFORE":                   Before,
	"DATE_ADD_MICROSECOND":     DateAddMicrosecond,
	"DATE_ADD_MILLISECOND":     DateAddMillisecond,
	"DATE_ADD_SECOND":          DateAddSecond,
	"DATE_ADD_MINUTE":          DateAddMinute,
	"DATE_ADD_HOUR":            DateAddHour,
	"DATE_ADD_DAY":             DateAddDay,
	"DATE_ADD_MONTH":           DateAddMonth,
	"DATE_ADD_YEAR":            DateAddYear,
	"DATE_DIFF_MICROSECOND":    DateDiffMicrosecond,
	"DATE_DIFF_MILLISECOND":    DateDiffMillisecond,
	"DATE_DIFF_SECOND":         DateDiffSecond,
	"DATE_DIFF_MINUTE":         DateDiffMinute,
	"DATE_DIFF_HOUR":           DateDiffHour,
	"DATE_DIFF_DAY":            DateDiffDay,
	"DATE_DIFF_MONTH":          DateDiffMonth,
	"DATE_DIFF_YEAR":           DateDiffYear,
	"DATE_EXTRACT_MICROSECOND": DateExtractMicrosecond,
	"DATE_EXTRACT_MILLISECOND": DateExtractMillisecond,
	"DATE_EXTRACT_SECOND":      DateExtractSecond,
	"DATE_EXTRACT_MINUTE":      DateExtractMinute,
	"DATE_EXTRACT_HOUR":        DateExtractHour,
	"DATE_EXTRACT_DAY":         DateExtractDay,
	"DATE_EXTRACT_MONTH":       DateExtractMonth,
	"DATE_EXTRACT_YEAR":        DateExtractYear,
	"DATE_TRUNC_MICROSECOND":   DateTruncMicrosecond,
	"DATE_TRUNC_MILLISECOND":   DateTruncMillisecond,
	"DATE_TRUNC_SECOND":        DateTruncSecond,
	"DATE_TRUNC_MINUTE":        DateTruncMinute,
	"DATE_TRUNC_HOUR":          DateTruncHour,
	"DATE_TRUNC_DAY":           DateTruncDay,
	"DATE_TRUNC_MONTH":         DateTruncMonth,
	"DATE_TRUNC_YEAR":          DateTruncYear,
	"GEO_HASH":                 GeoHash,
	"GEO_TILE_X":               GeoTileX,
	"GEO_TILE_Y":               GeoTileY,
	"GEO_TILE_ES":              GeoTileES,
	"GEO_DISTANCE":             GeoDistance,
	"IN_SUBQUERY":              InSubquery,
	"HASH_LOOKUP":              HashLookup,
	"IN_REPLACEMENT":           InReplacement,
	"HASH_REPLACEMENT":         HashReplacement,
	"SCALAR_REPLACEMENT":       ScalarReplacement,
	"STRUCT_REPLACEMENT":       StructReplacement,
	"LIST_REPLACEMENT":         ListReplacement,
	"TIME_BUCKET":              TimeBucket,
	"TO_UNIX_EPOCH":            DateToUnixEpoch,
	"TO_UNIX_MICRO":            DateToUnixMicro,
	"SIZE":                     ObjectSize,
	"TABLE_GLOB":               TableGlob,
	"TABLE_PATTERN":            TablePattern,
	"MAKE_LIST":                MakeList,
	"MAKE_STRUCT":              MakeStruct,
}

var builtin2Name [maxBuiltin]string

func init() {
	// take the shortest name in name2Builtin
	// and create the reverse-mapping
	for k, v := range name2Builtin {
		cur := builtin2Name[v]
		if cur == "" || len(cur) > len(k) {
			builtin2Name[v] = k
		}
	}
}

func (b BuiltinOp) String() string {
	if b >= 0 && b < Unspecified {
		return builtin2Name[b]
	}
	return "UNKNOWN"
}

func checkBefore(h Hint, args []Node) error {
	if len(args) < 2 {
		return mismatch(len(args), 2)
	}
	for i := range args {
		if !TypeOf(args[i], h).AnyOf(TimeType) {
			return errtype(args[i], "not a timestamp")
		}
	}
	return nil
}

func simplifyBefore(h Hint, args []Node) Node {
	for i := range args[:len(args)-1] {
		left, right := args[i], args[i+1]
		if lt, ok := left.(*Timestamp); ok {
			if rt, ok := right.(*Timestamp); ok {
				return Bool(lt.Value.Before(rt.Value))
			}
		}
	}
	return nil
}

func checkContains(h Hint, args []Node) error {
	if len(args) != 2 {
		return mismatch(len(args), 2)
	}
	if _, ok := args[1].(String); !ok {
		return errsyntax("CONTAINS requires a literal string argument")
	}
	if !TypeOf(args[0], h).AnyOf(StringType) {
		return errtype(args[0], "not a string")
	}
	return nil
}

func simplifyRtrim(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}
	args[0] = missingUnless(args[0], h, StringType)
	if innerTerm1, ok := args[0].(*Builtin); ok {
		switch innerTerm1.Func {
		case Ltrim: // RTRIM(LTRIM(x)) -> TRIM(x)
			return CallOp(Trim, innerTerm1.Args[0])
		case Rtrim: // redundant RTRIM:  RTRIM(RTRIM(x)) -> RTRIM(x)
			return CallOp(Rtrim, innerTerm1.Args[0])
		case Trim: // redundant LTRIM: RTRIM(TRIM(x)) -> TRIM(x)
			return CallOp(Trim, innerTerm1.Args[0])
		case Upper: // push TRIM downwards: RTRIM(UPPER(x)) -> UPPER(RTRIM(x))
			return CallOp(Upper, CallOp(Rtrim, innerTerm1.Args[0]))
		case Lower: // push TRIM downwards: RTRIM(LOWER(x)) -> LOWER(RTRIM(x))
			return CallOp(Lower, CallOp(Rtrim, innerTerm1.Args[0]))
		}
	}
	return nil
}

func simplifyLtrim(h Hint, args []Node) Node {
	args[0] = missingUnless(args[0], h, StringType)
	if len(args) != 1 {
		return nil
	}
	// LTRIM() function returns a string after removing all leading characters (remChars) from str.
	// If remChar is not provided all leading whitespaces are trimmed.
	// LTRIM([remChars] FROM str)
	if innerTerm1, ok := args[0].(*Builtin); ok {
		switch innerTerm1.Func {
		case Rtrim: // LTRIM(RTRIM(x)) -> TRIM(x)
			return CallOp(Trim, innerTerm1.Args[0])
		case Ltrim: // redundant LTRIM:  LTRIM(LTRIM(x)) -> LTRIM(x)
			return CallOp(Ltrim, innerTerm1.Args[0])
		case Trim: // redundant TRIM:  TRIM(TRIM(x)) -> TRIM(x)
			return CallOp(Trim, innerTerm1.Args[0])
		case Upper: // push TRIM downwards: LTRIM(UPPER(x)) -> UPPER(LTRIM(x))
			return CallOp(Upper, CallOp(Ltrim, innerTerm1.Args[0]))
		case Lower: // push TRIM downwards: LTRIM(LOWER(x)) -> LOWER(LTRIM(x))
			return CallOp(Lower, CallOp(Rtrim, innerTerm1.Args[0]))
		}
	}
	return nil
}

func simplifyTrim(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}
	// TRIM() function returns a string after removing all prefixes or suffixes (remstr) from str.
	// If remStr is not provided all leading or trailing whitespaces are trimmed.
	// TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM ] str)
	args[0] = missingUnless(args[0], h, StringType)
	if term, ok := args[0].(*Builtin); ok {
		switch term.Func {
		case Ltrim: // redundant LTRIM: TRIM(LTRIM(x)) -> TRIM(x)
			return CallOp(Trim, term.Args[0])
		case Rtrim: // redundant RTRIM:  TRIM(RTRIM(x)) -> TRIM(x)
			return CallOp(Trim, term.Args[0])
		case Trim: // redundant TRIM: TRIM(TRIM(x)) -> TRIM(x)
			return CallOp(Trim, term.Args[0])
		case Upper: // push TRIM downwards: RTRIM(UPPER(x)) -> UPPER(TRIM(x))
			return CallOp(Upper, CallOp(Trim, term.Args[0]))
		case Lower: // push TRIM downwards: RTRIM(LOWER(x)) -> LOWER(TRIM(x))
			return CallOp(Lower, CallOp(Trim, term.Args[0]))
		}
	}
	return nil
}

func simplifyContains(h Hint, args []Node) Node {
	args[0] = missingUnless(args[0], h, StringType)
	if term1, ok := args[0].(*Builtin); ok {
		switch term1.Func {
		case Upper:
			term2String := string(args[1].(String))
			if strings.ToUpper(term2String) != term2String {
				// CONTAINS(UPPER(x), "fred") -> FALSE
				return Bool(false)
			}
			// CONTAINS(UPPER(x), "FRED") -> CONTAINS_CI(x, "FRED")
			return CallOp(ContainsCI, term1.Args[0], args[1])
		case Lower:
			term2String := string(args[1].(String))
			if strings.ToLower(term2String) != term2String {
				// CONTAINS(LOWER(x), "FRED") -> FALSE
				return Bool(false)
			}
			// CONTAINS(LOWER(x), "fred") -> CONTAINS_CI(x, "fred")
			return CallOp(ContainsCI, term1.Args[0], args[1])
		}
	}
	return nil
}

func simplifyUpper(h Hint, args []Node) Node {
	arg0 := missingUnless(args[0], h, StringType)
	if s, ok := arg0.(String); ok {
		return String(strings.ToUpper(string(s)))
	}

	return nil
}

func simplifyLower(h Hint, args []Node) Node {
	arg0 := missingUnless(args[0], h, StringType)
	if s, ok := arg0.(String); ok {
		return String(strings.ToLower(string(s)))
	}

	return nil
}

func checkIsSubnetOf(h Hint, args []Node) error {
	nArgs := len(args)
	if nArgs != 2 && nArgs != 3 {
		return errsyntaxf("IS_SUBNET_OF expects 2 or 3 arguments, but found %d", nArgs)
	}
	arg0, ok := args[0].(String)
	if !ok {
		return errtypef(args[0], "not a string but a %T", args[0])
	}
	arg1, ok := args[1].(String)
	if !ok {
		return errtypef(args[1], "not a string but a %T", args[1])
	}
	if nArgs == 2 {
		if _, _, err := net.ParseCIDR(string(arg0)); err != nil {
			return errtypef(args[0], "%s", err)
		}
	} else {
		if net.ParseIP(string(arg0)) == nil {
			return errtypef(args[0], "not an IP address")
		}
		if net.ParseIP(string(arg1)) == nil {
			return errtypef(args[1], "not an IP address")
		}
		if !TypeOf(args[2], h).AnyOf(StringType) {
			return errtypef(args[2], "not a string but a %T", args[2])
		}
	}
	return nil
}

func simplifyIsSubnetOf(h Hint, args []Node) Node {
	if len(args) == 2 { // first argument is a CIDR subnet e.g. 192.1.2.3/8
		arg0, ok := args[0].(String)
		if !ok {
			return nil // found an error: let checkIsSubnetOf handle this
		}
		_, ipv4Net, err := net.ParseCIDR(string(arg0))
		if err != nil {
			return nil // found an error: let checkIsSubnetOf handle this
		}
		mask := binary.BigEndian.Uint32(ipv4Net.Mask)
		start := binary.BigEndian.Uint32(ipv4Net.IP)
		finish := (start & mask) | (mask ^ 0xffffffff)

		minIP := make(net.IP, 4)
		binary.BigEndian.PutUint32(minIP, start)
		maxIP := make(net.IP, 4)
		binary.BigEndian.PutUint32(maxIP, finish)

		arg1 := missingUnless(args[1], h, StringType)
		return CallOp(IsSubnetOf, Node(String(minIP.String())), Node(String(maxIP.String())), arg1)
	} else if len(args) == 3 { // first and second argument are an IP address
		arg0, ok := args[0].(String)
		if !ok {
			return nil // found an error: let checkIsSubnetOf handle this
		}
		arg1, ok := args[1].(String)
		if !ok {
			return nil // found an invalid IP address: let checkIsSubnetOf handle this
		}
		minIP := net.ParseIP(string(arg0))
		if minIP == nil {
			return nil // found an invalid IP address: let checkIsSubnetOf handle this
		}
		maxIP := net.ParseIP(string(arg1))
		if maxIP == nil {
			return nil // found an invalid IP address: let checkIsSubnetOf handle this
		}

		switch bytes.Compare(minIP.To4(), maxIP.To4()) {
		case 0: // min == max: simplify to trivial str cmp
			return Compare(Equals, args[0], args[1])
		case 1: // min > max has no solutions
			return Bool(false)
		}
	}
	return nil
}

func simplifyCharLength(h Hint, args []Node) Node {
	switch v := args[0].(type) {
	case String:
		return Integer(utf8.RuneCountInString(string(v)))

	case *Builtin:
		switch v.Func {
		case Lower, Upper:
			return CallOp(CharLength, v.Args[0])

		case Concat:
			strlength := 0
			var nodes []Node
			for i := range v.Args {
				if s, ok := v.Args[i].(String); ok {
					strlength += utf8.RuneCountInString(string(s))
				} else {
					nodes = append(nodes, CallOp(CharLength, v.Args[i]))
				}
			}

			if strlength > 0 {
				nodes = append(nodes, Integer(strlength))
			}

			if len(nodes) > 0 {
				return reduce(nodes, func(n1, n2 Node) Node {
					return Add(n1, n2)
				})
			}
		}
	}
	return nil
}

func reduce(nodes []Node, binop func(n1, n2 Node) Node) Node {
	if len(nodes) == 1 {
		return nodes[0]
	}

	top := binop(nodes[0], nodes[1])
	for i := 2; i < len(nodes); i++ {
		top = binop(top, nodes[i])
	}

	return top
}

func checkTrim(h Hint, args []Node) error {
	switch len(args) {
	case 2:
		// [LR]?TRIM(cutset, str)
		if _, ok := args[0].(String); !ok {
			return errsyntax("TRIM-family functions require a constant string argument for cutset")
		}
		if !TypeOf(args[1], h).AnyOf(StringType) {
			return errtype(args[1], "not a string")
		}
		return nil
	case 1:
		// [LR]?TRIM(str)
		if !TypeOf(args[0], h).AnyOf(StringType) {
			return errtype(args[0], "not a string")
		}
		return nil
	default:
		return errsyntaxf("TRIM-family functions expect 1 or 2 arguments, but found %d", len(args))
	}
}

func simplifySubString(h Hint, args []Node) (result Node) {
	returnNewNode := false
	arg0 := args[0]
	arg1 := args[1]
	var arg2 Node

	if len(args) == 2 { // third arguments 'length' is optional; when not present it signals 'till the end of the string'
		arg2 = Node(Integer(math.MaxInt32))
		returnNewNode = true
	} else {
		arg2 = args[2]
	}

	if term, ok := arg1.(Integer); ok {
		// according to this doc (https://docs.aws.amazon.com/qldb/latest/developerguide/ql-functions.substring.html)
		// offsets smaller than 1 are equal to offsets equal to 1
		if int(term) < 1 {
			arg1 = Integer(1)
			returnNewNode = true
		}
	}

	if length, ok := arg2.(Integer); ok {
		if position, ok := arg1.(Integer); ok {
			// SUBSTRING(s, 1) => s
			// SUBSTRING(s, 1, num<=0) => s
			if position == 1 && (length == math.MaxInt32 || length < 0) {
				return arg0
			}
		}
	}

	if term, ok := arg0.(*Builtin); ok {
		switch term.Func {
		case Upper: // push SUBSTRING downwards: SUBSTRING(UPPER(x),a,b) -> UPPER(SUBSTRING(x,a,b))
			return CallOp(Upper, CallOp(SubString, term.Args[0], arg1, arg2))
		case Lower: // push SUBSTRING downwards: SUBSTRING(LOWER(x),a,b) -> LOWER(SUBSTRING(x,a,b))
			return CallOp(Lower, CallOp(SubString, term.Args[0], arg1, arg2))
		}
	}
	if returnNewNode {
		return CallOp(SubString, arg0, arg1, arg2)
	}
	return nil
}

func checkSubString(h Hint, args []Node) error {
	nArgs := len(args)
	if nArgs != 2 && nArgs != 3 {
		return errsyntaxf("SUBSTRING expects 2 or 3 arguments, but found %d", nArgs)
	}
	if !TypeOf(args[0], h).AnyOf(StringType) {
		return errtype(args[0], "not a string")
	}
	if !TypeOf(args[1], h).AnyOf(NumericType) {
		return errtype(args[1], "not a number")
	}
	if nArgs == 3 {
		if !TypeOf(args[2], h).AnyOf(NumericType) {
			return errtype(args[2], "not a number")
		}
	}
	return nil
}

func checkSplitPart(h Hint, args []Node) error {
	nArgs := len(args)
	if nArgs != 3 {
		return errsyntaxf("SPLIT_PART expects 3 arguments, but found %d", nArgs)
	}
	if str, ok := args[1].(String); !ok {
		return errsyntaxf("SPLIT_PART argument 1 is not a string")
	} else if len(str) != 1 {
		return errsyntaxf("SPLIT_PART only accepts single-character delimiters")
	}
	if !TypeOf(args[2], h).AnyOf(NumericType) {
		return errtype(args[2], "not a integer")
	}
	return nil
}

var unaryStringArgs = fixedArgs(StringType)
var variadicNumeric = variadicArgs(NumericType)
var fixedTime = fixedArgs(TimeType)

func simplifyDateExtract(part Timepart) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 1 {
			return nil
		}
		if ts, ok := args[0].(*Timestamp); ok {
			return DateExtract(part, ts)
		}
		return nil
	}
}

func simplifyToUnixEpoch(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}
	ts, ok := args[0].(*Timestamp)
	if !ok {
		return nil
	}
	return Integer(ts.Value.Unix())
}

func simplifyToUnixMicro(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}
	ts, ok := args[0].(*Timestamp)
	if !ok {
		return nil
	}
	return Integer(ts.Value.UnixMicro())
}

func simplifyDateTrunc(part Timepart) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 1 {
			return nil
		}
		if ts, ok := args[0].(*Timestamp); ok {
			return DateTrunc(part, ts)
		}
		return nil
	}
}

func checkInSubquery(h Hint, args []Node) error {
	if len(args) != 2 {
		return mismatch(2, len(args))
	}
	if _, ok := args[1].(*Select); !ok {
		return errsyntaxf("second argument to IN_SUBQUERY is %q", args[1])
	}
	return nil
}

// HASH_LOOKUP(value, if_first, then_first, ..., [otherwise])
func checkHashLookup(h Hint, args []Node) error {
	if len(args) < 3 || len(args)&1 == 0 {
		return mismatch(3, len(args))
	}
	tail := args[1:]
	for i := range tail {
		_, ok := tail[i].(Constant)
		if !ok {
			errsyntaxf("argument %s to HASH_LOOKUP not a literal", tail[i])
		}
	}
	return nil
}

func checkInReplacement(h Hint, args []Node) error {
	if len(args) != 2 {
		return mismatch(2, len(args))
	}
	if _, ok := args[1].(Integer); !ok {
		return errsyntaxf("second argument to IN_REPLACEMENT is %q", args[1])
	}
	return nil
}

// HASH_REPLACEMENT(id, kind, key, x [, default])
func checkHashReplacement(h Hint, args []Node) error {
	if len(args) != 4 && len(args) != 5 {
		return mismatch(4, len(args))
	}
	if _, ok := args[0].(Integer); !ok {
		return errsyntaxf("first argument to HASH_REPLACEMENT is %q", ToString(args[0]))
	}
	kind, ok := args[1].(String)
	if !ok {
		return errsyntaxf("second argument to HASH_REPLACEMENT is %q", ToString(args[1]))
	}
	switch k := string(kind); k {
	case "scalar", "struct", "list":
		// ok
	default:
		return errsyntaxf("second argument to HASH_REPLACEMENT is %q", k)
	}
	if _, ok := args[2].(String); !ok {
		return errsyntaxf("third argument to HASH_REPLACEMENT is %q", ToString(args[2]))
	}
	return nil
}

func checkScalarReplacement(h Hint, args []Node) error {
	if len(args) != 1 {
		return mismatch(1, len(args))
	}
	if _, ok := args[0].(Integer); !ok {
		return errsyntaxf("bad argument to SCALAR_REPLACEMENT %q", args[0])
	}
	return nil
}

func nodeTypeName(node Node) string {
	switch node.(type) {
	case String:
		return "string"
	case Integer:
		return "integer"
	case Float:
		return "float"
	case Bool:
		return "bool"
	case *Rational:
		return "rational"
	case *Timestamp:
		return "timestamp"
	case Null:
		return "null"
	}

	return fmt.Sprintf("%T", node)
}

func checkObjectSize(h Hint, args []Node) error {
	if len(args) != 1 {
		return errsyntaxf("SIZE expects one argument, but found %d", len(args))
	}

	switch args[0].(type) {
	case *Path, *List, *Struct:
		return nil
	}

	return errtypef(args[0], "SIZE is undefined for values of type %s", nodeTypeName(args[0]))
}

func simplifyObjectSize(h Hint, args []Node) (result Node) {
	switch v := args[0].(type) {
	case *Struct:
		return Integer(len(v.Fields))

	case *List:
		return Integer(len(v.Values))

	case Missing:
		return v

	case Null:
		return v
	}

	return nil
}

// Flattens all CONCATs into the output array of nodes with literals joined and empty strings removed
func flattenConcatRecurse(output []Node, args []Node) []Node {
	for _, node := range args {
		if fn, ok := node.(*Builtin); ok && fn.Func == Concat {
			output = flattenConcatRecurse(output, fn.Args)
			continue
		}

		if str, ok := node.(String); ok {
			// Remove empty strings
			if str == "" {
				continue
			}

			if len(output) > 0 {
				if prevStr, prevIsString := output[len(output)-1].(String); prevIsString {
					output[len(output)-1] = prevStr + str
					continue
				}
			}
		}

		output = append(output, node)
	}

	return output
}

func flattenConcat(args []Node) []Node {
	output := flattenConcatRecurse(make([]Node, 0, len(args)), args)

	// If all strings were "" there would be no string in the output node list
	// as we have eliminated them all - so add an empty string in such case.
	if len(output) == 0 {
		output = append(output, String(""))
	}

	return output
}

func simplifyConcat(h Hint, args []Node) Node {
	flattenedArgs := flattenConcat(args)

	// Return a string literal if all arguments were simplified to a single string literal.
	if len(flattenedArgs) == 1 {
		if str, ok := flattenedArgs[0].(String); ok {
			return str
		}
	}

	if op := concatLowerUpper(flattenedArgs); op != Unspecified {
		final := make([]Node, len(flattenedArgs))
		for i := range flattenedArgs {
			if builtin, ok := flattenedArgs[i].(*Builtin); ok {
				final[i] = builtin.Args[0]
			} else {
				final[i] = flattenedArgs[i]
			}
		}

		return CallOp(op, CallOp(Concat, final...))
	}

	return CallOp(Concat, flattenedArgs...)
}

// concatLowerUpper returns whether all arguments
// are either: uppercase strings and calls to UPPER,
// or lowercase strings and calls to LOWER. Returns
// appropriate operation or Unspecified otherwise.
//
// Note that len(args) > 1 and at least one item
// on that list is not a string -- that properties
// are guaranteed by the prior steps of simplify.
func concatLowerUpper(args []Node) BuiltinOp {
	result := Unspecified

	for i := range args {
		op := Unspecified
		switch n := args[i].(type) {
		case *Builtin:
			switch n.Func {
			case Lower:
				op = Lower
			case Upper:
				op = Upper
			}

		case String:
			if isLower(string(n)) {
				op = Lower
			} else if isUpper(string(n)) {
				op = Upper
			}
		}

		if op == Unspecified {
			// not a string nor LOWER/UPPER
			return Unspecified
		}

		if result == Unspecified {
			result = op
		} else if result != op {
			// case mismatch
			return Unspecified
		}
	}

	return result
}

func checkTableGlob(h Hint, args []Node) error {
	if len(args) != 1 {
		return mismatch(1, len(args))
	}
	if _, ok := args[0].(*Path); !ok {
		return errsyntaxf("argument to TABLE_GLOB is %q", ToString(args[0]))
	}
	return nil
}

func checkTablePattern(h Hint, args []Node) error {
	if len(args) != 1 {
		return mismatch(1, len(args))
	}
	if _, ok := args[0].(*Path); !ok {
		return errsyntaxf("argument to TABLE_PATTERN is %q", ToString(args[0]))
	}
	return nil
}

// convert MAKE_LIST(...) into a constant list
// when all the arguments are constant:
func simplifyMakeList(h Hint, args []Node) Node {
	var items []Constant
	for i := range args {
		c, ok := args[i].(Constant)
		if !ok {
			return nil
		}
		items = append(items, c)
	}
	return &List{Values: items}
}

func simplifyMakeStruct(h Hint, args []Node) Node {
	var fields []Field
	for i := 0; i < len(args); i += 2 {
		str, ok := args[i].(String)
		if !ok {
			return nil
		}
		if i+1 == len(args) {
			return nil
		}
		val, ok := args[i+1].(Constant)
		if !ok {
			return nil
		}
		fields = append(fields, Field{
			Label: string(str),
			Value: val,
		})
	}
	return &Struct{Fields: fields}
}

func makeListText(args []Node, dst *strings.Builder, redact bool) {
	dst.WriteByte('[')
	for i := range args {
		if i > 0 {
			dst.WriteString(", ")
		}
		args[i].text(dst, redact)
	}
	dst.WriteByte(']')
}

func makeStructText(args []Node, dst *strings.Builder, redact bool) {
	dst.WriteByte('{')
	sep := []string{", ", ": "}
	for i := range args {
		if i > 0 {
			dst.WriteString(sep[i&1])
		}
		args[i].text(dst, redact)
	}
	dst.WriteByte('}')
}

func adjtime(fn func(x int64, val date.Time) date.Time) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 2 {
			return nil
		}
		amt, ok := args[0].(Integer)
		if !ok {
			return nil
		}
		stamp, ok := args[1].(*Timestamp)
		if !ok {
			return nil
		}
		return &Timestamp{Value: fn(int64(amt), stamp.Value)}
	}
}

func adjpart(part Timepart) func(x int64, val date.Time) date.Time {
	return func(x int64, val date.Time) date.Time {
		year, month := val.Year(), val.Month()
		day, hour := val.Day(), val.Hour()
		minute, sec := val.Minute(), val.Second()
		switch part {
		default:
			panic("bad timepart")
		case Second:
			sec += int(x)
		case Minute:
			minute += int(x)
		case Hour:
			hour += int(x)
		case Day:
			day += int(x)
		case Month:
			month += int(x)
		case Year:
			year += int(x)
		}
		return date.Date(year, month, day, hour, minute, sec, val.Nanosecond())
	}
}

var (
	dateAddMicrosecond = adjtime(func(x int64, val date.Time) date.Time {
		return date.UnixMicro(val.UnixMicro() + x)
	})
	dateAddMillisecond = adjtime(func(x int64, val date.Time) date.Time {
		us := val.UnixMicro() + (1000 * x)
		return date.UnixMicro(us)
	})
	dateAddSecond = adjtime(adjpart(Second))
	dateAddMinute = adjtime(adjpart(Minute))
	dateAddHour   = adjtime(adjpart(Hour))
	dateAddDay    = adjtime(adjpart(Day))
	dateAddMonth  = adjtime(adjpart(Month))
	dateAddYear   = adjtime(adjpart(Year))
)

var builtinInfo = [maxBuiltin]binfo{
	Concat:     {check: variadicArgs(StringType), private: true, ret: StringType | MissingType, simplify: simplifyConcat},
	Trim:       {check: checkTrim, ret: StringType | MissingType, simplify: simplifyTrim},
	Ltrim:      {check: checkTrim, ret: StringType | MissingType, simplify: simplifyLtrim},
	Rtrim:      {check: checkTrim, ret: StringType | MissingType, simplify: simplifyRtrim},
	Upper:      {check: unaryStringArgs, ret: StringType | MissingType, simplify: simplifyUpper},
	Lower:      {check: unaryStringArgs, ret: StringType | MissingType, simplify: simplifyLower},
	Contains:   {check: checkContains, private: true, ret: LogicalType, simplify: simplifyContains},
	ContainsCI: {check: checkContains, private: true, ret: LogicalType},
	CharLength: {check: unaryStringArgs, ret: UnsignedType | MissingType, simplify: simplifyCharLength},
	IsSubnetOf: {check: checkIsSubnetOf, ret: LogicalType, simplify: simplifyIsSubnetOf},
	SubString:  {check: checkSubString, ret: StringType | MissingType, simplify: simplifySubString},
	SplitPart:  {check: checkSplitPart, ret: StringType | MissingType},
	EqualsCI:   {ret: LogicalType},

	BitCount:  {check: fixedArgs(NumericType), ret: IntegerType},
	Abs:       {check: fixedArgs(NumericType), ret: NumericType, simplify: simplifyAbs},
	Sign:      {check: fixedArgs(NumericType), ret: NumericType, simplify: simplifySign},
	Round:     {check: fixedArgs(NumericType), ret: FloatType, simplify: simplifyRound},
	RoundEven: {check: fixedArgs(NumericType), ret: FloatType, simplify: simplifyRoundEven},
	Trunc:     {check: fixedArgs(NumericType), ret: FloatType, simplify: simplifyTrunc},
	Floor:     {check: fixedArgs(NumericType), ret: FloatType, simplify: simplifyFloor},
	Ceil:      {check: fixedArgs(NumericType), ret: FloatType, simplify: simplifyCeil},
	Sqrt:      {check: fixedArgs(NumericType), ret: FloatType},
	Cbrt:      {check: fixedArgs(NumericType), ret: FloatType},
	Exp:       {check: fixedArgs(NumericType), ret: FloatType},
	Exp2:      {check: fixedArgs(NumericType), ret: FloatType},
	Exp10:     {check: fixedArgs(NumericType), ret: FloatType},
	ExpM1:     {check: fixedArgs(NumericType), ret: FloatType},
	Hypot:     {check: fixedArgs(NumericType, NumericType), ret: FloatType},
	Ln:        {check: fixedArgs(NumericType), ret: FloatType},
	Log:       {check: variadicArgs(NumericType), ret: FloatType},
	Log2:      {check: fixedArgs(NumericType), ret: FloatType},
	Log10:     {check: fixedArgs(NumericType), ret: FloatType},
	Pow:       {check: fixedArgs(NumericType, NumericType), ret: FloatType},
	Pi:        {check: fixedArgs(), ret: FloatType},
	Degrees:   {check: fixedArgs(NumericType), ret: FloatType},
	Radians:   {check: fixedArgs(NumericType), ret: FloatType},
	Sin:       {check: fixedArgs(NumericType), ret: FloatType},
	Cos:       {check: fixedArgs(NumericType), ret: FloatType},
	Tan:       {check: fixedArgs(NumericType), ret: FloatType},
	Asin:      {check: fixedArgs(NumericType), ret: FloatType},
	Acos:      {check: fixedArgs(NumericType), ret: FloatType},
	Atan:      {check: fixedArgs(NumericType), ret: FloatType},
	Atan2:     {check: fixedArgs(NumericType, NumericType), ret: FloatType},

	Least:       {check: variadicNumeric, ret: NumericType | MissingType},
	Greatest:    {check: variadicNumeric, ret: NumericType | MissingType},
	WidthBucket: {check: fixedArgs(NumericType, NumericType, NumericType, NumericType), ret: NumericType},
	Before:      {check: checkBefore, ret: LogicalType, simplify: simplifyBefore},

	DateAddMicrosecond:     {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMicrosecond},
	DateAddMillisecond:     {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMillisecond},
	DateAddSecond:          {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddSecond},
	DateAddMinute:          {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMinute},
	DateAddHour:            {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddHour},
	DateAddDay:             {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddDay},
	DateAddMonth:           {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMonth},
	DateAddYear:            {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddYear},
	DateDiffMicrosecond:    {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMillisecond:    {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffSecond:         {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMinute:         {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffHour:           {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffDay:            {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMonth:          {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffYear:           {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractMicrosecond: {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Microsecond)},
	DateExtractMillisecond: {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Millisecond)},
	DateExtractSecond:      {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Second)},
	DateExtractMinute:      {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Minute)},
	DateExtractHour:        {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Hour)},
	DateExtractDay:         {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Day)},
	DateExtractMonth:       {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Month)},
	DateExtractYear:        {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType, simplify: simplifyDateExtract(Year)},
	DateTruncMicrosecond:   {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Microsecond)},
	DateTruncMillisecond:   {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Millisecond)},
	DateTruncSecond:        {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Second)},
	DateTruncMinute:        {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Minute)},
	DateTruncHour:          {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Hour)},
	DateTruncDay:           {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Day)},
	DateTruncMonth:         {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Month)},
	DateTruncYear:          {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Year)},
	DateToUnixEpoch:        {check: fixedTime, ret: IntegerType | MissingType, simplify: simplifyToUnixEpoch},
	DateToUnixMicro:        {check: fixedTime, ret: IntegerType | MissingType, simplify: simplifyToUnixMicro},

	GeoHash:     {check: fixedArgs(NumericType, NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileX:    {check: fixedArgs(NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileY:    {check: fixedArgs(NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileES:   {check: fixedArgs(NumericType, NumericType, IntegerType), ret: StringType | MissingType},
	GeoDistance: {check: fixedArgs(NumericType, NumericType, NumericType, NumericType), ret: FloatType | MissingType},

	ObjectSize: {check: checkObjectSize, ret: NumericType | MissingType, simplify: simplifyObjectSize},

	InSubquery:        {check: checkInSubquery, private: true, ret: LogicalType},
	HashLookup:        {check: checkHashLookup, private: true, ret: AnyType},
	InReplacement:     {check: checkInReplacement, private: true, ret: LogicalType},
	HashReplacement:   {check: checkHashReplacement, private: true, ret: AnyType},
	ScalarReplacement: {check: checkScalarReplacement, private: true, ret: AnyType},
	ListReplacement:   {check: checkScalarReplacement, private: true, ret: ListType},
	StructReplacement: {check: checkScalarReplacement, private: true, ret: StructType},

	TimeBucket: {check: fixedArgs(TimeType, NumericType), ret: NumericType},

	MakeList:   {simplify: simplifyMakeList, ret: ListType, private: true, text: makeListText},
	MakeStruct: {simplify: simplifyMakeStruct, ret: StructType, private: true, text: makeStructText},

	TableGlob:    {check: checkTableGlob, ret: AnyType, isTable: true},
	TablePattern: {check: checkTablePattern, ret: AnyType, isTable: true},
}

func (b *Builtin) isTable() bool {
	i := b.info()
	return i == nil || i.isTable
}

func (b *Builtin) info() *binfo {
	if b.Func >= 0 && b.Func < Unspecified {
		return &builtinInfo[b.Func]
	}
	return nil
}

func (b *Builtin) check(h Hint) error {
	bi := b.info()
	if bi == nil {
		return errsyntaxf("unrecognized builtin %q", b.Name())
	}
	if bi.check != nil {
		err := bi.check(h, b.Args)
		if err != nil {
			errat(err, b)
			return err
		}
	}
	return nil
}

func (b *Builtin) typeof(h Hint) TypeSet {
	bi := b.info()
	if bi == nil {
		return AnyType
	}
	return bi.ret
}

func (b *Builtin) simplify(h Hint) Node {
	bi := b.info()
	if bi == nil || bi.simplify == nil {
		return b
	}
	if n := bi.simplify(h, b.Args); n != nil {
		return n
	}
	return b
}

// Private returns whether or not
// the builtin has been reserved for
// use by the query planner or intermediate
// optimizations.
// Private functions are illegal in user-provided input.
func (b *Builtin) Private() bool {
	bi := b.info()
	if bi != nil {
		return bi.private
	}
	return false
}
