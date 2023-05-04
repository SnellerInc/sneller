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

//go:generate go run _generate/builtin_names.go
//go:generate goimports -w builtin_names.go

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
	"github.com/SnellerInc/sneller/ion"
)

func mismatch(want, got int) error {
	return errsyntaxf("got %d args; need %d", got, want)
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
				return errtype(args[i], "not compatible with type %s", lst[i])
			}
		}
		return nil
	}
}

func variadicArgs(kind TypeSet) func(Hint, []Node) error {
	return func(h Hint, args []Node) error {
		for i := range args {
			if !TypeOf(args[i], h).AnyOf(kind) {
				return errtype(args[i], "not compatible with type %s", kind)
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
	// Note: names of builin functions that appear in SQL
	// are derived from the constant name. If a function
	// has non-trival const-to-name mapping or there
	// are aliases, the names are provied in the comment,
	// after "sql:" prefix.
	// See _generate/builtin_names.go
	Concat BuiltinOp = iota
	Trim
	Ltrim
	Rtrim
	Upper
	Lower
	Contains
	ContainsCI // sql:CONTAINS_CI
	EqualsCI   // sql:EQUALS_CI
	EqualsFuzzy
	EqualsFuzzyUnicode
	ContainsFuzzy
	ContainsFuzzyUnicode
	OctetLength
	CharLength // sql:CHAR_LENGTH sql:CHARACTER_LENGTH
	IsSubnetOf
	Substring
	SplitPart

	BitCount

	Abs
	Sign

	Round
	RoundEven
	Trunc
	Floor
	Ceil // sql:CEIL sql:CEILING

	Sqrt
	Cbrt
	Exp
	ExpM1 // sql:EXPM1
	Exp2
	Exp10
	Hypot
	Ln
	Ln1p // sql:LN1P
	Log
	Log2
	Log10
	Pow     // sql:POW sql:POWER
	PowUint // sql:POW_UINT

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

	Pmod

	Least
	Greatest
	WidthBucket

	DateAddMicrosecond
	DateAddMillisecond
	DateAddSecond
	DateAddMinute
	DateAddHour
	DateAddDay
	DateAddWeek
	DateAddMonth
	DateAddQuarter
	DateAddYear

	DateBin

	DateDiffMicrosecond
	DateDiffMillisecond
	DateDiffSecond
	DateDiffMinute
	DateDiffHour
	DateDiffDay
	DateDiffWeek
	DateDiffMonth
	DateDiffQuarter
	DateDiffYear

	DateExtractMicrosecond
	DateExtractMillisecond
	DateExtractSecond
	DateExtractMinute
	DateExtractHour
	DateExtractDay
	DateExtractDOW // sql:DATE_EXTRACT_DOW
	DateExtractDOY // sql:DATE_EXTRACT_DOY
	DateExtractMonth
	DateExtractQuarter
	DateExtractYear

	DateTruncMicrosecond
	DateTruncMillisecond
	DateTruncSecond
	DateTruncMinute
	DateTruncHour
	DateTruncDay
	DateTruncDOW // sql:DATE_TRUNC_DOW
	DateTruncMonth
	DateTruncQuarter
	DateTruncYear

	ToUnixEpoch
	ToUnixMicro

	GeoHash
	GeoTileX
	GeoTileY
	GeoTileES // sql:GEO_TILE_ES
	GeoDistance

	ObjectSize // sql:SIZE
	ArrayContains
	ArraySize
	ArrayPosition

	TableGlob
	TablePattern

	// used by query planner:
	InSubquery        // matches IN (SELECT ...)
	InReplacement     // IN_REPLACEMENT(x, id)
	HashReplacement   // HASH_REPLACEMENT(id, kind, k, x)
	ScalarReplacement // SCALAR_REPLACEMENT(id)
	StructReplacement // STRUCT_REPLACEMENT(id)
	ListReplacement   // LIST_REPLACEMENT(id)

	TimeBucket

	MakeList   // MAKE_LIST(args...) constructs a list
	MakeStruct // MAKE_STRUCT(field, value, ...) constructs a structure

	TypeBit // TYPE_BIT(arg) produces the bits associated with the type of arg
	AssertIonType

	PartitionValue // PARTITION_VALUE(int) is used as a placeholder during query planning

	Unspecified // catch-all for opaque built-ins; sql:UNKNOWN
	maxBuiltin
)

// IsDateAdd checks whether the built-in function is `DATE_ADD_xxx`
func (b BuiltinOp) IsDateAdd() bool {
	return b >= DateAddMicrosecond && b <= DateAddYear
}

// IsDateDiff checks whether the built-in function is `DATE_DIFF_xxx`
func (b BuiltinOp) IsDateDiff() bool {
	return b >= DateDiffMicrosecond && b <= DateDiffYear
}

// IsDateExtract checks whether the built-in function is `EXTRACT_xxx`
func (b BuiltinOp) IsDateExtract() bool {
	return b >= DateExtractMicrosecond && b <= DateExtractYear
}

// IsDateTrunc checks whether the built-in function is `DATE_TRUNC_xxx`
func (b BuiltinOp) IsDateTrunc() bool {
	return b >= DateTruncMicrosecond && b <= DateTruncYear
}

// TimePart returns a time part of a built-in date function
func (b BuiltinOp) TimePart() (Timepart, bool) {
	switch b {
	case DateAddMicrosecond:
		return Microsecond, true
	case DateAddMillisecond:
		return Millisecond, true
	case DateAddSecond:
		return Second, true
	case DateAddMinute:
		return Minute, true
	case DateAddHour:
		return Hour, true
	case DateAddDay:
		return Day, true
	case DateAddWeek:
		return Week, true
	case DateAddMonth:
		return Month, true
	case DateAddQuarter:
		return Quarter, true
	case DateAddYear:
		return Year, true
	case DateDiffMicrosecond:
		return Microsecond, true
	case DateDiffMillisecond:
		return Millisecond, true
	case DateDiffSecond:
		return Second, true
	case DateDiffMinute:
		return Minute, true
	case DateDiffHour:
		return Hour, true
	case DateDiffDay:
		return Day, true
	case DateDiffWeek:
		return Week, true
	case DateDiffMonth:
		return Month, true
	case DateDiffQuarter:
		return Quarter, true
	case DateDiffYear:
		return Year, true
	case DateExtractMicrosecond:
		return Microsecond, true
	case DateExtractMillisecond:
		return Millisecond, true
	case DateExtractSecond:
		return Second, true
	case DateExtractMinute:
		return Minute, true
	case DateExtractHour:
		return Hour, true
	case DateExtractDay:
		return Day, true
	case DateExtractDOW:
		return DOW, true
	case DateExtractDOY:
		return DOY, true
	case DateExtractMonth:
		return Month, true
	case DateExtractQuarter:
		return Quarter, true
	case DateExtractYear:
		return Year, true
	case DateTruncMicrosecond:
		return Microsecond, true
	case DateTruncMillisecond:
		return Millisecond, true
	case DateTruncSecond:
		return Second, true
	case DateTruncMinute:
		return Minute, true
	case DateTruncHour:
		return Hour, true
	case DateTruncDay:
		return Day, true
	case DateTruncDOW:
		return DOW, true
	case DateTruncMonth:
		return Month, true
	case DateTruncQuarter:
		return Quarter, true
	case DateTruncYear:
		return Year, true
	}

	return 0, false
}

func init() {
	if len(builtin2Name) != int(Unspecified) {
		// In the case of error please check _generate/builtin_names.go
		panic("builtin2Name was incorrectly constructed")
	}
}

func (b BuiltinOp) String() string {
	if b >= 0 && b < Unspecified {
		return builtin2Name[b]
	}
	return "UNKNOWN"
}

func checkContains(h Hint, args []Node) error {
	if len(args) != 2 {
		return mismatch(2, len(args))
	}
	if _, ok := args[1].(String); !ok {
		return errsyntaxf("second argument requires a literal string, not %v (%T)", args[1], args[1])
	}
	if !TypeOf(args[0], h).AnyOf(StringType) {
		return errtype(args[0], "not a string")
	}
	return nil
}

func checkEqualsContainsFuzzy(h Hint, args []Node) error {
	if len(args) != 3 {
		return mismatch(3, len(args))
	}
	if _, ok := args[1].(String); !ok {
		return errsyntaxf("second argument requires a literal string, not %v (%T)", args[1], args[1])
	}
	if !TypeOf(args[0], h).AnyOf(StringType) {
		return errtype(args[0], "not a string")
	}
	if !TypeOf(args[2], h).AnyOf(IntegerType) {
		return errsyntaxf("third argument requires a integer, not %q (%T)", args[2], args[2])
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
		return errtype(args[0], "not a string but a %T", args[0])
	}
	arg1, ok := args[1].(String)
	if !ok {
		return errtype(args[1], "not a string but a %T", args[1])
	}
	if nArgs == 2 {
		if _, _, err := net.ParseCIDR(string(arg0)); err != nil {
			return errtype(args[0], "%s", err)
		}
	} else {
		if net.ParseIP(string(arg0)) == nil {
			return errtype(args[0], "not an IP address")
		}
		if net.ParseIP(string(arg1)) == nil {
			return errtype(args[1], "not an IP address")
		}
		if !TypeOf(args[2], h).AnyOf(StringType) {
			return errtype(args[2], "not a string but a %T", args[2])
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
		return Call(IsSubnetOf, Node(String(minIP.String())), Node(String(maxIP.String())), arg1)
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

func checkTrim(op BuiltinOp) func(Hint, []Node) error {
	return func(h Hint, args []Node) error {
		switch len(args) {
		case 2:
			// [LR]?TRIM(str, cutset)
			if !TypeOf(args[0], h).AnyOf(StringType) {
				return errtype(args[0], "not a string")
			}
			s, ok := args[1].(String)
			if !ok {
				return errsyntaxf("%s requires a constant string argument for cutset", op)
			}

			// Note: the constraints imposed by the current implementation
			n := len(s)
			if n != utf8.RuneCount([]byte(s)) {
				return errsyntaxf("cutset must contain only ASCII chars")
			}
			if n < 1 || n > 4 {
				return errsyntaxf("the length of cutset has to be from 1 to 4, it is %d", n)
			}

			return nil
		case 1:
			// [LR]?TRIM(str)
			if !TypeOf(args[0], h).AnyOf(StringType) {
				return errtype(args[0], "not a string")
			}
			return nil
		default:
			return errsyntaxf("%s functions expect 1 or 2 arguments, but found %d", op, len(args))
		}
	}
}

func checkSubstring(h Hint, args []Node) error {
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

func simplifyDateTrunc(part Timepart) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 1 {
			return nil
		}
		if ts, ok := args[0].(*Timestamp); ok {
			simplified := ts.Trunc(part)
			return &simplified
		}
		return nil
	}
}

func simplifyDateBin(h Hint, args []Node) Node {
	if len(args) != 3 {
		return nil
	}

	if interval, ok := args[0].(Integer); ok {
		if ts, ok := args[1].(*Timestamp); ok {
			if origin, ok := args[2].(*Timestamp); ok {
				simplified := ts.Bin(int64(interval), *origin)
				return &simplified
			}
		}
	}
	return nil
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

func checkObjectSize(h Hint, args []Node) error {
	if len(args) != 1 {
		return errsyntaxf("SIZE expects one argument, but found %d", len(args))
	}
	if !TypeOf(args[0], h).AnyOf(StructType|ListType) || args[0] == (Star{}) {
		return errtype(args[0], "SIZE expects a structure or list argument")
	}
	return nil
}

func checkArrayContains(h Hint, args []Node) error {
	if len(args) != 2 {
		return errsyntaxf("ARRAY_CONTAINS expects two arguments, but found %d", len(args))
	}
	if !TypeOf(args[0], h).AnyOf(ListType) {
		return errtype(args[0], "first argument to ARRAY_CONTAINS must be a list")
	}
	return nil
}

func checkArraySize(h Hint, args []Node) error {
	if len(args) != 1 {
		return errsyntaxf("ARRAY_SIZE expects one arguments, but found %d", len(args))
	}
	if !TypeOf(args[0], h).AnyOf(ListType) {
		return errtype(args[0], "first argument to ARRAY_SIZE must be a list")
	}
	return nil
}

func checkArrayPosition(h Hint, args []Node) error {
	if len(args) != 2 {
		return errsyntaxf("ARRAY_POSITION expects two arguments, but found %d", len(args))
	}
	if !TypeOf(args[0], h).AnyOf(ListType) {
		return errtype(args[0], "first argument to ARRAY_POSITION must be a list")
	}
	return nil
}

func checkTableGlob(h Hint, args []Node) error {
	if len(args) != 1 {
		return mismatch(1, len(args))
	}
	if !IsPath(args[0]) {
		return errsyntaxf("argument to TABLE_GLOB is %q", ToString(args[0]))
	}
	return nil
}

func checkTablePattern(h Hint, args []Node) error {
	if len(args) != 1 {
		return mismatch(1, len(args))
	}
	if !IsPath(args[0]) {
		return errsyntaxf("argument to TABLE_PATTERN is %q", ToString(args[0]))
	}
	return nil
}

func checkAssertIonType(h Hint, args []Node) error {
	if len(args) < 2 {
		return errsyntaxf("requires at least 2 arguments")
	}

	validType := func(t int) bool {
		switch ion.Type(t) {
		case ion.BoolType,
			ion.UintType,
			ion.IntType,
			ion.FloatType,
			ion.DecimalType,
			ion.TimestampType,
			ion.SymbolType,
			ion.StringType,
			ion.BlobType,
			ion.ListType,
			ion.StructType:
			return true
		}

		return false
	}

	for i := 1; i < len(args); i++ {
		t, ok := args[i].(Integer)
		if !ok {
			return errsyntaxf("argument %d has to be an integer", i)
		}

		if !validType(int(t)) {
			return fmt.Errorf("value %d is not a supported Ion type", t)
		}
	}

	return nil
}

func simplifyAssertIonType(h Hint, args []Node) Node {

	hasType := func(t ion.Type) bool {
		for i := 1; i < len(args); i++ {
			v := args[i].(Integer)
			if ion.Type(v) == t {
				return true
			}
		}

		return false
	}

	t := TypeOf(args[0], h) &^ MissingType
	if t == 0 {
		return Missing{}
	}
	if t == NumericType {
		// Since NumericType is an union of ints and floats
		// try to figure out a single type from bare argument.
		switch args[0].(type) {
		case Float:
			t = FloatType
		case Integer:
			t = IntegerType
		}
	}

	switch t {
	case StringType:
		if hasType(ion.StringType) {
			return args[0]
		}

		return Missing{}

	case IntegerType:
		if v, ok := args[0].(Integer); ok {
			if v >= 0 && hasType(ion.UintType) {
				return args[0]
			}

			if v < 0 && hasType(ion.IntType) {
				return args[0]
			}
		}

		if hasType(ion.UintType) && hasType(ion.IntType) {
			return args[0]
		}

		return Missing{}

	case FloatType:
		if hasType(ion.FloatType) {
			return args[0]
		}

		return Missing{}

	case BoolType:
		if hasType(ion.BoolType) {
			return args[0]
		}

		return Missing{}

	case TimeType:
		if hasType(ion.TimestampType) {
			return args[0]
		}

		return Missing{}

	case NullType, MissingType:
		return Missing{}
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
		// for these dateparts, avoid date math entirely
		// and just perform integer math in microseconds:
		const micro = 1e6
		switch part {
		case Second:
			return date.UnixMicro(val.UnixMicro() + (x * micro))
		case Minute:
			return date.UnixMicro(val.UnixMicro() + (x * 60 * micro))
		case Hour:
			return date.UnixMicro(val.UnixMicro() + (x * 60 * 60 * micro))
		case Day:
			return date.UnixMicro(val.UnixMicro() + (x * 24 * 60 * 60 * micro))
		}

		year := val.Year()
		month := val.Month()
		day := val.Day()
		hour := val.Hour()
		minute, sec := val.Minute(), val.Second()

		switch part {
		default:
			panic("bad timepart")
		case Week:
			day += int(x) * 7
		case Month:
			month += int(x)
		case Quarter:
			month += int(x) * 3
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
	dateAddSecond  = adjtime(adjpart(Second))
	dateAddMinute  = adjtime(adjpart(Minute))
	dateAddHour    = adjtime(adjpart(Hour))
	dateAddDay     = adjtime(adjpart(Day))
	dateAddWeek    = adjtime(adjpart(Week))
	dateAddMonth   = adjtime(adjpart(Month))
	dateAddQuarter = adjtime(adjpart(Quarter))
	dateAddYear    = adjtime(adjpart(Year))
)

func missingIfNaN(x float64) Node {
	if math.IsNaN(x) {
		return Missing{}
	}

	return Float(x)
}

func mathfunc(fn func(float64) float64) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 1 {
			return nil
		}
		f, ok := args[0].(Float)
		if !ok {
			i, ok := args[0].(Integer)
			if !ok {
				return nil
			}
			f = Float(int64(i))
		}
		return missingIfNaN(fn(float64(f)))
	}
}

func mathfunc2(fn func(float64, float64) float64) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) != 2 {
			return nil
		}

		var f1 float64
		switch v := args[0].(type) {
		case Float:
			f1 = float64(v)
		case Integer:
			f1 = float64(int64(v))
		default:
			return nil
		}

		var f2 float64
		switch v := args[1].(type) {
		case Float:
			f2 = float64(v)
		case Integer:
			f2 = float64(int64(v))
		default:
			return nil
		}

		return missingIfNaN(fn(f1, f2))
	}
}

func mathfuncreduce(fn func(float64, float64) float64) func(Hint, []Node) Node {
	return func(h Hint, args []Node) Node {
		if len(args) == 0 {
			return nil
		}

		var val float64
		for i := range args {
			var f float64
			switch v := args[i].(type) {
			case Float:
				f = float64(v)
			case Integer:
				f = float64(int64(v))
			default:
				return nil
			}

			if i == 0 {
				val = f
			} else {
				val = fn(val, f)
			}
		}

		return missingIfNaN(val)
	}
}

func exp10(x float64) float64 {
	return math.Pow(10, x)
}

var builtinInfo = [maxBuiltin]binfo{
	Concat:               {check: fixedArgs(StringType, StringType), private: true, ret: StringType | MissingType},
	Trim:                 {check: checkTrim(Trim), ret: StringType | MissingType},
	Ltrim:                {check: checkTrim(Ltrim), ret: StringType | MissingType},
	Rtrim:                {check: checkTrim(Rtrim), ret: StringType | MissingType},
	Upper:                {check: unaryStringArgs, ret: StringType | MissingType},
	Lower:                {check: unaryStringArgs, ret: StringType | MissingType},
	Contains:             {check: checkContains, private: true, ret: LogicalType},
	ContainsCI:           {check: checkContains, private: true, ret: LogicalType},
	CharLength:           {check: unaryStringArgs, ret: UnsignedType | MissingType},
	OctetLength:          {check: unaryStringArgs, ret: UnsignedType | MissingType},
	IsSubnetOf:           {check: checkIsSubnetOf, ret: LogicalType, simplify: simplifyIsSubnetOf},
	Substring:            {check: checkSubstring, ret: StringType | MissingType},
	SplitPart:            {check: checkSplitPart, ret: StringType | MissingType},
	EqualsCI:             {ret: LogicalType, private: true},
	EqualsFuzzy:          {check: checkEqualsContainsFuzzy, ret: LogicalType},
	EqualsFuzzyUnicode:   {check: checkEqualsContainsFuzzy, ret: LogicalType},
	ContainsFuzzy:        {check: checkEqualsContainsFuzzy, ret: LogicalType},
	ContainsFuzzyUnicode: {check: checkEqualsContainsFuzzy, ret: LogicalType},

	BitCount:  {check: fixedArgs(NumericType), ret: IntegerType | MissingType},
	Abs:       {check: fixedArgs(NumericType), ret: NumericType},
	Sign:      {check: fixedArgs(NumericType), ret: NumericType},
	Round:     {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: simplifyRound},
	RoundEven: {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: simplifyRoundEven},
	Trunc:     {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: simplifyTrunc},
	Floor:     {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: simplifyFloor},
	Ceil:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: simplifyCeil},
	Sqrt:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Sqrt)},
	Cbrt:      {check: fixedArgs(NumericType), ret: FloatType | MissingType},
	Exp:       {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Exp)},
	Exp2:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Exp2)},
	Exp10:     {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(exp10)},
	ExpM1:     {check: fixedArgs(NumericType), ret: FloatType | MissingType},
	Hypot:     {check: fixedArgs(NumericType, NumericType), ret: FloatType | MissingType},
	Ln:        {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Log)},
	Log:       {check: variadicArgs(NumericType), ret: FloatType | MissingType},
	Log2:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Log2)},
	Log10:     {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Log10)},
	Pow:       {check: fixedArgs(NumericType, NumericType), ret: FloatType | MissingType, simplify: mathfunc2(math.Pow)},
	PowUint:   {private: true, ret: FloatType | MissingType},
	Pi:        {check: fixedArgs(), ret: FloatType | MissingType},
	Degrees:   {check: fixedArgs(NumericType), ret: FloatType | MissingType},
	Radians:   {check: fixedArgs(NumericType), ret: FloatType | MissingType},
	Sin:       {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Sin)},
	Cos:       {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Cos)},
	Tan:       {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Tan)},
	Asin:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Asin)},
	Acos:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Acos)},
	Atan:      {check: fixedArgs(NumericType), ret: FloatType | MissingType, simplify: mathfunc(math.Atan)},
	Atan2:     {check: fixedArgs(NumericType, NumericType), ret: FloatType | MissingType, simplify: mathfunc2(math.Atan2)},
	Pmod:      {check: fixedArgs(NumericType, NumericType), ret: NumericType | MissingType, simplify: simplifyPmod},

	Least:       {check: variadicNumeric, ret: NumericType | MissingType, simplify: mathfuncreduce(math.Min)},
	Greatest:    {check: variadicNumeric, ret: NumericType | MissingType, simplify: mathfuncreduce(math.Max)},
	WidthBucket: {check: fixedArgs(NumericType, NumericType, NumericType, NumericType), ret: NumericType | MissingType},

	DateAddMicrosecond:     {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMicrosecond},
	DateAddMillisecond:     {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMillisecond},
	DateAddSecond:          {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddSecond},
	DateAddMinute:          {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMinute},
	DateAddHour:            {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddHour},
	DateAddDay:             {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddDay},
	DateAddWeek:            {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddWeek},
	DateAddMonth:           {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddMonth},
	DateAddQuarter:         {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddQuarter},
	DateAddYear:            {check: fixedArgs(IntegerType, TimeType), private: true, ret: TimeType | MissingType, simplify: dateAddYear},
	DateBin:                {check: fixedArgs(IntegerType, TimeType, TimeType), ret: TimeType | MissingType, simplify: simplifyDateBin},
	DateDiffMicrosecond:    {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMillisecond:    {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffSecond:         {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMinute:         {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffHour:           {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffDay:            {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffWeek:           {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffMonth:          {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffQuarter:        {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateDiffYear:           {check: fixedArgs(TimeType, TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractMicrosecond: {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractMillisecond: {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractSecond:      {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractMinute:      {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractHour:        {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractDay:         {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractDOW:         {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractDOY:         {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractMonth:       {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractQuarter:     {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateExtractYear:        {check: fixedArgs(TimeType), private: true, ret: IntegerType | MissingType},
	DateTruncMicrosecond:   {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Microsecond)},
	DateTruncMillisecond:   {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Millisecond)},
	DateTruncSecond:        {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Second)},
	DateTruncMinute:        {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Minute)},
	DateTruncHour:          {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Hour)},
	DateTruncDay:           {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Day)},
	DateTruncDOW:           {check: fixedArgs(TimeType, IntegerType), private: true, ret: TimeType | MissingType},
	DateTruncMonth:         {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Month)},
	DateTruncQuarter:       {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Quarter)},
	DateTruncYear:          {check: fixedTime, private: true, ret: TimeType | MissingType, simplify: simplifyDateTrunc(Year)},
	ToUnixEpoch:            {check: fixedTime, ret: IntegerType | MissingType},
	ToUnixMicro:            {check: fixedTime, ret: IntegerType | MissingType},

	GeoHash:     {check: fixedArgs(NumericType, NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileX:    {check: fixedArgs(NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileY:    {check: fixedArgs(NumericType, IntegerType), ret: StringType | MissingType},
	GeoTileES:   {check: fixedArgs(NumericType, NumericType, IntegerType), ret: StringType | MissingType},
	GeoDistance: {check: fixedArgs(NumericType, NumericType, NumericType, NumericType), ret: FloatType | MissingType},

	ObjectSize:    {check: checkObjectSize, ret: NumericType | MissingType},
	ArraySize:     {check: checkArraySize, ret: NumericType | MissingType},
	ArrayContains: {check: checkArrayContains, ret: LogicalType | MissingType},
	ArrayPosition: {check: checkArrayPosition, ret: NumericType | MissingType},

	InSubquery:        {check: checkInSubquery, private: true, ret: LogicalType},
	InReplacement:     {check: checkInReplacement, private: true, ret: LogicalType},
	HashReplacement:   {check: checkHashReplacement, private: true, ret: AnyType},
	ScalarReplacement: {check: checkScalarReplacement, private: true, ret: AnyType},
	ListReplacement:   {check: checkScalarReplacement, private: true, ret: ListType},
	StructReplacement: {check: checkScalarReplacement, private: true, ret: StructType},

	TimeBucket: {check: fixedArgs(TimeType, NumericType), ret: NumericType | MissingType},

	MakeList:   {ret: ListType, private: true, text: makeListText, simplify: simplifyMakeList},
	MakeStruct: {ret: StructType, private: true, text: makeStructText, simplify: simplifyMakeStruct},

	TypeBit:        {check: fixedArgs(AnyType), ret: UnsignedType, simplify: simplifyTypeBit},
	AssertIonType:  {check: checkAssertIonType, ret: AnyType, simplify: simplifyAssertIonType, private: true},
	TableGlob:      {check: checkTableGlob, ret: AnyType, isTable: true},
	TablePattern:   {check: checkTablePattern, ret: AnyType, isTable: true},
	PartitionValue: {ret: AnyType, private: true},
}

// JSONTypeBits returns a unique bit pattern
// associated with the given ion type.
// (This is the constprop'd version of the TYPE_BIT function.)
func JSONTypeBits(typ ion.Type) uint {
	switch typ {
	case ion.NullType:
		return 1 << 0
	case ion.BoolType:
		return 1 << 1
	case ion.UintType, ion.IntType, ion.FloatType, ion.DecimalType:
		return 1 << 2
	case ion.TimestampType:
		return 1 << 3
	case ion.StringType, ion.SymbolType:
		return 1 << 4
	case ion.ListType:
		return 1 << 5
	case ion.StructType:
		return 1 << 6
	default:
		return 0
	}
}

func simplifyTypeBit(h Hint, args []Node) Node {
	if len(args) != 1 {
		return nil
	}
	arg := args[0]
	if c, ok := arg.(Constant); ok {
		return Integer(JSONTypeBits(c.Datum().Type()))
	}
	if arg == (Missing{}) {
		return Integer(0)
	}
	return nil
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
