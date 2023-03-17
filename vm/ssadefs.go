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
	"fmt"
	"strings"
)

type ssaop int
type ssatype int

const (
	sinvalid     ssaop = iota
	sinit              // initial lane pointer and mask
	sinitmem           // initial memory state
	sundef             // initial scalar value (undefined)
	smergemem          // merge memory
	sbroadcast0k       // mask = 0
	sbroadcast1k       // mask = 1
	skfalse            // logical bottom value; FALSE and also MISSING
	sand               // mask = (mask0 & mask1)
	sandn              // mask = (!mask0 & mask1)
	sor                // mask = (mask0 | mask1)
	sxor               // mask = (mask0 ^ mask1)  (unequal bits)
	sxnor              // mask = (mask0 ^ ^mask1) (equal bits)

	sunboxktoi64 // val = unboxktoi(v)
	sunboxcoercei64
	sunboxcoercef64
	sunboxcvti64
	sunboxcvtf64

	// comparison ops
	scmpv
	scmpvk
	scmpvimmk
	scmpvi64
	scmpvimmi64
	scmpvf64
	scmpvimmf64

	scmpltstr
	scmplestr
	scmpgtstr
	scmpgestr

	scmpltk
	scmpltimmk
	scmpgtk
	scmpgtimmk
	scmplek
	scmpleimmk
	scmpgek
	scmpgeimmk

	scmpeqf
	scmpeqimmf
	scmpeqi
	scmpeqimmi
	scmpltf
	scmpltimmf
	scmplti
	scmpltimmi
	scmpgtf
	scmpgtimmf
	scmpgti
	scmpgtimmi
	scmplef
	scmpleimmf
	scmplei
	scmpleimmi
	scmpgef
	scmpgeimmf
	scmpgei
	scmpgeimmi

	scmpeqts
	scmpltts
	scmplets
	scmpgtts
	scmpgets

	scmpeqstr // str = str

	// compare scalar against value;
	// effectively just a memcmp() operation
	scmpeqv // mask = arg0.mask == arg1.mask

	// raw value test ops
	sisnull    // mask = arg0.mask == null
	sisnonnull // mask = arg0.mask != null
	sisfalse   // mask = arg0.mask == false
	sistrue    // mask = arg0.mask == true

	stostr
	stolist
	stoblob
	sunsymbolize

	scvtktoi64   // bool to 0 or 1
	scvtktof64   // bool to 0.0 or 1.0
	scvti64tok   // i64 to bool
	scvtf64tok   // f64 to bool
	scvtf64toi64 // f64 to i64, round nearest
	scvti64tof64 // i64 to f64

	scvti64tostr // int64 to string

	sstrconcat // string concatenation

	slowerstr
	supperstr

	// #region raw string comparison
	sStrCmpEqCs              // Ascii string compare equality case-sensitive
	sStrCmpEqCi              // Ascii string compare equality case-insensitive
	sStrCmpEqUTF8Ci          // UTF-8 string compare equality case-insensitive
	sEqPatternCs             // String equals pattern case-sensitive
	sEqPatternCi             // String equals pattern case-insensitive
	sEqPatternUTF8Ci         // String equals pattern case-insensitive
	sCmpFuzzyA3              // Ascii string fuzzy equality: Damerau–Levenshtein up to provided number of operations
	sCmpFuzzyUnicodeA3       // unicode string fuzzy equality: Damerau–Levenshtein up to provided number of operations
	sHasSubstrFuzzyA3        // Ascii string contains with fuzzy string compare
	sHasSubstrFuzzyUnicodeA3 // unicode string contains with fuzzy string compare

	sStrTrimCharLeft  // String trim specific chars left
	sStrTrimCharRight // String trim specific chars right
	sStrTrimWsLeft    // String trim whitespace left
	sStrTrimWsRight   // String trim whitespace right

	sStrContainsPrefixCs      // String contains prefix case-sensitive
	sStrContainsPrefixCi      // String contains prefix case-insensitive
	sStrContainsPrefixUTF8Ci  // String contains prefix case-insensitive
	sStrContainsSuffixCs      // String contains suffix case-sensitive
	sStrContainsSuffixCi      // String contains suffix case-insensitive
	sStrContainsSuffixUTF8Ci  // String contains suffix case-insensitive
	sStrContainsSubstrCs      // String contains substr case-sensitive
	sStrContainsSubstrCi      // String contains substr case-insensitive
	sStrContainsSubstrUTF8Ci  // String contains substr case-insensitive
	sStrContainsPatternCs     // String contains pattern case-sensitive
	sStrContainsPatternCi     // String contains pattern case-insensitive
	sStrContainsPatternUTF8Ci // String contains pattern case-insensitive

	sIsSubnetOfIP4 // IP subnet matching

	sStrSkip1CharLeft  // String skip 1 unicode code-point from left
	sStrSkip1CharRight // String skip 1 unicode code-point from right
	sStrSkipNCharLeft  // String skip n unicode code-point from left
	sStrSkipNCharRight // String skip n unicode code-point from right

	soctetlength     // count number of bytes in a string
	scharacterlength // count number of character in a string
	sSubStr          // select a substring
	sSplitPart       // Presto split_part

	sDfaT6  // DFA tiny 6-bit
	sDfaT7  // DFA tiny 7-bit
	sDfaT8  // DFA tiny 8-bit
	sDfaT6Z // DFA tiny 6-bit Zero remaining length assertion
	sDfaT7Z // DFA tiny 7-bit Zero remaining length assertion
	sDfaT8Z // DFA tiny 8-bit Zero remaining length assertion
	sDfaLZ  // DFA large Zero remaining length assertion

	// raw literal comparison
	sequalconst // arg0.mask == const

	stuples  // compute interior structure pointer from value
	sdot     // compute 'value . arg0.mask'
	sdot2    // compute 'value . arg0.mask' from previous offset
	ssplit   // compute 'value[0] and value[1:]'
	sliteral // literal operand
	sauxval  // auxilliary literal

	shashvalue  // hash a value
	shashvaluep // hash a value and add it to the current hash
	shashmember // look up a hash in a tree for existence; returns predicate
	shashlookup // look up a hash in a tree for a value; returns boxed

	sstorev // copy a value from one slot to another

	sretm   // return mem
	sretmk  // return mem+predicate tuple
	sretmsk // return mem+scalar+predicate
	sretbk  // return base+predicate tuple
	sretbhk // return base+hash+predicate

	smakev
	smakevk
	sfloatk
	snotmissing // not missing (extract mask)

	// blend ops (just conditional moves)
	sblendv
	sblendi64
	sblendf64
	sblendslice

	// broadcasts a constant to all lanes
	sbroadcastf // out = broadcast(float64(imm))
	sbroadcasti // out = broadcast(int64(imm))

	// unary operators and functions
	sabsf       // out = abs(x)
	sabsi       // out = abs(x)
	snegf       // out = -x
	snegi       // out = -x
	ssignf      // out = sign(x)
	ssigni      // out = sign(x)
	ssquaref    // out = x * x
	ssquarei    // out = x * x
	sbitnoti    // out = ~x
	sbitcounti  // out = bit_count(x)
	sroundf     // out = round(x)
	sroundevenf // out = roundeven(x)
	struncf     // out = trunc(x)
	sfloorf     // out = floor(x)
	sceilf      // out = ceil(x)
	sroundi     // out = int(round(x))
	ssqrtf      // out = sqrt(x)
	scbrtf      // out = cbrt(x)
	sexpf       // out = exp(x)
	sexpm1f     // out = exp(x) - 1
	sexp2f      // out = exp2(x)
	sexp10f     // out = exp10(x)
	slnf        // out = ln(x)
	sln1pf      // out = ln(x + 1)
	slog2f      // out = log2(x)
	slog10f     // out = log10(x)
	ssinf       // out = sin(x)
	scosf       // out = cos(x)
	stanf       // out = tan(x)
	sasinf      // out = asin(x)
	sacosf      // out = acos(x)
	satanf      // out = atan(x)

	// binary operators and functions
	saddf         // out = x + y
	saddi         // out = x + y
	saddimmf      // out = x + imm
	saddimmi      // out = x + imm
	ssubf         // out = x - y
	ssubi         // out = x - y
	ssubimmf      // out = x - imm
	ssubimmi      // out = x - imm
	srsubf        // out = y - x
	srsubi        // out = y - x
	srsubimmf     // out = imm - x
	srsubimmi     // out = imm - x
	smulf         // out = x * y
	smuli         // out = x * y
	smulimmf      // out = x * imm
	smulimmi      // out = x * imm
	sdivf         // out = x / y
	sdivi         // out = x / y
	sdivimmf      // out = x / imm
	sdivimmi      // out = x / imm
	srdivimmf     // out = imm / x
	srdivimmi     // out = imm / x
	smodf         // out = x % y
	smodi         // out = x % y
	smodimmf      // out = x % imm
	smodimmi      // out = x % imm
	srmodimmf     // out = imm % x
	srmodimmi     // out = imm % x
	sminvaluef    // out = min(x, y)
	sminvaluei    // out = min(x, y)
	sminvalueimmf // out = min(x, imm)
	sminvalueimmi // out = min(x, imm)
	smaxvaluef    // out = max(x, y)
	smaxvaluei    // out = max(x, y)
	smaxvalueimmf // out = max(x, imm)
	smaxvalueimmi // out = max(x, imm)
	sandi         // out = x & y]
	sandimmi      // out = x & imm
	sori          // out = x | y
	sorimmi       // out = x | imm
	sxori         // out = x ^ y
	sxorimmi      // out = x ^ imm
	sslli         // out = x << y
	ssllimmi      // out = x << imm
	ssrai         // out = x >> y
	ssraimmi      // out = x >> imm
	ssrli         // out = x >>> y
	ssrlimmi      // out = x >>> imm
	satan2f       // out = atan2(x, y)
	shypotf       // out = hypot(x, y)
	spowf         // out = pow(x, y)
	spowuintf     // out = powuint(x, uint_y)

	swidthbucketf // out = width_bucket(val, min, max, bucket_count)
	swidthbucketi // out = width_bucket(val, min, max, bucket_count)
	stimebucketts // out = time_bucket(val, interval)

	saggandk
	saggork
	saggsumf
	saggsumi
	saggavgf
	saggavgi
	saggminf
	saggmini
	saggmaxf
	saggmaxi
	saggmints
	saggmaxts
	saggandi
	saggori
	saggxori
	saggcount

	saggbucket
	saggslotandk
	saggslotork
	saggslotsumf
	saggslotsumi
	saggslotavgf
	saggslotavgi
	saggslotminf
	saggslotmini
	saggslotmaxf
	saggslotmaxi
	saggslotmints
	saggslotmaxts
	saggslotandi
	saggslotori
	saggslotxori
	saggslotcount

	sbroadcastts
	sunix
	sunixmicro
	sunboxtime
	sdateadd
	sdateaddimm
	sdateaddmulimm
	sdateaddmonth
	sdateaddmonthimm
	sdateaddquarter
	sdateaddyear
	sdatediffmicro
	sdatediffparam
	sdatediffmonth
	sdatediffquarter
	sdatediffyear
	sdateextractmicrosecond
	sdateextractmillisecond
	sdateextractsecond
	sdateextractminute
	sdateextracthour
	sdateextractday
	sdateextractdow
	sdateextractdoy
	sdateextractmonth
	sdateextractquarter
	sdateextractyear
	sdatetounixepoch
	sdatetounixmicro
	sdatetruncmillisecond
	sdatetruncsecond
	sdatetruncminute
	sdatetrunchour
	sdatetruncday
	sdatetruncdow
	sdatetruncmonth
	sdatetruncquarter
	sdatetruncyear

	sgeohash
	sgeohashimm
	sgeotilex
	sgeotiley
	sgeotilees
	sgeotileesimm
	sgeodistance

	sobjectsize // built-in function SIZE()
	sarraysize
	sarrayposition

	sboxmask  // box a mask
	sboxint   // box an integer
	sboxfloat // box a float
	sboxstr   // box a string
	sboxts    // box a timestamp (unpacked)

	smakelist
	smakestruct
	smakestructkey
	sboxlist

	stypebits                  // get encoded tag bits
	schecktag                  // check encoded tag bits
	saggapproxcount            // APPROX_COUNT_DISTINCT
	saggapproxcountpartial     // the partial step of APPROX_COUNT_DISTINCT (for split queries)
	saggapproxcountmerge       // the merge step of APPROX_COUNT_DISTINCT (for split queries)
	saggslotapproxcount        // APPROX_COUNT_DISTINCT aggregate in GROUP BY
	saggslotapproxcountpartial // the partial step of APPROX_COUNT_DISTINCT (for split queries with GROUP BY)
	saggslotapproxcountmerge   // the merge step of APPROX_COUNT_DISTINCT (for split queries with GROUP BY)

	_ssamax
)

const (
	stBool   = 1 << iota // only a mask
	stBase               // inner bytes of a structure
	stValue              // opaque ion value
	stFloat              // unpacked float
	stInt                // unpacked signed integer
	stString             // unpacked string pointer
	stList               // unpacked list slice
	stTime               // datetime representation in microseconds as int64
	stHash               // hash of a value
	stBucket             // displacement used for hash aggregates
	stMem                // memory is modified

	// generally, functions return
	// composite types: a real return value,
	// plus a new mask containing the lanes
	// in which the operation was successful
	stScalar       = stFloat | stInt | stString | stList | stTime
	stBaseMasked   = stBase | stBool
	stValueMasked  = stValue | stBool
	stFloatMasked  = stFloat | stBool
	stIntMasked    = stInt | stBool
	stStringMasked = stString | stBool
	stListMasked   = stList | stBool
	stTimeMasked   = stTime | stBool

	// list-splitting ops return this
	stListAndValueMasked = stList | stValue | stBool

	// just aliases
	stBlob       = stString
	stBlobMasked = stStringMasked
)

func (s ssatype) char() byte {
	switch s {
	case stBool:
		return 'k'
	case stValue:
		return 'v'
	case stFloat:
		return 'f'
	case stInt:
		return 'i'
	case stString:
		return 's'
	case stList:
		return 'l'
	case stTime:
		return 't'
	case stBase:
		return 'b'
	case stScalar:
		return 'u'
	case stHash:
		return 'h'
	case stBucket:
		return 'L'
	case stMem:
		return 'm'
	default:
		return '?'
	}
}

func (s ssatype) String() string {
	var b strings.Builder

	lookup := []struct {
		bit  ssatype
		name string
	}{
		{bit: stBool, name: "bool"},
		{bit: stBase, name: "base"},
		{bit: stValue, name: "value"},
		{bit: stFloat, name: "float"},
		{bit: stInt, name: "int"},
		{bit: stString, name: "string"},
		{bit: stList, name: "list"},
		{bit: stTime, name: "time"},
		{bit: stHash, name: "hash"},
		{bit: stBucket, name: "bucket"},
		{bit: stMem, name: "mem"},
	}

	first := true
	b.WriteString("{")
	for i := range lookup {
		if s&lookup[i].bit == 0 {
			continue
		}

		if !first {
			b.WriteString("|")
		}
		first = false

		b.WriteString(lookup[i].name)
	}
	b.WriteString("}")

	return b.String()
}

type ssaopinfo struct {
	text     string
	argtypes []ssatype

	// vaArgs indicates arguments tuple that follow mandatory arguments. If
	// vaArgs is for example [X, Y] then the function accepts variable arguments
	// as [X, Y] pairs, so [], [X, Y], [X, Y, X, Y], [X, Y, X, Y, ...] signatures
	// are valid, but not [X, Y, X]. This makes sure to enforce values with predicates.
	vaArgs  []ssatype
	rettype ssatype

	priority int // instruction scheduling priority; high = early, low = late

	// the emit function, if we're not using the default
	emit func(v *value, c *compilestate)
	// when non-zero, the corresponding bytecode op
	bc bcop

	// immfmt indicates the format
	// of the immediate value in value.imm
	// when emitted as code
	immfmt immfmt

	// disjunctive must be set if an op
	// can produce meaningful results when
	// its canonical mask argument (the last argument) is false;
	// examples include blends, OR, XOR, etc.
	//
	// *most* ops that yield a mask are conjunctive
	// (i.e. they yield a mask that has no bits set that
	// weren't already set in the input)
	disjunctive bool

	// returnOp specifies whether this operator terminates the execution
	// of the program by either returning void or a value.
	returnOp bool

	// safeValueMask means that the operator's value contains zeroed fields that
	// correspond to `p.mask(v)` and thus a mov operation to an output reserved
	// slot is eliminable.
	safeValueMask bool
}

func (o *ssaopinfo) argType(index int) ssatype {
	// most instructions don't have variable arguments, so this is a likely path
	if index < len(o.argtypes) {
		return o.argtypes[index]
	}

	if len(o.vaArgs) == 0 {
		panic(fmt.Sprintf("%s doesn't have argument at %d", o.text, index))
	}

	vaIndex := len(o.argtypes)
	vaTupleSize := len(o.vaArgs)

	vaTupleIndex := (index - vaIndex) % vaTupleSize
	return o.vaArgs[vaTupleIndex]
}

// aggregateslot is an offset within aggregates buffer
type aggregateslot uint32

// immfmt is an immediate format indicator
type immfmt uint8

const (
	fmtnone       immfmt = iota // no immediate
	fmtslot                     // immediate should be encoded as a uint16 slot reference from an integer
	fmtbool                     // immediate should be encoded as an uint8 and represents a BOOL value
	fmti64                      // immediate should be encoded as an int64
	fmtf64                      // immediate is a float64; should be encoded as 8 bytes (little-endian)
	fmtdict                     // immediate is a string; emit a dict reference
	fmtslotx2hash               // immediate is input hash slot; encode 1-byte input hash slot + 1-byte output
	fmtaggslot                  // immediate should be encoded as a uint32 slot reference from an integer

	fmtother // immediate is present, but not available for automatic encoding
)

// canonically, the last argument of any function
// is the operation's mask
var memArgs = []ssatype{stMem}
var value2Args = []ssatype{stValue, stValue, stBool}

var int1Args = []ssatype{stInt, stBool}
var fp1Args = []ssatype{stFloat, stBool}
var str1Args = []ssatype{stString, stBool}
var str2Args = []ssatype{stString, stString, stBool}

var scalar1Args = []ssatype{stValue, stBool}
var scalar2Args = []ssatype{stValue, stValue, stBool}

var argsIntIntBool = []ssatype{stInt, stInt, stBool}
var argsFloatFloatBool = []ssatype{stFloat, stFloat, stBool}
var argsBoolBool = []ssatype{stBool, stBool}
var argsBoolBoolBool = []ssatype{stBool, stBool, stBool}

// due to an initialization loop, there
// are two copies of this table
var ssainfo [_ssamax]ssaopinfo

func init() {
	copy(ssainfo[:], _ssainfo[:])
	setavx512level(avx512highestlevel)
}

const (
	prioInit  = 100000
	prioMem   = 10000
	prioHash  = 9999
	prioParse = -100000
)

var _ssainfo = [_ssamax]ssaopinfo{
	sinvalid: {text: "INVALID"},
	// initial top-level values:
	sinit:     {text: "init", rettype: stBase | stBool, bc: opinit, priority: prioInit},
	sinitmem:  {text: "initmem", rettype: stMem, emit: emitNone, priority: prioMem},
	smergemem: {text: "mergemem", vaArgs: memArgs, rettype: stMem, emit: emitNone, priority: prioMem},
	// initial scalar register value;
	// not legal to use except to overwrite
	// with value-parsing ops
	sundef: {text: "undef", rettype: stFloat | stInt | stString, emit: emitNone, priority: prioInit - 1},
	// kfalse is the canonical 'bottom' mask value;
	// it is also the MISSING value
	// (kfalse is overloaded to mean "no result"
	// because sometimes we determine that certain
	// path expressions must yield no result due to
	// the symbol not being present in the symbol table)
	sbroadcast0k: {text: "broadcast0.k", rettype: stBool},
	sbroadcast1k: {text: "broadcast1.k", rettype: stBool},
	skfalse:      {text: "false", rettype: stValueMasked, bc: opfalse, safeValueMask: true},
	sand:         {text: "and.k", argtypes: argsBoolBool, rettype: stBool, bc: opandk},
	sandn:        {text: "andn.k", argtypes: argsBoolBool, rettype: stBool, bc: opandnk},
	sor:          {text: "or.k", argtypes: argsBoolBool, rettype: stBool, bc: opork, disjunctive: true},
	sxor:         {text: "xor.k", argtypes: argsBoolBool, rettype: stBool, bc: opxork, disjunctive: true},
	sxnor:        {text: "xnor.k", argtypes: argsBoolBool, rettype: stBool, bc: opxnork, disjunctive: true},

	sunboxktoi64:    {text: "unbox.k@i64", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxktoi64},
	sunboxcoercef64: {text: "unboxcoerce.f64", argtypes: scalar1Args, rettype: stFloatMasked, bc: opunboxcoercef64},
	sunboxcoercei64: {text: "unboxcoerce.i64", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxcoercei64},
	sunboxcvtf64:    {text: "unboxcvt.f64", argtypes: scalar1Args, rettype: stFloatMasked, bc: opunboxcvtf64},
	sunboxcvti64:    {text: "unboxcvt.i64", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxcvti64},

	// two-operand comparison ops
	scmpv:       {text: "cmpv", argtypes: value2Args, rettype: stInt | stBool, bc: opcmpv},
	scmpvk:      {text: "cmpv.k", argtypes: []ssatype{stValue, stBool, stBool}, rettype: stInt | stBool, bc: opcmpvk},
	scmpvimmk:   {text: "cmpv.k.imm", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, immfmt: fmtbool, bc: opcmpvkimm},
	scmpvi64:    {text: "cmpv.i64", argtypes: []ssatype{stValue, stInt, stBool}, rettype: stInt | stBool, bc: opcmpvi64},
	scmpvimmi64: {text: "cmpv.i64.imm", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, immfmt: fmti64, bc: opcmpvi64imm},
	scmpvf64:    {text: "cmpv.f64", argtypes: []ssatype{stValue, stFloat, stBool}, rettype: stInt | stBool, bc: opcmpvf64},
	scmpvimmf64: {text: "cmpv.f64.imm", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, immfmt: fmtf64, bc: opcmpvf64imm},
	scmpltstr:   {text: "cmplt.str", argtypes: str2Args, rettype: stBool, bc: opcmpltstr},
	scmplestr:   {text: "cmple.str", argtypes: str2Args, rettype: stBool, bc: opcmplestr},
	scmpgtstr:   {text: "cmpgt.str", argtypes: str2Args, rettype: stBool, bc: opcmpgtstr},
	scmpgestr:   {text: "cmpge.str", argtypes: str2Args, rettype: stBool, bc: opcmpgestr},
	scmpeqstr:   {text: "cmpeq.str", argtypes: []ssatype{stString, stString, stBool}, rettype: stBool, bc: opcmpeqslice},

	scmpltk:    {text: "cmplt.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpltk},
	scmpltimmk: {text: "cmplt.k@imm", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpltkimm},
	scmplek:    {text: "cmple.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmplek},
	scmpleimmk: {text: "cmple.k@imm", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmplekimm},
	scmpgtk:    {text: "cmpgt.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpgtk},
	scmpgtimmk: {text: "cmpgt.k@imm", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpgtkimm},
	scmpgek:    {text: "cmpge.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpgek},
	scmpgeimmk: {text: "cmpge.k@imm", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpgekimm},

	scmpeqf:    {text: "cmpeq.f64", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpeqf64},
	scmpeqimmf: {text: "cmpeq.f64@imm", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpeqf64imm},
	scmpeqi:    {text: "cmpeq.i64", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpeqi64},
	scmpeqimmi: {text: "cmpeq.i64@imm", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpeqi64imm},
	scmpltf:    {text: "cmplt.f64", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpltf64},
	scmpltimmf: {text: "cmplt.f64@imm", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpltf64imm},
	scmplti:    {text: "cmplt.i64", argtypes: argsIntIntBool, rettype: stBool, bc: opcmplti64},
	scmpltimmi: {text: "cmplt.i64@imm", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmplti64imm},
	scmplef:    {text: "cmple.f64", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmplef64},
	scmpleimmf: {text: "cmple.f64@imm", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmplef64imm},
	scmplei:    {text: "cmple.i64", argtypes: argsIntIntBool, rettype: stBool, bc: opcmplei64},
	scmpleimmi: {text: "cmple.i64@imm", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmplei64imm},
	scmpgtf:    {text: "cmpgt.f64", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpgtf64},
	scmpgtimmf: {text: "cmpgt.f64@imm", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgtf64imm},
	scmpgti:    {text: "cmpgt.i64", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpgti64},
	scmpgtimmi: {text: "cmpgt.i64@imm", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgti64imm},
	scmpgef:    {text: "cmpge.f64", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpgef64},
	scmpgeimmf: {text: "cmpge.f64@imm", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgef64imm},
	scmpgei:    {text: "cmpge.i64", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpgei64},
	scmpgeimmi: {text: "cmpge.i64@imm", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgei64imm},

	scmpeqts: {text: "cmpeq.ts", rettype: stBool, argtypes: []ssatype{stTime, stTime, stBool}, bc: opcmpeqi64},
	scmpltts: {text: "cmplt.ts", rettype: stBool, argtypes: []ssatype{stTime, stTime, stBool}, bc: opcmplti64},
	scmplets: {text: "cmple.ts", rettype: stBool, argtypes: []ssatype{stTime, stTime, stBool}, bc: opcmplei64},
	scmpgtts: {text: "cmpgt.ts", rettype: stBool, argtypes: []ssatype{stTime, stTime, stBool}, bc: opcmpgti64},
	scmpgets: {text: "cmpge.ts", rettype: stBool, argtypes: []ssatype{stTime, stTime, stBool}, bc: opcmpgei64},

	// generic equality comparison
	scmpeqv: {text: "cmpeq.v", argtypes: scalar2Args, rettype: stBool, bc: opcmpeqv},

	// single-operand on values
	sisnull:    {text: "isnull", argtypes: scalar1Args, rettype: stBool, bc: opisnullv},
	sisnonnull: {text: "isnonnull", argtypes: scalar1Args, rettype: stBool, bc: opisnotnullv},
	sistrue:    {text: "istrue", argtypes: scalar1Args, rettype: stBool, bc: opistruev},
	sisfalse:   {text: "isfalse", argtypes: scalar1Args, rettype: stBool, bc: opisfalsev},

	// conversion ops
	// parse value reg -> {float, int} in scalar reg;
	// we have an optional scalar reg value that
	// is merged into, in which case we are guaranteed
	// to preserve the non-updated lanes
	stostr:  {text: "tostr", argtypes: scalar1Args, rettype: stStringMasked, bc: opunpack, emit: emitslice},
	stolist: {text: "tolist", argtypes: scalar1Args, rettype: stListMasked, bc: opunpack, emit: emitslice},
	stoblob: {text: "toblob", argtypes: scalar1Args, rettype: stBlobMasked, bc: opunpack, emit: emitslice},

	sunsymbolize: {text: "unsymbolize", argtypes: scalar1Args, rettype: stValue, bc: opunsymbolize, safeValueMask: true},

	// boolean -> scalar conversions;
	// first argument is true/false; second is present/missing
	scvtktoi64: {text: "cvt.k@i64", argtypes: []ssatype{stBool, stBool}, rettype: stInt, bc: opcvtktoi64, emit: emitBoolConv},
	scvtktof64: {text: "cvt.k@f64", argtypes: []ssatype{stBool, stBool}, rettype: stFloat, bc: opcvtktof64, emit: emitBoolConv},

	// f64/i64/k conversion operations
	scvti64tok:   {text: "cvt.i64@k", argtypes: int1Args, rettype: stBool, bc: opcvti64tok},
	scvtf64tok:   {text: "cvt.f64@k", argtypes: fp1Args, rettype: stBool, bc: opcvtf64tok},
	scvti64tof64: {text: "cvt.i64@f64", argtypes: int1Args, rettype: stFloatMasked, bc: opcvti64tof64},
	scvtf64toi64: {text: "cvt.f64@i64", argtypes: fp1Args, rettype: stIntMasked, bc: opcvttruncf64toi64},
	scvti64tostr: {text: "cvt.i64@str", argtypes: int1Args, rettype: stStringMasked, bc: opcvti64tostr},

	sstrconcat: {text: "strconcat", rettype: stStringMasked, argtypes: []ssatype{}, vaArgs: []ssatype{stString, stBool}, bc: opconcatstr, emit: emitConcatStr},

	//#region string operations
	slowerstr: {text: "lower.str", argtypes: str1Args, rettype: stStringMasked, bc: opslower},
	supperstr: {text: "upper.str", argtypes: str1Args, rettype: stStringMasked, bc: opsupper},

	sStrCmpEqCs:      {text: "cmp_str_eq_cs", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCs},
	sStrCmpEqCi:      {text: "cmp_str_eq_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCi},
	sStrCmpEqUTF8Ci:  {text: "cmp_str_eq_utf8_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqUTF8Ci},
	sEqPatternCs:     {text: "eq_pattern_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opEqPatternCs},
	sEqPatternCi:     {text: "eq_pattern_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opEqPatternCi},
	sEqPatternUTF8Ci: {text: "eq_pattern_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opEqPatternUTF8Ci},

	sCmpFuzzyA3:              {text: "cmp_str_fuzzy_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opCmpStrFuzzyA3},
	sCmpFuzzyUnicodeA3:       {text: "cmp_str_fuzzy_unicode_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opCmpStrFuzzyUnicodeA3},
	sHasSubstrFuzzyA3:        {text: "has_substr_fuzzy_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opHasSubstrFuzzyA3},
	sHasSubstrFuzzyUnicodeA3: {text: "has_substr_fuzzy_unicode_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opHasSubstrFuzzyUnicodeA3},

	sStrTrimWsLeft:    {text: "trim_ws_left", argtypes: str1Args, rettype: stString, bc: opTrimWsLeft},
	sStrTrimWsRight:   {text: "trim_ws_right", argtypes: str1Args, rettype: stString, bc: opTrimWsRight},
	sStrTrimCharLeft:  {text: "trim_char_left", argtypes: str1Args, rettype: stString, immfmt: fmtdict, bc: opTrim4charLeft},
	sStrTrimCharRight: {text: "trim_char_right", argtypes: str1Args, rettype: stString, immfmt: fmtdict, bc: opTrim4charRight},

	// s, k = contains_prefix_cs s, k, $const
	sStrContainsPrefixCs:     {text: "contains_prefix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixCs},
	sStrContainsPrefixCi:     {text: "contains_prefix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixCi},
	sStrContainsPrefixUTF8Ci: {text: "contains_prefix_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixUTF8Ci},

	// s, k = contains_suffix_cs s, k, $const
	sStrContainsSuffixCs:     {text: "contains_suffix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixCs},
	sStrContainsSuffixCi:     {text: "contains_suffix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixCi},
	sStrContainsSuffixUTF8Ci: {text: "contains_suffix_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixUTF8Ci},

	// s, k = contains_substr_cs s, k, $const
	sStrContainsSubstrCs:     {text: "contains_substr_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSubstrCs},
	sStrContainsSubstrCi:     {text: "contains_substr_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSubstrCi},
	sStrContainsSubstrUTF8Ci: {text: "contains_substr_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSubstrUTF8Ci},

	// s, k = contains_pattern_cs s, k, $const
	sStrContainsPatternCs:     {text: "contains_pattern_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPatternCs},
	sStrContainsPatternCi:     {text: "contains_pattern_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPatternCi},
	sStrContainsPatternUTF8Ci: {text: "contains_pattern_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPatternUTF8Ci},

	// ip matching
	sIsSubnetOfIP4: {text: "is_subnet_of_ip4", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opIsSubnetOfIP4},

	// s, k = skip_1char_left s, k -- skip one unicode character at the beginning (left) of a string slice
	sStrSkip1CharLeft: {text: "skip_1char_left", argtypes: str1Args, rettype: stStringMasked, bc: opSkip1charLeft},
	// s, k = skip_1char_right s, k -- skip one unicode character off the end (right) of a string slice
	sStrSkip1CharRight: {text: "skip_1char_right", argtypes: str1Args, rettype: stStringMasked, bc: opSkip1charRight},
	// s, k = skip_nchar_left s, k -- skip n unicode character at the beginning (left) of a string slice
	sStrSkipNCharLeft: {text: "skip_nchar_left", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, bc: opSkipNcharLeft},
	// s, k = skip_nchar_right s, k -- skip n unicode character off the end (right) of a string slice
	sStrSkipNCharRight: {text: "skip_nchar_right", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, bc: opSkipNcharRight},

	soctetlength:     {text: "octetlength", argtypes: str1Args, rettype: stIntMasked, bc: opoctetlength},
	scharacterlength: {text: "characterlength", argtypes: str1Args, rettype: stIntMasked, bc: opcharlength},
	sSubStr:          {text: "substr", argtypes: []ssatype{stString, stInt, stInt, stBool}, rettype: stString, bc: opSubstr},
	sSplitPart:       {text: "split_part", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, immfmt: fmtdict, bc: opSplitPart},

	sDfaT6:  {text: "dfa_tiny6", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT6},
	sDfaT7:  {text: "dfa_tiny7", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT7},
	sDfaT8:  {text: "dfa_tiny8", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT8},
	sDfaT6Z: {text: "dfa_tiny6Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT6Z},
	sDfaT7Z: {text: "dfa_tiny7Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT7Z},
	sDfaT8Z: {text: "dfa_tiny8Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT8Z},
	sDfaLZ:  {text: "dfa_largeZ", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaLZ},

	// compare against a constant exactly
	sequalconst: {text: "equalconst", argtypes: scalar1Args, rettype: stBool, immfmt: fmtother, emit: emitconstcmp},

	ssplit: {text: "split", argtypes: []ssatype{stList, stBool}, rettype: stListAndValueMasked, bc: opsplit, priority: prioParse},

	// convert value to base pointer
	// when it is structure-typed
	stuples: {text: "tuples", argtypes: []ssatype{stValue, stBool}, rettype: stBaseMasked, bc: optuple, priority: prioParse},

	// find a struct field by name relative to a base pointer
	sdot: {text: "dot", argtypes: []ssatype{stBase, stBool}, rettype: stValueMasked, immfmt: fmtother, bc: opfindsym, priority: prioParse},
	// find a struct field by name relative
	// to a previously-computed base pointer;
	// arguments are: (base, prevV, prevK, wantedK)
	sdot2: {text: "dot2", argtypes: []ssatype{stBase, stValue, stBool, stBool}, rettype: stValueMasked, immfmt: fmtother, bc: opfindsym2, priority: prioParse},

	sauxval: {text: "auxval", argtypes: []ssatype{}, rettype: stValueMasked, immfmt: fmtslot, priority: prioParse, bc: opauxval},

	// hash and hash-with-seed ops
	shashvalue:  {text: "hashvalue", argtypes: []ssatype{stValue, stBool}, rettype: stHash, immfmt: fmtslot, bc: ophashvalue, priority: prioHash},
	shashvaluep: {text: "hashvalue+", argtypes: []ssatype{stHash, stValue, stBool}, rettype: stHash, immfmt: fmtslotx2hash, bc: ophashvalueplus, priority: prioHash},

	shashmember: {text: "hashmember", argtypes: []ssatype{stHash, stBool}, rettype: stBool, immfmt: fmtother, bc: ophashmember, emit: emithashmember},
	shashlookup: {text: "hashlookup", argtypes: []ssatype{stHash, stBool}, rettype: stValueMasked, immfmt: fmtother, bc: ophashlookup, emit: emithashlookup},

	sliteral: {text: "literal", rettype: stValue, immfmt: fmtother, bc: oplitref, safeValueMask: true}, // yields <value>.kinit

	sstorev: {text: "store.v", rettype: stMem, argtypes: []ssatype{stMem, stValue, stBool}, immfmt: fmtother, emit: emitstorev, priority: prioMem},

	// return operations construct a value into a single
	// argument that can be passed to `prog.returnValue()`
	sretm:   {text: "ret.m", rettype: stMem, argtypes: []ssatype{stMem}, bc: opret, disjunctive: true, returnOp: true},
	sretmk:  {text: "ret.mk", rettype: stMem, argtypes: []ssatype{stMem, stBool}, bc: opretk, disjunctive: true, returnOp: true},
	sretmsk: {text: "ret.msk", rettype: stMem, argtypes: []ssatype{stMem, stScalar, stBool}, bc: opretsk, disjunctive: true, returnOp: true},
	sretbk:  {text: "ret.bk", rettype: stMem, argtypes: []ssatype{stBase, stBool}, bc: opretbk, disjunctive: true, returnOp: true},
	sretbhk: {text: "ret.bhk", rettype: stMem, argtypes: []ssatype{stBase, stHash, stBool}, bc: opretbhk, disjunctive: true, returnOp: true},

	// identity ops; these ops just return their input
	// associated with a different not-missing mask
	smakev:      {text: "make.v", rettype: stValue, argtypes: []ssatype{stValue, stBool}, bc: opmovv},
	smakevk:     {text: "make.vk", rettype: stValueMasked, argtypes: []ssatype{stValue, stBool}, bc: opmovvk},
	sfloatk:     {text: "floatk", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opmovf64},
	snotmissing: {text: "notmissing", rettype: stBool, argtypes: []ssatype{stBool}, bc: opmovk}, // notmissing exists to coerce st*Masked into stBool

	sblendv:     {text: "blend.v", rettype: stValueMasked, argtypes: []ssatype{stValue, stBool, stValue, stBool}, bc: opblendv, disjunctive: true, safeValueMask: true},
	sblendi64:   {text: "blend.i64", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool, stInt, stBool}, bc: opblendi64, disjunctive: true},
	sblendf64:   {text: "blend.f64", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool, stFloat, stBool}, bc: opblendf64, disjunctive: true},
	sblendslice: {text: "blend.slice", rettype: stStringMasked, argtypes: []ssatype{stString, stBool, stString, stBool}, bc: opblendslice, disjunctive: true},

	sbroadcastf: {text: "broadcast.f", rettype: stFloat, argtypes: []ssatype{}, immfmt: fmtf64, bc: opbroadcastf64},
	sbroadcasti: {text: "broadcast.i", rettype: stInt, argtypes: []ssatype{}, immfmt: fmti64, bc: opbroadcasti64},
	sabsf:       {text: "abs.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opabsf64},
	sabsi:       {text: "abs.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, bc: opabsi64},
	snegf:       {text: "neg.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opnegf64},
	snegi:       {text: "neg.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, bc: opnegi64},
	ssignf:      {text: "sign.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opsignf64},
	ssigni:      {text: "sign.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, bc: opsigni64},
	ssquaref:    {text: "square.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opsquaref64},
	ssquarei:    {text: "square.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, bc: opsquarei64},
	sbitnoti:    {text: "bitnot.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opbitnoti64},
	sbitcounti:  {text: "bitcount.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opbitcounti64},
	sroundf:     {text: "round.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oproundf64},
	sroundevenf: {text: "roundeven.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oproundevenf64},
	struncf:     {text: "trunc.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: optruncf64},
	sfloorf:     {text: "floor.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opfloorf64},
	sceilf:      {text: "ceil.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opceilf64},
	sroundi:     {text: "round.i", rettype: stIntMasked, argtypes: []ssatype{stFloat, stBool}, bc: opcvttruncf64toi64},
	ssqrtf:      {text: "sqrt.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opsqrtf64},
	scbrtf:      {text: "cbrt.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opcbrtf64},
	sexpf:       {text: "exp.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opexpf64},
	sexpm1f:     {text: "expm1.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opexpm1f64},
	sexp2f:      {text: "exp2.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opexp2f64},
	sexp10f:     {text: "exp10.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opexp10f64},
	slnf:        {text: "ln.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: oplnf64},
	sln1pf:      {text: "ln1p.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opln1pf64},
	slog2f:      {text: "log2.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: oplog2f64},
	slog10f:     {text: "log10.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: oplog10f64},
	ssinf:       {text: "sin.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opsinf64},
	scosf:       {text: "cos.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opcosf64},
	stanf:       {text: "tan.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: optanf64},
	sasinf:      {text: "asin.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opasinf64},
	sacosf:      {text: "acos.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opacosf64},
	satanf:      {text: "atan.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, bc: opatanf64},
	satan2f:     {text: "atan2.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opatan2f64},

	saddf:         {text: "add.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opaddf64},
	saddi:         {text: "add.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stInt, stBool}, bc: opaddi64},
	saddimmf:      {text: "add.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opaddf64imm},
	saddimmi:      {text: "add.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opaddi64imm},
	ssubf:         {text: "sub.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opsubf64},
	ssubi:         {text: "sub.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsubi64},
	ssubimmf:      {text: "sub.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opsubf64imm},
	ssubimmi:      {text: "sub.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsubi64imm},
	srsubimmf:     {text: "rsub.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprsubf64imm},
	srsubimmi:     {text: "rsub.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprsubi64imm},
	smulf:         {text: "mul.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmulf64},
	smuli:         {text: "mul.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmuli64},
	smulimmf:      {text: "mul.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmulf64imm},
	smulimmi:      {text: "mul.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmuli64imm},
	sdivf:         {text: "div.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opdivf64},
	sdivi:         {text: "div.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stInt, stBool}, bc: opdivi64},
	sdivimmf:      {text: "div.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opdivf64imm},
	sdivimmi:      {text: "div.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opdivi64imm},
	srdivimmf:     {text: "rdiv.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprdivf64imm},
	srdivimmi:     {text: "rdiv.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprdivi64imm},
	smodf:         {text: "mod.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmodf64},
	smodi:         {text: "mod.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmodi64},
	smodimmf:      {text: "mod.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmodf64imm},
	smodimmi:      {text: "mod.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmodi64imm},
	srmodimmf:     {text: "rmod.imm.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprmodf64imm},
	srmodimmi:     {text: "rmod.imm.i", rettype: stIntMasked, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprmodi64imm},
	sminvaluef:    {text: "minvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opminvaluef64},
	sminvaluei:    {text: "minvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opminvaluei64},
	sminvalueimmf: {text: "minvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opminvaluef64imm},
	sminvalueimmi: {text: "minvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opminvaluei64imm},
	smaxvaluef:    {text: "maxvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmaxvaluef64},
	smaxvaluei:    {text: "maxvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmaxvaluei64},
	smaxvalueimmf: {text: "maxvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmaxvaluef64imm},
	smaxvalueimmi: {text: "maxvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmaxvaluei64imm},
	sandi:         {text: "and.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opandi64},
	sandimmi:      {text: "and.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opandi64imm},
	sori:          {text: "or.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opori64},
	sorimmi:       {text: "or.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opori64imm},
	sxori:         {text: "xor.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opxori64},
	sxorimmi:      {text: "xor.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opxori64imm},
	sslli:         {text: "sll.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opslli64},
	ssllimmi:      {text: "sll.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opslli64imm},
	ssrai:         {text: "sra.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsrai64},
	ssraimmi:      {text: "sra.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsrai64imm},
	ssrli:         {text: "srl.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsrli64},
	ssrlimmi:      {text: "srl.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsrli64imm},

	shypotf:   {text: "hypot.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: ophypotf64},
	spowf:     {text: "pow.f", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: oppowf64},
	spowuintf: {text: "powuint.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmti64, bc: oppowuintf64},

	swidthbucketf: {text: "widthbucket.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opwidthbucketf64},
	swidthbucketi: {text: "widthbucket.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stInt, stInt, stBool}, bc: opwidthbucketi64},

	saggandk:  {text: "aggand.k", rettype: stMem, argtypes: []ssatype{stMem, stBool, stBool}, immfmt: fmtaggslot, bc: opaggandk, priority: prioMem},
	saggork:   {text: "aggor.k", rettype: stMem, argtypes: []ssatype{stMem, stBool, stBool}, immfmt: fmtaggslot, bc: opaggork, priority: prioMem},
	saggsumf:  {text: "aggsum.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggsumf, priority: prioMem},
	saggsumi:  {text: "aggsum.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggsumi, priority: prioMem},
	saggavgf:  {text: "aggavg.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggsumf, priority: prioMem},
	saggavgi:  {text: "aggavg.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggsumi, priority: prioMem},
	saggminf:  {text: "aggmin.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggminf, priority: prioMem},
	saggmini:  {text: "aggmin.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggmini, priority: prioMem},
	saggmaxf:  {text: "aggmax.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggmaxf, priority: prioMem},
	saggmaxi:  {text: "aggmax.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggmaxi, priority: prioMem},
	saggmints: {text: "aggmin.ts", rettype: stMem, argtypes: []ssatype{stMem, stTime, stBool}, immfmt: fmtaggslot, bc: opaggmini, priority: prioMem},
	saggmaxts: {text: "aggmax.ts", rettype: stMem, argtypes: []ssatype{stMem, stTime, stBool}, immfmt: fmtaggslot, bc: opaggmaxi, priority: prioMem},
	saggandi:  {text: "aggand.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggandi, priority: prioMem},
	saggori:   {text: "aggor.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggori, priority: prioMem},
	saggxori:  {text: "aggxor.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggxori, priority: prioMem},
	saggcount: {text: "aggcount", rettype: stMem, argtypes: []ssatype{stMem, stBool}, immfmt: fmtaggslot, bc: opaggcount, priority: prioMem + 1},

	// compute hash aggregate bucket location; encoded immediate will be input hash slot to use
	saggbucket: {text: "aggbucket", argtypes: []ssatype{stMem, stHash, stBool}, rettype: stBucket, immfmt: fmtslot, bc: opaggbucket},

	// hash aggregate bucket ops (count, min, max, sum, ...)
	saggslotandk:  {text: "aggslotand.k", argtypes: []ssatype{stMem, stBucket, stBool, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotandk, priority: prioMem},
	saggslotork:   {text: "aggslotor.k", argtypes: []ssatype{stMem, stBucket, stBool, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotork, priority: prioMem},
	saggslotsumf:  {text: "aggslotsum.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotsumf, priority: prioMem},
	saggslotsumi:  {text: "aggslotsum.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotsumi, priority: prioMem},
	saggslotavgf:  {text: "aggslotavg.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotavgf, priority: prioMem},
	saggslotavgi:  {text: "aggslotavg.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotavgi, priority: prioMem},
	saggslotminf:  {text: "aggslotmin.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotminf, priority: prioMem},
	saggslotmini:  {text: "aggslotmin.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxf:  {text: "aggslotmax.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxf, priority: prioMem},
	saggslotmaxi:  {text: "aggslotmax.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotmints: {text: "aggslotmin.ts", argtypes: []ssatype{stMem, stBucket, stTime, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxts: {text: "aggslotmax.ts", argtypes: []ssatype{stMem, stBucket, stTime, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotandi:  {text: "aggslotand.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotandi, priority: prioMem},
	saggslotori:   {text: "aggslotor.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotori, priority: prioMem},
	saggslotxori:  {text: "aggslotxor.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotxori, priority: prioMem},
	saggslotcount: {text: "aggslotcount", argtypes: []ssatype{stMem, stBucket, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotcount, priority: prioMem},

	// boxing ops
	//
	// turn two masks into TRUE/FALSE/MISSING according to 3VL
	sboxmask:  {text: "boxmask", argtypes: []ssatype{stBool, stBool}, rettype: stValue, bc: opboxk, safeValueMask: true},
	sboxint:   {text: "boxint", argtypes: []ssatype{stInt, stBool}, rettype: stValue, bc: opboxi64, safeValueMask: true},
	sboxfloat: {text: "boxfloat", argtypes: []ssatype{stFloat, stBool}, rettype: stValue, bc: opboxf64, safeValueMask: true},
	sboxstr:   {text: "boxstr", argtypes: []ssatype{stString, stBool}, rettype: stValue, bc: opboxstr, safeValueMask: true},

	// timestamp operations
	sbroadcastts:            {text: "broadcast.ts", rettype: stTime, argtypes: []ssatype{}, immfmt: fmti64, bc: opbroadcasti64},
	sunboxtime:              {text: "unboxtime", argtypes: []ssatype{stValue, stBool}, rettype: stTimeMasked, bc: opunboxts},
	sdateadd:                {text: "dateadd", rettype: stTimeMasked, argtypes: []ssatype{stTime, stInt, stBool}, bc: opaddi64},
	sdateaddimm:             {text: "dateadd.imm", rettype: stTimeMasked, argtypes: []ssatype{stTime, stBool}, immfmt: fmti64, bc: opaddi64imm},
	sdateaddmulimm:          {text: "dateaddmul.imm", rettype: stTimeMasked, argtypes: []ssatype{stTime, stInt, stBool}, immfmt: fmti64, bc: opaddmuli64imm},
	sdateaddmonth:           {text: "dateaddmonth", rettype: stTimeMasked, argtypes: []ssatype{stTime, stInt, stBool}, bc: opdateaddmonth},
	sdateaddmonthimm:        {text: "dateaddmonth.imm", rettype: stTimeMasked, argtypes: []ssatype{stTime, stBool}, immfmt: fmti64, bc: opdateaddmonthimm},
	sdateaddquarter:         {text: "dateaddquarter", rettype: stTimeMasked, argtypes: []ssatype{stTime, stInt, stBool}, bc: opdateaddquarter},
	sdateaddyear:            {text: "dateaddyear", rettype: stTimeMasked, argtypes: []ssatype{stTime, stInt, stBool}, bc: opdateaddyear},
	sdatediffmicro:          {text: "datediffmicro", rettype: stIntMasked, argtypes: []ssatype{stTime, stTime, stBool}, bc: opdatediffmicrosecond},
	sdatediffparam:          {text: "datediffparam", rettype: stIntMasked, argtypes: []ssatype{stTime, stTime, stBool}, bc: opdatediffparam, immfmt: fmti64},
	sdatediffmonth:          {text: "datediffmonth", rettype: stIntMasked, argtypes: []ssatype{stTime, stTime, stBool}, bc: opdatediffmqy, emit: emitDateDiffMQY},
	sdatediffquarter:        {text: "datediffquarter", rettype: stIntMasked, argtypes: []ssatype{stTime, stTime, stBool}, bc: opdatediffmqy, emit: emitDateDiffMQY},
	sdatediffyear:           {text: "datediffyear", rettype: stIntMasked, argtypes: []ssatype{stTime, stTime, stBool}, bc: opdatediffmqy, emit: emitDateDiffMQY},
	sdateextractmicrosecond: {text: "dateextractmicrosecond", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractmicrosecond},
	sdateextractmillisecond: {text: "dateextractmillisecond", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractmillisecond},
	sdateextractsecond:      {text: "dateextractsecond", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractsecond},
	sdateextractminute:      {text: "dateextractminute", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractminute},
	sdateextracthour:        {text: "dateextracthour", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextracthour},
	sdateextractday:         {text: "dateextractday", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractday},
	sdateextractdow:         {text: "dateextractdow", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractdow},
	sdateextractdoy:         {text: "dateextractdoy", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractdoy},
	sdateextractmonth:       {text: "dateextractmonth", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractmonth},
	sdateextractquarter:     {text: "dateextractquarter", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractquarter},
	sdateextractyear:        {text: "dateextractyear", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdateextractyear},
	sdatetounixepoch:        {text: "datetounixepoch", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdatetounixepoch},
	sdatetounixmicro:        {text: "datetounixmicro", rettype: stInt, argtypes: []ssatype{stTime, stBool}, bc: opdatetounixmicro},
	sdatetruncmillisecond:   {text: "datetruncmillisecond", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncmillisecond},
	sdatetruncsecond:        {text: "datetruncsecond", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncsecond},
	sdatetruncminute:        {text: "datetruncminute", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncminute},
	sdatetrunchour:          {text: "datetrunchour", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetrunchour},
	sdatetruncday:           {text: "datetruncday", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncday},
	sdatetruncdow:           {text: "datetruncdow", rettype: stTime, argtypes: []ssatype{stTime, stBool}, immfmt: fmti64, bc: opdatetruncdow},
	sdatetruncmonth:         {text: "datetruncmonth", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncmonth},
	sdatetruncquarter:       {text: "datetruncquarter", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncquarter},
	sdatetruncyear:          {text: "datetruncyear", rettype: stTime, argtypes: []ssatype{stTime, stBool}, bc: opdatetruncyear},
	stimebucketts:           {text: "timebucket.ts", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: optimebucketts},
	sboxts:                  {text: "boxts", argtypes: []ssatype{stTime, stBool}, rettype: stValue, bc: opboxts},

	sboxlist:       {text: "boxlist", rettype: stValue, argtypes: []ssatype{stList, stBool}, bc: opboxlist, safeValueMask: true},
	smakelist:      {text: "makelist", rettype: stValueMasked, argtypes: []ssatype{stBool}, vaArgs: []ssatype{stValue, stBool}, bc: opmakelist, safeValueMask: true, emit: emitMakeList},
	smakestruct:    {text: "makestruct", rettype: stValueMasked, argtypes: []ssatype{stBool}, vaArgs: []ssatype{stString, stValue, stBool}, bc: opmakestruct, safeValueMask: true, emit: emitMakeStruct},
	smakestructkey: {text: "makestructkey", rettype: stString, immfmt: fmtother, emit: emitNone},

	// GEO functions
	sgeohash:      {text: "geohash", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeohash},
	sgeohashimm:   {text: "geohash.imm", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeohashimm},
	sgeotilex:     {text: "geotilex", rettype: stInt, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotilex},
	sgeotiley:     {text: "geotiley", rettype: stInt, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotiley},
	sgeotilees:    {text: "geotilees", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeotilees},
	sgeotileesimm: {text: "geotilees.imm", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeotileesimm},
	sgeodistance:  {text: "geodistance", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opgeodistance},

	schecktag: {text: "checktag", argtypes: []ssatype{stValue, stBool}, rettype: stValueMasked, immfmt: fmtother, bc: opchecktag},
	stypebits: {text: "typebits", argtypes: []ssatype{stValue, stBool}, rettype: stInt, bc: optypebits},

	sobjectsize:    {text: "objectsize", argtypes: []ssatype{stValue, stBool}, rettype: stIntMasked, bc: opobjectsize},
	sarraysize:     {text: "arraysize", argtypes: []ssatype{stList, stBool}, rettype: stInt, bc: oparraysize},
	sarrayposition: {text: "arrayposition", argtypes: []ssatype{stList, stValue, stBool}, rettype: stIntMasked, bc: oparrayposition},

	saggapproxcount: {
		text:     "aggapproxcount",
		argtypes: []ssatype{stHash, stBool},
		rettype:  stMem,
		bc:       opaggapproxcount,
		emit:     emitaggapproxcount,
		immfmt:   fmtother,
	},
	saggapproxcountpartial: {
		text:     "aggapproxcount.partial",
		argtypes: []ssatype{stHash, stBool},
		rettype:  stMem,
		bc:       opaggapproxcount,
		emit:     emitaggapproxcount,
		immfmt:   fmtother,
	},
	saggapproxcountmerge: {
		text:     "aggapproxcount.merge",
		argtypes: []ssatype{stBlob, stBool},
		rettype:  stMem,
		bc:       opaggapproxcountmerge,
		emit:     emitaggapproxcountmerge,
		immfmt:   fmtaggslot,
	},
	saggslotapproxcount: {
		text:     "aggslotapproxcount",
		argtypes: []ssatype{stMem, stBucket, stHash, stBool},
		rettype:  stMem,
		bc:       opaggslotapproxcount,
		emit:     emitaggslotapproxcount,
		immfmt:   fmti64,
		priority: prioMem,
	},
	saggslotapproxcountpartial: {
		text:     "aggslotapproxcount.partial",
		argtypes: []ssatype{stMem, stBucket, stHash, stBool},
		rettype:  stMem,
		bc:       opaggslotapproxcount,
		emit:     emitaggslotapproxcount,
		immfmt:   fmtother,
		priority: prioMem,
	},
	saggslotapproxcountmerge: {
		text:     "aggslotapproxcount.merge",
		argtypes: []ssatype{stMem, stBucket, stBlob, stBool},
		rettype:  stMem,
		bc:       opaggslotapproxcountmerge,
		emit:     emitaggslotapproxcountmerge,
		immfmt:   fmtaggslot,
		priority: prioMem,
	},
}
