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
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
	"golang.org/x/sys/cpu"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/heap"
	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
)

type ssaop int

const (
	sinvalid    ssaop = iota
	sinit             // initial lane pointer and mask
	sinitmem          // initial memory state
	sundef            // initial scalar value (undefined)
	smergemem         // merge memory
	sbroadcastk       // mask = broadcastk(m)
	skfalse           // logical bottom value; FALSE and also MISSING
	sand              // mask = (mask0 & mask1)
	sor               // mask = (mask0 | mask1)
	snand             // mask = (^mask0 & mask1)
	sxor              // mask = (mask0 ^ mask1)  (unequal bits)
	sxnor             // mask = (mask0 ^ ^mask1) (equal bits)

	sunboxktoi // val = unboxktoi(v)
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

	scmpltts
	scmplets
	scmpgtts
	scmpgets

	seqstr  // str = str
	seqtime // time = time

	// compare scalar against value;
	// effectively just a memcmp() operation
	sequalv // mask = arg0.mask == arg1.mask

	// raw value test ops
	sisnull    // mask = arg0.mask == null
	sisnonnull // mask = arg0.mask != null
	sisfalse   // mask = arg0.mask == false
	sistrue    // mask = arg0.mask == true

	stoint
	stofloat
	stostr
	stolist
	stotime
	stoblob
	sunsymbolize

	scvtktoi // bool to 0 or 1
	scvtktof // bool to 0.0 or 1.0
	scvtitok // int64 to bool
	scvtftok // fp to bool
	scvtftoi // fp to int, round nearest
	scvtitof // int to fp

	scvti64tostr // int64 to string

	sconcatstr2 // string concatenation (two strings)
	sconcatstr3 // string concatenation (three strings)
	sconcatstr4 // string concatenation (four strings)

	slowerstr
	supperstr

	// raw string comparison
	sStrCmpEqCs              // Ascii string compare equality case-sensitive
	sStrCmpEqCi              // Ascii string compare equality case-insensitive
	sStrCmpEqUTF8Ci          // UTF-8 string compare equality case-insensitive
	sCmpFuzzyA3              // Ascii string fuzzy equality: Damerau–Levenshtein up to provided number of operations
	sCmpFuzzyUnicodeA3       // unicode string fuzzy equality: Damerau–Levenshtein up to provided number of operations
	sHasSubstrFuzzyA3        // Ascii string contains with fuzzy string compare
	sHasSubstrFuzzyUnicodeA3 // unicode string contains with fuzzy string compare

	sStrTrimCharLeft  // String trim specific chars left
	sStrTrimCharRight // String trim specific chars right
	sStrTrimWsLeft    // String trim whitespace left
	sStrTrimWsRight   // String trim whitespace right

	sStrContainsPrefixCs     // String contains prefix case-sensitive
	sStrContainsPrefixCi     // String contains prefix case-insensitive
	sStrContainsPrefixUTF8Ci // String contains prefix case-insensitive

	sStrContainsSuffixCs     // String contains suffix case-sensitive
	sStrContainsSuffixCi     // String contains suffix case-insensitive
	sStrContainsSuffixUTF8Ci // String contains suffix case-insensitive

	sStrContainsSubstrCs     // String contains substr case-sensitive
	sStrContainsSubstrCi     // String contains substr case-insensitive
	sStrContainsSubstrUTF8Ci // String contains substr case-insensitive

	sStrContainsPatternCs     // String contains pattern case-sensitive
	sStrContainsPatternCi     // String contains pattern case-insensitive
	sStrContainsPatternUTF8Ci // String contains pattern case-insensitive

	sIsSubnetOfIP4 // IP subnet matching

	sStrSkip1CharLeft  // String skip 1 unicode code-point from left
	sStrSkip1CharRight // String skip 1 unicode code-point from right
	sStrSkipNCharLeft  // String skip n unicode code-point from left
	sStrSkipNCharRight // String skip n unicode code-point from right

	sCharLength // count number of unicode-points
	sSubStr     // select a substring
	sSplitPart  // Presto split_part

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

	sstorev // store value in a stack slot
	sloadv  // load value from a stack slot
	sloadvperm

	sstorelist
	sloadlist
	smsk // mem+scalar+predicate
	sbhk // base+hash+predicate
	sbk  // base+predicate tuple
	smk  // mem+predicate tuple
	svk
	sfloatk

	// blend ops (just conditional moves)
	sblendv
	sblendint
	sblendfloat
	sblendstr

	// timestamp comparison ops
	sgtconsttm // val > timestamp constant
	sltconsttm // val < timestamp constant
	stmextract

	// broadcasts a constant to all lanes
	sbroadcastf // val = broadcast(float64(imm))
	sbroadcasti // val = broadcast(int64(imm))

	// unary operators and functions
	sabsf       // val = abs(val)
	sabsi       // val = abs(val)
	snegf       // val = -val
	snegi       // val = -val
	ssignf      // val = sign(val)
	ssigni      // val = sign(val)
	ssquaref    // val = val * val
	ssquarei    // val = val * val
	sbitnoti    // val = ~val
	sbitcounti  // val = bit_count(val)
	sroundf     // val = round(val)
	sroundevenf // val = roundeven(val)
	struncf     // val = trunc(val)
	sfloorf     // val = floor(val)
	sceilf      // val = ceil(val)
	sroundi     // val = int(round(val))
	ssqrtf      // val = sqrt(val)
	scbrtf      // val = cbrt(val)
	sexpf       // val = exp(val)
	sexpm1f     // val = exp(val) - 1
	sexp2f      // val = exp2(val)
	sexp10f     // val = exp10(val)
	slnf        // val = ln(val)
	sln1pf      // val = ln(val + 1)
	slog2f      // val = log2(val)
	slog10f     // val = log10(val)
	ssinf       // val = sin(x)
	scosf       // val = cos(x)
	stanf       // val = tan(x)
	sasinf      // val = asin(x)
	sacosf      // val = acos(x)
	satanf      // val = atan(x)

	// binary operators and functions
	saddf         // val = val + slot[imm]
	saddi         // val = val + slot[imm]
	saddimmf      // val = val + imm
	saddimmi      // val = val + imm
	ssubf         // val = val - slot[imm]
	ssubi         // val = val - slot[imm]
	ssubimmf      // val = val - imm
	ssubimmi      // val = val - imm
	srsubf        // val = slot[imm] - val
	srsubi        // val = slot[imm] - val
	srsubimmf     // val = imm - val
	srsubimmi     // val = imm - val
	smulf         // val = val * slot[imm]
	smuli         // val = val * slot[imm]
	smulimmf      // val = val * imm
	smulimmi      // val = val * imm
	sdivf         // val = val / slot[imm]
	sdivi         // val = val / slot[imm]
	sdivimmf      // val = val / imm
	sdivimmi      // val = val / imm
	srdivf        // val = slot[imm] / val
	srdivi        // val = slot[imm] / val
	srdivimmf     // val = imm / val
	srdivimmi     // val = imm / val
	smodf         // val = val % slot[imm]
	smodi         // val = val % slot[imm]
	smodimmf      // val = val % imm
	smodimmi      // val = val % imm
	srmodf        // val = slot[imm] % val
	srmodi        // val = slot[imm] % val
	srmodimmf     // val = imm % val
	srmodimmi     // val = imm % val
	sminvaluef    // val = min(val, slot[imm])
	sminvaluei    // val = min(val, slot[imm])
	sminvalueimmf // val = min(val, imm)
	sminvalueimmi // val = min(val, imm)
	smaxvaluef    // val = max(val, slot[imm])
	smaxvaluei    // val = max(val, slot[imm])
	smaxvalueimmf // val = max(val, imm)
	smaxvalueimmi // val = max(val, imm)
	sandi         // val = val & slot[imm]
	sandimmi      // val = val & imm
	sori          // val = val | slot[imm]
	sorimmi       // val = val | imm
	sxori         // val = val ^ slot[imm]
	sxorimmi      // val = val ^ imm
	sslli         // val = val << slot[imm]
	ssllimmi      // val = val << imm
	ssrai         // val = val >> slot[imm]
	ssraimmi      // val = val >> imm
	ssrli         // val = val >>> slot[imm]
	ssrlimmi      // val = val >>> imm
	satan2f       // val = atan2(y, slot[imm])
	shypotf       // val = hypot(val, slot[imm])
	spowf         // val = pow(val, slot[imm])

	swidthbucketf // val = width_bucket(val, min, max, bucket_count)
	swidthbucketi // val = width_bucket(val, min, max, bucket_count)
	stimebucketts // val = time_bucket(val, interval)

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

	sboxmask   // box a mask
	sboxint    // box an integer
	sboxfloat  // box a float
	sboxstring // box a string
	sboxts     // box a timestamp (unpacked)

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

type ssatype int

const (
	stBool    = 1 << iota // only a mask
	stBase                // inner bytes of a structure
	stValue               // opaque ion value
	stFloat               // unpacked float
	stInt                 // unpacked signed integer
	stString              // unpacked string pointer
	stList                // unpacked list slice
	stTime                // unpacked time slice
	stTimeInt             // datetime representation in microseconds as int64
	stHash                // hash of a value
	stBucket              // displacement used for hash aggregates
	stMem                 // memory is modified

	// generally, functions return
	// composite types: a real return value,
	// plus a new mask containing the lanes
	// in which the operation was successful
	stScalar        = stFloat | stInt | stString | stList | stTime | stTimeInt
	stBaseMasked    = stBase | stBool
	stValueMasked   = stValue | stBool
	stFloatMasked   = stFloat | stBool
	stIntMasked     = stInt | stBool
	stStringMasked  = stString | stBool
	stListMasked    = stList | stBool
	stTimeMasked    = stTime | stBool
	stTimeIntMasked = stTimeInt | stBool

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
		return 'T'
	case stTimeInt:
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
		{bit: stTimeInt, name: "time-int"},
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
	// are valid, but not [X, Y, X]. This makes sure to enfore values with predicates.
	vaArgs  []ssatype
	rettype ssatype

	inverse  ssaop // for two-operand ops, flip the arguments
	priority int   // instruction scheduling priority; high = early, low = late

	// the emit function, if we're not using the default
	emit func(v *value, c *compilestate)
	// when non-zero, the corresponding bytecode op
	bc    bcop
	bcrev bcop

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
var str3Args = []ssatype{stString, stString, stString, stBool}
var str4Args = []ssatype{stString, stString, stString, stString, stBool}
var time1Args = []ssatype{stTime, stBool}

var parseValueArgs = []ssatype{stScalar, stValue, stBool}

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
	sinit:     {text: "init", rettype: stBase | stBool, emit: emitinit, priority: prioInit},
	sinitmem:  {text: "initmem", rettype: stMem, emit: emitinit, priority: prioMem},
	smergemem: {text: "mergemem", vaArgs: memArgs, rettype: stMem, emit: emitinit, priority: prioMem},
	// initial scalar register value;
	// not legal to use except to overwrite
	// with value-parsing ops
	sundef: {text: "undef", rettype: stFloat | stInt | stString, emit: emitinit, priority: prioInit - 1},
	// kfalse is the canonical 'bottom' mask value;
	// it is also the MISSING value
	// (kfalse is overloaded to mean "no result"
	// because sometimes we determine that certain
	// path expressions must yield no result due to
	// the symbol not being present in the symbol table)
	sbroadcastk: {text: "broadcast.k", rettype: stBool},
	skfalse:     {text: "false", rettype: stValue | stBool, emit: emitfalse},
	sand:        {text: "and.k", argtypes: argsBoolBool, rettype: stBool, emit: emitlogical, bc: opandk},
	snand:       {text: "nand.k", argtypes: argsBoolBool, rettype: stBool, emit: emitnand, bc: opnandk, disjunctive: true},
	sor:         {text: "or.k", argtypes: argsBoolBool, rettype: stBool, emit: emitlogical, bc: opork, disjunctive: true},
	sxor:        {text: "xor.k", argtypes: argsBoolBool, rettype: stBool, emit: emitlogical, bc: opxork, disjunctive: true},
	sxnor:       {text: "xnor.k", argtypes: argsBoolBool, rettype: stBool, emit: emitlogical, bc: opxnork, disjunctive: true},

	sunboxktoi:      {text: "unbox.k@i", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxktoi64},
	sunboxcoercef64: {text: "unboxcoerce.f64", argtypes: scalar1Args, rettype: stFloatMasked, bc: opunboxcoercef64},
	sunboxcoercei64: {text: "unboxcoerce.i64", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxcoercei64},
	sunboxcvtf64:    {text: "unboxcvt.f64", argtypes: scalar1Args, rettype: stFloatMasked, bc: opunboxcvtf64},
	sunboxcvti64:    {text: "unboxcvt.i64", argtypes: scalar1Args, rettype: stIntMasked, bc: opunboxcvti64},

	// two-operand comparison ops
	scmpv:       {text: "cmpv", argtypes: value2Args, rettype: stInt | stBool, bc: opcmpv, emit: emitauto2},
	scmpvk:      {text: "cmpv.k", argtypes: []ssatype{stValue, stBool, stBool}, rettype: stInt | stBool, bc: opcmpvk, emit: emitauto2},
	scmpvimmk:   {text: "cmpv.imm.k", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, bc: opcmpvimmk, immfmt: fmtother, emit: emitauto2},
	scmpvi64:    {text: "cmpv.i64", argtypes: []ssatype{stValue, stInt, stBool}, rettype: stInt | stBool, bc: opcmpvi64, emit: emitauto2},
	scmpvimmi64: {text: "cmpv.imm.i64", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, bc: opcmpvimmi64, immfmt: fmti64, emit: emitauto2},
	scmpvf64:    {text: "cmpv.f64", argtypes: []ssatype{stValue, stFloat, stBool}, rettype: stInt | stBool, bc: opcmpvf64, emit: emitauto2},
	scmpvimmf64: {text: "cmpv.imm.f64", argtypes: []ssatype{stValue, stBool}, rettype: stInt | stBool, bc: opcmpvimmf64, immfmt: fmtf64, emit: emitauto2},
	scmpltstr:   {text: "cmplt.str", argtypes: str2Args, rettype: stBool, bc: opcmpltstr, emit: emitauto2},
	scmplestr:   {text: "cmple.str", argtypes: str2Args, rettype: stBool, bc: opcmplestr, emit: emitauto2},
	scmpgtstr:   {text: "cmpgt.str", argtypes: str2Args, rettype: stBool, bc: opcmpgtstr, emit: emitauto2},
	scmpgestr:   {text: "cmpge.str", argtypes: str2Args, rettype: stBool, bc: opcmpgestr, emit: emitauto2},

	scmpltk:    {text: "cmplt.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpltk, inverse: scmpgtk, emit: emitauto2},
	scmpltimmk: {text: "cmplt.imm.k", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpltimmk, emit: emitauto2},
	scmplek:    {text: "cmple.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmplek, inverse: scmpgek, emit: emitauto2},
	scmpleimmk: {text: "cmple.imm.k", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpleimmk, emit: emitauto2},
	scmpgtk:    {text: "cmpgt.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpgtk, inverse: scmpltk, emit: emitauto2},
	scmpgtimmk: {text: "cmpgt.imm.k", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpgtimmk, emit: emitauto2},
	scmpgek:    {text: "cmpge.k", argtypes: argsBoolBoolBool, rettype: stBool, bc: opcmpgek, inverse: scmplek, emit: emitauto2},
	scmpgeimmk: {text: "cmpge.imm.k", argtypes: argsBoolBool, rettype: stBool, immfmt: fmtbool, bc: opcmpgeimmk, emit: emitauto2},

	scmpeqf:    {text: "cmpeq.f", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpeqf, inverse: scmpeqf, emit: emitcmp},
	scmpeqimmf: {text: "cmpeq.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpeqimmf},
	scmpeqi:    {text: "cmpeq.i", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpeqi, inverse: scmpeqi, emit: emitcmp},
	scmpeqimmi: {text: "cmpeq.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpeqimmi},
	scmpltf:    {text: "cmplt.f", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpltf, inverse: scmpgtf, emit: emitcmp},
	scmpltimmf: {text: "cmplt.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpltimmf},
	scmplti:    {text: "cmplt.i", argtypes: argsIntIntBool, rettype: stBool, bc: opcmplti, inverse: scmpgti, emit: emitcmp},
	scmpltimmi: {text: "cmplt.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpltimmi},
	scmplef:    {text: "cmple.f", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmplef, inverse: scmpgef, emit: emitcmp},
	scmpleimmf: {text: "cmple.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpleimmf},
	scmplei:    {text: "cmple.i", argtypes: argsIntIntBool, rettype: stBool, bc: opcmplei, inverse: scmpgei, emit: emitcmp},
	scmpleimmi: {text: "cmple.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpleimmi},
	scmpgtf:    {text: "cmpgt.f", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpgtf, inverse: scmpltf, emit: emitcmp},
	scmpgtimmf: {text: "cmpgt.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgtimmf},
	scmpgti:    {text: "cmpgt.i", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpgti, inverse: scmplti, emit: emitcmp},
	scmpgtimmi: {text: "cmpgt.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgtimmi},
	scmpgef:    {text: "cmpge.f", argtypes: argsFloatFloatBool, rettype: stBool, bc: opcmpgef, inverse: scmplef, emit: emitcmp},
	scmpgeimmf: {text: "cmpge.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgeimmf},
	scmpgei:    {text: "cmpge.i", argtypes: argsIntIntBool, rettype: stBool, bc: opcmpgei, inverse: scmplei, emit: emitcmp},
	scmpgeimmi: {text: "cmpge.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgeimmi},

	scmpltts: {text: "cmplt.ts", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmplti, emit: emitcmp, inverse: scmpgtts},
	scmplets: {text: "cmple.ts", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmplei, emit: emitcmp, inverse: scmpgets},
	scmpgtts: {text: "cmpgt.ts", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmpgti, emit: emitcmp, inverse: scmpltts},
	scmpgets: {text: "cmpge.ts", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmpgei, emit: emitcmp, inverse: scmplets},

	seqstr:  {text: "eqstr", bc: opeqslice, argtypes: []ssatype{stString, stString, stBool}, inverse: seqstr, rettype: stBool, emit: emitcmp},
	seqtime: {text: "eqtime", bc: opeqslice, argtypes: []ssatype{stTime, stTime, stBool}, inverse: seqstr, rettype: stBool, emit: emitcmp},

	// generic equality comparison
	sequalv: {text: "equalv", argtypes: scalar2Args, rettype: stBool, emit: emitequalv},

	// single-operand on values
	sisnull:    {text: "isnull", argtypes: scalar1Args, rettype: stBool, bc: opisnull, inverse: sisnonnull},
	sisnonnull: {text: "isnonnull", argtypes: scalar1Args, rettype: stBool, bc: opisnotnull, inverse: sisnull},
	sistrue:    {text: "istrue", argtypes: scalar1Args, rettype: stBool, bc: opistrue},
	sisfalse:   {text: "isfalse", argtypes: scalar1Args, rettype: stBool, bc: opisfalse},

	// conversion ops
	// parse value reg -> {float, int} in scalar reg;
	// we have an optional scalar reg value that
	// is merged into, in which case we are guaranteed
	// to preserve the non-updated lanes
	stoint:   {text: "toint", argtypes: parseValueArgs, rettype: stIntMasked, bc: optoint},
	stofloat: {text: "tofloat", argtypes: parseValueArgs, rettype: stFloatMasked, bc: optof64},

	stostr:  {text: "tostr", argtypes: scalar1Args, rettype: stStringMasked, bc: opunpack, emit: emitslice},
	stolist: {text: "tolist", argtypes: scalar1Args, rettype: stListMasked, bc: opunpack, emit: emitslice},
	stotime: {text: "totime", argtypes: scalar1Args, rettype: stTimeMasked, bc: opunpack, emit: emitslice},
	stoblob: {text: "toblob", argtypes: scalar1Args, rettype: stBlobMasked, bc: opunpack, emit: emitslice},

	sunsymbolize: {text: "unsymbolize", argtypes: scalar1Args, rettype: stValue, bc: opunsymbolize},

	// boolean -> scalar conversions;
	// first argument is true/false; second is present/missing
	scvtktoi: {text: "cvt.k@i", argtypes: []ssatype{stBool, stBool}, rettype: stInt, bc: opcvtktoi64, emit: emitboolconv},
	scvtktof: {text: "cvt.k@f", argtypes: []ssatype{stBool, stBool}, rettype: stFloat, bc: opcvtktof64, emit: emitboolconv},

	// fp <-> int conversion ops
	scvtitok: {text: "cvt.i@k", argtypes: int1Args, rettype: stBool, bc: opcvti64tok},
	scvtitof: {text: "cvt.i@f", argtypes: int1Args, rettype: stFloatMasked, bc: opcvti64tof64},
	scvtftok: {text: "cvt.f@k", argtypes: fp1Args, rettype: stBool, bc: opcvtf64tok},
	scvtftoi: {text: "cvt.f@i", argtypes: fp1Args, rettype: stIntMasked, bc: opcvtf64toi64},

	scvti64tostr: {text: "cvti64tostr", argtypes: int1Args, rettype: stString, bc: opcvti64tostr},
	sconcatstr2:  {text: "concat2.str", argtypes: str2Args, rettype: stStringMasked, emit: emitConcatStr},
	sconcatstr3:  {text: "concat3.str", argtypes: str3Args, rettype: stStringMasked, emit: emitConcatStr},
	sconcatstr4:  {text: "concat4.str", argtypes: str4Args, rettype: stStringMasked, emit: emitConcatStr},

	slowerstr: {text: "lower.str", argtypes: str1Args, rettype: stStringMasked, emit: emitStringCaseChange(opslower)},
	supperstr: {text: "upper.str", argtypes: str1Args, rettype: stStringMasked, emit: emitStringCaseChange(opsupper)},

	sStrCmpEqCs:              {text: "cmp_str_eq_cs", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCs},
	sStrCmpEqCi:              {text: "cmp_str_eq_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCi},
	sStrCmpEqUTF8Ci:          {text: "cmp_str_eq_utf8_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqUTF8Ci},
	sCmpFuzzyA3:              {text: "cmp_str_fuzzy_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opCmpStrFuzzyA3, emit: emitStrEditStack1x1},
	sCmpFuzzyUnicodeA3:       {text: "cmp_str_fuzzy_unicode_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opCmpStrFuzzyUnicodeA3, emit: emitStrEditStack1x1},
	sHasSubstrFuzzyA3:        {text: "has_substr_fuzzy_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opHasSubstrFuzzyA3, emit: emitStrEditStack1x1},
	sHasSubstrFuzzyUnicodeA3: {text: "has_substr_fuzzy_unicode_A3", argtypes: []ssatype{stString, stInt, stBool}, rettype: stBool, immfmt: fmtother, bc: opHasSubstrFuzzyUnicodeA3, emit: emitStrEditStack1x1},

	sStrTrimWsLeft:    {text: "trim_ws_left", argtypes: str1Args, rettype: stStringMasked, bc: opTrimWsLeft},
	sStrTrimWsRight:   {text: "trim_ws_right", argtypes: str1Args, rettype: stStringMasked, bc: opTrimWsRight},
	sStrTrimCharLeft:  {text: "trim_char_left", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrim4charLeft},
	sStrTrimCharRight: {text: "trim_char_right", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrim4charRight},

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
	sStrSkipNCharLeft: {text: "skip_nchar_left", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, immfmt: fmtother, bc: opSkipNcharLeft, emit: emitStrEditStack1},
	// s, k = skip_nchar_right s, k -- skip n unicode character off the end (right) of a string slice
	sStrSkipNCharRight: {text: "skip_nchar_right", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, immfmt: fmtother, bc: opSkipNcharRight, emit: emitStrEditStack1},

	sCharLength: {text: "char_length", argtypes: str1Args, rettype: stInt, bc: opLengthStr},
	sSubStr:     {text: "substr", argtypes: []ssatype{stString, stInt, stInt, stBool}, rettype: stString, immfmt: fmtother, bc: opSubstr, emit: emitStrEditStack2},
	sSplitPart:  {text: "split_part", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, immfmt: fmtother, bc: opSplitPart, emit: emitStrEditStack1x1},

	sDfaT6:  {text: "dfa_tiny6", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT6},
	sDfaT7:  {text: "dfa_tiny7", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT7},
	sDfaT8:  {text: "dfa_tiny8", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT8},
	sDfaT6Z: {text: "dfa_tiny6Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT6Z},
	sDfaT7Z: {text: "dfa_tiny7Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT7Z},
	sDfaT8Z: {text: "dfa_tiny8Z", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaT8Z},
	sDfaLZ:  {text: "dfa_largeZ", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opDfaLZ},

	// compare against a constant exactly
	sequalconst: {text: "equalconst", argtypes: scalar1Args, rettype: stBool, immfmt: fmtother, emit: emitconstcmp},

	ssplit: {text: "split", argtypes: []ssatype{stList, stBool}, rettype: stListAndValueMasked, emit: emitsplit, priority: prioParse},

	// convert value to base pointer
	// when it is structure-typed
	stuples: {text: "tuples", argtypes: []ssatype{stValue, stBool}, rettype: stBase | stBool, emit: emittuple, bc: optuple, priority: prioParse},

	// find a struct field by name relative to a base pointer
	sdot: {text: "dot", argtypes: []ssatype{stBase, stBool}, rettype: stValueMasked, immfmt: fmtother, emit: emitdot, priority: prioParse},

	sauxval: {text: "auxval", argtypes: []ssatype{}, rettype: stValueMasked, immfmt: fmtslot, priority: prioParse, bc: opauxval},

	// find a struct field by name relative
	// to a previously-computed base pointer;
	// arguments are: (base, prevV, prevK, wantedK)
	sdot2: {text: "dot2", argtypes: []ssatype{stBase, stValue, stBool, stBool}, rettype: stValueMasked, immfmt: fmtother, emit: emitdot2, priority: prioParse},

	// hash and hash-with-seed ops
	shashvalue:  {text: "hashvalue", argtypes: []ssatype{stValue, stBool}, rettype: stHash, immfmt: fmtslot, bc: ophashvalue, priority: prioHash},
	shashvaluep: {text: "hashvalue+", argtypes: []ssatype{stHash, stValue, stBool}, rettype: stHash, immfmt: fmtslotx2hash, bc: ophashvalueplus, priority: prioHash},

	shashmember: {text: "hashmember", argtypes: []ssatype{stHash, stBool}, rettype: stBool, immfmt: fmtother, bc: ophashmember, emit: emithashmember},
	shashlookup: {text: "hashlookup", argtypes: []ssatype{stHash, stBool}, rettype: stValue | stBool, immfmt: fmtother, bc: ophashlookup, emit: emithashlookup},

	sliteral: {text: "literal", rettype: stValue, immfmt: fmtother, emit: emitconst}, // yields <value>.kinit

	// store value m, v, k, $slot
	sstorev:    {text: "store.z", rettype: stMem, argtypes: []ssatype{stMem, stValue, stBool}, immfmt: fmtslot, emit: emitstorev, priority: prioMem},
	sloadv:     {text: "load.z", rettype: stValueMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, bc: oploadzerov, priority: prioParse},
	sloadvperm: {text: "load.perm.z", rettype: stValueMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, bc: oploadpermzerov, priority: prioParse},

	sloadlist:  {text: "loadlist.z", rettype: stListMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, priority: prioParse},
	sstorelist: {text: "storelist.z", rettype: stMem, argtypes: []ssatype{stMem, stList, stBool}, immfmt: fmtother, emit: emitstores, priority: prioMem},

	// these tuple-construction ops
	// simply combine a set of separate instructions
	// into a single arugment value;
	// note that they are all compiled similarly
	// (they just load the arguments into the appropriate
	// input registers)
	smsk: {text: "msk", rettype: stMem | stScalar | stBool, argtypes: []ssatype{stMem, stScalar, stBool}, emit: emittuple2regs},
	sbhk: {text: "bhk", rettype: stBase | stHash | stBool, argtypes: []ssatype{stBase, stHash, stBool}, emit: emittuple2regs},
	sbk:  {text: "bk", rettype: stBaseMasked, argtypes: []ssatype{stBase, stBool}, emit: emittuple2regs},
	smk:  {text: "mk", rettype: stMem | stBool, argtypes: []ssatype{stMem, stBool}, emit: emittuple2regs},

	// identity ops; these ops just return their input
	// associated with a different not-missing mask
	// (generally they do not lead to any code being emitted)
	sfloatk: {text: "floatk", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, emit: emittuple2regs},
	svk:     {text: "vk", rettype: stValue, argtypes: []ssatype{stValue, stBool}, emit: emittuple2regs},

	sblendv:     {text: "blendv", rettype: stValue, argtypes: []ssatype{stValue, stValue, stBool}, bc: opblendv, emit: emitblendv, disjunctive: true},
	sblendint:   {text: "blendint", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opblendnum, emit: emitblends, disjunctive: true},
	sblendstr:   {text: "blendstr", rettype: stString, argtypes: []ssatype{stString, stString, stBool}, bc: opblendslice, emit: emitblends, disjunctive: true},
	sblendfloat: {text: "blendfloat", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opblendnum, emit: emitblends, disjunctive: true},

	// compare timestamp against immediate
	sltconsttm: {text: "ltconsttm", argtypes: time1Args, rettype: stBool, immfmt: fmtother, bc: optimelt, emit: emitcmptm, inverse: sgtconsttm},
	sgtconsttm: {text: "gtconsttm", argtypes: time1Args, rettype: stBool, immfmt: fmtother, bc: optimegt, emit: emitcmptm, inverse: sltconsttm},
	stmextract: {text: "tmextract", argtypes: time1Args, rettype: stInt, immfmt: fmtother, bc: optmextract, emit: emittmwithconst},

	sbroadcastf: {text: "broadcast.f", rettype: stFloat, argtypes: []ssatype{}, immfmt: fmtf64, bc: opbroadcastimmf},
	sbroadcasti: {text: "broadcast.i", rettype: stInt, argtypes: []ssatype{}, immfmt: fmti64, bc: opbroadcastimmi},
	sabsf:       {text: "abs.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opabsf},
	sabsi:       {text: "abs.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opabsi},
	snegf:       {text: "neg.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opnegf},
	snegi:       {text: "neg.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opnegi},
	ssignf:      {text: "sign.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opsignf},
	ssigni:      {text: "sign.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opsigni},
	ssquaref:    {text: "square.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opsquaref},
	ssquarei:    {text: "square.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opsquarei},
	sbitnoti:    {text: "bitnot.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opbitnoti},
	sbitcounti:  {text: "bitcount.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opbitcounti},
	sroundf:     {text: "round.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oproundf},
	sroundevenf: {text: "roundeven.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oproundevenf},
	struncf:     {text: "trunc.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: optruncf},
	sfloorf:     {text: "floor.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opfloorf},
	sceilf:      {text: "ceil.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opceilf},
	sroundi:     {text: "round.i", rettype: stInt, argtypes: []ssatype{stFloat, stBool}, bc: opfproundd},
	ssqrtf:      {text: "sqrt.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opsqrtf},
	scbrtf:      {text: "cbrt.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opcbrtf},
	sexpf:       {text: "exp.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opexpf},
	sexpm1f:     {text: "expm1.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opexpm1f},
	sexp2f:      {text: "exp2.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opexp2f},
	sexp10f:     {text: "exp10.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opexp10f},
	slnf:        {text: "ln.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oplnf},
	sln1pf:      {text: "ln1p.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opln1pf},
	slog2f:      {text: "log2.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oplog2f},
	slog10f:     {text: "log10.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: oplog10f},
	ssinf:       {text: "sin.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opsinf},
	scosf:       {text: "cos.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opcosf},
	stanf:       {text: "tan.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: optanf},
	sasinf:      {text: "asin.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opasinf},
	sacosf:      {text: "acos.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opacosf},
	satanf:      {text: "atan.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, bc: opatanf},
	satan2f:     {text: "atan2.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opatan2f, emit: emitBinaryALUOp},

	saddf:         {text: "add.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opaddf, bcrev: opaddf, emit: emitBinaryALUOp},
	saddi:         {text: "add.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opaddi, bcrev: opaddi, emit: emitBinaryALUOp},
	saddimmf:      {text: "add.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opaddimmf, bcrev: opaddimmf},
	saddimmi:      {text: "add.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opaddimmi, bcrev: opaddimmi},
	ssubf:         {text: "sub.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opsubf, bcrev: oprsubf, emit: emitBinaryALUOp},
	ssubi:         {text: "sub.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsubi, bcrev: oprsubi, emit: emitBinaryALUOp},
	ssubimmf:      {text: "sub.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opsubimmf, bcrev: oprsubimmf},
	ssubimmi:      {text: "sub.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsubimmi, bcrev: oprsubimmi},
	srsubimmf:     {text: "rsub.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprsubimmf, bcrev: opsubimmf},
	srsubimmi:     {text: "rsub.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprsubimmi, bcrev: opsubimmi},
	smulf:         {text: "mul.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmulf, bcrev: opmulf, emit: emitBinaryALUOp},
	smuli:         {text: "mul.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmuli, bcrev: opmuli, emit: emitBinaryALUOp},
	smulimmf:      {text: "mul.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmulimmf, bcrev: opmulimmf},
	smulimmi:      {text: "mul.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmulimmi, bcrev: opmulimmi},
	sdivf:         {text: "div.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opdivf, bcrev: oprdivf, emit: emitBinaryALUOp},
	sdivi:         {text: "div.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opdivi, bcrev: oprdivi, emit: emitBinaryALUOp},
	sdivimmf:      {text: "div.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opdivimmf, bcrev: oprdivimmf},
	sdivimmi:      {text: "div.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opdivimmi, bcrev: oprdivimmi},
	srdivimmf:     {text: "rdiv.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprdivimmf, bcrev: opdivimmf},
	srdivimmi:     {text: "rdiv.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprdivimmi, bcrev: opdivimmi},
	smodf:         {text: "mod.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmodf, bcrev: oprmodf, emit: emitBinaryALUOp},
	smodi:         {text: "mod.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmodi, bcrev: oprmodi, emit: emitBinaryALUOp},
	smodimmf:      {text: "mod.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmodimmf, bcrev: oprmodimmf},
	smodimmi:      {text: "mod.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmodimmi, bcrev: oprmodimmi},
	srmodimmf:     {text: "rmod.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprmodimmf, bcrev: opmodimmf},
	srmodimmi:     {text: "rmod.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprmodimmi, bcrev: opmodimmi},
	sminvaluef:    {text: "minvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opminvaluef, bcrev: opminvaluef, emit: emitBinaryALUOp},
	sminvaluei:    {text: "minvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opminvaluei, bcrev: opminvaluei, emit: emitBinaryALUOp},
	sminvalueimmf: {text: "minvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opminvalueimmf, bcrev: opminvalueimmf},
	sminvalueimmi: {text: "minvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opminvalueimmi, bcrev: opminvalueimmi},
	smaxvaluef:    {text: "maxvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmaxvaluef, bcrev: opmaxvaluef, emit: emitBinaryALUOp},
	smaxvaluei:    {text: "maxvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmaxvaluei, bcrev: opmaxvaluei, emit: emitBinaryALUOp},
	smaxvalueimmf: {text: "maxvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmaxvalueimmf, bcrev: opmaxvalueimmf},
	smaxvalueimmi: {text: "maxvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmaxvalueimmi, bcrev: opmaxvalueimmi},
	sandi:         {text: "and.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opandi, bcrev: opandi, emit: emitBinaryALUOp},
	sandimmi:      {text: "and.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opandimmi, bcrev: opandimmi},
	sori:          {text: "or.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opori, bcrev: opori, emit: emitBinaryALUOp},
	sorimmi:       {text: "or.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oporimmi, bcrev: oporimmi},
	sxori:         {text: "xor.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opxori, bcrev: opxori, emit: emitBinaryALUOp},
	sxorimmi:      {text: "xor.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opxorimmi, bcrev: opxorimmi},
	sslli:         {text: "sll.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opslli, emit: emitBinaryALUOp},
	ssllimmi:      {text: "sll.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsllimmi},
	ssrai:         {text: "sra.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsrai, emit: emitBinaryALUOp},
	ssraimmi:      {text: "sra.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsraimmi},
	ssrli:         {text: "srl.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsrli, emit: emitBinaryALUOp},
	ssrlimmi:      {text: "srl.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsrlimmi},

	shypotf: {text: "hypot.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: ophypotf, bcrev: ophypotf, emit: emitBinaryALUOp},
	spowf:   {text: "pow.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: oppowf, emit: emitBinaryALUOp},

	swidthbucketf: {text: "widthbucket.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opwidthbucketf, emit: emitauto2},
	swidthbucketi: {text: "widthbucket.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stInt, stInt, stBool}, bc: opwidthbucketi, emit: emitauto2},

	saggandk:  {text: "aggand.k", rettype: stMem, argtypes: []ssatype{stMem, stBool, stBool}, immfmt: fmtaggslot, bc: opaggandk, priority: prioMem, emit: emitAggK},
	saggork:   {text: "aggor.k", rettype: stMem, argtypes: []ssatype{stMem, stBool, stBool}, immfmt: fmtaggslot, bc: opaggork, priority: prioMem, emit: emitAggK},
	saggsumf:  {text: "aggsum.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggsumf, priority: prioMem},
	saggsumi:  {text: "aggsum.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggsumi, priority: prioMem},
	saggavgf:  {text: "aggavg.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggsumf, priority: prioMem},
	saggavgi:  {text: "aggavg.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggsumi, priority: prioMem},
	saggminf:  {text: "aggmin.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggminf, priority: prioMem},
	saggmini:  {text: "aggmin.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggmini, priority: prioMem},
	saggmaxf:  {text: "aggmax.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtaggslot, bc: opaggmaxf, priority: prioMem},
	saggmaxi:  {text: "aggmax.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggmaxi, priority: prioMem},
	saggmints: {text: "aggmin.ts", rettype: stMem, argtypes: []ssatype{stMem, stTimeInt, stBool}, immfmt: fmtaggslot, bc: opaggmini, priority: prioMem},
	saggmaxts: {text: "aggmax.ts", rettype: stMem, argtypes: []ssatype{stMem, stTimeInt, stBool}, immfmt: fmtaggslot, bc: opaggmaxi, priority: prioMem},
	saggandi:  {text: "aggand.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggandi, priority: prioMem},
	saggori:   {text: "aggor.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggori, priority: prioMem},
	saggxori:  {text: "aggxor.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtaggslot, bc: opaggxori, priority: prioMem},
	saggcount: {text: "aggcount", rettype: stMem, argtypes: []ssatype{stMem, stBool}, immfmt: fmtaggslot, bc: opaggcount, priority: prioMem + 1},

	// compute hash aggregate bucket location; encoded immediate will be input hash slot to use
	saggbucket: {text: "aggbucket", argtypes: []ssatype{stMem, stHash, stBool}, rettype: stBucket, immfmt: fmtslot, bc: opaggbucket},

	// hash aggregate bucket ops (count, min, max, sum, ...)
	saggslotandk:  {text: "aggslotand.k", argtypes: []ssatype{stMem, stBucket, stBool, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotandk, priority: prioMem, emit: emitSlotAggK},
	saggslotork:   {text: "aggslotor.k", argtypes: []ssatype{stMem, stBucket, stBool, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotork, priority: prioMem, emit: emitSlotAggK},
	saggslotsumf:  {text: "aggslotadd.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotaddf, priority: prioMem},
	saggslotsumi:  {text: "aggslotadd.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotaddi, priority: prioMem},
	saggslotavgf:  {text: "aggslotavg.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotavgf, priority: prioMem},
	saggslotavgi:  {text: "aggslotavg.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotavgi, priority: prioMem},
	saggslotminf:  {text: "aggslotmin.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotminf, priority: prioMem},
	saggslotmini:  {text: "aggslotmin.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxf:  {text: "aggslotmax.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotmaxf, priority: prioMem},
	saggslotmaxi:  {text: "aggslotmax.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotmints: {text: "aggslotmin.ts", argtypes: []ssatype{stMem, stBucket, stTimeInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxts: {text: "aggslotmax.ts", argtypes: []ssatype{stMem, stBucket, stTimeInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotandi:  {text: "aggslotand.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotandi, priority: prioMem},
	saggslotori:   {text: "aggslotor.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotori, priority: prioMem},
	saggslotxori:  {text: "aggslotxor.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotxori, priority: prioMem},
	saggslotcount: {text: "aggslotcount", argtypes: []ssatype{stMem, stBucket, stBool}, rettype: stMem, immfmt: fmtaggslot, bc: opaggslotcount, priority: prioMem},

	// boxing ops
	//
	// turn two masks into TRUE/FALSE/MISSING according to 3VL
	sboxmask:   {text: "boxmask", argtypes: []ssatype{stBool, stBool}, rettype: stValue, emit: emitboxmask},
	sboxint:    {text: "boxint", argtypes: []ssatype{stInt, stBool}, rettype: stValue, bc: opboxint},
	sboxfloat:  {text: "boxfloat", argtypes: []ssatype{stFloat, stBool}, rettype: stValue, bc: opboxfloat},
	sboxstring: {text: "boxstring", argtypes: []ssatype{stString, stBool}, rettype: stValue, bc: opboxstring},

	// timestamp operations
	sbroadcastts:            {text: "broadcast.ts", rettype: stTimeInt, argtypes: []ssatype{}, immfmt: fmti64, bc: opbroadcastimmi},
	sunboxtime:              {text: "unboxtime", argtypes: []ssatype{stTime, stBool}, rettype: stTimeInt, bc: opunboxts},
	sdateadd:                {text: "dateadd", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opaddi, emit: emitauto2},
	sdateaddimm:             {text: "dateadd.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, immfmt: fmti64, bc: opaddimmi},
	sdateaddmulimm:          {text: "dateaddmul.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, immfmt: fmti64, bc: opaddmulimmi, emit: emitauto2},
	sdateaddmonth:           {text: "dateaddmonth", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opdateaddmonth, emit: emitauto2},
	sdateaddmonthimm:        {text: "dateaddmonth.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, immfmt: fmti64, bc: opdateaddmonthimm},
	sdateaddquarter:         {text: "dateaddquarter", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opdateaddquarter, emit: emitauto2},
	sdateaddyear:            {text: "dateaddyear", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opdateaddyear, emit: emitauto2},
	sdatediffmicro:          {text: "datediffmicro", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: oprsubi, emit: emitauto2},
	sdatediffparam:          {text: "datediffparam", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffparam, immfmt: fmti64, emit: emitauto2},
	sdatediffmonth:          {text: "datediffmonth", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffmonthyear, emit: emitDateDiffMQY},
	sdatediffquarter:        {text: "datediffquarter", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffmonthyear, emit: emitDateDiffMQY},
	sdatediffyear:           {text: "datediffyear", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffmonthyear, emit: emitDateDiffMQY},
	sdateextractmicrosecond: {text: "dateextractmicrosecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmicrosecond, emit: emitauto2},
	sdateextractmillisecond: {text: "dateextractmillisecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmillisecond, emit: emitauto2},
	sdateextractsecond:      {text: "dateextractsecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractsecond, emit: emitauto2},
	sdateextractminute:      {text: "dateextractminute", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractminute, emit: emitauto2},
	sdateextracthour:        {text: "dateextracthour", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextracthour, emit: emitauto2},
	sdateextractday:         {text: "dateextractday", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractday, emit: emitauto2},
	sdateextractdow:         {text: "dateextractdow", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractdow, emit: emitauto2},
	sdateextractdoy:         {text: "dateextractdoy", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractdoy, emit: emitauto2},
	sdateextractmonth:       {text: "dateextractmonth", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmonth, emit: emitauto2},
	sdateextractquarter:     {text: "dateextractquarter", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractquarter, emit: emitauto2},
	sdateextractyear:        {text: "dateextractyear", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractyear, emit: emitauto2},
	sdatetounixepoch:        {text: "datetounixepoch", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetounixepoch, emit: emitauto2},
	sdatetounixmicro:        {text: "datetounixmicro", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetounixepoch, emit: emitdatecasttoint},
	sdatetruncmillisecond:   {text: "datetruncmillisecond", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncmillisecond},
	sdatetruncsecond:        {text: "datetruncsecond", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncsecond},
	sdatetruncminute:        {text: "datetruncminute", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncminute},
	sdatetrunchour:          {text: "datetrunchour", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetrunchour},
	sdatetruncday:           {text: "datetruncday", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncday},
	sdatetruncdow:           {text: "datetruncdow", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, immfmt: fmti64, bc: opdatetruncdow, emit: emitauto2},
	sdatetruncmonth:         {text: "datetruncmonth", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncmonth},
	sdatetruncquarter:       {text: "datetruncquarter", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncquarter},
	sdatetruncyear:          {text: "datetruncyear", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncyear},
	stimebucketts:           {text: "timebucket.ts", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: optimebucketts, emit: emitauto2},
	sboxts:                  {text: "boxts", argtypes: []ssatype{stTimeInt, stBool}, rettype: stValue, bc: opboxts},

	sboxlist:       {text: "boxlist", rettype: stValue, argtypes: []ssatype{stList, stBool}},
	smakelist:      {text: "makelist", rettype: stValueMasked, argtypes: []ssatype{stBool}, vaArgs: []ssatype{stValue, stBool}, bc: opmakelist, emit: emitMakeList},
	smakestruct:    {text: "makestruct", rettype: stValueMasked, argtypes: []ssatype{stBool}, vaArgs: []ssatype{stString, stValue, stBool}, bc: opmakestruct, emit: emitMakeStruct},
	smakestructkey: {text: "makestructkey", rettype: stString, immfmt: fmtother, emit: emitNone},

	// GEO functions
	sgeohash:      {text: "geohash", rettype: stString, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeohash, emit: emitauto2},
	sgeohashimm:   {text: "geohash.imm", rettype: stString, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeohashimm, emit: emitauto2},
	sgeotilex:     {text: "geotilex", rettype: stInt, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotilex, emit: emitauto2},
	sgeotiley:     {text: "geotiley", rettype: stInt, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotiley, emit: emitauto2},
	sgeotilees:    {text: "geotilees", rettype: stString, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeotilees, emit: emitauto2},
	sgeotileesimm: {text: "geotilees.imm", rettype: stString, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeotileesimm, emit: emitauto2},
	sgeodistance:  {text: "geodistance", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opgeodistance, emit: emitauto2},

	schecktag:   {text: "checktag", argtypes: []ssatype{stValue, stBool}, rettype: stValueMasked, immfmt: fmtother, emit: emitchecktag},
	stypebits:   {text: "typebits", argtypes: []ssatype{stValue, stBool}, rettype: stInt, bc: optypebits},
	sobjectsize: {text: "objectsize", argtypes: []ssatype{stValue, stBool}, rettype: stIntMasked, bc: opobjectsize},

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

type value struct {
	id   int
	op   ssaop
	args []*value

	// if this value has non-standard
	// not-missing-ness, then that is set here
	notMissing *value

	imm interface{}
}

type hashcode [6]uint64

type sympair struct {
	sym ion.Symbol
	val string
}

type prog struct {
	values []*value // all values in program
	ret    *value   // value actually yielded by program

	// used to find common expressions
	dict  []string            // common strings
	exprs map[hashcode]*value // common expressions

	reserved []stackslot

	// symbolized records whether
	// the program has been symbolized
	symbolized bool
	// literals records whether
	// there are complex literals in
	// the bytecode that may reference
	// the input symbol table
	literals bool
	// if symbolized is set,
	// resolved is the list of symbols
	// and their IDs when symbolization
	// happens; we use this to determine
	// staleness
	resolved []sympair
}

// ReserveSlot reserves a stack slot
// for use by the program (independently
// of any register saving and reloading
// that has to be performed).
func (p *prog) reserveSlot(slot stackslot) {
	for i := range p.reserved {
		if p.reserved[i] == slot {
			return
		}
	}
	p.reserved = append(p.reserved, slot)
}

// dictionary strings must be padded to
// multiples of 4 bytes so that we never
// cross a page boundary when reading past
// the end of the string
func pad(x string) string {
	buf := []byte(x)
	for len(buf)&3 != 0 {
		buf = append(buf, 0)
	}
	return string(buf)[:len(x)]
}

// used to produce a consistent bit pattern
// for hashing common subexpressions
func (p *prog) tobits(imm interface{}) uint64 {
	switch v := imm.(type) {
	case stackslot:
		panic("Stack slot must be converted to int when storing it in value.imm")
	case float64:
		return math.Float64bits(v)
	case float32:
		return math.Float64bits(float64(v))
	case int64:
		return uint64(v)
	case uint64:
		return v
	case uint16:
		return uint64(v)
	case uint:
		return uint64(v)
	case int:
		return uint64(v)
	case ion.Symbol:
		return uint64(v)
	case aggregateslot:
		return uint64(v)
	case string:
		for i := range p.dict {
			if v == p.dict[i] {
				return uint64(i)
			}
		}
		p.dict = append(p.dict, pad(v))
		return uint64(len(p.dict) - 1)
	case bool:
		if v {
			return 1
		}
		return 0
	case date.Time:
		var buf ion.Buffer
		buf.WriteTime(v)
		str := string(buf.Bytes()[1:])
		for i := range p.dict {
			if p.dict[i] == str {
				return uint64(i)
			}
		}
		p.dict = append(p.dict, pad(str))
		return uint64(len(p.dict) - 1)
	default:
		panic(fmt.Sprintf("invalid immediate %+v with type %T", imm, imm))
	}
}

// overwrite a value with a message
// indicating why it is invalid
func (v *value) errf(f string, args ...interface{}) {
	v.op = sinvalid
	v.args = nil
	v.imm = fmt.Sprintf(f, args...)
}

func (v *value) setimm(imm interface{}) {
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, v.op)
		return
	}

	v.imm = imm
}

func (p *prog) errorf(f string, args ...interface{}) *value {
	v := p.val()
	v.errf(f, args...)
	return v
}

func (p *prog) begin() {
	p.exprs = make(map[hashcode]*value)
	p.values = nil
	p.ret = nil
	p.dict = nil

	// op 0 is always 'init'
	v := p.val()
	v.op = sinit
	// op 1 is always 'undef'
	v = p.val()
	v.op = sundef

	p.symbolized = false
	p.resolved = p.resolved[:0]
}

func (p *prog) undef() *value {
	return p.values[1]
}

func (p *prog) val() *value {
	v := new(value)
	p.values = append(p.values, v)
	v.id = len(p.values) - 1
	return v
}

func (p *prog) errf(s string, args ...any) *value {
	v := p.val()
	v.errf(s, args...)
	return v
}

func (s ssaop) String() string {
	return ssainfo[s].text
}

func (v *value) checkarg(arg *value, idx int) {
	if v.op == sinvalid {
		return
	}

	in := ssainfo[arg.op].rettype
	argtype := ssainfo[v.op].argType(idx)

	if arg.op == sinvalid {
		v.op = sinvalid
		v.args = nil
		v.imm = arg.imm
		return
	}
	// the type of this assignment should be unambiguous;
	// we can specify multiple possible return and argument
	// types for a given return value and argument position,
	// but only one of them should be valid
	//
	// (the only case where this doesn't hold is if the
	// input argument is an undef value)
	want := argtype
	if bits.OnesCount(uint(in&want)) != 1 && arg.op != sundef {
		v.errf("ambiguous assignment type (%s=%s as argument of type %s to %s of type %s)",
			arg.Name(), arg, in.String(), v.op, want.String())
	}
}

func (p *prog) validLanes() *value {
	return p.values[0]
}

// helper for simplification rules
func (p *prog) choose(yes bool) *value {
	if yes {
		return p.values[0]
	}
	return p.ssa0(skfalse)
}

func (p *prog) ssa0(op ssaop) *value {
	var hc hashcode
	hc[0] = uint64(op)
	if v := p.exprs[hc]; v != nil {
		return v
	}
	v := p.val()
	v.op = op
	p.exprs[hc] = v
	return v
}

func (p *prog) ssa0imm(op ssaop, imm interface{}) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = p.tobits(imm)

	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.setimm(imm)
	v.args = []*value{}

	if v.op != sinvalid {
		p.exprs[hc] = v
	}

	return v
}

func (p *prog) ssa1imm(op ssaop, arg *value, imm interface{}) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg.id)
	hc[2] = p.tobits(imm)
	if v := p.exprs[hc]; v != nil {
		return v
	}
	v := p.val()
	v.op = op
	v.setimm(imm)
	v.args = []*value{arg}
	v.checkarg(arg, 0)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa2imm(op ssaop, arg0, arg1 *value, imm interface{}) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = p.tobits(imm)
	if v := p.exprs[hc]; v != nil {
		return v
	}
	v := p.val()
	v.op = op
	v.setimm(imm)
	v.args = []*value{arg0, arg1}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa2(op ssaop, arg0 *value, arg1 *value) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.args = []*value{arg0, arg1}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa3(op ssaop, arg0, arg1, arg2 *value) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = uint64(arg2.id)
	if v := p.exprs[hc]; v != nil {
		return v
	}
	v := p.val()
	v.op = op
	v.args = []*value{arg0, arg1, arg2}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa3imm(op ssaop, arg0, arg1, arg2 *value, imm interface{}) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = uint64(arg2.id)
	hc[4] = p.tobits(imm)
	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.setimm(imm)
	v.args = []*value{arg0, arg1, arg2}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	return v
}

func (p *prog) ssa4(op ssaop, arg0, arg1, arg2, arg3 *value) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = uint64(arg2.id)
	hc[4] = uint64(arg3.id)
	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.args = []*value{arg0, arg1, arg2, arg3}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	v.checkarg(arg3, 3)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa4imm(op ssaop, arg0, arg1, arg2, arg3 *value, imm interface{}) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = uint64(arg2.id)
	hc[4] = uint64(arg3.id)
	hc[5] = p.tobits(imm)
	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.setimm(imm)
	v.args = []*value{arg0, arg1, arg2, arg3}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	v.checkarg(arg3, 3)
	return v
}

func (p *prog) ssa5(op ssaop, arg0, arg1, arg2, arg3, arg4 *value) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg0.id)
	hc[2] = uint64(arg1.id)
	hc[3] = uint64(arg2.id)
	hc[4] = uint64(arg3.id)
	hc[5] = uint64(arg4.id)
	if v := p.exprs[hc]; v != nil {
		return v
	}

	v := p.val()
	v.op = op
	v.args = []*value{arg0, arg1, arg2, arg3, arg4}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	v.checkarg(arg3, 3)
	v.checkarg(arg4, 4)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

// overwrite a value with new opcode + args, etc.
func (p *prog) setssa(v *value, op ssaop, imm interface{}, args ...*value) *value {
	v.notMissing = nil
	v.op = op
	v.args = shrink(v.args, len(args))
	copy(v.args, args)
	for i := range args {
		v.checkarg(args[i], i)
	}
	if imm == nil {
		v.imm = nil
	} else {
		v.setimm(imm)
	}
	return v
}

func (p *prog) ssaimm(op ssaop, imm interface{}, args ...*value) *value {
	v := p.val()
	v.op = op
	v.args = args
	if imm != nil {
		v.setimm(imm)
	}
	for i := range args {
		v.checkarg(args[i], i)
	}
	if v.op == sinvalid {
		panic("invalid op " + v.String())
	}
	return v
}

func (p *prog) ssava(op ssaop, args []*value) *value {
	opInfo := &ssainfo[op]
	baseArgCount := len(opInfo.argtypes)

	v := p.val()
	v.op = op
	v.args = args

	if len(opInfo.vaArgs) == 0 {
		v.errf("%s doesn't support variable arguments", op)
		return v
	}

	if len(args) < baseArgCount {
		v.errf("%s requires at least %d arguments (%d given)", op, baseArgCount, len(args))
		return v
	}

	for i := range args {
		v.checkarg(args[i], i)
	}

	return v
}

func (p *prog) constant(imm interface{}) *value {
	v := p.val()
	v.op = sliteral
	v.imm = imm
	return v
}

// returnValue sets the return value of the program
// as a single value (will be returned in a register)
func (p *prog) returnValue(v *value) {
	p.ret = v
}

// InitMem returns the memory token associated
// with the initial memory state.
func (p *prog) initMem() *value {
	return p.ssa0(sinitmem)
}

// Store stores a value to a stack slot and
// returns the associated memory token.
// The store operation is guaranteed to happen
// after the 'mem' op.
func (p *prog) store(mem *value, v *value, slot stackslot) (*value, error) {
	p.reserveSlot(slot)
	if v.op == skfalse {
		return p.ssa3imm(sstorev, mem, v, p.validLanes(), int(slot)), nil
	}
	switch v.primary() {
	case stValue:
		return p.ssa3imm(sstorev, mem, v, p.mask(v), int(slot)), nil
	default:
		return nil, fmt.Errorf("cannot store value %s", v)
	}
}

func (p *prog) isMissing(v *value) *value {
	return p.not(p.notMissing(v))
}

// notMissing walks logical expressions until
// it finds a terminal true/false value
// or an expression that computes a real return
// value that could be MISSING (i.e. mask=0)
func (p *prog) notMissing(v *value) *value {
	if v.notMissing != nil {
		return v.notMissing
	}
	nonLogical := func(v *value) bool {
		info := ssainfo[v.op].argtypes
		for i := range info {
			if info[i] != stBool {
				return true
			}
		}
		return false
	}
	// non-logical insructions
	// (scalar comparisons, etc.) only operate
	// on non-MISSING lanes, so the mask argument
	// is equivalent to NOT MISSING
	if nonLogical(v) {
		rt := v.ret()
		switch {
		case rt == stBool:
			// this is a comparison; the mask arg
			// is the set of lanes to compare
			// (and therefore NOT MISSING)
			return v.maskarg()
		case rt&stBool != 0:
			// the result is equivalent to NOT MISSING
			return v
		default:
			// arithmetic or other op with no return mask;
			// the mask argument is implicitly the NOT MISSING value
			return p.mask(v)
		}
	}
	switch v.op {
	case skfalse, sinit:
		return v
	case snand:
		return p.and(p.notMissing(v.args[0]), v.args[1])
	case sxor, sxnor:
		// for xor and xnor, the result is only
		// non-missing if both sides of the comparison
		// are non-MISSING values
		return p.and(p.notMissing(v.args[0]), p.notMissing(v.args[1]))
	case sand:
		// we need
		//          | TRUE    | FALSE | MISSING
		//  --------+---------+-------+--------
		//  TRUE    | TRUE    | FALSE | MISSING
		//  FALSE   | FALSE   | FALSE | FALSE
		//  MISSING | MISSING | FALSE | MISSING
		//
		return p.or(v, p.or(
			p.isFalse(v.args[0]),
			p.isFalse(v.args[1]),
		))
	case sor:
		// we need
		//          | TRUE    | FALSE    | MISSING
		//  --------+---------+----------+--------
		//  TRUE    | TRUE    | TRUE     | TRUE
		//  FALSE   | TRUE    | FALSE    | MISSING
		//  MISSING | TRUE    | MISSING  | MISSING
		//
		// so, the NOT MISSING mask is
		//   (A OR B) OR (A IS NOT MISSING AND B IS NOT MISSING)
		return p.or(v, p.and(p.notMissing(v.args[0]), p.notMissing(v.args[1])))
	default:
		m := v.maskarg()
		if m == nil {
			return p.validLanes()
		}
		return p.notMissing(m)
	}
}

func (p *prog) storeList(mem *value, v *value, slot stackslot) *value {
	p.reserveSlot(slot)
	l := p.tolist(v)
	return p.ssa3imm(sstorelist, mem, l, l, int(slot))
}

// LoadList loads a list slice from
// a stack slot and returns the slice and
// a predicate indicating whether the loaded
// value has a non-zero length component
func (p *prog) loadList(mem *value, slot stackslot) *value {
	p.reserveSlot(slot)
	return p.ssa1imm(sloadlist, mem, int(slot))
}

// Loadvalue loads a value from a stack slot
// and returns the value and a predicate
// indicating whether the loaded value
// has a non-zero length component
func (p *prog) loadvalue(mem *value, slot stackslot) *value {
	p.reserveSlot(slot)
	return p.ssa1imm(sloadv, mem, int(slot))
}

// Upvalue loads an upvalue (a value bound by
// an enclosing binding context) from a parent's
// stack slot
func (p *prog) upvalue(mem *value, slot stackslot) *value {
	return p.ssa1imm(sloadvperm, mem, int(slot))
}

// MergeMem merges memory tokens into a memory token.
// (This can be used to create a partial ordering
// constraint for memory operations.)
func (p *prog) mergeMem(args ...*value) *value {
	if len(args) == 1 {
		return args[0]
	}
	v := p.val()
	v.op = smergemem
	v.args = args
	return v
}

// various tuple constructors:
// these just combine a non-mask
// register value (S, V, etc.)
// with a mask value into a single value;
// they don't actually emit any code
// other than the necessary register
// manipulation

// V+K tuple
func (p *prog) vk(v, k *value) *value {
	if v == k {
		return v
	}
	return p.ssa2(svk, v, k)
}

// float+K tuple
func (p *prog) floatk(f, k *value) *value {
	return p.ssa2(sfloatk, f, k)
}

// RowsMasked constructs a (base value, predicate) tuple
func (p *prog) rowsMasked(base *value, pred *value) *value {
	return p.ssa2(sbk, base, pred)
}

// mem+S+K tuple
func (p *prog) msk(mem *value, scalar *value, pred *value) *value {
	return p.ssa3(smsk, mem, scalar, pred)
}

// base+hash+K tuple
func (p *prog) bhk(base *value, hash *value, pred *value) *value {
	return p.ssa3(sbhk, base, hash, pred)
}

// mem+K tuple
func (p *prog) mk(mem *value, pred *value) *value {
	return p.ssa2(smk, mem, pred)
}

// Dot computes <base>.col
func (p *prog) dot(col string, base *value) *value {
	if base != p.values[0] {
		// need to perform a conversion from
		// a value pointer to an interior-of-structure pointer
		base = p.ssa2(stuples, base, base)
	}
	return p.ssa2imm(sdot, base, base, col)
}

func (p *prog) tolist(v *value) *value {
	switch v.ret() {
	case stListMasked, stListAndValueMasked:
		return v
	case stValue, stValueMasked:
		return p.ssa2(stolist, v, p.mask(v))
	default:
		return p.errorf("cannot convert value %s to list", v)
	}
}

func (p *prog) isFalse(v *value) *value {
	switch v.primary() {
	case stBool:
		// need to differentiate between
		// the zero predicate from MISSING
		// and the zero predicate from FALSE
		return p.ssa2(snand, v, p.notMissing(v))
	case stValue:
		return p.ssa2(sisfalse, v, p.mask(v))
	default:
		return p.errorf("bad argument %s to IsFalse", v)
	}
}

func (p *prog) isTrue(v *value) *value {
	switch v.primary() {
	case stBool:
		return v
	case stValue:
		return p.ssa2(sistrue, v, p.mask(v))
	default:
		return p.errorf("bad argument %s to IsTrue", v)
	}
}

func (p *prog) isNotTrue(v *value) *value {
	// we compute predicates as IS TRUE,
	// so IS NOT TRUE is simply the complement
	return p.not(v)
}

func (p *prog) isNotFalse(v *value) *value {
	return p.or(p.isTrue(v), p.isMissing(v))
}

// Index evaluates v[i] for a constant index.
// The returned value is v[i] if evaluated as
// a value, or v[i+1:] when evaluated as a list.
//
// FIXME: make the multiple-return-value behavior
// here less confusing.
// NOTE: array access is linear- rather than
// constant-time, so accessing large offsets
// can be very slow.
func (p *prog) index(v *value, i int) *value {
	l := p.tolist(v)
	for i >= 0 {
		// NOTE: CSE will take care of
		// ensuring that the access of
		// list[n] occurs before list[n+1]
		// since computing list[n+1] implicitly
		// computes list[n]!
		l = p.ssa2(ssplit, l, l)
		i--
	}
	return l
}

func (s ssatype) ordnum() int {
	switch s {
	case stBool:
		return 0
	case stValue:
		return 1
	case stInt:
		return 2
	case stFloat:
		return 3
	case stString:
		return 4
	case stTime:
		return 5
	default:
		return 6
	}
}

// Equals computes 'left == right'
func (p *prog) equals(left, right *value) *value {
	if (left.op == sliteral) && (right.op == sliteral) {
		// TODO: int64(1) == float64(1.0) ??
		return p.constant(left.imm == right.imm)
	}
	// make ordering deterministic:
	// if there is a constant, put it on the right-hand-side;
	// otherwise pick an ordering for input argtypes and enforce it
	if left.op == sliteral || left.primary().ordnum() > right.primary().ordnum() {
		left, right = right, left
	}
	switch left.primary() {
	case stBool:
		// (bool) = (bool)
		// is an xnor op, but additionally
		// we have to check that the values
		// are not MISSING
		if right.op == sliteral {
			b, ok := right.imm.(bool)
			if !ok {
				// left = <not a bool> -> nope
				return p.ssa0(skfalse)
			}
			if b {
				// left = TRUE -> left mask
				return left
			}
			// left = FALSE -> !left and left is not missing
			return p.nand(left, p.notMissing(left))
		}
		if right.ret()&stBool == 0 {
			return p.errorf("cannot compare bool(%s) and other(%s)", left, right)
		}
		// mask = value -> mask = (istrue value)
		if right.primary() == stValue {
			right = p.isTrue(right)
		}
		allok := p.and(p.notMissing(left), p.notMissing(right))
		return p.and(p.xnor(left, right), allok)
	case stValue:
		if right.op == sliteral {
			if _, ok := right.imm.(string); ok {
				// only need this for string comparison
				left = p.unsymbolized(left)
			}
			return p.ssa2imm(sequalconst, left, p.mask(left), right.imm)
		}
		switch right.primary() {
		case stValue:
			left = p.unsymbolized(left)
			right = p.unsymbolized(right)
			return p.ssa3(sequalv, left, right, p.ssa2(sand, p.mask(left), p.mask(right)))
		case stInt:
			lefti, k := p.coerceInt(left)
			return p.ssa3(scmpeqi, lefti, right, p.and(k, p.mask(right)))
		case stFloat:
			leftf, k := p.coercefp(left)
			return p.ssa3(scmpeqf, leftf, right, p.and(k, p.mask(right)))
		case stString:
			leftstr := p.toStr(left)
			return p.ssa3(seqstr, leftstr, right, p.and(p.mask(leftstr), p.mask(right)))
		case stTime:
			lefttm := p.toTime(left)
			return p.ssa3(seqtime, lefttm, right, p.and(p.mask(lefttm), p.mask(right)))
		default:
			return p.errorf("cannot compare value %s and other %s", left, right)
		}
	case stInt:
		if right.op == sliteral {
			return p.ssa2imm(scmpeqimmi, left, p.mask(left), right.imm)
		}
		if right.primary() == stInt {
			return p.ssa3(scmpeqi, left, right, p.and(p.mask(left), p.mask(right)))
		}
		// falthrough to floating-point comparison
		left = p.ssa2(scvtitof, left, p.mask(left))
		fallthrough
	case stFloat:
		if right.op == sliteral {
			return p.ssa2imm(scmpeqimmf, left, p.mask(left), right.imm)
		}
		switch right.primary() {
		case stInt:
			right = p.ssa2(scvtitof, right, p.mask(right))
			fallthrough
		case stFloat:
			return p.ssa3(scmpeqf, left, right, p.and(p.mask(left), p.mask(right)))
		default:
			return p.ssa0(skfalse) // FALSE/MISSING
		}
	case stString:
		if right.op == sliteral {
			return p.ssa2imm(sStrCmpEqCs, left, left, right.imm)
		}
		switch right.primary() {
		case stString:
			return p.ssa3(seqstr, left, right, p.and(p.mask(left), p.mask(right)))
		default:
			return p.ssa0(skfalse) // FALSE/MISSING
		}
	case stTime:
		switch right.primary() {
		case stTime:
			return p.ssa3(seqtime, left, right, p.and(p.mask(left), p.mask(right)))
		}
		fallthrough
	default:
		return p.errorf("cannot compare %s and %s", left, right)
	}
}

// EqualStr computes equality between strings
func (p *prog) equalStr(left, right *value, caseSensitive bool) *value {
	if (left.op == sliteral) && (right.op == sliteral) {
		if caseSensitive {
			return p.constant(left.imm == right.imm)
		}
		leftStr, _ := left.imm.(string)
		rightStr, _ := right.imm.(string)
		return p.constant(strings.EqualFold(leftStr, rightStr))
	}

	if left.op == sliteral { // swap literal to the right
		left, right = right, left
	}

	if right.op == sliteral { // ideally, we can compare against an immediate
		needle, _ := right.imm.(string)
		if !caseSensitive && !stringext.HasCaseSensitiveChar(needle) {
			// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
			caseSensitive = true
		}
		if caseSensitive {
			enc := p.constant(stringext.EncodeEqualStringCS(needle)).imm
			return p.ssa2imm(sStrCmpEqCs, left, left, enc)
		}
		if stringext.HasNtnString(needle) { // needle has non-trivial normalization
			enc := p.constant(stringext.EncodeEqualStringUTF8CI(needle)).imm
			return p.ssa2imm(sStrCmpEqUTF8Ci, left, left, enc)
		}
		enc := p.constant(stringext.EncodeEqualStringCI(needle)).imm
		return p.ssa2imm(sStrCmpEqCi, left, left, enc)
	}
	v := p.val()
	v.errf("not yet supported comparison %v", ssainfo[left.op].rettype)
	return v
}

// CharLength returns the number of unicode code-points in v
func (p *prog) charLength(v *value) *value {
	v = p.toStr(v)
	return p.ssa2(sCharLength, v, v)
}

// Substring returns a substring at the provided startIndex with length
func (p *prog) substring(v, substrOffset, substrLength *value) *value {
	offsetInt, offsetMask := p.coerceInt(substrOffset)
	lengthInt, lengthMask := p.coerceInt(substrLength)
	mask := p.and(v, p.and(offsetMask, lengthMask))
	return p.ssa4(sSubStr, v, offsetInt, lengthInt, mask)
}

// SplitPart splits string on delimiter and returns the field index. Field indexes start with 1.
func (p *prog) splitPart(v *value, delimiter byte, index *value) *value {
	delimiterStr := string(delimiter)
	indexInt, indexMask := p.coerceInt(index)
	mask := p.and(v, indexMask)
	return p.ssa3imm(sSplitPart, v, indexInt, mask, delimiterStr)
}

// is v an ion null value?
func (p *prog) isnull(v *value) *value {
	if v.primary() != stValue {
		return p.ssa0(skfalse)
	}
	return p.ssa2(sisnull, v, p.mask(v))
}

// is v distinct from null?
// (i.e. non-missing and non-null?)
func (p *prog) isnonnull(v *value) *value {
	if v.primary() != stValue {
		return p.validLanes() // TRUE
	}
	return p.ssa2(sisnonnull, v, p.mask(v))
}

func isBoolImmediate(imm interface{}) bool {
	switch imm.(type) {
	case bool:
		return true
	default:
		return false
	}
}

func isIntImmediate(imm interface{}) bool {
	switch v := imm.(type) {
	case int, int64, uint, uint64:
		return true
	case float64:
		return float64(int64(v)) == v
	default:
		return false
	}
}

func isFloatImmediate(imm interface{}) bool {
	switch imm.(type) {
	case float64:
		return true
	default:
		return false
	}
}

func isNumericImmediate(imm interface{}) bool {
	return isFloatImmediate(imm) || isIntImmediate(imm)
}

func isStringImmediate(imm interface{}) bool {
	switch imm.(type) {
	case string:
		return true
	default:
		return false
	}
}

func isTimestampImmediate(imm interface{}) bool {
	switch imm.(type) {
	case date.Time:
		return true
	default:
		return false
	}
}

func tobool(imm interface{}) bool {
	switch v := imm.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case uint64:
		return v != 0
	case uint:
		return v != 0
	case float64:
		return v != 0
	case float32:
		return v != 0
	default:
		panic("invalid immediate for tobool()")
	}
}

func tof64(imm interface{}) float64 {
	switch i := imm.(type) {
	case bool:
		if i {
			return float64(1)
		}
		return float64(0)
	case int:
		return float64(i)
	case int64:
		return float64(i)
	case uint64:
		return float64(i)
	case uint:
		return float64(i)
	case float64:
		return i
	case float32:
		return float64(i)
	default:
		panic("invalid immediate for tof64()")
	}
}

func toi64(imm interface{}) uint64 {
	switch i := imm.(type) {
	case bool:
		if i {
			return 1
		}
		return 0
	case int:
		return uint64(i)
	case int64:
		return uint64(i)
	case uint:
		return uint64(i)
	case uint64:
		return i
	case float64:
		return uint64(int64(i))
	case float32:
		return uint64(int64(i))
	default:
		panic("invalid immediate for toi64()")
	}
}

// coerce a value to boolean
func (p *prog) coerceBool(arg *value) (*value, *value) {
	if arg.op == sliteral {
		imm := toi64(arg.imm)
		if imm != 0 {
			imm = 0xFFFF
		}
		return p.ssa0imm(sbroadcastk, imm), p.validLanes()
	}

	if arg.primary() == stBool {
		return arg, p.notMissing(arg)
	}

	if arg.primary() == stValue {
		k := p.mask(arg)
		i := p.ssa2(sunboxktoi, arg, k)
		return p.ssa2(scvtitok, i, p.mask(i)), p.mask(i)
	}

	err := p.val()
	err.errf("cannot convert %s to BOOL", arg)
	return err, err
}

// coerce a value to floating point,
// taking care to promote integers appropriately
func (p *prog) coercefp(v *value) (*value, *value) {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcastf, v.imm), p.validLanes()
	}
	switch v.primary() {
	case stFloat:
		return v, p.mask(v)
	case stInt:
		ret := p.ssa2(scvtitof, v, p.mask(v))
		return ret, p.mask(v)
	case stValue:
		ret := p.ssa2(sunboxcoercef64, v, p.mask(v))
		return ret, p.mask(ret)
	default:
		err := p.val()
		err.errf("cannot convert %s to a floating point", v)
		return err, err
	}
}

// coerceInt coerces a value to integer
func (p *prog) coerceInt(v *value) (*value, *value) {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcasti, v.imm), p.validLanes()
	}
	switch v.primary() {
	case stInt:
		return v, p.mask(v)
	case stFloat:
		return p.ssa2(scvtftoi, v, p.mask(v)), p.mask(v)
	case stValue:
		ret := p.ssa3(stoint, p.undef(), v, p.mask(v))
		return ret, ret
	default:
		err := p.errf("cannot convert %s to an integer", v)
		return err, err
	}
}

// for a current FP value 'into', a value argument 'arg',
// and a predicate 'when', parse arg and use the predicate
// when to blend the floating-point-converted results
// into 'into'
func (p *prog) blendv2fp(into, arg, when *value) (*value, *value) {
	if arg.op == sliteral {
		return p.ssa0imm(sbroadcastf, arg.imm), p.validLanes()
	}
	easy := p.ssa3(stofloat, into, arg, when)
	intv := p.ssa3(stoint, easy, arg, when)
	conv := p.ssa2(scvtitof, intv, intv)
	return conv, p.or(easy, conv)
}

func (p *prog) toint(v *value) *value {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcasti, v.imm)
	}
	switch v.primary() {
	case stInt:
		return v
	case stValue:
		return p.ssa3(stoint, p.undef(), v, p.mask(v))
	case stFloat:
		// so, ordinarily we shouldn't hit this,
		// but if we promoted a math expression
		// to floating-point, then we have to
		// convert back here (and hope that we
		// didn't lose too much precision...)
		return p.ssa2(scvtftoi, v, p.mask(v))
	default:
		return p.errf("cannot convert %s to int", v.String())
	}
}

func (p *prog) toStr(str *value) *value {
	switch str.primary() {
	case stString:
		return str // no need to parse
	case stValue:
		str = p.unsymbolized(str)
		return p.ssa2(stostr, str, p.mask(str))
	default:
		v := p.val()
		v.errf("internal error: unsupported value %v", str.String())
		return v
	}
}

func (p *prog) concat(args ...*value) *value {
	if len(args) == 0 {
		panic("CONCAT cannot be empty")
	}

	k := p.mask(args[0])
	for i := 1; i < len(args); i++ {
		k = p.and(k, p.mask(args[i]))
	}

	var v [4]*value
	vIndex := 0

	for i := 0; i < len(args); i++ {
		v[vIndex] = p.toStr(args[i])
		vIndex++

		if vIndex >= 4 {
			vIndex = 1
			v[0] = p.ssa5(sconcatstr4, v[0], v[1], v[2], v[3], k)
		}
	}

	switch vIndex {
	case 1:
		return v[0]
	case 2:
		return p.ssa3(sconcatstr2, v[0], v[1], k)
	case 3:
		return p.ssa4(sconcatstr3, v[0], v[1], v[2], k)
	default:
		panic(fmt.Sprintf("invalid number of remaining items (%d) in Concat()", vIndex))
	}
}

func (p *prog) makeList(args ...*value) *value {
	var values []*value = make([]*value, 0, len(args)*2+1)

	values = append(values, p.validLanes())
	for _, arg := range args {
		if arg.primary() != stValue {
			panic("MakeList arguments must be values, and values only")
		}
		values = append(values, arg, p.mask(arg))
	}
	return p.ssava(smakelist, values)
}

func (p *prog) makeStruct(args []*value) *value {
	return p.ssava(smakestruct, args)
}

type trimType uint8

const (
	trimLeading  = 1
	trimTrailing = 2
	trimBoth     = trimLeading | trimTrailing
)

func trimtype(op expr.BuiltinOp) trimType {
	switch op {
	case expr.Ltrim:
		return trimLeading
	case expr.Rtrim:
		return trimTrailing
	case expr.Trim:
		return trimBoth
	}

	return trimBoth
}

// TrimWhitespace trim chars: ' ', '\t', '\n', '\v', '\f', '\r'
func (p *prog) trimWhitespace(str *value, trimtype trimType) *value {
	str = p.toStr(str)
	if trimtype&trimLeading != 0 {
		str = p.ssa2(sStrTrimWsLeft, str, p.mask(str))
	}
	if trimtype&trimTrailing != 0 {
		str = p.ssa2(sStrTrimWsRight, str, p.mask(str))
	}
	return str
}

// TrimSpace trim char: ' '
func (p *prog) trimSpace(str *value, trimtype trimType) *value {
	return p.trimChar(str, " ", trimtype)
}

// TrimChar trim provided chars
func (p *prog) trimChar(str *value, chars string, trimtype trimType) *value {
	str = p.toStr(str)
	numberOfChars := len(chars)
	if numberOfChars == 0 {
		return str
	}
	if numberOfChars > 4 {
		v := p.val()
		v.errf(fmt.Sprintf("only 4 chars are supported in TrimChar, %v char(s) provided in %v", numberOfChars, chars))
		return v
	}
	charsByteArray := make([]byte, 4)
	for i := 0; i < 4; i++ {
		if i < numberOfChars {
			charsByteArray[i] = chars[i]
		} else {
			charsByteArray[i] = chars[numberOfChars-1]
		}
	}
	preparedChars := string(charsByteArray)
	if trimtype&trimLeading != 0 {
		str = p.ssa2imm(sStrTrimCharLeft, str, p.mask(str), preparedChars)
	}
	if trimtype&trimTrailing != 0 {
		str = p.ssa2imm(sStrTrimCharRight, str, p.mask(str), preparedChars)
	}
	return str
}

// HasPrefix returns true when str contains the provided prefix; false otherwise
func (p *prog) hasPrefix(str *value, prefix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if prefix == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(prefix) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := p.constant(stringext.EncodeContainsPrefixCS(prefix)).imm
		return p.ssa2imm(sStrContainsPrefixCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(prefix) { // prefix has non-trivial normalization
		enc := p.constant(stringext.EncodeContainsPrefixUTF8CI(prefix)).imm
		return p.ssa2imm(sStrContainsPrefixUTF8Ci, str, p.mask(str), enc)
	}
	enc := p.constant(stringext.EncodeContainsPrefixCI(prefix)).imm
	return p.ssa2imm(sStrContainsPrefixCi, str, p.mask(str), enc)
}

// HasSuffix returns true when str contains the provided suffix; false otherwise
func (p *prog) hasSuffix(str *value, suffix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if suffix == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(suffix) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := p.constant(stringext.EncodeContainsSuffixCS(suffix)).imm
		return p.ssa2imm(sStrContainsSuffixCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(suffix) { // suffix has non-trivial normalization
		enc := p.constant(stringext.EncodeContainsSuffixUTF8CI(suffix)).imm
		return p.ssa2imm(sStrContainsSuffixUTF8Ci, str, p.mask(str), enc)
	}
	enc := p.constant(stringext.EncodeContainsSuffixCI(suffix)).imm
	return p.ssa2imm(sStrContainsSuffixCi, str, p.mask(str), enc)
}

// Contains returns whether the given value
// is a string containing 'needle' as a substring.
// (The return value is always 'true' if 'str' is
// a string and 'needle' is the empty string.)
func (p *prog) contains(str *value, needle string, caseSensitive bool) *value {
	// n.b. the 'contains' code doesn't actually
	// handle the empty string; just return whether
	// this value is a string
	str = p.toStr(str)
	if needle == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := p.constant(stringext.EncodeContainsSubstrCS(needle)).imm
		return p.ssa2imm(sStrContainsSubstrCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(needle) { // needle has non-trivial normalization
		enc := p.constant(stringext.EncodeContainsSubstrUTF8CI(needle)).imm
		return p.ssa2imm(sStrContainsSubstrUTF8Ci, str, p.mask(str), enc)
	}
	enc := p.constant(stringext.EncodeContainsSubstrCI(needle)).imm
	return p.ssa2imm(sStrContainsSubstrCi, str, p.mask(str), enc)
}

// IsSubnetOfIP4 returns whether the give value is an IPv4 address between (and including) min and max
func (p *prog) isSubnetOfIP4(str *value, min, max [4]byte) *value {
	str = p.toStr(str)
	return p.ssa2imm(sIsSubnetOfIP4, str, p.mask(str), stringext.ToBCD(&min, &max))
}

// SkipCharLeft skips a variable number of UTF-8 code-points from the left side of a string
func (p *prog) skipCharLeft(str, nChars *value) *value {
	str = p.toStr(str)
	return p.ssa3(sStrSkipNCharLeft, str, nChars, p.and(p.mask(str), p.mask(nChars)))
}

// SkipCharRight skips a variable number of UTF-8 code-points from the right side of a string
func (p *prog) skipCharRight(str, nChars *value) *value {
	str = p.toStr(str)
	return p.ssa3(sStrSkipNCharRight, str, nChars, p.and(p.mask(str), p.mask(nChars)))
}

// SkipCharLeftConst skips a constant number of UTF-8 code-points from the left side of a string
func (p *prog) skipCharLeftConst(str *value, nChars int) *value {
	str = p.toStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharLeft, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceInt(p.constant(int64(nChars)))
		return p.ssa3(sStrSkipNCharLeft, str, nCharsInt, p.and(p.mask(str), nCharsMask))
	}
}

// SkipCharRightConst skips a constant number of UTF-8 code-points from the right side of a string
func (p *prog) skipCharRightConst(str *value, nChars int) *value {
	str = p.toStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharRight, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceInt(p.constant(int64(nChars)))
		return p.ssa3(sStrSkipNCharRight, str, nCharsInt, p.and(p.mask(str), nCharsMask))
	}
}

// Like matches 'str' as a string against
// a SQL 'LIKE' pattern
//
// The '%' character will match zero or more
// unicode points, and the '_' character will
// match exactly one unicode point.
func (p *prog) like(str *value, expr string, escape rune, caseSensitive bool) *value {
	return p.likeInternal(str, expr, '_', '%', escape, caseSensitive)
}

// Glob matches 'str' as a string against
// a simple glob pattern.
//
// The '*' character will match zero or more
// unicode points, and the '?' character will
// match exactly one unicode point.
func (p *prog) glob(str *value, expr string, caseSensitive bool) *value {
	return p.likeInternal(str, expr, '?', '*', stringext.NoEscape, caseSensitive)
}

// likeInternal matches a 'LIKE' pattern using any single character 'wc' and
// any string of zero or more characters 'ks', and given the provided escape rune
func (p *prog) likeInternal(str *value, expr string, wc, ks, escape rune, caseSensitive bool) *value {

	// encodes the appropriate immediate for a pattern-matching operation
	patMatch := func(pattern string) *value {
		if len(pattern) == 0 {
			return str
		}
		patternRune := []rune(pattern)

		// we can't pass a wildcard as the first
		// or last segment to the assembly code;
		// that needs to be handled at a higher level
		if (stringext.IndexRuneEscape(patternRune, wc, escape) == 0) ||
			(stringext.LastIndexRuneEscape(patternRune, wc, escape) == len(patternRune)-1) {
			panic("internal error: bad pattern-matching string")
		}
		// remove the escape character
		pattern = strings.ReplaceAll(pattern, string(escape), "")
		patternRune = []rune(pattern)
		wildcard := make([]bool, len(patternRune))
		hasWildCard := false

		for i := 0; i < len(patternRune); i++ {
			if patternRune[i] == wc {
				wildcard[i] = true
				hasWildCard = true
			}
		}
		if !caseSensitive && !stringext.HasCaseSensitiveChar(pattern) {
			// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
			caseSensitive = true
		}
		if !hasWildCard {
			return p.contains(str, pattern, caseSensitive)
		}
		if caseSensitive {
			enc := p.constant(stringext.EncodeContainsPatternCS(pattern, wildcard)).imm
			return p.ssa2imm(sStrContainsPatternCs, str, p.mask(str), enc)
		}
		if stringext.HasNtnString(pattern) { // pattern has non-trivial normalization
			enc := p.constant(stringext.EncodeContainsPatternUTF8CI(pattern, wildcard)).imm
			return p.ssa2imm(sStrContainsPatternUTF8Ci, str, p.mask(str), enc)
		}
		enc := p.constant(stringext.EncodeContainsPatternCI(pattern, wildcard)).imm
		return p.ssa2imm(sStrContainsPatternCi, str, p.mask(str), enc)
	}

	// matches '<start>*<middle0>...*<middleN>*<end>'
	pattern := func(startStr string, middle []string, endStr string) *value {
		start := []rune(startStr)
		end := []rune(endStr)

		// match pattern anchored at start;
		// match forwards by repeatedly trimming literal prefixes or single characters with wildcard
		for len(start) > 0 {
			// skip all leading code-points with wildcard
			nRunesToSkip := 0
			for (len(start) > 0) && (start[0] == wc) {
				start = start[1:] // skip the first code-point
				nRunesToSkip++
			}
			str = p.skipCharLeftConst(str, nRunesToSkip)

			// if anything remaining, match with prefix
			if len(start) > 0 {
				qi := stringext.IndexRuneEscape(start, wc, escape)
				if qi == -1 {
					qi = len(start)
				}
				prefix := string(start[:qi])
				if escape != stringext.NoEscape {
					prefix = strings.ReplaceAll(prefix, string(escape), "")
				}
				str = p.hasPrefix(str, prefix, caseSensitive)
				start = start[qi:]
			}
		}
		// match pattern anchored at end;
		// we match this pattern backwards by trimming matching suffixes off of the string or single characters with '?'
		for len(end) > 0 {
			// skip all trailing code-points with '?'
			nCharsToSkip := 0
			for (len(end) > 0) && (stringext.LastIndexRuneEscape(end, wc, escape) == len(end)-1) {
				end = end[:len(end)-1] // skip the last code-point
				nCharsToSkip++
			}
			str = p.skipCharRightConst(str, nCharsToSkip)

			// if anything remaining, match with suffix
			if len(end) > 0 {
				var seg []rune
				si := stringext.LastIndexRuneEscape(end, wc, escape)
				if si == -1 {
					seg = end
					end = make([]rune, 0)
				} else {
					seg = end[si+1:]
					end = end[:si+1]
				}
				suffix := string(seg)
				if escape != stringext.NoEscape {
					suffix = strings.ReplaceAll(suffix, string(escape), "")
				}
				str = p.hasSuffix(str, suffix, caseSensitive)
				end = end[:si+1]
			}
		}

		for i := range middle {
			// any '?' at the beginning of an un-anchored match simply becomes a 'skipchar'
			mid := []rune(middle[i])

			nCharsToSkip := 0
			for len(mid) > 0 && mid[0] == wc {
				mid = mid[1:]
				nCharsToSkip++
			}
			str = p.skipCharLeftConst(str, nCharsToSkip)

			// similarly, and '?' at the end of an unanchored match becomes a 'skipchar' after the inner match
			nCharsToChomp := 0
			for stringext.LastIndexRuneEscape(mid, wc, escape) == len(mid)-1 {
				mid = mid[:len(mid)-1]
				nCharsToChomp++
			}

			// do the difficult matching
			if len(mid) > 0 {
				str = patMatch(string(mid))
			}
			str = p.skipCharLeftConst(str, nCharsToChomp)
		}
		return str
	}

	str = p.toStr(str)
	if !caseSensitive { // Bytecode for case-insensitive comparing expects that needles and patterns are in normalized (UPPER) case
		expr = stringext.NormalizeString(expr)
	}
	exprRune := []rune(expr)

	lefti := stringext.IndexRuneEscape(exprRune, ks, escape)
	if lefti == -1 {
		return pattern(expr, nil, "")
	}
	left := exprRune[:lefti]
	exprRune = exprRune[lefti+1:]

	var middle []string

	for len(exprRune) > 0 {
		runeIdx := stringext.IndexRuneEscape(exprRune, ks, escape)
		if runeIdx == -1 {
			return pattern(string(left), middle, string(exprRune))
		}
		middlePat := exprRune[:runeIdx]
		exprRune = exprRune[runeIdx+1:]
		if len(middlePat) > 0 {
			middle = append(middle, string(middlePat))
		}
	}
	return pattern(string(left), middle, "")
}

// RegexMatch matches 'str' as a string against regex
func (p *prog) regexMatch(str *value, store *regexp2.DFAStore) (*value, error) {
	if cpu.X86.HasAVX512VBMI && !store.HasUnicodeEdge() {
		hasRLZA := store.HasRLZA()
		hasWildcard, wildcardRange := store.HasUnicodeWildcard()
		if dsTiny, err := regexp2.NewDsTiny(store); err == nil {
			if ds, valid := dsTiny.Data(6, hasWildcard, wildcardRange); valid {
				if hasRLZA {
					return p.ssa2imm(sDfaT6Z, str, p.mask(str), p.constant(string(ds)).imm), nil
				}
				return p.ssa2imm(sDfaT6, str, p.mask(str), p.constant(string(ds)).imm), nil
			}
			if ds, valid := dsTiny.Data(7, hasWildcard, wildcardRange); valid {
				if hasRLZA {
					return p.ssa2imm(sDfaT7Z, str, p.mask(str), p.constant(string(ds)).imm), nil
				}
				return p.ssa2imm(sDfaT7, str, p.mask(str), p.constant(string(ds)).imm), nil
			}
			if ds, valid := dsTiny.Data(8, hasWildcard, wildcardRange); valid {
				if hasRLZA {
					return p.ssa2imm(sDfaT8Z, str, p.mask(str), p.constant(string(ds)).imm), nil
				}
				return p.ssa2imm(sDfaT8, str, p.mask(str), p.constant(string(ds)).imm), nil
			}
		}
	}
	// NOTE: when you end up here, the DFA could not be handled with Tiny implementation. Continue to try Large.
	if dsLarge, err := regexp2.NewDsLarge(store); err == nil {
		return p.ssa2imm(sDfaLZ, str, p.mask(str), p.constant(string(dsLarge.Data())).imm), nil
	}
	return nil, fmt.Errorf("internal error: generation of data-structure for Large failed")
}

// EqualsFuzzy does a fuzzy string equality of 'str' as a string against needle.
// Equality is computed with Damerau–Levenshtein distance estimation based on three
// character horizon. If the distance exceeds the provided threshold, the match is
// rejected; that is, str and needle are considered unequal.
func (p *prog) equalsFuzzy(str *value, needle string, threshold *value, ascii bool) *value {
	thresholdInt, thresholdMask := p.coerceInt(threshold)
	mask := p.and(str, thresholdMask)
	if ascii {
		needleEnc := p.constant(stringext.EncodeFuzzyNeedleASCII(needle)).imm
		return p.ssa3imm(sCmpFuzzyA3, str, thresholdInt, mask, needleEnc)
	}
	needleEnc := p.constant(stringext.EncodeFuzzyNeedleUnicode(needle)).imm
	return p.ssa3imm(sCmpFuzzyUnicodeA3, str, thresholdInt, mask, needleEnc)
}

// ContainsFuzzy does a fuzzy string contains of needle in 'str'.
// Equality is computed with Damerau–Levenshtein distance estimation based on three
// character horizon. If the distance exceeds the provided threshold, the match is
// rejected; that is, str and needle are considered unequal.
func (p *prog) containsFuzzy(str *value, needle string, threshold *value, ascii bool) *value {
	thresholdInt, thresholdMask := p.coerceInt(threshold)
	mask := p.and(str, thresholdMask)
	if ascii {
		needleEnc := p.constant(stringext.EncodeFuzzyNeedleASCII(needle)).imm
		return p.ssa3imm(sHasSubstrFuzzyA3, str, thresholdInt, mask, needleEnc)
	}
	needleEnc := p.constant(stringext.EncodeFuzzyNeedleUnicode(needle)).imm
	return p.ssa3imm(sHasSubstrFuzzyUnicodeA3, str, thresholdInt, mask, needleEnc)
}

type compareOp uint8

const (
	comparelt compareOp = iota
	comparele
	comparegt
	comparege
)

type compareOpInfo struct {
	cmpk    ssaop
	cmpimmk ssaop
	cmpi    ssaop
	cmpimmi ssaop
	cmpf    ssaop
	cmpimmf ssaop
	cmps    ssaop
	cmpts   ssaop
}

var compareOpReverseTable = [...]compareOp{
	comparelt: comparegt,
	comparele: comparege,
	comparegt: comparelt,
	comparege: comparele,
}

var compareOpInfoTable = [...]compareOpInfo{
	comparelt: {cmpk: scmpltk, cmpimmk: scmpltimmk, cmpi: scmplti, cmpimmi: scmpltimmi, cmpf: scmpltf, cmpimmf: scmpltimmf, cmps: scmpltstr, cmpts: scmpltts},
	comparele: {cmpk: scmplek, cmpimmk: scmpleimmk, cmpi: scmplei, cmpimmi: scmpleimmi, cmpf: scmplef, cmpimmf: scmpleimmf, cmps: scmplestr, cmpts: scmplets},
	comparegt: {cmpk: scmpgtk, cmpimmk: scmpgtimmk, cmpi: scmpgti, cmpimmi: scmpgtimmi, cmpf: scmpgtf, cmpimmf: scmpgtimmf, cmps: scmpgtstr, cmpts: scmpgtts},
	comparege: {cmpk: scmpgek, cmpimmk: scmpgeimmk, cmpi: scmpgei, cmpimmi: scmpgeimmi, cmpf: scmpgef, cmpimmf: scmpgeimmf, cmps: scmpgestr, cmpts: scmpgets},
}

// compareValueWith computes 'left <op> right' when left is guaranteed to be a value
//
// This function is only designed to be used by `compare()`
func (p *prog) compareValueWith(left, right *value, op compareOp) *value {
	info := compareOpInfoTable[op]

	// Compare value vs scalar/immediate
	if right.op == sliteral {
		imm := right.imm
		if isBoolImmediate(imm) {
			cmpv := p.ssa2imm(scmpvimmk, left, p.mask(left), tobool(imm))
			return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
		}
		if isIntImmediate(imm) {
			cmpv := p.ssa2imm(scmpvimmi64, left, p.mask(left), toi64(imm))
			return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
		}
		if isFloatImmediate(imm) {
			cmpv := p.ssa2imm(scmpvimmf64, left, p.mask(left), tof64(imm))
			return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
		}
		if isStringImmediate(imm) {
			left = p.toStr(left)
			right = p.toStr(right)
			return p.ssa3(info.cmps, left, right, p.and(p.mask(left), p.mask(right)))
		}
		if isTimestampImmediate(imm) {
			lhs, lhk := p.coerceTimestamp(left)
			rhs, rhk := p.coerceTimestamp(right)
			return p.ssa3(info.cmpts, lhs, rhs, p.and(lhk, rhk))
		}
	}

	rType := right.primary()
	if rType == stBool {
		cmpv := p.ssa3(scmpvk, left, right, p.and(p.mask(left), p.mask(right)))
		return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stInt {
		cmpv := p.ssa3(scmpvi64, left, right, p.and(p.mask(left), p.mask(right)))
		return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stFloat {
		cmpv := p.ssa3(scmpvf64, left, right, p.and(p.mask(left), p.mask(right)))
		return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stString {
		left = p.toStr(left)
		return p.ssa3(info.cmps, left, right, p.and(p.mask(left), p.mask(right)))
	}
	if rType == stTimeInt || rType == stTime {
		lhs, lhk := p.coerceTimestamp(left)
		rhs, rhk := p.coerceTimestamp(right)
		return p.ssa3(info.cmpts, lhs, rhs, p.and(lhk, p.mask(rhk)))
	}

	return nil
}

// compare computes 'left <op> right'
func (p *prog) compare(left, right *value, op compareOp) *value {
	info := compareOpInfoTable[op]
	revInfo := compareOpInfoTable[compareOpReverseTable[op]]

	lLiteral := left.op == sliteral
	rLiteral := right.op == sliteral

	lType := left.primary()
	rType := right.primary()

	// compare value vs non-value (scalar/immediate)
	if lType == stValue {
		v := p.compareValueWith(left, right, op)
		if v != nil {
			return v
		}
	}

	// compare non-value (scalar/immediate) vs value
	if rType == stValue {
		v := p.compareValueWith(right, left, compareOpReverseTable[op])
		if v != nil {
			return v
		}
	}

	// compare bool vs immediate
	if lType == stBool && rLiteral {
		if isBoolImmediate(right.imm) {
			return p.ssa2imm(info.cmpimmk, left, p.mask(left), tobool(right.imm))
		}
		return p.ssa0(skfalse)
	}

	// compare immediate vs bool
	if lLiteral && rType == stBool {
		if isBoolImmediate(left.imm) {
			return p.ssa2imm(revInfo.cmpimmk, right, p.mask(right), tobool(left.imm))
		}
		return p.ssa0(skfalse)
	}

	// compare bool vs bool
	if lType == stBool && rType == stBool {
		return p.ssa3(info.cmpk, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// compare int/float vs immediate
	if lType == stInt && rLiteral {
		if isIntImmediate(right.imm) {
			return p.ssa2imm(info.cmpimmi, left, p.mask(left), toi64(right.imm))
		}

		lhs, lhk := p.coercefp(left)
		return p.ssa2imm(info.cmpimmf, lhs, lhk, tof64(right.imm))
	}

	if lType == stFloat && rLiteral {
		return p.ssa2imm(info.cmpimmf, left, p.mask(left), tof64(right.imm))
	}

	// compare immediate vs int/float
	if lLiteral && rType == stInt {
		if isIntImmediate(left.imm) {
			return p.ssa2imm(revInfo.cmpimmi, right, p.mask(right), toi64(left.imm))
		}

		rhs, rhk := p.coercefp(right)
		return p.ssa2imm(info.cmpimmf, rhs, rhk, tof64(left.imm))
	}

	if lLiteral && rType == stFloat {
		return p.ssa2imm(revInfo.cmpimmf, right, p.mask(right), tof64(left.imm))
	}

	// compare int/float vs int/float (if the types are mixed, int is coerced to float)
	if lType == stInt && rType == stInt {
		return p.ssa3(info.cmpi, left, right, p.and(p.mask(left), p.mask(right)))
	}

	if lType == stInt && rType == stFloat {
		lhs, lhk := p.coercefp(left)
		return p.ssa3(info.cmpi, lhs, right, p.and(lhk, p.mask(right)))
	}

	if lType == stFloat && rType == stInt {
		rhs, rhk := p.coercefp(right)
		return p.ssa3(info.cmpi, left, rhs, p.and(p.mask(left), rhk))
	}

	if lType == stFloat && rType == stFloat {
		return p.ssa3(info.cmpf, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// compare timestamp vs timestamp
	lTimeCompat := lType == stTimeInt || lType == stTime || (lLiteral && isTimestampImmediate(left.imm))
	rTimeCompat := rType == stTimeInt || rType == stTime || (rLiteral && isTimestampImmediate(right.imm))

	if lTimeCompat && rTimeCompat {
		lhs, lhk := p.coerceTimestamp(left)
		rhs, rhk := p.coerceTimestamp(right)
		return p.ssa3(info.cmpts, lhs, rhs, p.and(p.mask(lhk), p.mask(rhk)))
	}

	// Compare string vs string
	lStringCompat := lType == stString || (lLiteral && isStringImmediate(left.imm))
	rStringCompat := rType == stString || (rLiteral && isStringImmediate(right.imm))

	if lStringCompat && rStringCompat {
		left = p.toStr(left)
		right = p.toStr(right)
		return p.ssa3(info.cmps, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// Compare value vs value
	if lType == stValue && rType == stValue {
		mask := p.and(p.mask(left), p.mask(right))
		cmpv := p.ssa3(scmpv, left, right, mask)
		return p.ssa2imm(info.cmpimmi, cmpv, p.mask(cmpv), int64(0))
	}

	// Uncomparable...
	return p.ssa0(skfalse)
}

// Less computes 'left < right'
func (p *prog) less(left, right *value) *value {
	return p.compare(left, right, comparelt)
}

// LessEqual computes 'left <= right'
func (p *prog) lessEqual(left, right *value) *value {
	return p.compare(left, right, comparele)
}

// Greater computes 'left > right'
func (p *prog) greater(left, right *value) *value {
	return p.compare(left, right, comparegt)
}

// GreaterEqual computes 'left >= right'
func (p *prog) greaterEqual(left, right *value) *value {
	return p.compare(left, right, comparege)
}

// And computes 'left AND right'
func (p *prog) and(left, right *value) *value {
	if left == right {
		return left
	}
	if left.op == sinit {
		return right
	}
	if right.op == sinit {
		return left
	}
	return p.ssa2(sand, left, right)
}

// (^left & right)
func (p *prog) nand(left, right *value) *value {
	// !false & x -> x
	if left.op == skfalse {
		return right
	}
	// !true & x -> false
	if left.op == sinit {
		return p.ssa0(skfalse)
	}
	// !x & false -> false
	if right.op == skfalse {
		return p.ssa0(skfalse)
	}
	// !x & x -> false
	if left == right {
		return p.ssa0(skfalse)
	}
	// !(!x & y) & y -> x & y
	//
	// usually we hit this with Not(Not(x)),
	// as it would show up as (nand (nand x true) true)
	if left.op == snand && left.args[1] == right {
		return p.and(left, right)
	}
	return p.ssa2(snand, left, right)
}

// xor computes 'left != right' for boolean values
func (p *prog) xor(left, right *value) *value {
	if left == right {
		return p.ssa0(skfalse)
	}
	// true ^ x -> !x
	if left.op == sinit {
		return p.nand(right, left)
	}
	if right.op == sinit {
		return p.nand(left, right)
	}
	// false ^ x -> x
	if left.op == skfalse {
		return right
	}
	if right.op == skfalse {
		return left
	}
	return p.ssa2(sxor, left, right)
}

// xnor computes 'left = right' for boolean values
func (p *prog) xnor(left, right *value) *value {
	if left == right {
		return p.validLanes()
	}
	return p.ssa2(sxnor, left, right)
}

// Or computes 'left OR right'
func (p *prog) or(left, right *value) *value {
	// true || x => true
	if left.op == sinit {
		return left
	}
	// x || true => true
	if right.op == sinit {
		return right
	}
	return p.ssa2(sor, left, right)
}

// Not computes 'NOT v'
func (p *prog) not(v *value) *value {
	// we model this as (^v AND TRUE)
	// so that we can narrow the mask further
	// if we determine that we don't care
	// about the truthiness under some circumstances
	//
	// we just emit a 'not' op if this doesn't get optimized
	if v.op == sistrue {
		return p.ssa2(sisfalse, v.args[0], v.args[1])
	} else if v.op == sisfalse {
		return p.ssa2(sistrue, v.args[0], v.args[1])
	}
	return p.nand(v, p.validLanes())
}

func (p *prog) makeBroadcastOp(child *value) *value {
	if child.op != sliteral {
		panic(fmt.Sprintf("BroadcastOp requires a literal value, not %s", child.op.String()))
	}

	return p.ssa0imm(sbroadcastf, child.imm)
}

func (p *prog) broadcastI64(child *value) *value {
	if child.op != sliteral {
		panic(fmt.Sprintf("broadcastI64() requires a literal value, not %s", child.op.String()))
	}

	return p.ssa0imm(sbroadcasti, child.imm)
}

func isIntValue(v *value) bool {
	if v.op == sliteral {
		return isIntImmediate(v.imm)
	}

	return v.primary() == stInt
}

// Unary arithmetic operators and functions
func (p *prog) makeUnaryArithmeticOp(regOpF, regOpI ssaop, child *value) *value {
	if (isIntValue(child) && child.op != sliteral) || regOpF == sinvalid {
		s, k := p.coerceInt(child)
		return p.ssa2(regOpI, s, k)
	}

	return p.makeUnaryArithmeticOpFp(regOpF, child)
}

func (p *prog) makeUnaryArithmeticOpInt(op ssaop, child *value) *value {
	s, k := p.coerceInt(child)
	return p.ssa2(op, s, k)
}

func (p *prog) makeUnaryArithmeticOpFp(op ssaop, child *value) *value {
	if child.op == sliteral {
		child = p.makeBroadcastOp(child)
	}

	s, k := p.coercefp(child)
	return p.ssa2(op, s, k)
}

func (p *prog) neg(child *value) *value {
	return p.makeUnaryArithmeticOp(snegf, snegi, child)
}

func (p *prog) abs(child *value) *value {
	return p.makeUnaryArithmeticOp(sabsf, sabsi, child)
}

func (p *prog) sign(child *value) *value {
	return p.makeUnaryArithmeticOp(ssignf, ssigni, child)
}

func (p *prog) bitNot(child *value) *value {
	return p.makeUnaryArithmeticOpInt(sbitnoti, child)
}

func (p *prog) bitCount(child *value) *value {
	return p.makeUnaryArithmeticOpInt(sbitcounti, child)
}

func (p *prog) round(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sroundf, child)
}

func (p *prog) roundEven(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sroundevenf, child)
}

func (p *prog) trunc(child *value) *value {
	return p.makeUnaryArithmeticOpFp(struncf, child)
}

func (p *prog) floor(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sfloorf, child)
}

func (p *prog) ceil(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sceilf, child)
}

func (p *prog) sqrt(child *value) *value {
	return p.makeUnaryArithmeticOpFp(ssqrtf, child)
}

func (p *prog) cbrt(child *value) *value {
	return p.makeUnaryArithmeticOpFp(scbrtf, child)
}

func (p *prog) exp(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexpf, child)
}

func (p *prog) expM1(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexpm1f, child)
}

func (p *prog) exp2(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexp2f, child)
}

func (p *prog) exp10(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexp10f, child)
}

func (p *prog) ln(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slnf, child)
}

func (p *prog) ln1p(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sln1pf, child)
}

func (p *prog) log2(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slog2f, child)
}

func (p *prog) log10(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slog10f, child)
}

func (p *prog) sin(child *value) *value {
	return p.makeUnaryArithmeticOpFp(ssinf, child)
}

func (p *prog) cos(child *value) *value {
	return p.makeUnaryArithmeticOpFp(scosf, child)
}

func (p *prog) tan(child *value) *value {
	return p.makeUnaryArithmeticOpFp(stanf, child)
}

func (p *prog) asin(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sasinf, child)
}

func (p *prog) acos(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sacosf, child)
}

func (p *prog) atan(child *value) *value {
	return p.makeUnaryArithmeticOpFp(satanf, child)
}

// Binary arithmetic operators and functions
func (p *prog) makeBinaryArithmeticOpImm(regOpF, regOpI ssaop, v *value, imm interface{}) *value {
	if isIntValue(v) && isIntImmediate(imm) {
		s, k := p.coerceInt(v)
		i64Imm := toi64(imm)
		return p.ssa2imm(regOpI, s, k, i64Imm)
	}

	s, k := p.coercefp(v)
	f64Imm := tof64(imm)
	return p.ssa2imm(regOpF, s, k, f64Imm)
}

func (p *prog) makeBinaryArithmeticOp(regOpF, regOpI, immOpF, immOpI, reverseImmOpF, reverseImmOpI ssaop, left *value, right *value) *value {
	if left.op == sliteral && right.op == sliteral {
		right = p.makeBroadcastOp(right)
	}

	if right.op == sliteral {
		return p.makeBinaryArithmeticOpImm(immOpF, immOpI, left, right.imm)
	}

	if left.op == sliteral {
		return p.makeBinaryArithmeticOpImm(reverseImmOpF, reverseImmOpI, right, left.imm)
	}

	if isIntValue(left) && isIntValue(right) {
		return p.ssa3(regOpI, left, right, p.and(p.mask(left), p.mask(right)))
	}

	lhs, lhk := p.coercefp(left)
	rhs, rhk := p.coercefp(right)
	return p.ssa3(regOpF, lhs, rhs, p.and(lhk, rhk))
}

func (p *prog) makeBinaryArithmeticOpFp(op ssaop, left *value, right *value) *value {
	if left.op == sliteral {
		left = p.makeBroadcastOp(left)
	}

	if right.op == sliteral {
		right = p.makeBroadcastOp(right)
	}

	lhs, lhk := p.coercefp(left)
	rhs, rhk := p.coercefp(right)
	return p.ssa3(op, lhs, rhs, p.and(lhk, rhk))
}

func (p *prog) add(left, right *value) *value {
	if left == right {
		return p.makeBinaryArithmeticOpImm(smulimmf, smulimmi, left, 2)
	}
	return p.makeBinaryArithmeticOp(saddf, saddi, saddimmf, saddimmi, saddimmf, saddimmi, left, right)
}

func (p *prog) sub(left, right *value) *value {
	if left == right {
		return p.makeBinaryArithmeticOpImm(smulimmf, smulimmi, left, 0)
	}
	return p.makeBinaryArithmeticOp(ssubf, ssubi, ssubimmf, ssubimmi, srsubimmf, srsubimmi, left, right)
}

func (p *prog) mul(left, right *value) *value {
	if left == right {
		return p.makeUnaryArithmeticOp(ssquaref, ssquarei, left)
	}
	return p.makeBinaryArithmeticOp(smulf, smuli, smulimmf, smulimmi, smulimmf, smulimmi, left, right)
}

func (p *prog) div(left, right *value) *value {
	return p.makeBinaryArithmeticOp(sdivf, sdivi, sdivimmf, sdivimmi, srdivimmf, srdivimmi, left, right)
}

func (p *prog) mod(left, right *value) *value {
	return p.makeBinaryArithmeticOp(smodf, smodi, smodimmf, smodimmi, srmodimmf, srmodimmi, left, right)
}

func (p *prog) makeBitwiseOp(regOp, immOp ssaop, canSwap bool, left *value, right *value) *value {
	if left.op == sliteral && canSwap {
		left, right = right, left
	}

	if left.op == sliteral {
		left = p.broadcastI64(left)
	}

	lhs, lhk := p.coerceInt(left)
	if right.op == sliteral {
		i64Imm := toi64(right.imm)
		return p.ssa2imm(immOp, lhs, lhk, i64Imm)
	}

	rhs, rhk := p.coerceInt(right)
	return p.ssa3(regOp, lhs, rhs, p.and(lhk, rhk))
}

func (p *prog) bitAnd(left, right *value) *value {
	return p.makeBitwiseOp(sandi, sandimmi, true, left, right)
}

func (p *prog) bitOr(left, right *value) *value {
	return p.makeBitwiseOp(sori, sorimmi, true, left, right)
}

func (p *prog) bitXor(left, right *value) *value {
	return p.makeBitwiseOp(sxori, sxorimmi, true, left, right)
}

func (p *prog) shiftLeftLogical(left, right *value) *value {
	return p.makeBitwiseOp(sslli, ssllimmi, false, left, right)
}

func (p *prog) shiftRightArithmetic(left, right *value) *value {
	return p.makeBitwiseOp(ssrai, ssraimmi, false, left, right)
}

func (p *prog) shiftRightLogical(left, right *value) *value {
	return p.makeBitwiseOp(ssrli, ssrlimmi, false, left, right)
}

func (p *prog) minValue(left, right *value) *value {
	if left == right {
		return left
	}
	return p.makeBinaryArithmeticOp(sminvaluef, sminvaluei, sminvalueimmf, sminvalueimmi, sminvalueimmf, sminvalueimmi, left, right)
}

func (p *prog) maxValue(left, right *value) *value {
	if left == right {
		return left
	}
	return p.makeBinaryArithmeticOp(smaxvaluef, smaxvaluei, smaxvalueimmf, smaxvalueimmi, smaxvalueimmf, smaxvalueimmi, left, right)
}

func (p *prog) hypot(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(shypotf, left, right)
}

func (p *prog) pow(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(spowf, left, right)
}

func (p *prog) atan2(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(satan2f, left, right)
}

func (p *prog) widthBucket(val, min, max, bucketCount *value) *value {
	if isIntValue(val) && isIntValue(min) && isIntValue(max) {
		vali, valk := p.coerceInt(val)
		mini, mink := p.coerceInt(min)
		maxi, maxk := p.coerceInt(max)
		cnti, cntk := p.coerceInt(bucketCount)

		mask := p.and(valk, p.and(cntk, p.and(mink, maxk)))
		return p.ssa5(swidthbucketi, vali, mini, maxi, cnti, mask)
	}

	valf, valk := p.coercefp(val)
	minf, mink := p.coercefp(min)
	maxf, maxk := p.coercefp(max)
	cntf, cntk := p.coercefp(bucketCount)

	mask := p.and(valk, p.and(cntk, p.and(mink, maxk)))
	return p.ssa5(swidthbucketf, valf, minf, maxf, cntf, mask)
}

func (p *prog) coerceTimestamp(v *value) (*value, *value) {
	if v.op == sliteral {
		ts, ok := v.imm.(date.Time)
		if !ok {
			return p.errorf("cannot use result of %T as TIMESTAMP", v.imm), p.validLanes()
		}
		return p.ssa0imm(sbroadcastts, ts.UnixMicro()), p.validLanes()
	}

	switch v.primary() {
	case stValue:
		v = p.ssa2(stotime, v, p.mask(v))
		fallthrough
	case stTime:
		mask := p.mask(v)
		return p.ssa2(sunboxtime, v, mask), mask
	case stTimeInt:
		return v, p.mask(v)
	default:
		return p.errorf("cannot use result of %s as TIMESTAMP", v), p.validLanes()
	}
}

// These are simple cases that require no decomposition to operate on Timestamp.
var timePartMultiplier = [...]uint64{
	expr.Microsecond: 1,
	expr.Millisecond: 1000,
	expr.Second:      1000000,
	expr.Minute:      1000000 * 60,
	expr.Hour:        1000000 * 60 * 60,
	expr.Day:         1000000 * 60 * 60 * 24,
	expr.DOW:         0,
	expr.DOY:         0,
	expr.Week:        1000000 * 60 * 60 * 24 * 7,
	expr.Month:       0,
	expr.Quarter:     0,
	expr.Year:        0,
}

func (p *prog) dateAdd(part expr.Timepart, arg0, arg1 *value) *value {
	arg1Time, arg1Mask := p.coerceTimestamp(arg1)
	if arg0.op == sliteral && isIntImmediate(arg0.imm) {
		i64Imm := toi64(arg0.imm)
		if timePartMultiplier[part] != 0 {
			i64Imm *= timePartMultiplier[part]
			return p.ssa2imm(sdateaddimm, arg1Time, arg1Mask, i64Imm)
		}

		if part == expr.Month {
			return p.ssa2imm(sdateaddmonthimm, arg1Time, arg1Mask, i64Imm)
		}

		if part == expr.Quarter {
			return p.ssa2imm(sdateaddmonthimm, arg1Time, arg1Mask, i64Imm*3)
		}

		if part == expr.Year {
			return p.ssa2imm(sdateaddmonthimm, arg1Time, arg1Mask, i64Imm*12)
		}
	} else {
		arg0Int, arg0Mask := p.coerceInt(arg0)

		// Microseconds need no multiplication of the input, thus use the simplest operation available.
		if part == expr.Microsecond {
			return p.ssa3(sdateadd, arg1Time, arg0Int, p.and(arg1Mask, arg0Mask))
		}

		// If the part is lesser than Month, we can just use addmulimm operation with the required scale.
		if timePartMultiplier[part] != 0 {
			return p.ssa3imm(sdateaddmulimm, arg1Time, arg0Int, p.and(arg1Mask, arg0Mask), timePartMultiplier[part])
		}

		if part == expr.Month {
			return p.ssa3(sdateaddmonth, arg1Time, arg0Int, p.and(arg1Mask, arg0Mask))
		}

		if part == expr.Quarter {
			return p.ssa3(sdateaddquarter, arg1Time, arg0Int, p.and(arg1Mask, arg0Mask))
		}

		if part == expr.Year {
			return p.ssa3(sdateaddyear, arg1Time, arg0Int, p.and(arg1Mask, arg0Mask))
		}
	}

	return p.errorf("unhandled date part %v in DateAdd()", part)
}

func (p *prog) dateDiff(part expr.Timepart, arg0, arg1 *value) *value {
	t0, m0 := p.coerceTimestamp(arg0)
	t1, m1 := p.coerceTimestamp(arg1)

	if part == expr.Microsecond {
		return p.ssa3(sdatediffmicro, t0, t1, p.and(m0, m1))
	}

	if timePartMultiplier[part] != 0 {
		imm := timePartMultiplier[part]
		return p.ssa3imm(sdatediffparam, t0, t1, p.and(m0, m1), imm)
	}

	if part == expr.Month {
		return p.ssa3(sdatediffmonth, t0, t1, p.and(m0, m1))
	}

	if part == expr.Quarter {
		return p.ssa3(sdatediffquarter, t0, t1, p.and(m0, m1))
	}

	if part == expr.Year {
		return p.ssa3(sdatediffyear, t0, t1, p.and(m0, m1))
	}

	return p.errorf("unhandled date part in DateDiff()")
}

func immediateForBoxedDateInstruction(part expr.Timepart) int {
	switch part {
	case expr.Second:
		return 5
	case expr.Minute:
		return 4
	case expr.Hour:
		return 3
	case expr.Day:
		return 2
	case expr.Month:
		return 1
	case expr.Year:
		return 0
	default:
		panic(fmt.Sprintf("Time part %v is invalid here", part))
	}
}

func (p *prog) dateExtract(part expr.Timepart, val *value) *value {
	if val.primary() == stTimeInt || part < expr.Second || part == expr.Quarter || part == expr.DOW || part == expr.DOY {
		v, m := p.coerceTimestamp(val)
		switch part {
		case expr.Microsecond:
			return p.ssa2(sdateextractmicrosecond, v, m)
		case expr.Millisecond:
			return p.ssa2(sdateextractmillisecond, v, m)
		case expr.Second:
			return p.ssa2(sdateextractsecond, v, m)
		case expr.Minute:
			return p.ssa2(sdateextractminute, v, m)
		case expr.Hour:
			return p.ssa2(sdateextracthour, v, m)
		case expr.Day:
			return p.ssa2(sdateextractday, v, m)
		case expr.DOW:
			return p.ssa2(sdateextractdow, v, m)
		case expr.DOY:
			return p.ssa2(sdateextractdoy, v, m)
		case expr.Month:
			return p.ssa2(sdateextractmonth, v, m)
		case expr.Quarter:
			return p.ssa2(sdateextractquarter, v, m)
		case expr.Year:
			return p.ssa2(sdateextractyear, v, m)
		default:
			return p.errorf("unhandled date part in DateExtract()")
		}
	}

	v := p.toTime(val)
	return p.ssa2imm(stmextract, v, p.mask(v), immediateForBoxedDateInstruction(part))
}

func (p *prog) dateToUnixEpoch(val *value) *value {
	v, m := p.coerceTimestamp(val)
	return p.ssa2(sdatetounixepoch, v, m)
}

func (p *prog) dateToUnixMicro(val *value) *value {
	v, m := p.coerceTimestamp(val)
	return p.ssa2(sdatetounixmicro, v, m)
}

func (p *prog) dateTrunc(part expr.Timepart, val *value) *value {
	if part == expr.Microsecond {
		return val
	}

	v, m := p.coerceTimestamp(val)
	switch part {
	case expr.Millisecond:
		return p.ssa2(sdatetruncmillisecond, v, m)
	case expr.Second:
		return p.ssa2(sdatetruncsecond, v, m)
	case expr.Minute:
		return p.ssa2(sdatetruncminute, v, m)
	case expr.Hour:
		return p.ssa2(sdatetrunchour, v, m)
	case expr.Day:
		return p.ssa2(sdatetruncday, v, m)
	case expr.Month:
		return p.ssa2(sdatetruncmonth, v, m)
	case expr.Quarter:
		return p.ssa2(sdatetruncquarter, v, m)
	case expr.Year:
		return p.ssa2(sdatetruncyear, v, m)
	default:
		return p.errorf("unhandled date part in DateTrunc()")
	}
}

func (p *prog) dateTruncWeekday(val *value, dow expr.Weekday) *value {
	v, m := p.coerceTimestamp(val)
	return p.ssa2imm(sdatetruncdow, v, m, int64(dow))
}

func (p *prog) timeBucket(timestamp, interval *value) *value {
	tv := p.dateToUnixEpoch(timestamp)
	iv, im := p.coerceInt(interval)
	return p.ssa3(stimebucketts, tv, iv, p.and(p.mask(tv), im))
}

func (p *prog) geoHash(latitude, longitude, numChars *value) *value {
	latV, latM := p.coercefp(latitude)
	lonV, lonM := p.coercefp(longitude)

	if numChars.op == sliteral && isIntImmediate(numChars.imm) {
		return p.ssa3imm(sgeohashimm, latV, lonV, p.and(latM, lonM), numChars.imm)
	}

	charsV, charsM := p.coerceInt(numChars)
	mask := p.and(p.and(latM, lonM), charsM)
	return p.ssa4(sgeohash, latV, lonV, charsV, mask)
}

func (p *prog) geoTileX(longitude, precision *value) *value {
	lonV, lonM := p.coercefp(longitude)
	precV, precM := p.coerceInt(precision)
	mask := p.and(lonM, precM)
	return p.ssa3(sgeotilex, lonV, precV, mask)
}

func (p *prog) geoTileY(latitude, precision *value) *value {
	latV, latM := p.coercefp(latitude)
	precV, precM := p.coerceInt(precision)
	mask := p.and(latM, precM)
	return p.ssa3(sgeotiley, latV, precV, mask)
}

func (p *prog) geoTileES(latitude, longitude, precision *value) *value {
	latV, latM := p.coercefp(latitude)
	lonV, lonM := p.coercefp(longitude)

	if precision.op == sliteral && isIntImmediate(precision.imm) {
		return p.ssa3imm(sgeotileesimm, latV, lonV, p.and(latM, lonM), precision.imm)
	}

	charsV, charsM := p.coerceInt(precision)
	mask := p.and(p.and(latM, lonM), charsM)
	return p.ssa4(sgeotilees, latV, lonV, charsV, mask)
}

func (p *prog) geoDistance(latitude1, longitude1, latitude2, longitude2 *value) *value {
	lat1V, lat1M := p.coercefp(latitude1)
	lon1V, lon1M := p.coercefp(longitude1)
	lat2V, lat2M := p.coercefp(latitude2)
	lon2V, lon2M := p.coercefp(longitude2)

	mask := p.and(p.and(lat1M, lon1M), p.and(lat2M, lon2M))
	return p.ssa5(sgeodistance, lat1V, lon1V, lat2V, lon2V, mask)
}

func (p *prog) lower(s *value) *value {
	return p.ssa2(slowerstr, s, p.mask(s))
}

func (p *prog) upper(s *value) *value {
	return p.ssa2(supperstr, s, p.mask(s))
}

func emitNone(v *value, c *compilestate) {
	// does nothing...
}

func dateDiffMQYImm(op ssaop) int {
	switch op {
	case sdatediffquarter:
		return 1
	case sdatediffyear:
		return 2
	default:
		return 0
	}
}

func emitDateDiffMQY(v *value, c *compilestate) {
	arg0 := v.args[0]                            // t0
	arg1Slot := c.forceStackRef(v.args[1], regS) // t1
	mask := v.args[2]                            // predicate

	info := &ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16u16(v, bc, arg1Slot, uint16(dateDiffMQYImm(v.op)))
}

func emitdatecasttoint(v *value, c *compilestate) {
	arg0 := v.args[0] // t0
	mask := v.args[1] // predicate

	c.loadk(v, mask)
	c.loads(v, arg0)
	// FIXME: we need this here in order
	// to not confuse the stack allocator,
	// but in principle we wouldn't need to do
	// a clobber if we could teach the stack allocator
	// that these registers are actually equivalent
	c.clobbers(v)
}

// Simple aggregate operations
func (p *prog) makeAggregateBoolOp(aggBoolOp, aggIntOp ssaop, v, filter *value, slot aggregateslot) *value {
	mem := p.initMem()

	// In general we have to coerce to BOOL, however, if the input is a boxed value we
	// will just unbox BOOL to INT64 and use INT64 aggregation instead of converting such
	// INT64 to BOOL. This saves us some instructions.
	if v.primary() == stValue {
		k := p.mask(v)
		intVal := p.ssa2(sunboxktoi, v, k)
		mask := p.mask(intVal)
		if filter != nil {
			mask = p.and(mask, filter)
		}
		return p.ssa3imm(aggIntOp, mem, intVal, mask, slot)
	}

	boolVal, mask := p.coerceBool(v)
	if filter != nil {
		mask = p.and(mask, filter)
	}
	return p.ssa3imm(aggBoolOp, mem, boolVal, mask, slot)
}

func (p *prog) makeAggregateOp(opF, opI ssaop, child, filter *value, slot aggregateslot) (v *value, fp bool) {
	if isIntValue(child) || opF == sinvalid {
		scalar, mask := p.coerceInt(child)
		if filter != nil {
			mask = p.and(mask, filter)
		}
		mem := p.initMem()
		return p.ssa3imm(opI, mem, scalar, mask, slot), false
	}

	scalar, mask := p.coercefp(child)
	if filter != nil {
		mask = p.and(mask, filter)
	}

	mem := p.initMem()
	return p.ssa3imm(opF, mem, scalar, mask, slot), true
}

func (p *prog) makeTimeAggregateOp(op ssaop, child, filter *value, slot aggregateslot) *value {
	scalar, mask := p.coerceTimestamp(child)
	if filter != nil {
		mask = p.and(mask, filter)
	}
	mem := p.initMem()
	return p.ssa3imm(op, mem, scalar, mask, slot)
}

func (p *prog) aggregateBoolAnd(child, filter *value, slot aggregateslot) *value {
	return p.makeAggregateBoolOp(saggandk, saggandi, child, filter, slot)
}

func (p *prog) aggregateBoolOr(child, filter *value, slot aggregateslot) *value {
	return p.makeAggregateBoolOp(saggork, saggori, child, filter, slot)
}

func (p *prog) aggregateSumInt(child, filter *value, slot aggregateslot) *value {
	child = p.toint(child)
	mask := p.mask(child)
	if filter != nil {
		mask = p.and(mask, filter)
	}
	return p.ssa3imm(saggsumi, p.initMem(), child, mask, slot)
}

func (p *prog) aggregateSum(child, filter *value, slot aggregateslot) (v *value, fp bool) {
	return p.makeAggregateOp(saggsumf, saggsumi, child, filter, slot)
}

func (p *prog) aggregateAvg(child, filter *value, slot aggregateslot) (v *value, fp bool) {
	return p.makeAggregateOp(saggavgf, saggavgi, child, filter, slot)
}

func (p *prog) aggregateMin(child, filter *value, slot aggregateslot) (v *value, fp bool) {
	return p.makeAggregateOp(saggminf, saggmini, child, filter, slot)
}

func (p *prog) aggregateMax(child, filter *value, slot aggregateslot) (v *value, fp bool) {
	return p.makeAggregateOp(saggmaxf, saggmaxi, child, filter, slot)
}

func (p *prog) aggregateAnd(child, filter *value, slot aggregateslot) *value {
	val, _ := p.makeAggregateOp(sinvalid, saggandi, child, filter, slot)
	return val
}

func (p *prog) aggregateOr(child, filter *value, slot aggregateslot) *value {
	val, _ := p.makeAggregateOp(sinvalid, saggori, child, filter, slot)
	return val
}

func (p *prog) aggregateXor(child, filter *value, slot aggregateslot) *value {
	val, _ := p.makeAggregateOp(sinvalid, saggxori, child, filter, slot)
	return val
}

func (p *prog) aggregateEarliest(child, filter *value, slot aggregateslot) *value {
	return p.makeTimeAggregateOp(saggmints, child, filter, slot)
}

func (p *prog) aggregateLatest(child, filter *value, slot aggregateslot) *value {
	return p.makeTimeAggregateOp(saggmaxts, child, filter, slot)
}

func (p *prog) aggregateCount(child, filter *value, slot aggregateslot) *value {
	mask := p.notMissing(child)
	if filter != nil {
		mask = p.and(mask, filter)
	}
	return p.ssa2imm(saggcount, p.initMem(), mask, slot)
}

func (p *prog) aacd(op ssaop, child, filter *value, slot aggregateslot, precision uint8) *value {
	mask := p.mask(child)
	if filter != nil {
		mask = p.and(mask, filter)
	}

	h := p.hash(child)

	return p.ssa2imm(saggapproxcount, h, mask, (uint64(slot)<<8)|uint64(precision))
}

func (p *prog) aggregateApproxCountDistinct(child, filter *value, slot aggregateslot, precision uint8) *value {
	return p.aacd(saggapproxcount, child, filter, slot, precision)
}

func (p *prog) aggregateApproxCountDistinctPartial(child, filter *value, slot aggregateslot, precision uint8) *value {
	return p.aacd(saggapproxcountpartial, child, filter, slot, precision)
}

func (p *prog) aggregateApproxCountDistinctMerge(child *value, slot aggregateslot, precision uint8) *value {
	blob := p.ssa2(stoblob, child, p.mask(child))
	return p.ssa2imm(saggapproxcountmerge, blob, p.mask(blob), (uint64(slot)<<8)|uint64(precision))
}

// Slot aggregate operations
func (p *prog) makeAggregateSlotBoolOp(op ssaop, mem, bucket, v, mask *value, slot aggregateslot) *value {
	boolVal, m := p.coerceBool(v)
	if mask != nil {
		m = p.and(m, mask)
	}
	return p.ssa4imm(op, mem, bucket, boolVal, m, slot)
}

func (p *prog) makeAggregateSlotOp(opF, opI ssaop, mem, bucket, v, mask *value, offset aggregateslot) (rv *value, fp bool) {
	if isIntValue(v) || opF == sinvalid {
		scalar, m := p.coerceInt(v)
		if mask != nil {
			m = p.and(m, mask)
		}
		return p.ssa4imm(opI, mem, bucket, scalar, m, offset), false
	}

	scalar, m := p.coercefp(v)
	if mask != nil {
		m = p.and(m, mask)
	}
	return p.ssa4imm(opF, mem, bucket, scalar, m, offset), true
}

func (p *prog) makeTimeAggregateSlotOp(op ssaop, mem, bucket, v, mask *value, offset aggregateslot) *value {
	scalar, m := p.coerceTimestamp(v)
	if mask != nil {
		m = p.and(m, mask)
	}
	return p.ssa4imm(op, mem, bucket, scalar, m, offset)
}

func (p *prog) aggregateSlotSum(mem, bucket, value, mask *value, offset aggregateslot) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotsumf, saggslotsumi, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotSumInt(mem, bucket, value, mask *value, offset aggregateslot) *value {
	scalar, m := p.coerceInt(value)
	if mask != nil {
		m = p.and(m, mask)
	}
	return p.ssa4imm(saggslotsumi, mem, bucket, scalar, m, offset)
}

func (p *prog) aggregateSlotAvg(mem, bucket, value, mask *value, offset aggregateslot) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotavgf, saggslotavgi, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotMin(mem, bucket, value, mask *value, offset aggregateslot) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotminf, saggslotmini, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotMax(mem, bucket, value, mask *value, offset aggregateslot) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotmaxf, saggslotmaxi, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotAnd(mem, bucket, value, mask *value, offset aggregateslot) *value {
	val, _ := p.makeAggregateSlotOp(sinvalid, saggslotandi, mem, bucket, value, mask, offset)
	return val
}

func (p *prog) aggregateSlotOr(mem, bucket, value, mask *value, offset aggregateslot) *value {
	val, _ := p.makeAggregateSlotOp(sinvalid, saggslotori, mem, bucket, value, mask, offset)
	return val
}

func (p *prog) aggregateSlotXor(mem, bucket, value, mask *value, offset aggregateslot) *value {
	val, _ := p.makeAggregateSlotOp(sinvalid, saggslotxori, mem, bucket, value, mask, offset)
	return val
}

func (p *prog) aggregateSlotBoolAnd(mem, bucket, value, mask *value, offset aggregateslot) *value {
	return p.makeAggregateSlotBoolOp(saggslotandk, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotBoolOr(mem, bucket, value, mask *value, offset aggregateslot) *value {
	return p.makeAggregateSlotBoolOp(saggslotork, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotEarliest(mem, bucket, value, mask *value, offset aggregateslot) *value {
	return p.makeTimeAggregateSlotOp(saggslotmints, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotLatest(mem, bucket, value, mask *value, offset aggregateslot) *value {
	return p.makeTimeAggregateSlotOp(saggslotmaxts, mem, bucket, value, mask, offset)
}

func (p *prog) aggregateSlotCount(mem, bucket, mask *value, offset aggregateslot) *value {
	return p.ssa3imm(saggslotcount, mem, bucket, mask, offset)
}

func (p *prog) asacd(op ssaop, mem, bucket, argv, mask *value, offset aggregateslot, precision uint8) *value {
	k := p.mask(argv)
	if mask != nil {
		k = p.and(k, mask)
	}
	h := p.hash(argv)
	return p.ssa4imm(op, mem, bucket, h, k, (uint64(offset)<<8)|uint64(precision))
}

func (p *prog) aggregateSlotApproxCountDistinct(mem, bucket, argv, mask *value, offset aggregateslot, precision uint8) *value {
	return p.asacd(saggslotapproxcount, mem, bucket, argv, mask, offset, precision)
}

func (p *prog) aggregateSlotApproxCountDistinctPartial(mem, bucket, argv, mask *value, offset aggregateslot, precision uint8) *value {
	return p.asacd(saggslotapproxcountpartial, mem, bucket, argv, mask, offset, precision)
}

func (p *prog) aggregateSlotApproxCountDistinctMerge(mem, bucket, argv, mask *value, offset aggregateslot, precision uint8) *value {
	blob := p.ssa2(stoblob, argv, mask)
	return p.ssa4imm(saggslotapproxcountmerge, mem, bucket, blob, p.mask(blob), (uint64(offset)<<8)|uint64(precision))
}

// note: the 'mem' argument to aggbucket
// is for ordering the store(s) that write
// out the names of the fields being aggregated against
// in case they need to be written into the table
//
// TODO: perform this store only on early abort?
func (p *prog) aggbucket(mem, h, k *value) *value {
	return p.ssa3(saggbucket, mem, h, k)
}

func (p *prog) hash(v *value) *value {
	v = p.unsymbolized(v)
	switch v.primary() {
	case stValue:
		return p.ssa2(shashvalue, v, p.mask(v))
	default:
		return p.errorf("bad value %v passed to prog.hash()", v)
	}
}

func (p *prog) hashplus(h *value, v *value) *value {
	v = p.unsymbolized(v)
	switch v.primary() {
	case stValue:
		return p.ssa3(shashvaluep, h, v, p.mask(v))
	default:
		return p.errorf("bad value %v, %v passed to prog.hashplus()", h, v)
	}
}

// Name returns the textual SSA name of this value
func (v *value) Name() string {
	if v.op == sinvalid {
		return "(invalid)"
	}
	rt := ssainfo[v.op].rettype
	value := rt &^ stBool
	str := ""
	if value != 0 {
		str = string(value.char()) + strconv.Itoa(v.id) + "."
	}
	if rt&stBool != 0 {
		str += "k" + strconv.Itoa(v.id)
	}
	return str
}

func (v *value) String() string {
	if v.op == sinvalid {
		return fmt.Sprintf("invalid(%q)", v.imm.(string))
	}
	str := v.op.String()
	info := &ssainfo[v.op]

	for i := range v.args {
		argtype := info.argType(i)
		str += " " + string(argtype.char()) + strconv.Itoa(v.args[i].id)
	}
	if v.imm != nil {
		str += fmt.Sprintf(" $%v", v.imm)
	}
	return str
}

func (p *prog) writeTo(w io.Writer) (int64, error) {
	var nn int64
	values := p.values
	for i := range values {
		n, _ := io.WriteString(w, values[i].Name())
		nn += int64(n)
		n, _ = io.WriteString(w, " = ")
		nn += int64(n)
		n, _ = io.WriteString(w, values[i].String())
		nn += int64(n)
		n, _ = io.WriteString(w, "\n")
		nn += int64(n)
	}
	n, err := fmt.Fprintf(w, "ret: %s\n", p.ret.Name())
	nn += int64(n)
	return nn, err
}

// Graphviz writes out the program in a format
// that the dot(1) tool can turn into a visual graph
func (p *prog) graphviz(w io.Writer) {
	fmt.Fprintf(w, "digraph prog {\n")
	for i := range p.values {
		v := p.values[i]
		fmt.Fprintf(w, "\t%q [label=%q];\n", v.Name(), v.Name()+" = "+v.String())
		for _, arg := range v.args {
			// write arg -> v
			fmt.Fprintf(w, "\t%q -> %q;\n", arg.Name(), v.Name())
		}
	}
	if p.ret != nil {
		fmt.Fprintf(w, "\t%q -> ret;\n", p.ret.Name())
	}
	io.WriteString(w, "}\n")
}

// core post-order instruction scheduling logic
//
// TODO: make this smarter than simply leftmost-first
func (p *prog) sched(v *value, dst []*value, scheduled, parent []bool) []*value {
	if parent[v.id] {
		p.panicdump()
		panic(fmt.Sprintf("circular reference at %s", v.Name()))
	}
	if scheduled[v.id] {
		return dst
	}
	// instructions have a mask register as the
	// last argument, and we only use one physical
	// register for the mask carried across instructions,
	// so trying to schedule the rightmost argument as close
	// as possible to the current instruction minimizes the
	// number of spills of the mask register
	parent[v.id] = true
	for i := len(v.args) - 1; i >= 0; i-- {
		dst = p.sched(v.args[i], dst, scheduled, parent)
	}
	parent[v.id] = false
	scheduled[v.id] = true
	return append(dst, v)
}

func (v *value) setmask(m *value) {
	v.args[len(v.args)-1] = m
}

func (v *value) maskarg() *value {
	if len(v.args) == 0 {
		return nil
	}
	m := v.args[len(v.args)-1]
	if m.ret()&stBool == 0 {
		return nil
	}
	return m
}

func (v *value) setfalse() {
	v.op = skfalse
	v.args = nil
	v.imm = nil
}

// determine the output predicate associated with v
//
// if v returns a mask, then mask(v) is v
// or, if v accepts a mask, then mask(v) is the mask argument
// otherwise, the mask is all valid lanes
func (p *prog) mask(v *value) *value {
	if v.ret()&stBool != 0 {
		return v
	}
	if arg := v.maskarg(); arg != nil {
		return arg
	}
	// broadcast, etc. instructions
	// are valid in every lane
	return p.validLanes()
}

// compute a post-order numbering of values
func (p *prog) numbering(pi *proginfo) []int {
	if len(pi.num) != 0 {
		return pi.num
	}
	ord := p.order(pi)
	if cap(pi.num) < len(p.values) {
		pi.num = make([]int, len(p.values))
	} else {
		pi.num = pi.num[:len(p.values)]
		for i := range pi.num {
			pi.num[i] = 0
		}
	}
	for i := range ord {
		pi.num[ord[i].id] = i
	}
	return pi.num
}

// proginfo caches data structures computed
// during optimization passes; we can use
// it to avoid repeatedly allocating slices
// for dominator trees, etc.
type proginfo struct {
	num []int    // execution numbering for next bit
	rpo []*value // valid execution ordering
}

func (i *proginfo) invalidate() {
	i.rpo = i.rpo[:0]
}

// order computes an execution ordering for p,
// or returns a cached one from pi
func (p *prog) order(pi *proginfo) []*value {
	if len(pi.rpo) != 0 {
		return pi.rpo
	}
	return p.rpo(pi.rpo)
}

// finalorder computes the final instruction ordering
//
// the ordering is determined by static scheduling priority
// for each instruction, plus a heuristic that instructions
// should be grouped close to their uses
func (p *prog) finalorder(rpo []*value, numbering []int) []*value {
	// priority determines the heap priority
	// of instructions that can be scheduled
	//
	// higher-numbered priorities are scheduled
	// before lower-numbered priorities
	priority := func(v *value) int {
		p := ssainfo[v.op].priority
		if p != 0 {
			return p
		}
		// schedule things in reverse-post-order
		// when we don't have any other indication
		return -numbering[v.id]
	}
	var hvalues []*value
	vless := func(x, y *value) bool {
		return priority(x) < priority(y)
	}

	// count the number of times each
	// instruction is used; this will
	// tell us when an instruction can
	// legally be scheduled
	refcount := make([]int, len(p.values))
	for _, v := range rpo {
		for _, arg := range v.args {
			refcount[arg.id]++
		}
	}

	// build the instruction schedule in reverse:
	// start with the return value and add instructions
	// once all of their uses have been scheduled
	if refcount[p.ret.id] != 0 {
		panic("ret has non-zero refcount?")
	}
	hvalues = append(hvalues, p.ret)
	out := make([]*value, len(rpo))
	nv := len(out)
	for len(hvalues) > 0 {
		next := heap.PopSlice(&hvalues, vless)
		for _, arg := range next.args {
			refcount[arg.id]--
			if refcount[arg.id] == 0 {
				heap.PushSlice(&hvalues, arg, vless)
			}
			if refcount[arg.id] < 0 {
				panic("negative refcount")
			}
		}
		nv--
		out[nv] = next
	}
	out = out[nv:]
	return out
}

// try to order accesses to structure fields
// FIXME: only handles access relative to 'b0' right now
// since the instructions trivially must not depend on
// one another
func (p *prog) ordersyms(pi *proginfo) {

	// accumulate the list of values that
	// are used as structure base pointers;
	// these are either value 0 (top-level row)
	// or a 'tuples' op
	bases := []int{0}

	for i := range p.values {
		if p.values[i].op == stuples {
			bases = append(bases, p.values[i].id)
		}
	}

	// for each base pointer, sort accesses
	// by the value of the symbol ID
	var access []*value
	for _, baseid := range bases {
		access = access[:0]
		for i := range p.values {
			v := p.values[i]
			if v.op != sdot || v.args[0].id != baseid || v.args[1].id != baseid {
				continue
			}
			access = append(access, v)
		}
		if len(access) <= 1 {
			continue
		}
		pi.invalidate()
		slices.SortFunc(access, func(x, y *value) bool {
			return x.imm.(ion.Symbol) < y.imm.(ion.Symbol)
		})
		prev := access[0]
		rest := access[1:]
		for i := range rest {
			v := rest[i]
			v.op = sdot2
			// rewrite 'dot b0 k0' -> 'dot2 b0 vx kx k0'
			v.args = []*value{v.args[0], prev, prev, v.args[1]}
			prev = v
		}
	}
}

func (p *prog) panicdump() {
	for i := range p.values {
		v := p.values[i]
		println(v.Name(), "=", v.String())
	}
	if p.ret != nil {
		println("ret:", p.ret.Name())
	}
}

// compute a valid execution ordering of ssa values
// and append them to 'out'
func (p *prog) rpo(out []*value) []*value {
	if p.ret == nil {
		return nil
	}

	// always schedule init and ?invalid at the top;
	// they don't emit any instructions, but they do
	// represent the initial register state
	out = append(out, p.values[0], p.values[1])
	scheduled := make([]bool, len(p.values))
	parent := make([]bool, len(p.values))
	scheduled[0] = true
	scheduled[1] = true
	return p.sched(p.ret, out, scheduled, parent)
}

// optimize the program and set
// p.values to the values in program order
func (p *prog) optimize() {
	var pi proginfo
	// optimization passes
	p.simplify(&pi)
	p.exprs = nil // invalidated in ordersyms
	p.ordersyms(&pi)

	// final dead code elimination and scheduling
	order := p.finalorder(p.order(&pi), p.numbering(&pi))
	for i := range order {
		order[i].id = i
	}
	p.values = p.values[:copy(p.values, order)]
	pi.invalidate()
}

type lranges struct {
	krange []int // last use of a mask
	vrange []int // last use of a value
}

// regclass is a virtual register class
type regclass uint8

const (
	regK regclass = iota // K reg
	regS                 // the scalar reg
	regV                 // the current value reg
	regB                 // the current row reg
	regH                 // the current hash reg
	regL                 // the current aggregate bucket offset

	_maxregclass
)

type regset uint8

func onlyreg(class regclass) regset {
	return regset(1 << class)
}

func (r regset) contains(class regclass) bool {
	return (r & (1 << class)) != 0
}

func (r *regset) add(class regclass) {
	*r |= (1 << class)
}

func (r *regset) del(class regclass) {
	*r &^= (1 << class)
}

func (s ssatype) vregs() regset {
	r := regset(0)
	if s&stBool != 0 {
		r.add(regK)
	}
	if s&stScalar != 0 {
		r.add(regS)
	}
	if s&stValue != 0 {
		r.add(regV)
	}
	if s&stBase != 0 {
		r.add(regB)
	}
	if s&stHash != 0 {
		r.add(regH)
	}
	if s&stBucket != 0 {
		r.add(regL)
	}
	return r
}

func (r regset) String() string {
	if r&(1<<regK) != 0 {
		if r&^(1<<regK) != 0 {
			return "v.k"
		}
		return "k"
	}
	if r != 0 {
		return "v"
	}
	return "(no)"
}

// order instructions in executable order
// and compute the live range of each instruction's
// output mask and value
//
// live ranges are written into 'dst'
// and the execution ordering of instructions
// is returned
func (p *prog) liveranges(dst *lranges) {
	p.optimize()
	dst.krange = make([]int, len(p.values))
	dst.vrange = make([]int, len(p.values))
	for i, v := range p.values {
		if v.id != i {
			panic("liveranges() before re-numbering")
		}

		op := v.op
		args := v.args

		if op == smergemem {
			// variadic, and only
			// memory args anyway...
			continue
		}

		info := &ssainfo[op]
		for j := range args {
			switch info.argType(j) {
			case stBool:
				dst.krange[args[j].id] = i
			case stMem:
				// ignore memory args
			default:
				dst.vrange[args[j].id] = i
			}
		}
	}

	// return value is live through the
	// end of the program
	dst.krange[p.ret.id] = len(p.values)
	dst.vrange[p.ret.id] = len(p.values)
}

// regstate is the register + stack state
// for a bytecode program
type regstate struct {
	stack stackmap          // rest of the values are in their respective stacks
	cur   [_maxregclass]int // current value IDs in registers
}

func (r *regstate) init(size int) {
	r.stack.init()

	for i := regclass(0); i < _maxregclass; i++ {
		r.cur[i] = -1
	}

	// These two are default initialized to 0 as sinit yields both the initial mask and initial base
	r.cur[regK] = 0
	r.cur[regB] = 0
}

// TODO:
// These are tiny wrappers around stackmap. The reason we use these for now is that we have multiple
// stackmaps that we would like to merge into a single one in the future.
func (r *regstate) allocValue(rc regclass, valueID int) stackslot {
	return r.stack.allocValue(rc, valueID)
}

func (r *regstate) freeValue(rc regclass, valueID int) {
	r.stack.freeValue(rc, valueID)
}

func (r *regstate) slotOf(rc regclass, valueID int) stackslot {
	return r.stack.slotOf(rc, valueID)
}

func (r *regstate) hasSlot(rc regclass, valueID int) bool {
	return r.stack.hasSlot(rc, valueID)
}

type compilestate struct {
	lr   lranges  // variable live ranges
	regs regstate // register state

	trees  []*radixTree64
	asm    assembler
	dict   []string
	litbuf []byte // output datum literals
}

func checkImmediateBeforeEmit1(op bcop, imm0Size int) {
	info := &opinfo[op]
	if len(info.imms) != 1 {
		panic(fmt.Sprintf("bytecode op '%s' requires %d immediate(s), not %d", info.text, len(info.imms), 1))
	}

	if int(bcImmWidth[info.imms[0]]) != imm0Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the first immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[0]], imm0Size))
	}
}

func checkImmediateBeforeEmit2(op bcop, imm0Size, imm1Size int) {
	info := &opinfo[op]
	if len(info.imms) != 2 {
		panic(fmt.Sprintf("bytecode op '%s' requires %d immediate(s), not %d", info.text, len(info.imms), 2))
	}

	if int(bcImmWidth[info.imms[0]]) != imm0Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the first immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[0]], imm0Size))
	}

	if int(bcImmWidth[info.imms[1]]) != imm1Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the second immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[1]], imm1Size))
	}
}

func checkImmediateBeforeEmit3(op bcop, imm0Size, imm1Size, imm2Size int) {
	info := &opinfo[op]
	if len(info.imms) != 3 {
		panic(fmt.Sprintf("bytecode op '%s' requires %d immediate(s), not %d", info.text, len(info.imms), 3))
	}

	if int(bcImmWidth[info.imms[0]]) != imm0Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the first immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[0]], imm0Size))
	}

	if int(bcImmWidth[info.imms[1]]) != imm1Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the second immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[1]], imm1Size))
	}

	if int(bcImmWidth[info.imms[2]]) != imm2Size {
		panic(fmt.Sprintf("bytecode op '%s' requires the third immediate to be %d bytes, not %d", info.text, bcImmWidth[info.imms[2]], imm2Size))
	}
}

func (c *compilestate) op(v *value, op bcop) {
	info := &opinfo[op]
	if len(info.imms) != 0 {
		panic(fmt.Sprintf("bytecode op '%s' requires %d immediate(s), not %d", info.text, len(info.imms), 0))
	}
	c.asm.emitOpcode(op)
}

func (c *compilestate) opvar(op bcop, slots []stackslot, imm interface{}) {
	info := &opinfo[op]
	immCount := len(slots)

	if imm != nil {
		immCount++
	}

	if immCount != len(info.imms) {
		panic(fmt.Sprintf("error when emitting '%s': required %d immediates, not %d", info.text, len(info.imms), immCount))
	}

	// emit opcode
	c.asm.emitOpcode(op)

	// emit optional stack slots
	for i, slot := range slots {
		if info.imms[i] != bcImmS16 {
			panic(fmt.Sprintf("error when emitting '%s': argument %d is not 'stackslot'", info.text, i))
		}
		c.asm.emitImmU16(uint16(slot))
	}

	// emit an optional immediate that follows
	if imm != nil {
		switch info.imms[len(info.imms)-1] {
		case bcImmI8, bcImmU8, bcImmU8Hex:
			c.asm.emitImmU8(uint8(toi64(imm)))
		case bcImmS16, bcImmI16, bcImmU16, bcImmU16Hex, bcImmDict:
			c.asm.emitImmU16(uint16(toi64(imm)))
		case bcImmI32, bcImmU32, bcImmU32Hex:
			c.asm.emitImmU32(uint32(toi64(imm)))
		case bcImmI64, bcImmU64, bcImmU64Hex:
			c.asm.emitImmU64(toi64(imm))
		case bcImmF64:
			c.asm.emitImmU64(math.Float64bits(tof64(imm)))
		}
	}
}

func (c *compilestate) opva(op bcop, imms []uint64) {
	info := &opinfo[op]

	// Verify the number of immediates matches the signature. vaImms contains a signature of each
	// immediate tuple that is considered a single va argument. For example if the bc instruction
	// uses [stString, stBool] tuple, it's a group of 2 immediate values for each va argument.
	baseImmCount := len(info.imms)
	vaTupleSize := len(info.vaImms)

	if vaTupleSize == 0 {
		panic(fmt.Sprintf("cannot use opva as the opcode '%v' doesn't provide variable operands", op))
	}

	if len(imms) < baseImmCount {
		panic(fmt.Sprintf("invalid immediate count while emitting opcode '%v' (count=%d mandatory=%d tupleSize=%d)",
			op, len(imms), baseImmCount, vaTupleSize))
	}

	vaLength := (len(imms) - baseImmCount) / vaTupleSize
	if baseImmCount+vaLength*vaTupleSize != len(imms) {
		panic(fmt.Sprintf("invalid immediate count while emitting opcode '%v' (count=%d mandatory=%d tupleSize=%d)",
			op, len(imms), baseImmCount, vaTupleSize))
	}

	// emit opcode + va_length
	c.asm.emitOpcode(op)
	c.asm.emitImmU32(uint32(vaLength))

	// emit base immediates
	for i := 0; i < baseImmCount; i++ {
		c.asm.emitImm(imms[i], int(bcImmWidth[info.imms[i]]))
	}

	// emit va immediates
	j := 0
	for i := baseImmCount; i < len(imms); i++ {
		c.asm.emitImm(imms[i], int(bcImmWidth[info.vaImms[j]]))
		j++
		if j >= len(info.vaImms) {
			j = 0
		}
	}
}

func (c *compilestate) opu8(v *value, op bcop, imm uint8) {
	checkImmediateBeforeEmit1(op, 1)
	c.asm.emitOpcode(op)
	c.asm.emitImmU8(imm)
}

func (c *compilestate) opu16(v *value, op bcop, imm0 uint16) {
	checkImmediateBeforeEmit1(op, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU16(imm0)
}

func (c *compilestate) opu32(v *value, op bcop, imm0 uint32) {
	checkImmediateBeforeEmit1(op, 4)
	c.asm.emitOpcode(op)
	c.asm.emitImmU32(imm0)
}

func (c *compilestate) opu64(v *value, op bcop, imm0 uint64) {
	checkImmediateBeforeEmit1(op, 8)
	c.asm.emitOpcode(op)
	c.asm.emitImmU64(imm0)
}

func (c *compilestate) opu16u16(v *value, op bcop, imm0, imm1 uint16) {
	checkImmediateBeforeEmit2(op, 2, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU16(imm0)
	c.asm.emitImmU16(imm1)
}

func (c *compilestate) opu16u32(v *value, op bcop, imm0 uint16, imm1 uint32) {
	checkImmediateBeforeEmit2(op, 2, 4)
	c.asm.emitOpcode(op)
	c.asm.emitImmU16(imm0)
	c.asm.emitImmU32(imm1)
}

func (c *compilestate) opu16u16u16(v *value, op bcop, imm0, imm1, imm2 uint16) {
	checkImmediateBeforeEmit3(op, 2, 2, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU16(imm0)
	c.asm.emitImmU16(imm1)
	c.asm.emitImmU16(imm2)
}

func (c *compilestate) opu32u32(v *value, op bcop, imm0 uint32, imm1 uint32) {
	checkImmediateBeforeEmit2(op, 4, 4)
	c.asm.emitOpcode(op)
	c.asm.emitImmU32(imm0)
	c.asm.emitImmU32(imm1)
}

func (c *compilestate) ops16(v *value, o bcop, slot stackslot) {
	c.opu16(v, o, uint16(slot))
}

func (c *compilestate) ops16u16(v *value, op bcop, imm0 stackslot, imm1 uint16) {
	c.opu16u16(v, op, uint16(imm0), imm1)
}

func (c *compilestate) ops16u32(v *value, op bcop, imm0 stackslot, imm1 uint32) {
	c.opu16u32(v, op, uint16(imm0), imm1)
}

func (c *compilestate) opu16s16(v *value, op bcop, imm0 uint16, imm1 stackslot) {
	c.opu16u16(v, op, imm0, uint16(imm1))
}

func (c *compilestate) ops16s16(v *value, op bcop, imm0, imm1 stackslot) {
	c.opu16u16(v, op, uint16(imm0), uint16(imm1))
}

func (c *compilestate) ops16s16s16(v *value, op bcop, imm0, imm1, imm2 stackslot) {
	c.opu16u16u16(v, op, uint16(imm0), uint16(imm1), uint16(imm2))
}

func (c regclass) String() string {
	switch c {
	case regK:
		return "K"
	case regS:
		return "S"
	case regB:
		return "B"
	case regV:
		return "V"
	case regH:
		return "H"
	case regL:
		return "L"
	default:
		return "?"
	}
}

func (c regclass) loadop() bcop {
	switch c {
	case regK:
		return oploadk
	case regV:
		return oploadv
	case regB:
		return oploadb
	case regS:
		return oploads
	default:
		panic("invalid register class for load")
	}
}

func (c regclass) saveop() bcop {
	switch c {
	case regK:
		return opsavek
	case regV:
		return opsavev
	case regB:
		return opsaveb
	case regS:
		return opsaves
	default:
		panic("invalid register class for save")
	}
}

func ionType(imm interface{}) ion.Type {
	switch i := imm.(type) {
	case ion.Datum:
		return i.Type()
	case float64, float32:
		return ion.FloatType
	case uint64, int64, int:
		return ion.IntType
	case string:
		return ion.StringType
	case date.Time:
		return ion.TimestampType
	default:
		return 0
	}
}

func (c *compilestate) litcmp(v *value, i interface{}) {
	if b, ok := i.(bool); ok {
		// we have built-in ops for these!
		if b {
			c.op(v, opistrue)
		} else {
			c.op(v, opisfalse)
		}
		return
	}

	// if we get a datum object,
	// then encode it verbatim;
	// otherwise try to convert
	// to a datum...
	d, ok := i.(ion.Datum)
	if !ok {
		switch i := i.(type) {
		case float64:
			d = ion.Float(i)
		case float32:
			d = ion.Float(float64(i)) // TODO: maybe don't convert here...
		case int64:
			d = ion.Int(i)
		case int:
			d = ion.Int(int64(i))
		case uint64:
			d = ion.Uint(i)
		case string:
			d = ion.String(i)
		case []byte:
			d = ion.Blob(i)
		case date.Time:
			d = ion.Timestamp(i)
		default:
			panic("type not supported for literal comparison")
		}
	}
	var b ion.Buffer
	var st ion.Symtab // TODO: pass input symbol table in here!
	d.Encode(&b, &st)
	c.valuecmp(v, b.Bytes())
}

func (c *compilestate) valuecmp(v *value, lit []byte) {
	c.opu32(v, opleneq, uint32(len(lit)))
	resetoff := false
	for len(lit) >= 8 {
		op := opeqv8plus
		if !resetoff {
			op = opeqv8
			resetoff = true
		}
		c.opu64(v, op, binary.LittleEndian.Uint64(lit))
		lit = lit[8:]
	}
	for len(lit) >= 4 {
		op := opeqv4maskplus
		if !resetoff {
			op = opeqv4mask
			resetoff = true
		}
		lo := binary.LittleEndian.Uint32(lit)
		c.opu32u32(v, op, lo, 0xFFFFFFFF)
		lit = lit[4:]
	}
	if len(lit) > 0 {
		var buf [4]byte
		copy(buf[:], lit)
		op := opeqv4mask
		if resetoff {
			op = opeqv4maskplus
		}
		word := binary.LittleEndian.Uint32(buf[:])
		mask := uint32(0xFFFFFFFF) >> (32 - (len(lit) * 8))
		c.opu32u32(v, op, word, mask)
	}
}

// default behavior for finalizing the
// current compile state given a value:
// dereference its argument stack slots,
// and set the current registers to whatever
// the instruction outputs
func (c *compilestate) final(v *value) {
	if v.op == smergemem {
		return
	}
	if v.op == sundef {
		c.regs.cur[regS] = -1
		return
	}
	info := &ssainfo[v.op]
	for i := range v.args {
		arg := v.args[i]
		argType := info.argType(i)
		if argType == stMem {
			continue
		}

		rng := c.lr.krange
		rc := regK
		if argType != stBool {
			rng = c.lr.vrange
			rc = regV
		}
		if rng[arg.id] < v.id {
			panic("arg not live up to use?")
		}
		// anything live only up to here is now dead
		if rng[arg.id] == v.id && c.regs.hasSlot(rc, arg.id) {
			c.regs.freeValue(rc, arg.id)
		}
	}
	rettypes := info.rettype.vregs()
	for i := regclass(0); i < _maxregclass; i++ {
		if rettypes.contains(i) {
			c.regs.cur[i] = v.id
		}
	}
}

func (c *compilestate) clobberk(v *value) {
	cur := c.regs.cur[regK]
	if cur >= 0 && c.lr.krange[cur] > v.id && !c.regs.hasSlot(regK, cur) {
		c.ops16(v, opsavek, c.regs.allocValue(regK, cur))
	}
}

// at value v, get the H register number of arg
//
// if arg is only live up to this instruction,
// then it will be dropped from the register
func (c *compilestate) href(v, arg *value) stackslot {
	ret := c.regs.slotOf(regH, arg.id)
	if c.lr.vrange[arg.id] == v.id {
		// value is no longer live, so free the slot
		c.regs.freeValue(regH, arg.id)
	}
	return ret
}

// allocate an H register number for value v
func (c *compilestate) hput(v *value) stackslot {
	return c.regs.allocValue(regH, v.id)
}

// load a value into the k reg
//
// if the value 'v' is known to clobber 'k'
// then the loaded value is also saved automatically
func (c *compilestate) loadk(v, k *value) {
	curmask := c.regs.cur[regK]
	clobber := k.id != curmask || v.ret().vregs().contains(regK)
	if clobber && c.lr.krange[curmask] > v.id && !c.regs.hasSlot(regK, curmask) {
		// shortcut: if we need to do a save *and* restore
		// and we are restoring a value that will be dead
		// after this instruction, perform an xchg instead
		if c.lr.krange[k.id] == v.id {
			slot := c.regs.slotOf(regK, k.id)
			c.regs.stack.replaceValue(regK, k.id, curmask)
			c.ops16(v, opxchgk, slot)
			c.regs.cur[regK] = k.id
			return
		}
		c.ops16(v, opsavek, c.regs.allocValue(regK, curmask))
	}
	if k.id != curmask {
		c.regs.cur[regK] = k.id
		c.ops16(v, oploadk, c.existingStackRef(k, regK))
	}
}

func (c *compilestate) loadclass(v, arg *value, rc regclass) {
	cur := c.regs.cur[rc]
	if arg.op == sundef {
		// for undef, we don't care what's in the register;
		// just consider it clobbered
		c.clobberclass(v, rc)
		c.regs.cur[rc] = -1
		return
	}
	if arg.id != cur {
		c.clobberclass(v, rc)
		// we don't bother doing anything if
		// the argument is 'undef'; we can keep
		// using whatever happens to be in the
		// register
		if arg.op != skfalse {
			c.ops16(v, rc.loadop(), c.existingStackRef(arg, rc))
		}
		c.regs.cur[rc] = arg.id
	}
}

func (c *compilestate) clobberclass(v *value, rc regclass) {
	cur := c.regs.cur[rc]
	if cur >= 0 && c.lr.vrange[cur] > v.id && !c.regs.hasSlot(rc, cur) {
		slot := c.regs.allocValue(rc, cur)
		c.ops16(v, rc.saveop(), slot)
	}
}

func (c *compilestate) loadv(v, arg *value) {
	c.loadclass(v, arg, regV)
}

func (c *compilestate) clobberv(v *value) {
	c.clobberclass(v, regV)
}

func (c *compilestate) loads(v, arg *value) {
	c.loadclass(v, arg, regS)
}

func (c *compilestate) clobbers(v *value) {
	c.clobberclass(v, regS)
}

func (c *compilestate) loadb(v, arg *value) {
	c.loadclass(v, arg, regB)
}

func (c *compilestate) clobberb(v *value) {
	c.clobberclass(v, regB)
}

func (c *compilestate) existingStackRef(arg *value, rc regclass) stackslot {
	slot := c.regs.slotOf(rc, arg.id)
	if slot == invalidstackslot {
		panic(fmt.Sprintf("Cannot get a stack slot of %v, which is not allocated", arg))
	}
	return slot
}

// Returns a stack id of the given value. If the value was never
// saved on stack it would add a bytecode instruction to save it
// so it always exists.
func (c *compilestate) forceStackRef(v *value, rc regclass) stackslot {
	slot := c.regs.slotOf(rc, v.id)
	if slot != invalidstackslot {
		return slot
	}

	slot = c.regs.allocValue(rc, v.id)
	c.ops16(v, rc.saveop(), slot)
	return slot
}

func emitinit(v *value, c *compilestate) {}

type rawDatum []byte

func emitconst(v *value, c *compilestate) {
	var b ion.Buffer
	switch t := v.imm.(type) {
	case nil:
		b.WriteNull()
	case float64:
		b.WriteFloat64(t)
	case float32:
		b.WriteFloat32(t)
	case int:
		b.WriteInt(int64(t))
	case int64:
		b.WriteInt(t)
	case uint64:
		b.WriteUint(t)
	case uint:
		b.WriteUint(uint64(t))
	case bool:
		b.WriteBool(t)
	case string:
		b.WriteString(t)
	case date.Time:
		b.WriteTime(t)
	case rawDatum:
		b.Set([]byte(t))
	default:
		panic("unsupported literal datum")
	}
	off := len(c.litbuf)
	c.litbuf = append(c.litbuf, b.Bytes()...)

	// clobber V register
	c.clobberv(v)
	c.opu32u32(v, oplitref, uint32(off), uint32(len(b.Bytes())))
}

func emitfalse(v *value, c *compilestate) {
	c.clobberk(v)
	c.clobberv(v)
	c.op(v, opfalse)
}

// AND / OR / XOR / XNOR
// reads mask, writes to mask
func emitlogical(v *value, c *compilestate) {
	// pick actual argument ordering based
	// on the register state
	lhs, rhs := v.args[0], v.args[1]
	bc := ssainfo[v.op].bc
	if c.regs.cur[regK] == lhs.id {
		lhs, rhs = rhs, lhs
	}
	c.loadk(v, rhs)
	c.ops16(v, bc, c.existingStackRef(lhs, regK))
}

// emit NAND operation
func emitnand(v *value, c *compilestate) {
	// x nand true -> !x
	if v.args[1].op == sinit {
		// just emit 'not'
		c.loadk(v, v.args[0])
		c.op(v, opnotk)
		return
	}

	// we want to compute 'mask = ^lhs & rhs'
	lhs, rhs := v.args[0], v.args[1]

	if lhs == rhs {
		c.op(v, opfalse)
		return
	}

	if c.regs.cur[regK] == rhs.id {
		c.loadk(v, rhs)
		c.ops16(v, opnandk, c.existingStackRef(lhs, regK))
	} else {
		c.loadk(v, lhs)
		c.ops16(v, opandnotk, c.existingStackRef(rhs, regK))
	}
}

// tuple destructure into base pointer
func emittuple(v *value, c *compilestate) {
	value := v.args[0]
	mask := v.args[1]
	bc := ssainfo[v.op].bc
	c.loadk(v, mask)
	c.loadv(v, value)
	c.clobberb(v)
	c.op(v, bc)
}

func emitsplit(v *value, c *compilestate) {
	list := v.args[0]
	mask := v.args[1]
	c.loadk(v, mask)
	c.loads(v, list)
	c.clobberv(v)
	c.clobbers(v)
	c.op(v, opsplit)
}

func emithashlookup(v *value, c *compilestate) {
	h := v.args[0]
	k := v.args[1]
	hSlot := c.existingStackRef(h, regH)
	tslot := uint16(len(c.trees))
	hr := v.imm.(*hashResult)
	c.trees = append(c.trees, hr.tree)
	if len(c.litbuf) != 0 {
		// adjust the negated offsets of the literals
		// to reflect their final positions in c.litbuf
		base := uint32(len(c.litbuf))
		for i := 0; i < len(hr.tree.values); i += (aggregateTagSize + 8) {
			val := binary.LittleEndian.Uint32(hr.tree.values[i+aggregateTagSize:])
			val += base
			binary.LittleEndian.PutUint32(hr.tree.values[i+aggregateTagSize:], val)
		}
	}
	c.litbuf = append(c.litbuf, hr.literals...)
	c.loadk(v, k)
	c.clobberv(v)
	c.ops16u16(v, ssainfo[v.op].bc, hSlot, tslot)
}

func emithashmember(v *value, c *compilestate) {
	h := v.args[0]
	k := v.args[1]
	hSlot := c.existingStackRef(h, regH)
	tslot := uint16(len(c.trees))
	c.trees = append(c.trees, v.imm.(*radixTree64))
	c.loadk(v, k)
	c.ops16u16(v, ssainfo[v.op].bc, hSlot, tslot)
}

// seek inside structure
func emitdot2(v *value, c *compilestate) {
	base := v.args[0]
	val := v.args[1]
	addmask := v.args[2]
	mask := v.args[3]
	sym := v.imm.(ion.Symbol)

	c.loadb(v, base)
	c.loadv(v, val)
	// FIXME: if the current mask value
	// is 'addmask' then we should just
	// use a different header instruction
	c.clobberv(v)

	// coalesced version: mask arguments are identical
	// (the mask argument may never have been assigned
	// a stack slot, so we cannot provide an immediate
	// mask argument)
	if mask == addmask {
		c.loadk(v, mask)
		c.clobberk(v)
		c.opu32(v, opfindsym3, uint32(sym))
		return
	}
	if c.regs.cur[regK] == addmask.id {
		c.loadk(v, addmask)
		c.clobberk(v)
		c.ops16u32(v, opfindsym2rev, c.existingStackRef(mask, regK), uint32(sym))
		return
	}
	c.loadk(v, mask)
	c.clobberk(v)
	c.ops16u32(v, opfindsym2, c.existingStackRef(addmask, regK), uint32(sym))
}

// the '.' operator
// reads base & mask, writes to value & mask
func emitdot(v *value, c *compilestate) {
	base := v.args[0]
	mask := v.args[1]
	c.clobberv(v)
	c.loadk(v, mask)
	c.loadb(v, base)
	c.opu32(v, opfindsym, uint32(v.imm.(ion.Symbol)))
}

func emitboolconv(v *value, c *compilestate) {
	input := v.args[0]
	notmissing := v.args[1]

	// if the current register value is
	// 'notmissing' and we clobber it
	// when loading 'input', it will not be
	// saved since its live range only extends
	// to this instruction; force it to be
	// stack-allocated so that it can be re-loaded
	// after the instruction has executed
	_ = c.forceStackRef(notmissing, regK)

	c.loadk(v, input)
	c.clobbers(v)
	c.op(v, ssainfo[v.op].bc)
	c.loadk(v, notmissing)
}

func emitslice(v *value, c *compilestate) {
	var t ion.Type
	switch v.op {
	case stostr:
		t = ion.StringType
	case stolist:
		t = ion.ListType
	case stotime:
		t = ion.TimestampType
	case stoblob:
		t = ion.BlobType
	default:
		panic("unrecognized op for emitslice")
	}
	val := v.args[0]
	mask := v.args[1]
	c.loadk(v, mask)
	c.loadv(v, val)
	c.clobbers(v)
	c.opu8(v, opunpack, uint8(t))
}

// compare arg0 and arg1
func emitcmp(v *value, c *compilestate) {
	lhs := v.args[0]
	rhs := v.args[1]
	mask := v.args[2]
	op := v.op
	// comparison ops implicitly expect
	// the rhs to live in a stack slot;
	// reverse the sense of the comparison
	// if the rhs is in registers
	if c.regs.cur[regS] == rhs.id {
		op = ssainfo[op].inverse
		lhs, rhs = rhs, lhs
	}
	c.loadk(v, mask)
	c.loads(v, lhs)
	bc := ssainfo[op].bc
	c.ops16(v, bc, c.existingStackRef(rhs, regS))
}

// emit constant comparison
// reads value & immediate & mask, writes to mask
func emitconstcmp(v *value, c *compilestate) {
	val := v.args[0]
	mask := v.args[1]
	imm := v.imm
	c.loadk(v, mask)
	c.loadv(v, val)
	c.litcmp(v, imm)
}

func emitequalv(v *value, c *compilestate) {
	arg0 := v.args[0]
	arg1 := v.args[1]
	mask := v.args[2]
	c.loadk(v, mask)
	if c.regs.cur[regV] == arg1.id {
		arg0, arg1 = arg1, arg0
	}
	c.loadv(v, arg0)
	c.ops16(v, opequalv, c.existingStackRef(arg1, regV))
}

func (c *compilestate) dictimm(str string) uint16 {
	n := -1
	for i := range c.dict {
		if c.dict[i] == str {
			n = i
			break
		}
	}
	if n == -1 {
		n = len(c.dict)
		c.dict = append(c.dict, str)
	}
	if n > 65535 {
		panic("immediate for opu16 opcode > 65535!")
	}
	return uint16(n)
}

func emitConcatStr(v *value, c *compilestate) {
	argCount := len(v.args) - 1

	var slots [4]stackslot
	mask := v.args[argCount]

	for i := 0; i < argCount; i++ {
		slots[i] = c.forceStackRef(v.args[i], regS)
	}

	c.loadk(v, mask)
	c.loads(v, v.args[0])
	c.clobbers(v)

	switch argCount {
	case 2:
		c.ops16(v, opconcatlenget2, slots[1])
	case 3:
		c.ops16s16(v, opconcatlenget3, slots[1], slots[2])
	case 4:
		c.ops16s16s16(v, opconcatlenget4, slots[1], slots[2], slots[3])
	}

	c.op(v, opallocstr)

	for i := 0; i < argCount; i++ {
		c.ops16(v, opappendstr, slots[i])
	}
}

func emitStrEditStack1(v *value, c *compilestate) {
	str := v.args[0]
	imm := c.forceStackRef(v.args[1], regS)
	mask := v.args[2]

	c.loadk(v, mask)
	c.loads(v, str)
	c.clobbers(v)
	c.ops16(v, ssainfo[v.op].bc, imm)
}

func emitStrEditStack1x1(v *value, c *compilestate) {
	str := v.args[0]
	imm1 := c.forceStackRef(v.args[1], regS)
	imm2 := c.dictimm(v.imm.(string))
	mask := v.args[2]

	c.loadk(v, mask)
	c.loads(v, str)
	c.clobbers(v)
	c.opu16s16(v, ssainfo[v.op].bc, imm2, imm1)
}

func emitStrEditStack2(v *value, c *compilestate) {
	str := v.args[0]
	substrOffsetSlot := c.forceStackRef(v.args[1], regS)
	substrLengthSlot := c.forceStackRef(v.args[2], regS)
	mask := v.args[3]

	c.loadk(v, mask)
	c.loads(v, str)
	c.clobbers(v)
	c.ops16s16(v, ssainfo[v.op].bc, substrOffsetSlot, substrLengthSlot)
}

func emitBinaryALUOp(v *value, c *compilestate) {
	arg0 := v.args[0] // left
	arg1 := v.args[1] // right
	mask := v.args[2] // predicate

	info := ssainfo[v.op]
	bc := info.bc

	if info.bcrev != 0 && c.regs.cur[regS] == arg1.id {
		arg0, arg1 = arg1, arg0
		bc = info.bcrev
	}

	slot1 := c.forceStackRef(arg1, regS)

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16(v, bc, slot1)
}

func emitAggK(v *value, c *compilestate) {
	boolValSlot := c.forceStackRef(v.args[1], regK)
	mask := v.args[2]

	c.loadk(v, mask)

	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit2(op, 2, 4)
	c.asm.emitOpcode(op)
	c.asm.emitImmU16(uint16(boolValSlot))
	c.asm.emitImmU32(uint32(v.imm.(aggregateslot)))
}

func emitSlotAggK(v *value, c *compilestate) {
	boolValSlot := c.forceStackRef(v.args[2], regK)
	mask := v.args[3]

	c.loadk(v, mask)
	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit2(op, 4, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU32(uint32(v.imm.(aggregateslot)))
	c.asm.emitImmU16(uint16(boolValSlot))
}

func emitboxmask(v *value, c *compilestate) {
	truefalse := v.args[0]
	output := v.args[1]

	// we must have scratch space available
	// during program execution
	c.clobberv(v)

	// if the truefalse and output masks
	// are the same, then use the same-argument version
	if truefalse == output {
		c.loadk(v, truefalse)
		c.op(v, opboxmask3)
		return
	}

	// if the current K reg is true/false,
	// then use the reversed-argument version
	// (but note that K is clobbered now)
	if c.regs.cur[regK] == truefalse.id {
		c.loadk(v, truefalse)
		c.ops16(v, opboxmask2, c.existingStackRef(output, regK))
		return
	}

	// output mask is in K1,
	// true/false mask is on the stack
	c.loadk(v, output)
	c.ops16(v, opboxmask, c.existingStackRef(truefalse, regK))
}

func emitstorev(v *value, c *compilestate) {
	_ = v.args[0] // mem
	arg := v.args[1]
	mask := v.args[2]
	slot := v.imm.(int)

	if mask.op == skfalse {
		// don't care what is in the V register;
		// we are just zeroing the memory
		c.ops16(v, opzerov, stackslot(slot))
		return
	}

	c.loadk(v, mask)
	if c.regs.cur[regV] != arg.id {
		c.ops16s16(v, opdupv, c.existingStackRef(arg, regV), stackslot(slot))
		return
	}

	c.ops16(v, opsavezerov, stackslot(slot))
}

func emitstores(v *value, c *compilestate) {
	_ = v.args[0]
	arg := v.args[1]
	mask := v.args[2]
	slot := v.imm.(int)

	c.loadk(v, mask)
	if mask.op != skfalse && c.regs.cur[regS] != arg.id {
		c.ops16s16(v, opdupv, c.existingStackRef(arg, regV), stackslot(slot))
		return
	}
	c.ops16(v, opsavezeros, stackslot(slot))
}

// blend value in V register
func emitblendv(v *value, c *compilestate) {
	cur := v.args[0]
	extra := v.args[1]
	k := v.args[2]

	bc := ssainfo[v.op].bc
	stackarg := extra
	regarg := cur
	c.loadk(v, k)
	// if the argument order is reversed,
	// then pick the reversed opcode and
	// flip which argument we load from
	// the stack
	if c.regs.cur[regV] == extra.id {
		stackarg, regarg = regarg, stackarg
		bc++
	}
	c.loadv(v, regarg)
	c.clobberv(v)
	c.ops16(v, bc, c.existingStackRef(stackarg, regV))
}

// blend value in S register
func emitblends(v *value, c *compilestate) {
	cur := v.args[0]
	extra := v.args[1]
	k := v.args[2]
	c.loadk(v, k)
	bc := ssainfo[v.op].bc
	stackarg := extra
	regarg := cur
	if c.regs.cur[regS] == extra.id {
		stackarg, regarg = regarg, stackarg
		bc++
	}
	c.loads(v, regarg)
	c.clobbers(v)
	c.ops16(v, bc, c.existingStackRef(stackarg, regS))
}

// generic emit function for tuple-construction;
// all we need to do is ensure that the arguments
// to the op are in registers
func emittuple2regs(v *value, c *compilestate) {
	argtypes := ssainfo[v.op].argtypes
	for i := range v.args {
		at := argtypes[i]
		arg := v.args[i]
		switch at {
		case stBase:
			c.clobberb(v)
			c.loadb(v, arg)
		case stValue:
			c.clobberv(v)
			c.loadv(v, arg)
		case stBool:
			c.loadk(v, arg)
		case stInt, stFloat, stString, stTime, stScalar:
			c.clobbers(v)
			c.loads(v, arg)
		case stMem:
			// ignore; this is just an ordering dependency
		case stHash:
			// H register number of output is
			// equivalent to input
			v.imm = arg.imm
		default:
			panic("invalid instruction type spec")
		}
	}
}

func emitchecktag(v *value, c *compilestate) {
	val := v.args[0]
	k := v.args[1]
	imm := v.imm.(uint16)
	c.loadv(v, val)
	c.loadk(v, k)
	c.clobberv(v)
	c.opu16(v, opchecktag, imm)
}

// emit comparison for timestamps
func emitcmptm(v *value, c *compilestate) {
	tm := v.args[0]
	mask := v.args[1]

	var buf ion.Buffer
	buf.WriteTime(v.imm.(date.Time))
	offset := c.dictimm(string(buf.Bytes()))

	c.loadk(v, mask)
	c.loads(v, tm)

	// emitted as a two-instruction sequence:
	// first, load the two timestamp sequences
	// into registers, and then dispatch
	// the actual comparison op
	bc := ssainfo[v.op].bc
	c.opu16(v, opconsttm, offset)
	c.op(v, bc)
}

// emit code for either extract or date trunc
func emittmwithconst(v *value, c *compilestate) {
	val := v.args[0]
	msk := v.args[1]

	c.loadk(v, msk)
	c.loads(v, val)
	c.clobbers(v)
	c.opu8(v, ssainfo[v.op].bc, uint8(v.imm.(int)))
}

func emitauto(v *value, c *compilestate) {
	info := &ssainfo[v.op]
	if info.bc == 0 {
		panic("emitauto doesn't work if ssainfo.bc is not set")
	}
	if len(v.args) != len(info.argtypes) {
		panic("argument count mismatch")
	}
	clobbers := v.ret().vregs()
	var allregs regset
	for i := range v.args {
		if v.args[i].op == sundef {
			continue
		}
		// check that this argument has a trivial
		// input register+type that we haven't already used
		rt := v.args[i].ret() & info.argtypes[i]
		if rt == stMem {
			// memory arguments are just for ordering
			continue
		}
		reg := rt.vregs()
		if allregs&reg != 0 {
			panic("cannot emitauto operations with overlapping input/output registers")
		}
		allregs |= reg

		switch reg {
		case onlyreg(regK):
			c.loadk(v, v.args[i])
			clobbers.del(regK)
		case onlyreg(regS):
			c.loads(v, v.args[i])
			if clobbers.contains(regS) {
				if v.args[i].op != sundef {
					c.clobbers(v)
				}
			}
			clobbers.del(regS)
		case onlyreg(regV):
			c.loadv(v, v.args[i])
			if clobbers.contains(regV) {
				c.clobberv(v)
			}
			clobbers.del(regV)
		case onlyreg(regB):
			c.loadb(v, v.args[i])
			if clobbers.contains(regB) {
				c.clobberb(v)
			}
			clobbers.del(regB)
		case onlyreg(regH):
			if v.imm != nil {
				panic("emitauto: cannot handle hash input")
			}
			// the immediate is the H register number
			// of the hash argument
			v.imm = int(c.href(v, v.args[i]))
		case onlyreg(regL):
			if c.regs.cur[regL] != v.args[i].id {
				panic("L register clobbered?")
			}
		default:
			// do nothing; must already be loaded
		}
	}
	if v.op == sundef {
		clobbers.del(regS)
	}
	// before emitting the op,
	// save the current value of the
	// result register if we haven't already
	for i := regclass(0); i < _maxregclass; i++ {
		if clobbers == 0 {
			break
		}
		if !clobbers.contains(i) {
			continue
		}
		switch i {
		case regK:
			c.clobberk(v)
		case regH:
			// just allocate an output register
			slot := c.hput(v)
			if v.imm == nil {
				v.imm = int(slot)
			}
		case regS, regV, regB:
			c.clobberclass(v, i)
		default:
			// nothing
		}
		clobbers.del(i)
	}

	// finally, emit the op
	switch info.immfmt {
	case fmtnone:
		c.op(v, info.bc)
	case fmtslot:
		c.ops16(v, info.bc, stackslot(v.imm.(int)))
	case fmtaggslot:
		c.opu32(v, info.bc, uint32(v.imm.(aggregateslot)))
	case fmtbool:
		c.opu8(v, info.bc, uint8(toi64(v.imm)))
	case fmti64:
		c.opu64(v, info.bc, toi64(v.imm))
	case fmtf64:
		c.opu64(v, info.bc, math.Float64bits(tof64(v.imm)))
	case fmtdict:
		c.opu16(v, info.bc, c.dictimm(v.imm.(string)))
	case fmtslotx2hash:
		// encode input offset + output offset; just for functions that output a hash value
		c.ops16s16(v, info.bc, stackslot(v.imm.(int)), c.existingStackRef(v, regH))
	default:
		panic("unsupported immfmt for emitauto")
	}
}

// emitauto2 is a generic emitter that emits bytecode based on both SSA and BC info tables
//
// If the SSA/BC instruction is predicated, and the predicate is the last argument, it's
// allocated to a predicate register. The remaining arguments are allocated from left to
// right - the first argument of a specific register type is allocated in register, the
// remaining arguments are passed via stack slots.
func emitauto2(v *value, c *compilestate) {
	info := &ssainfo[v.op]
	argCount := len(v.args)

	if info.bc == 0 {
		panic("emitauto doesn't work if ssainfo.bc is not set")
	}

	if argCount != len(info.argtypes) {
		panic("argument count mismatch")
	}

	var slots []stackslot
	var kRegArg *value
	var sRegArg *value
	var vRegArg *value
	var bRegArg *value

	// process active lanes predicate first, as this is supposed to be in K1
	if argCount > 0 && info.argtypes[argCount-1] == stBool {
		arg := v.args[argCount-1]

		if (arg.ret() & stBool) == 0 {
			panic("Invalid argument found during SSA to BC lowering")
		}

		// Remove this argument from further processing as it's done
		kRegArg = arg
		argCount--
	}

	for i := 0; i < argCount; i++ {
		arg := v.args[i]
		infoArgType := info.argtypes[i]

		if arg.op == sundef {
			continue
		}

		ret := arg.ret() & infoArgType
		if ret == 0 {
			panic(fmt.Sprintf("argument %d doesn't reflect the required argument", i))
		}

		// memory arguments are just for ordering
		if ret == stMem {
			continue
		}

		reg := ret.vregs()

		switch reg {
		case onlyreg(regK):
			if kRegArg == nil {
				kRegArg = arg
			} else {
				slots = append(slots, c.forceStackRef(arg, regK))
			}
		case onlyreg(regS):
			if sRegArg == nil {
				sRegArg = arg
			} else {
				slots = append(slots, c.forceStackRef(arg, regS))
			}
		case onlyreg(regV):
			if vRegArg == nil {
				vRegArg = arg
			} else {
				slots = append(slots, c.forceStackRef(arg, regV))
			}
		case onlyreg(regB):
			if bRegArg == nil {
				bRegArg = arg
			} else {
				slots = append(slots, c.forceStackRef(arg, regB))
			}
		case onlyreg(regL):
			if c.regs.cur[regL] != arg.id {
				panic("L register cannot be clobbered")
			}
		default:
			panic("Unhandled register type")
		}
	}

	opInfo := &opinfo[info.bc]
	opFlags := opInfo.flags

	if kRegArg != nil {
		c.loadk(v, kRegArg)
	}

	if sRegArg != nil {
		c.loads(v, sRegArg)
	}

	if vRegArg != nil {
		c.loadv(v, vRegArg)
	}

	if bRegArg != nil {
		c.loadb(v, bRegArg)
	}

	if (opFlags & bcWriteK) != 0 {
		c.clobberk(v)
	}

	if (opFlags & bcWriteS) != 0 {
		c.clobbers(v)
	}

	if (opFlags & bcWriteV) != 0 {
		c.clobberv(v)
	}

	if (opFlags & bcWriteB) != 0 {
		c.clobberb(v)
	}

	c.opvar(info.bc, slots, v.imm)
}

func emitMakeList(v *value, c *compilestate) {
	imms := make([]uint64, 0, (len(v.args)-1)*2)
	for i := 1; i < len(v.args); i += 2 {
		imms = append(imms, uint64(c.forceStackRef(v.args[i], regV)), uint64(c.forceStackRef(v.args[i+1], regK)))
	}

	info := &ssainfo[v.op]
	op := info.bc

	c.loadk(v, v.args[0])
	c.clobberk(v)
	c.clobberv(v)
	c.opva(op, imms)
}

func encodeSymbolIDForMakeStruct(id ion.Symbol) uint32 {
	if id >= (1<<28)-1 {
		panic(fmt.Sprintf("symbol id too large: %d", id))
	}

	encoded := uint32((id & 0x7F) | 0x80)
	id >>= 7
	for id != 0 {
		encoded = (encoded << 8) | (uint32(id) & 0x7F)
		id >>= 7
	}
	return encoded
}

func emitMakeStruct(v *value, c *compilestate) {
	j := 0
	orderedSymbols := make([]uint64, (len(v.args)-1)/3)
	for i := 1; i < len(v.args); i += 3 {
		key := v.args[i]
		if key.op != smakestructkey {
			panic(fmt.Sprintf("invalid value '%v' in struct composition, only 'smakestructkey' expected", key.op))
		}
		sym := key.imm.(ion.Symbol)
		orderedSymbols[j] = (uint64(sym) << 32) | uint64(i)
		j++
	}
	slices.Sort(orderedSymbols)

	imms := make([]uint64, 0, (len(v.args) - 1))
	for _, orderedSymbol := range orderedSymbols {
		i := int(orderedSymbol & 0xFFFFFFFF)
		sym := ion.Symbol(orderedSymbol >> 32)

		val := v.args[i+1]
		mask := v.args[i+2]

		imms = append(imms,
			uint64(encodeSymbolIDForMakeStruct(sym)),
			uint64(c.forceStackRef(val, regV)),
			uint64(c.forceStackRef(mask, regK)))
	}

	info := &ssainfo[v.op]
	op := info.bc

	c.loadk(v, v.args[0])
	c.clobberk(v)
	c.clobberv(v)
	c.opva(op, imms)
}

func emitStringCaseChange(opcode bcop) func(*value, *compilestate) {
	return func(v *value, c *compilestate) {
		arg := v.args[0]  // string
		mask := v.args[1] // predicate

		originalInput := c.forceStackRef(arg, regS) // copy input refrence

		c.loadk(v, mask)
		c.loads(v, arg)
		c.clobbers(v)

		c.op(v, opconcatlenget1)
		c.op(v, opsadjustsize)
		c.op(v, opallocstr)
		c.ops16(v, opcode, originalInput)
	}
}

func emitaggapproxcount(v *value, c *compilestate) {
	hash := v.args[0]
	if hash.op == skfalse {
		v.setfalse()
		return
	}
	mask := v.args[1]
	hashSlot := c.existingStackRef(hash, regH)

	imm := v.imm.(uint64)
	aggSlot := imm >> 8
	precision := uint8(imm)

	c.loadk(v, mask)

	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit3(op, 8, 2, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU64(aggSlot)
	c.asm.emitImmU16(uint16(hashSlot))
	c.asm.emitImmU16(uint16(precision))
}

func emitaggapproxcountmerge(v *value, c *compilestate) {
	blob := v.args[0]
	mask := v.args[1]

	imm := v.imm.(uint64)
	aggSlot := imm >> 8
	precision := uint8(imm)

	c.loadk(v, mask)
	c.loads(v, blob)

	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit2(op, 8, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU64(aggSlot)
	c.asm.emitImmU16(uint16(precision))
}

func emitaggslotapproxcount(v *value, c *compilestate) {
	hash := v.args[2]
	if hash.op == skfalse {
		v.setfalse()
		return
	}
	mask := v.args[3]
	hashSlot := c.existingStackRef(hash, regH)

	imm := v.imm.(uint64)
	aggSlot := imm >> 8
	precision := uint8(imm)

	c.loadk(v, mask)

	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit3(op, 8, 2, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU64(aggSlot)
	c.asm.emitImmU16(uint16(hashSlot))
	c.asm.emitImmU16(uint16(precision))
}

func emitaggslotapproxcountmerge(v *value, c *compilestate) {
	blob := v.args[2]
	mask := v.args[3]

	imm := v.imm.(uint64)
	aggSlot := imm >> 8
	precision := uint8(imm)

	c.loadk(v, mask)
	c.loads(v, blob)

	op := ssainfo[v.op].bc
	checkImmediateBeforeEmit2(op, 8, 2)
	c.asm.emitOpcode(op)
	c.asm.emitImmU64(aggSlot)
	c.asm.emitImmU16(uint16(precision))
}

func (p *prog) emit1(v *value, c *compilestate) {
	defer func() {
		if err := recover(); err != nil {
			println(fmt.Sprintf("Error emitting %v: %v", v.String(), err))
			p.writeTo(os.Stderr)
			panic(err)
		}
	}()
	info := &ssainfo[v.op]
	emit := info.emit
	if emit == nil {
		emit = emitauto
	}
	emit(v, c)
	c.final(v)
}

// reserve stack slots for any stores that
// are explicitly performed
func (p *prog) reserveslots(c *compilestate) {
	for i := range p.reserved {
		c.regs.stack.reserveSlot(regV, p.reserved[i])
	}
}

func (p *prog) compile(dst *bytecode, st *symtab) error {
	var c compilestate

	if err := p.compileinto(&c); err != nil {
		return err
	}

	dst.vstacksize = c.regs.stack.stackSize(stackTypeV)
	dst.hstacksize = c.regs.stack.stackSize(stackTypeH)

	dst.allocStacks()
	dst.trees = c.trees
	dst.dict = c.dict
	dst.compiled = c.asm.grabCode()

	reserve := c.asm.scratchuse + len(c.litbuf)
	if reserve > PageSize {
		reserve = PageSize
	}
	dst.savedlit = c.litbuf
	dst.scratchtotal = reserve
	dst.restoreScratch(st) // populate everything
	return dst.finalize()
}

func (p *prog) compileinto(c *compilestate) error {
	var inval []*value
	for _, v := range p.values {
		if v.op == sinvalid {
			inval = append(inval, v)
		}
	}
	if len(inval) > 0 {
		if len(inval) == 1 {
			return fmt.Errorf("ill-typed ssa: %v", inval[0].imm)
		}
		return fmt.Errorf("ill-typed ssa: %s (and %d more errors)", inval[0].imm.(string), len(inval)-1)
	}

	p.liveranges(&c.lr)
	c.regs.init(len(p.values))
	p.reserveslots(c)

	for i := range p.values {
		v := p.values[i]
		p.emit1(v, c)
	}

	return nil
}

func (p *prog) clone(dst *prog) {
	dst.values = make([]*value, len(p.values))

	// first pass: copy the values literally
	for i := range p.values {
		v := p.values[i]
		if v.id != i {
			panic("prog.clone() before prog.Renumber()")
		}
		// NOTE: we're assuming here that
		// v.imm is a value like an int
		// or a string that is trivially
		// copied; if we ever use pointer-typed
		// immediates we would probably want
		// to deep-copy that here too...
		nv := new(value)
		dst.values[i] = nv
		*nv = *v
	}

	// second pass: update arguments
	for i := range dst.values {
		v := dst.values[i]
		args := make([]*value, len(v.args))
		copy(args, v.args)
		v.args = args
		for j, arg := range v.args {
			real := dst.values[arg.id]
			v.args[j] = real
		}
	}
	dst.reserved = make([]stackslot, len(p.reserved))
	copy(dst.reserved, p.reserved)
	dst.ret = dst.values[p.ret.id]
}

// MaxSymbolID is the largest symbol ID
// supported by the system.
const MaxSymbolID = (1 << 21) - 1

// Symbolize applies the symbol table from 'st'
// to the program by copying the old program
// to 'dst' and applying rewrites to findsym operations.
func (p *prog) cloneSymbolize(st syms, dst *prog, aux *auxbindings) error {
	p.clone(dst)
	return dst.symbolize(st, aux)
}

// unsymbolized takes an stValue-typed instruction
// and ensures that the result is never a symbol
func (p *prog) unsymbolized(v *value) *value {
	switch v.op {
	case sdot, sdot2, ssplit, sauxval:
		return p.ssa2(sunsymbolize, v, p.mask(v))
	case schecktag:
		// checktag that includes symbol bits
		// may also yield a symbol result:
		if v.imm.(uint16)&uint16(expr.SymbolType) != 0 {
			return p.ssa2(sunsymbolize, v, p.mask(v))
		}
		fallthrough
	default: // can never be a symbol
		return v
	}
}

// recompile updates the final bytecode
// to use the given symbol table given the template
// ssa program (src) and the symbolized program (dst);
// recompile also takes care of restoring a saved scratch
// buffer for final if it has been temporarily dropped
func recompile(st *symtab, src, dst *prog, final *bytecode, aux *auxbindings) error {
	final.symtab = st.symrefs
	if !dst.isStale(st) {
		// the scratch buffer may be invalid,
		// so ensure that it is populated correctly:
		final.restoreScratch(st)
		return nil
	}
	err := src.cloneSymbolize(st, dst, aux)
	if err != nil {
		return err
	}
	return dst.compile(final, st)
}

// IsStale returns whether the symbolized program
// (see prog.Symbolize) is stale with respect to
// the provided symbol table.
func (p *prog) isStale(st *symtab) bool {
	if !p.symbolized || p.literals {
		return true
	}
	for i := range p.resolved {
		// if the symbol is -1, then we expect
		// the symbol not to be defined; otherwise,
		// we expect it to be the same string as we saw before
		if p.resolved[i].sym == ^ion.Symbol(0) {
			if _, ok := st.Symbolize(p.resolved[i].val); ok {
				return true
			}
		} else if st.Get(p.resolved[i].sym) != p.resolved[i].val {
			return true
		}
	}
	return false
}

func (p *prog) record(str string, sym ion.Symbol) {
	for i := range p.resolved {
		if p.resolved[i].sym == sym {
			return
		}
	}
	p.resolved = append(p.resolved, sympair{
		sym: sym,
		val: str,
	})
}

func (p *prog) recordEmpty(str string) {
	for i := range p.resolved {
		if p.resolved[i].val == str {
			return
		}
	}
	p.resolved = append(p.resolved, sympair{
		val: str,
		sym: ^ion.Symbol(0),
	})
}

func (p *prog) symbolize(st syms, aux *auxbindings) error {
	p.resolved = p.resolved[:0]
	for i := range p.values {
		v := p.values[i]
		switch v.op {
		case shashmember:
			p.literals = true
			v.imm = p.mktree(st, v.imm)
		case shashlookup:
			p.literals = true
			v.imm = p.mkhash(st, v.imm)
		case sliteral:
			if d, ok := v.imm.(ion.Datum); ok {
				p.literals = true
				var tmp ion.Buffer
				d.Encode(&tmp, ionsyms(st))
				v.imm = rawDatum(tmp.Bytes())
			}
		case sdot:
			str := v.imm.(string)

			// for top-level "dot" operations,
			// check the auxilliary values first:
			if v.args[1].op == sinit {
				if id, ok := aux.id(str); ok {
					v.op = sauxval
					v.args = v.args[:0]
					v.imm = id
					continue
				}
			}

			sym, ok := st.Symbolize(str)
			if !ok {
				// if a symbol isn't present, the
				// search will always fail (and this
				// will cause the optimizer to eliminate
				// any code that depends on this value)
				v.op = skfalse
				v.args = nil
				v.imm = nil
				// the compilation of the program depends
				// on this symbol not existing, so we need
				// to record that fact for IsStale to work
				p.recordEmpty(str)
				continue
			}
			if sym > MaxSymbolID {
				return fmt.Errorf("symbol %x (%q) greater than max symbol ID", sym, str)
			}
			v.imm = sym
			p.record(str, sym)
		case smakestructkey:
			str := v.imm.(string)
			sym := st.Intern(str)
			if sym > MaxSymbolID {
				return fmt.Errorf("symbol %x (%q) greater than max symbol ID", sym, str)
			}
			v.imm = sym
			p.record(str, sym)
		}
	}
	p.symbolized = true
	return nil
}
