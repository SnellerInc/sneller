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
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/heap"
	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
)

type ssaop int

const (
	sinvalid  ssaop = iota
	sinit           // initial lane pointer and mask
	sinitmem        // initial memory state
	sundef          // initial scalar value (undefined)
	smergemem       // merge memory
	skfalse         // logical bottom value; FALSE and also MISSING
	sand            // mask = (mask0 & mask1)
	sor             // mask = (mask0 | mask1)
	snand           // mask = (^mask0 & mask1)
	sxor            // mask = (mask0 ^ mask1)  (unequal bits)
	sxnor           // mask = (mask0 ^ ^mask1) (equal bits)

	// integer/float comparison ops
	scmpeqf // floating-point equal
	scmpeqi // signed 64-bit integer equal
	scmplti // mask = arg0.mask < arg1.mask
	scmpltf
	scmpgti // mask = arg0.mask > arg1.mask
	scmpgtf
	scmplei // ...
	scmplef
	scmpgei
	scmpgef

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

	sfptoint   // fp to int, round nearest
	sinttofp   // int to fp
	sbooltoint // bool to 0 or 1
	sbooltofp  // bool to 0.0 or 1.0

	scvti64tostr // int64 to string

	// #region raw string comparison
	sStrCmpEqCs     // Ascii string compare equality case-sensitive
	sStrCmpEqCi     // Ascii string compare equality case-insensitive
	sStrCmpEqUTF8Ci // UTF-8 string compare equality case-insensitive

	sStrTrimCharLeft  // String trim specific chars left
	sStrTrimCharRight // String trim specific chars right
	sStrTrimWsLeft    // String trim whitespace left
	sStrTrimWsRight   // String trim whitespace right
	sStrTrimPrefixCs  // String trim prefix case-sensitive
	sStrTrimPrefixCi  // String trim prefix case-insensitive
	sStrTrimSuffixCs  // String trim suffix case-sensitive
	sStrTrimSuffixCi  // String trim suffix case-insensitive

	sStrMatchPatternCs       // String match pattern case-sensitive
	sStrMatchPatternCi       // String match pattern case-insensitive
	sStrMatchPatternUTF8Ci   // String match pattern case-insensitive
	sStrContainsPrefixCs     // String contains prefix case-sensitive
	sStrContainsPrefixCi     // String contains prefix case-insensitive
	sStrContainsPrefixUTF8Ci // String contains prefix case-insensitive
	sStrContainsSuffixCs     // String contains suffix case-sensitive
	sStrContainsSuffixCi     // String contains suffix case-insensitive
	sStrContainsSuffixUTF8Ci // String contains suffix case-insensitive
	sStrContainsSubstrCs     // String contains substring case-sensitive
	sStrContainsSubstrCi     // String contains substring case-insensitive

	sIsSubnetOfIP4 // IP subnet matching

	sStrSkip1CharLeft  // String skip 1 unicode code-point from left
	sStrSkip1CharRight // String skip 1 unicode code-point from right
	sStrSkipNCharLeft  // String skip n unicode code-point from left
	sStrSkipNCharRight // String skip n unicode code-point from right

	sCharLength // count number of unicode-points
	sSubStr     // select a substring
	sSplitPart  // Presto split_part
	// #endregion raw string comparison

	// immediate integer comparison ops
	scmpltimmi // arg0.mask < consti
	scmpgtimmi // arg0.mask > consti
	scmpleimmi // ...
	scmpgeimmi
	scmpeqimmi

	// floating-point comparison ops
	// (all implicitly on double-precision)
	scmpltimmf // val < constf
	scmpgtimmf // val > constf
	scmpleimmf // val <= constf
	scmpgeimmf // val >= constf
	scmpeqimmf // val == constf

	// raw literal comparison
	sequalconst // arg0.mask == const

	stuples  // compute interior structure pointer from value
	sdot     // compute 'value . arg0.mask'
	sdot2    // compute 'value . arg0.mask' from previous offset
	ssplit   // compute 'value[0] and value[1:]'
	sliteral // literal operand

	shashvalue  // hash a value
	shashvaluep // hash a value and add it to the current hash
	shashmember // look up a hash in a tree for existence; returns predicate
	shashlookup // look up a hash in a tree for a value; returns boxed

	sstorev // store value in a stack slot
	sstorevblend
	sloadv // load value from a stack slot
	sloadvperm

	sstorelist
	sloadlist
	smhk // mem+hash+predicate
	smsk // mem+scalar+predicate
	sbhk // base+hash+predicate
	sbk  // base+predicate tuple
	smk  // mem+predicate tuple
	svk
	sintk
	sfloatk
	sstrk

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
	satan2f       // val = atan2(y, slot[imm])
	shypotf       // val = hypot(val, slot[imm])
	spowf         // val = pow(val, slot[imm])

	swidthbucketf // val = width_bucket(val, min, max, bucket_count)
	swidthbucketi // val = width_bucket(val, min, max, bucket_count)
	stimebucketts // val = time_bucket(val, interval)

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
	saggcount

	saggbucket
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
	saggslotcount

	scmplttm
	scmpgttm
	sbroadcastts
	sunix
	sunixmicro
	sunboxtime
	sdateadd
	sdateaddimm
	sdateaddmulimm
	sdateaddmonth
	sdateaddmonthimm
	sdateaddyear
	sdatediffmicro
	sdatediffparam
	sdatediffmonth
	sdatediffyear
	sdateextractmicrosecond
	sdateextractmillisecond
	sdateextractsecond
	sdateextractminute
	sdateextracthour
	sdateextractday
	sdateextractmonth
	sdateextractyear
	sdatetounixepoch
	sdatetounixmicro
	sdatetruncmillisecond
	sdatetruncsecond
	sdatetruncminute
	sdatetrunchour
	sdatetruncday
	sdatetruncmonth
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

	schecktag // check encoded tag bits
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

type ssaopinfo struct {
	text     string
	argtypes []ssatype
	rettype  ssatype
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

	scratch bool // op uses scratch
	blend   bool // equivalent to args[0] when mask arg is false
}

// immfmt is an immediate format indicator
type immfmt uint8

const (
	fmtnone       immfmt = iota // no immediate
	fmtslot                     // immediate should be encoded as a uint16 slot reference from an integer
	fmti64                      // immediate should be encoded as an int64
	fmtf64                      // immediate is a float64; should be encoded as 8 bytes (little-endian)
	fmtdict                     // immediate is a string; emit a dict reference
	fmtslotx2hash               // immediate is input hash slot; encode 1-byte input hash slot + 1-byte output

	fmtother // immediate is present, but not available for automatic encoding
)

// canonically, the last argument of any function
// is the operation's mask
var logicalArgs = []ssatype{stBool, stBool}
var intcmpArgs = []ssatype{stInt, stInt, stBool}
var floatcmpArgs = []ssatype{stFloat, stFloat, stBool}

var int1Args = []ssatype{stInt, stBool}
var fp1Args = []ssatype{stFloat, stBool}
var str1Args = []ssatype{stString, stBool}
var time1Args = []ssatype{stTime, stBool}

var parseValueArgs = []ssatype{stScalar, stValue, stBool}

var scalar1Args = []ssatype{stValue, stBool}
var scalar2Args = []ssatype{stValue, stValue, stBool}

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
	smergemem: {text: "mergemem", rettype: stMem, emit: emitinit, priority: prioMem},
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
	skfalse: {text: "false", rettype: stValue | stBool, emit: emitfalse},

	sand:  {text: "and.k", argtypes: logicalArgs, rettype: stBool, emit: emitlogical, bc: opandk},
	snand: {text: "nand.k", argtypes: logicalArgs, rettype: stBool, emit: emitnand, bc: opnandk},
	sor:   {text: "or.k", argtypes: logicalArgs, rettype: stBool, emit: emitlogical, bc: opork},
	sxor:  {text: "xor.k", argtypes: logicalArgs, rettype: stBool, emit: emitlogical, bc: opxork},
	sxnor: {text: "xnor.k", argtypes: logicalArgs, rettype: stBool, emit: emitlogical, bc: opxnork},

	// two-operand fp and int comparison ops
	scmpeqf:    {text: "cmpeq.f", argtypes: floatcmpArgs, rettype: stBool, bc: opcmpeqf, inverse: scmpeqf, emit: emitcmp},
	scmpeqi:    {text: "cmpeq.i", argtypes: intcmpArgs, rettype: stBool, bc: opcmpeqi, inverse: scmpeqi, emit: emitcmp},
	scmpeqimmf: {text: "cmpeq.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpeqimmf},
	scmpeqimmi: {text: "cmpeq.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpeqimmi},
	scmpltf:    {text: "cmplt.f", argtypes: floatcmpArgs, rettype: stBool, bc: opcmpltf, inverse: scmpgtf, emit: emitcmp},
	scmplti:    {text: "cmplt.i", argtypes: intcmpArgs, rettype: stBool, bc: opcmplti, inverse: scmpgti, emit: emitcmp},
	scmpltimmf: {text: "cmplt.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpltimmf},
	scmpltimmi: {text: "cmplt.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpltimmi},
	scmplef:    {text: "cmple.f", argtypes: floatcmpArgs, rettype: stBool, bc: opcmplef, inverse: scmpgef, emit: emitcmp},
	scmplei:    {text: "cmple.i", argtypes: intcmpArgs, rettype: stBool, bc: opcmplei, inverse: scmpgei, emit: emitcmp},
	scmpleimmf: {text: "cmple.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpleimmf},
	scmpleimmi: {text: "cmple.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpleimmi},
	scmpgtf:    {text: "cmpgt.f", argtypes: floatcmpArgs, rettype: stBool, bc: opcmpgtf, inverse: scmpltf, emit: emitcmp},
	scmpgti:    {text: "cmpgt.i", argtypes: intcmpArgs, rettype: stBool, bc: opcmpgti, inverse: scmplti, emit: emitcmp},
	scmpgtimmf: {text: "cmpgt.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgtimmf},
	scmpgtimmi: {text: "cmpgt.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgtimmi},
	scmpgef:    {text: "cmpge.f", argtypes: floatcmpArgs, rettype: stBool, bc: opcmpgef, inverse: scmplef, emit: emitcmp},
	scmpgei:    {text: "cmpge.i", argtypes: intcmpArgs, rettype: stBool, bc: opcmpgei, inverse: scmplei, emit: emitcmp},
	scmpgeimmf: {text: "cmpge.imm.f", argtypes: fp1Args, rettype: stBool, immfmt: fmtf64, bc: opcmpgeimmf},
	scmpgeimmi: {text: "cmpge.imm.i", argtypes: int1Args, rettype: stBool, immfmt: fmti64, bc: opcmpgeimmi},

	seqstr:  {text: "eqstr", bc: opeqslice, argtypes: []ssatype{stString, stString, stBool}, rettype: stBool, emit: emitcmp},
	seqtime: {text: "eqtime", bc: opeqslice, argtypes: []ssatype{stTime, stTime, stBool}, rettype: stBool, emit: emitcmp},

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

	// fp <-> int conversion ops
	sinttofp: {text: "inttofp", argtypes: int1Args, rettype: stFloatMasked, bc: opcvti64tof64},
	sfptoint: {text: "fptoint", argtypes: fp1Args, rettype: stIntMasked, bc: opcvtf64toi64},

	scvti64tostr: {text: "cvti64tostr", argtypes: int1Args, rettype: stStringMasked, bc: opcvti64tostr},

	// boolean -> scalar conversions;
	// first argument is true/false; second is present/missing
	sbooltoint: {text: "booltoint", argtypes: []ssatype{stBool, stBool}, rettype: stInt, bc: opcvtktoi64, emit: emitboolconv},
	sbooltofp:  {text: "booltofp", argtypes: []ssatype{stBool, stBool}, rettype: stFloat, bc: opcvtktof64, emit: emitboolconv},

	//#region string operations
	sStrCmpEqCs:     {text: "cmp_str_eq_cs", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCs},
	sStrCmpEqCi:     {text: "cmp_str_eq_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqCi},
	sStrCmpEqUTF8Ci: {text: "cmp_str_eq_utf8_ci", argtypes: str1Args, rettype: stBool, immfmt: fmtdict, bc: opCmpStrEqUTF8Ci},

	sStrTrimWsLeft:  {text: "trim_ws_left", argtypes: str1Args, rettype: stStringMasked, bc: opTrimWsLeft},
	sStrTrimWsRight: {text: "trim_ws_right", argtypes: str1Args, rettype: stStringMasked, bc: opTrimWsRight},

	sStrTrimCharLeft:  {text: "trim_char_left", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrim4charLeft},
	sStrTrimCharRight: {text: "trim_char_right", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrim4charRight},
	sStrTrimPrefixCs:  {text: "trim_prefix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrimPrefixCs},
	sStrTrimPrefixCi:  {text: "trim_prefix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrimPrefixCi},
	sStrTrimSuffixCs:  {text: "trim_suffix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrimSuffixCs},
	sStrTrimSuffixCi:  {text: "trim_suffix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opTrimSuffixCi},

	// s, k = matchpat s, k, $const
	sStrMatchPatternCs:     {text: "match_pat_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opMatchpatCs},
	sStrMatchPatternCi:     {text: "match_pat_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opMatchpatCi},
	sStrMatchPatternUTF8Ci: {text: "match_pat_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opMatchpatUTF8Ci},

	// s, k = contains_prefix_cs s, k, $const
	sStrContainsPrefixCs:     {text: "contains_prefix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixCs},
	sStrContainsPrefixCi:     {text: "contains_prefix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixCi},
	sStrContainsPrefixUTF8Ci: {text: "contains_prefix_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsPrefixUTF8Ci},

	// s, k = contains_suffix_cs s, k, $const
	sStrContainsSuffixCs:     {text: "contains_suffix_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixCs},
	sStrContainsSuffixCi:     {text: "contains_suffix_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixCi},
	sStrContainsSuffixUTF8Ci: {text: "contains_suffix_utf8_ci", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSuffixUTF8Ci},

	// s, k = contains_substr_cs s, k, $const
	sStrContainsSubstrCs: {text: "contains_substr_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSubstrCs},
	// s, k = contains_substr_ci s, k, $const
	sStrContainsSubstrCi: {text: "contains_substr_cs", argtypes: str1Args, rettype: stStringMasked, immfmt: fmtdict, bc: opContainsSubstrCi},

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

	sCharLength: {text: "char_length", argtypes: str1Args, rettype: stIntMasked, bc: opLengthStr},
	sSubStr:     {text: "substr", argtypes: []ssatype{stString, stInt, stInt, stBool}, rettype: stStringMasked, immfmt: fmtother, bc: opSubstr, emit: emitStrEditStack2},
	sSplitPart:  {text: "split_part", argtypes: []ssatype{stString, stInt, stBool}, rettype: stStringMasked, immfmt: fmtother, bc: opSplitPart, emit: emitStrEditStack1x1},
	// #endregion string operations

	// compare against a constant exactly
	sequalconst: {text: "equalconst", argtypes: scalar1Args, rettype: stBool, immfmt: fmtother, emit: emitconstcmp},

	ssplit: {text: "split", argtypes: []ssatype{stList, stBool}, rettype: stListAndValueMasked, emit: emitsplit, priority: prioParse},

	// convert value to base pointer
	// when it is structure-typed
	stuples: {text: "tuples", argtypes: []ssatype{stValue, stBool}, rettype: stBase | stBool, emit: emittuple, bc: optuple, priority: prioParse},

	// find a struct field by name relative to a base pointer
	sdot: {text: "dot", argtypes: []ssatype{stBase, stBool}, rettype: stValueMasked, immfmt: fmtother, emit: emitdot, priority: prioParse},

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
	sstorev:      {text: "store.z", rettype: stMem, argtypes: []ssatype{stMem, stValue, stBool}, immfmt: fmtother, emit: emitstorev, priority: prioMem},
	sstorevblend: {text: "store.blend", rettype: stMem, argtypes: []ssatype{stMem, stValue, stBool}, immfmt: fmtother, emit: emitstorevblend, priority: prioMem},
	sloadv:       {text: "load.z", rettype: stValueMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, bc: oploadzerov, priority: prioParse},
	sloadvperm:   {text: "load.perm.z", rettype: stValueMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, bc: oploadpermzerov, priority: prioParse},

	sloadlist:  {text: "loadlist.z", rettype: stListMasked, argtypes: []ssatype{stMem}, immfmt: fmtslot, priority: prioParse},
	sstorelist: {text: "storelist.z", rettype: stMem, argtypes: []ssatype{stMem, stList, stBool}, immfmt: fmtother, emit: emitstores, priority: prioMem},

	// these tuple-construction ops
	// sipmly combine a set of separate instructions
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
	sintk:   {text: "intk", rettype: stInt, argtypes: []ssatype{stInt, stBool}, emit: emittuple2regs},
	sstrk:   {text: "strk", rettype: stString, argtypes: []ssatype{stString, stBool}, emit: emittuple2regs},
	svk:     {text: "vk", rettype: stValue, argtypes: []ssatype{stValue, stBool}, emit: emittuple2regs},

	sblendv:     {text: "blendv", rettype: stValue, argtypes: []ssatype{stValue, stValue, stBool}, bc: opblendv, emit: emitblendv, blend: true},
	sblendint:   {text: "blendint", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opblendnum, emit: emitblends, blend: true},
	sblendstr:   {text: "blendstr", rettype: stString, argtypes: []ssatype{stString, stString, stBool}, bc: opblendslice, emit: emitblends, blend: true},
	sblendfloat: {text: "blendfloat", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opblendnum, emit: emitblends, blend: true},

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
	ssquarei:    {text: "square.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, bc: opsquaref},
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
	satan2f:     {text: "atan2.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opatan2f, emit: emitBinaryArithmeticOp},

	saddf:         {text: "add.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opaddf, bcrev: opaddf, emit: emitBinaryArithmeticOp},
	saddi:         {text: "add.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opaddi, bcrev: opaddi, emit: emitBinaryArithmeticOp},
	saddimmf:      {text: "add.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opaddimmf, bcrev: opaddimmf},
	saddimmi:      {text: "add.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opaddimmi, bcrev: opaddimmi},
	ssubf:         {text: "sub.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opsubf, bcrev: oprsubf, emit: emitBinaryArithmeticOp},
	ssubi:         {text: "sub.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opsubi, bcrev: oprsubi, emit: emitBinaryArithmeticOp},
	ssubimmf:      {text: "sub.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opsubimmf, bcrev: oprsubimmf},
	ssubimmi:      {text: "sub.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opsubimmi, bcrev: oprsubimmi},
	srsubimmf:     {text: "rsub.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprsubimmf, bcrev: opsubimmf},
	srsubimmi:     {text: "rsub.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprsubimmi, bcrev: opsubimmi},
	smulf:         {text: "mul.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmulf, bcrev: opmulf, emit: emitBinaryArithmeticOp},
	smuli:         {text: "mul.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmuli, bcrev: opmuli, emit: emitBinaryArithmeticOp},
	smulimmf:      {text: "mul.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmulimmf, bcrev: opmulimmf},
	smulimmi:      {text: "mul.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmulimmi, bcrev: opmulimmi},
	sdivf:         {text: "div.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opdivf, bcrev: oprdivf, emit: emitBinaryArithmeticOp},
	sdivi:         {text: "div.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opdivi, bcrev: oprdivi, emit: emitBinaryArithmeticOp},
	sdivimmf:      {text: "div.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opdivimmf, bcrev: oprdivimmf},
	sdivimmi:      {text: "div.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opdivimmi, bcrev: oprdivimmi},
	srdivimmf:     {text: "rdiv.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprdivimmf, bcrev: opdivimmf},
	srdivimmi:     {text: "rdiv.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprdivimmi, bcrev: opdivimmi},
	smodf:         {text: "mod.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmodf, bcrev: oprmodf, emit: emitBinaryArithmeticOp},
	smodi:         {text: "mod.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmodi, bcrev: oprmodi, emit: emitBinaryArithmeticOp},
	smodimmf:      {text: "mod.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmodimmf, bcrev: oprmodimmf},
	smodimmi:      {text: "mod.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmodimmi, bcrev: oprmodimmi},
	srmodimmf:     {text: "rmod.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: oprmodimmf, bcrev: opmodimmf},
	srmodimmi:     {text: "rmod.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: oprmodimmi, bcrev: opmodimmi},
	sminvaluef:    {text: "minvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opminvaluef, bcrev: opminvaluef, emit: emitBinaryArithmeticOp},
	sminvaluei:    {text: "minvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opminvaluei, bcrev: opminvaluei, emit: emitBinaryArithmeticOp},
	sminvalueimmf: {text: "minvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opminvalueimmf, bcrev: opminvalueimmf},
	sminvalueimmi: {text: "minvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opminvalueimmi, bcrev: opminvalueimmi},
	smaxvaluef:    {text: "maxvalue.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: opmaxvaluef, bcrev: opmaxvaluef, emit: emitBinaryArithmeticOp},
	smaxvaluei:    {text: "maxvalue.i", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: opmaxvaluei, bcrev: opmaxvaluei, emit: emitBinaryArithmeticOp},
	smaxvalueimmf: {text: "maxvalue.imm.f", rettype: stFloat, argtypes: []ssatype{stFloat, stBool}, immfmt: fmtf64, bc: opmaxvalueimmf, bcrev: opmaxvalueimmf},
	smaxvalueimmi: {text: "maxvalue.imm.i", rettype: stInt, argtypes: []ssatype{stInt, stBool}, immfmt: fmti64, bc: opmaxvalueimmi, bcrev: opmaxvalueimmi},
	shypotf:       {text: "hypot.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: ophypotf, bcrev: ophypotf, emit: emitBinaryArithmeticOp},
	spowf:         {text: "pow.f", rettype: stFloat, argtypes: []ssatype{stFloat, stFloat, stBool}, bc: oppowf, emit: emitBinaryArithmeticOp},

	swidthbucketf: {text: "widthbucket.f", rettype: stFloat | stBool, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opwidthbucketf, emit: emitWidthBucket},
	swidthbucketi: {text: "widthbucket.i", rettype: stInt | stBool, argtypes: []ssatype{stInt, stInt, stInt, stInt, stBool}, bc: opwidthbucketi, emit: emitWidthBucket},

	saggsumf:  {text: "aggsum.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtslot, bc: opaggsumf, priority: prioMem},
	saggsumi:  {text: "aggsum.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtslot, bc: opaggsumi, priority: prioMem},
	saggavgf:  {text: "aggavg.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtslot, bc: opaggsumf, priority: prioMem},
	saggavgi:  {text: "aggavg.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtslot, bc: opaggsumi, priority: prioMem},
	saggminf:  {text: "aggmin.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtslot, bc: opaggminf, priority: prioMem},
	saggmini:  {text: "aggmin.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtslot, bc: opaggmini, priority: prioMem},
	saggmaxf:  {text: "aggmax.f", rettype: stMem, argtypes: []ssatype{stMem, stFloat, stBool}, immfmt: fmtslot, bc: opaggmaxf, priority: prioMem},
	saggmaxi:  {text: "aggmax.i", rettype: stMem, argtypes: []ssatype{stMem, stInt, stBool}, immfmt: fmtslot, bc: opaggmaxi, priority: prioMem},
	saggmints: {text: "aggmin.ts", rettype: stMem, argtypes: []ssatype{stMem, stTimeInt, stBool}, immfmt: fmtslot, bc: opaggmini, priority: prioMem},
	saggmaxts: {text: "aggmax.ts", rettype: stMem, argtypes: []ssatype{stMem, stTimeInt, stBool}, immfmt: fmtslot, bc: opaggmaxi, priority: prioMem},
	saggcount: {text: "aggcount", rettype: stMem, argtypes: []ssatype{stMem, stBool}, immfmt: fmtslot, bc: opaggcount, priority: prioMem + 1},

	// compute hash aggregate bucket location; encoded immediate will be input hash slot to use
	saggbucket: {text: "aggbucket", argtypes: []ssatype{stMem, stHash, stBool}, rettype: stBucket, immfmt: fmtslot, bc: opaggbucket},

	// hash aggregate bucket ops (count, min, max, sum)
	saggslotsumf:  {text: "aggslotadd.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotaddf, priority: prioMem},
	saggslotsumi:  {text: "aggslotadd.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotaddi, priority: prioMem},
	saggslotavgf:  {text: "aggslotavg.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotavgf, priority: prioMem},
	saggslotavgi:  {text: "aggslotavg.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotavgi, priority: prioMem},
	saggslotminf:  {text: "aggslotmin.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotminf, priority: prioMem},
	saggslotmini:  {text: "aggslotmin.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxf:  {text: "aggslotmax.f", argtypes: []ssatype{stMem, stBucket, stFloat, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxf, priority: prioMem},
	saggslotmaxi:  {text: "aggslotmax.i", argtypes: []ssatype{stMem, stBucket, stInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotmints: {text: "aggslotmin.ts", argtypes: []ssatype{stMem, stBucket, stTimeInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmini, priority: prioMem},
	saggslotmaxts: {text: "aggslotmax.ts", argtypes: []ssatype{stMem, stBucket, stTimeInt, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotmaxi, priority: prioMem},
	saggslotcount: {text: "aggslotcount", argtypes: []ssatype{stMem, stBucket, stBool}, rettype: stMem, immfmt: fmtslot, bc: opaggslotcount, priority: prioMem},

	// boxing ops
	//
	// turn two masks into TRUE/FALSE/MISSING according to 3VL
	sboxmask:   {text: "boxmask", argtypes: []ssatype{stBool, stBool}, rettype: stValue, emit: emitboxmask, scratch: true},
	sboxint:    {text: "boxint", argtypes: []ssatype{stInt, stBool}, rettype: stValue, bc: opboxint, scratch: true},
	sboxfloat:  {text: "boxfloat", argtypes: []ssatype{stFloat, stBool}, rettype: stValue, bc: opboxfloat, scratch: true},
	sboxstring: {text: "boxstring", argtypes: []ssatype{stString, stBool}, rettype: stValue, bc: opboxstring, scratch: true},

	// timestamp operations
	scmplttm:                {text: "cmplt.tm", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmplti, emit: emitcmp, inverse: scmpgttm},
	scmpgttm:                {text: "cmpgt.tm", rettype: stBool, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opcmpgti, emit: emitcmp, inverse: scmplttm},
	sbroadcastts:            {text: "broadcast.ts", rettype: stTimeInt, argtypes: []ssatype{}, immfmt: fmti64, bc: opbroadcastimmi},
	sunboxtime:              {text: "unboxtime", argtypes: []ssatype{stTime, stBool}, rettype: stTimeInt, bc: opunboxts},
	sdateadd:                {text: "dateadd", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opaddi, emit: emitBinaryOp},
	sdateaddimm:             {text: "dateadd.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, immfmt: fmti64, bc: opaddimmi},
	sdateaddmulimm:          {text: "dateaddmul.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, immfmt: fmti64, bc: opaddmulimmi, emit: emitAddMulImmI},
	sdateaddmonth:           {text: "dateaddmonth", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opdateaddmonth, emit: emitBinaryOp},
	sdateaddmonthimm:        {text: "dateaddmonth.imm", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, immfmt: fmti64, bc: opdateaddmonthimm},
	sdateaddyear:            {text: "dateaddyear", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stInt, stBool}, bc: opdateaddyear, emit: emitBinaryOp},
	sdatediffmicro:          {text: "datediffmicro", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: oprsubi, emit: emitBinaryOp},
	sdatediffparam:          {text: "datediffparam", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffparam, immfmt: fmti64, emit: emitdatediffparam},
	sdatediffmonth:          {text: "datediffmonth", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffmonthyear, emit: emitdatediffmonthyear},
	sdatediffyear:           {text: "datediffyear", rettype: stInt, argtypes: []ssatype{stTimeInt, stTimeInt, stBool}, bc: opdatediffmonthyear, emit: emitdatediffmonthyear},
	sdateextractmicrosecond: {text: "dateextractmicrosecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmicrosecond, emit: emitdateextract},
	sdateextractmillisecond: {text: "dateextractmillisecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmillisecond, emit: emitdateextract},
	sdateextractsecond:      {text: "dateextractsecond", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractsecond, emit: emitdateextract},
	sdateextractminute:      {text: "dateextractminute", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractminute, emit: emitdateextract},
	sdateextracthour:        {text: "dateextracthour", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextracthour, emit: emitdateextract},
	sdateextractday:         {text: "dateextractday", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractday, emit: emitdateextract},
	sdateextractmonth:       {text: "dateextractmonth", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractmonth, emit: emitdateextract},
	sdateextractyear:        {text: "dateextractyear", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdateextractyear, emit: emitdateextract},
	sdatetounixepoch:        {text: "datetounixepoch", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetounixepoch, emit: emitdateextract},
	sdatetounixmicro:        {text: "datetounixmicro", rettype: stInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetounixepoch, emit: emitdatecasttoint},
	sdatetruncmillisecond:   {text: "datetruncmillisecond", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncmillisecond},
	sdatetruncsecond:        {text: "datetruncsecond", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncsecond},
	sdatetruncminute:        {text: "datetruncminute", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncminute},
	sdatetrunchour:          {text: "datetrunchour", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetrunchour},
	sdatetruncday:           {text: "datetruncday", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncday},
	sdatetruncmonth:         {text: "datetruncmonth", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncmonth},
	sdatetruncyear:          {text: "datetruncyear", rettype: stTimeInt, argtypes: []ssatype{stTimeInt, stBool}, bc: opdatetruncyear},
	stimebucketts:           {text: "timebucket.ts", rettype: stInt, argtypes: []ssatype{stInt, stInt, stBool}, bc: optimebucketts, emit: emitBinaryOp},
	sboxts:                  {text: "boxts", argtypes: []ssatype{stTimeInt, stBool}, rettype: stValue, bc: opboxts, scratch: true},

	// GEO functions
	sgeohash:      {text: "geohash", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeohash, emit: emitGeoHash},
	sgeohashimm:   {text: "geohash.imm", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeohashimm, emit: emitGeoHashImm},
	sgeotilex:     {text: "geotilex", rettype: stIntMasked, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotilex, emit: emitGeoTileXY},
	sgeotiley:     {text: "geotiley", rettype: stIntMasked, argtypes: []ssatype{stFloat, stInt, stBool}, bc: opgeotiley, emit: emitGeoTileXY},
	sgeotilees:    {text: "geotilees", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stInt, stBool}, bc: opgeotilees, emit: emitGeoHash},
	sgeotileesimm: {text: "geotilees.imm", rettype: stStringMasked, argtypes: []ssatype{stFloat, stFloat, stBool}, immfmt: fmti64, bc: opgeotileesimm, emit: emitGeoHashImm},
	sgeodistance:  {text: "geodistance", rettype: stFloatMasked, argtypes: []ssatype{stFloat, stFloat, stFloat, stFloat, stBool}, bc: opgeodistance, emit: emitGeoDistance},

	schecktag: {text: "checktag", argtypes: []ssatype{stValue, stBool}, rettype: stValueMasked, immfmt: fmtother, emit: emitchecktag},

	sobjectsize: {text: "objectsize", argtypes: []ssatype{stValue, stBool}, rettype: stIntMasked, bc: opobjectsize},
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
	curpath []string
	values  []*value // all values in program
	ret     *value   // value actually yielded by program

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
func (p *prog) ReserveSlot(slot stackslot) {
	for i := range p.reserved {
		if p.reserved[i] == slot {
			return
		}
	}
	p.reserved = append(p.reserved, slot)
}

func (p *prog) PushPath(x string) {
	p.curpath = append(p.curpath, x)
}

func (p *prog) PopPath() {
	p.curpath = p.curpath[:len(p.curpath)-1]
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
	case string:
		for i := range p.dict {
			if v == p.dict[i] {
				return uint64(i)
			}
		}
		p.dict = append(p.dict, v)
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
		p.dict = append(p.dict, str)
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

func (p *prog) errorf(f string, args ...interface{}) *value {
	v := p.val()
	v.errf(f, args...)
	return v
}

func (p *prog) Begin() {
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

func (s ssaop) String() string {
	return ssainfo[s].text
}

func (v *value) checkarg(arg *value, idx int) {
	if v.op == sinvalid {
		return
	}
	in := ssainfo[arg.op].rettype
	argtypes := ssainfo[v.op].argtypes
	if len(argtypes) <= idx {
		v.errf("op %q does not have argument %d", v.op, idx+1)
		return
	}
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
	want := ssainfo[v.op].argtypes[idx]
	if bits.OnesCount(uint(in&want)) != 1 && arg.op != sundef {
		v.errf("ambiguous assignment type (%s=%s as argument to %s)", arg.Name(), arg, v.op)
	}
}

func (p *prog) ValidLanes() *value {
	return p.values[0]
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
	v.imm = imm
	v.args = []*value{}

	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, v.op)
	}

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
	v.imm = imm
	v.args = []*value{arg}
	v.checkarg(arg, 0)
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, v.op)
	}
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
	v.imm = imm
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
	v.imm = imm
	v.args = []*value{arg0, arg1, arg2}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, op)
	}
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
	v.imm = imm
	v.args = []*value{arg0, arg1, arg2, arg3}
	v.checkarg(arg0, 0)
	v.checkarg(arg1, 1)
	v.checkarg(arg2, 2)
	v.checkarg(arg3, 3)
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, op)
	}
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

func (p *prog) ssaimm(op ssaop, imm interface{}, args ...*value) *value {
	v := p.val()
	v.op = op
	v.args = args
	v.imm = imm
	for i := range args {
		v.checkarg(args[i], i)
	}
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assing immediate %v to op %s", imm, op)
	}
	if v.op == sinvalid {
		panic("invalid op " + v.String())
	}
	return v
}

func (p *prog) Constant(imm interface{}) *value {
	v := p.val()
	v.op = sliteral
	v.imm = imm
	return v
}

// Return sets the return value of the program
// as a single value (will be returned in a register)
func (p *prog) Return(v *value) {
	p.ret = v
}

// InitMem returns the memory token associated
// with the initial memory state.
func (p *prog) InitMem() *value {
	return p.ssa0(sinitmem)
}

func (p *prog) storeBlend(mem *value, v, k *value, slot stackslot) *value {
	return p.ssa3imm(sstorevblend, mem, v, k, int(slot))
}

// Store stores a value to a stack slot and
// returns the associated memory token.
// The store operation is guaranteed to happen
// after the 'mem' op.
func (p *prog) Store(mem *value, v *value, slot stackslot) (*value, error) {
	p.ReserveSlot(slot)
	if v.op == skfalse {
		return p.ssa3imm(sstorev, mem, v, p.ValidLanes(), int(slot)), nil
	}
	switch v.primary() {
	case stValue:
		return p.ssa3imm(sstorev, mem, v, p.mask(v), int(slot)), nil
	default:
		return nil, fmt.Errorf("cannot store value %s", v)
	}
}

func (p *prog) isMissing(v *value) *value {
	return p.Not(p.notMissing(v))
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
		return p.And(p.notMissing(v.args[0]), v.args[1])
	case sxor, sxnor:
		// for xor and xnor, the result is only
		// non-missing if both sides of the comparison
		// are non-MISSING values
		return p.And(p.notMissing(v.args[0]), p.notMissing(v.args[1]))
	case sand:
		// we need
		//          | TRUE    | FALSE | MISSING
		//  --------+---------+-------+--------
		//  TRUE    | TRUE    | FALSE | MISSING
		//  FALSE   | FALSE   | FALSE | FALSE
		//  MISSING | MISSING | FALSE | MISSING
		//
		return p.Or(v, p.Or(
			p.IsFalse(v.args[0]),
			p.IsFalse(v.args[1]),
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
		return p.Or(v, p.And(p.notMissing(v.args[0]), p.notMissing(v.args[1])))
	default:
		m := v.maskarg()
		if m == nil {
			return p.ValidLanes()
		}
		return p.notMissing(m)
	}
}

func (p *prog) StoreList(mem *value, v *value, slot stackslot) *value {
	p.ReserveSlot(slot)
	l := p.tolist(v)
	return p.ssa3imm(sstorelist, mem, l, l, int(slot))
}

// LoadList loads a list slice from
// a stack slot and returns the slice and
// a predicate indicating whether the loaded
// value has a non-zero length component
func (p *prog) LoadList(mem *value, slot stackslot) *value {
	p.ReserveSlot(slot)
	return p.ssa1imm(sloadlist, mem, int(slot))
}

// Loadvalue loads a value from a stack slot
// and returns the value and a predicate
// indicating whether the loaded value
// has a non-zero length component
func (p *prog) Loadvalue(mem *value, slot stackslot) *value {
	p.ReserveSlot(slot)
	return p.ssa1imm(sloadv, mem, int(slot))
}

// Upvalue loads an upvalue (a value bound by
// an enclosing binding context) from a parent's
// stack slot
func (p *prog) Upvalue(mem *value, slot stackslot) *value {
	return p.ssa1imm(sloadvperm, mem, int(slot))
}

// MergeMem merges memory tokens into a memory token.
// (This can be used to create a partial ordering
// constraint for memory operations.)
func (p *prog) MergeMem(args ...*value) *value {
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

// int+K tuple
func (p *prog) intk(f, k *value) *value {
	return p.ssa2(sintk, f, k)
}

// RowsMasked constructs a (base value, predicate) tuple
func (p *prog) RowsMasked(base *value, pred *value) *value {
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
func (p *prog) Dot(col string, base *value) *value {
	for i := range p.curpath {
		base = p.dot(p.curpath[i], base)
	}
	return p.dot(col, base)
}

func (p *prog) dot(col string, base *value) *value {
	if base != p.values[0] {
		// need to perform a conversion from
		// a value pointer to an interior-of-structure pointer
		base = p.ssa2(stuples, base, base)
	}
	return p.ssa2imm(sdot, base, base, col)
}

// Path returns the value corresponding to
// navigating the given path expression
// by splitting it on the '.' character.
// Additional path components specified
// by a call to PushPath are added to the
// full path.
//
//   prog.Path("a.b.c")
// is sugar for
//   prog.Dot("c", prog.Dot("b", prog.Dot("a", prog.ValidLanes()))),
// which is also equivalent to
//   prog.PushPath("a"); prog.Path("b.c")
func (p *prog) Path(str string) *value {
	return p.RelPath(p.ValidLanes(), str)
}

// RelPath computes the field expression 'str'
// relative to the base object 'base'
func (p *prog) RelPath(base *value, str string) *value {
	for i := range p.curpath {
		base = p.dot(p.curpath[i], base)
	}
	for len(str) > 0 {
		i := strings.IndexByte(str, '.')
		if i == -1 {
			return p.dot(str, base)
		}
		base = p.dot(str[:i], base)
		str = str[i+1:]
	}
	return base
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

func (p *prog) IsFalse(v *value) *value {
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

func (p *prog) IsTrue(v *value) *value {
	switch v.primary() {
	case stBool:
		return v
	case stValue:
		return p.ssa2(sistrue, v, p.mask(v))
	default:
		return p.errorf("bad argument %s to IsTrue", v)
	}
}

func (p *prog) IsNotTrue(v *value) *value {
	// we compute predicates as IS TRUE,
	// so IS NOT TRUE is simply the complement
	return p.Not(v)
}

func (p *prog) IsNotFalse(v *value) *value {
	return p.Or(p.IsTrue(v), p.isMissing(v))
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
func (p *prog) Index(v *value, i int) *value {
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
func (p *prog) Equals(left, right *value) *value {
	if (left.op == sliteral) && (right.op == sliteral) {
		// TODO: int64(1) == float64(1.0) ??
		return p.Constant(left.imm == right.imm)
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
			right = p.IsTrue(right)
		}
		allok := p.And(p.notMissing(left), p.notMissing(right))
		return p.And(p.xnor(left, right), allok)
	case stValue:
		if right.op == sliteral {
			return p.ssa2imm(sequalconst, left, p.mask(left), right.imm)
		}
		switch right.primary() {
		case stValue:
			return p.ssa3(sequalv, left, right, p.ssa2(sand, p.mask(left), p.mask(right)))
		case stInt:
			lefti, k := p.coerceInt(left)
			return p.ssa3(scmpeqi, lefti, right, p.And(k, p.mask(right)))
		case stFloat:
			leftf, k := p.coercefp(left)
			return p.ssa3(scmpeqf, leftf, right, p.And(k, p.mask(right)))
		case stString:
			leftstr := p.toStr(left)
			return p.ssa3(seqstr, leftstr, right, p.And(p.mask(leftstr), p.mask(right)))
		case stTime:
			lefttm := p.toTime(left)
			return p.ssa3(seqtime, lefttm, right, p.And(p.mask(lefttm), p.mask(right)))
		default:
			return p.errorf("cannot compare value %s and other %s", left, right)
		}
	case stInt:
		if right.op == sliteral {
			return p.ssa2imm(scmpeqimmi, left, p.mask(left), right.imm)
		}
		if right.primary() == stInt {
			return p.ssa3(scmpeqi, left, right, p.And(p.mask(left), p.mask(right)))
		}
		// falthrough to floating-point comparison
		left = p.ssa2(sinttofp, left, p.mask(left))
		fallthrough
	case stFloat:
		if right.op == sliteral {
			return p.ssa2imm(scmpeqimmf, left, p.mask(left), right.imm)
		}
		switch right.primary() {
		case stInt:
			right = p.ssa2(sinttofp, right, p.mask(right))
			fallthrough
		case stFloat:
			return p.ssa3(scmpeqf, left, right, p.And(p.mask(left), p.mask(right)))
		default:
			return p.ssa0(skfalse) // FALSE/MISSING
		}
	case stString:
		if right.op == sliteral {
			return p.ssa2imm(sStrCmpEqCs, left, left, right.imm)
		}
		switch right.primary() {
		case stString:
			return p.ssa3(seqstr, left, right, p.And(p.mask(left), p.mask(right)))
		default:
			return p.ssa0(skfalse) // FALSE/MISSING
		}
	case stTime:
		switch right.primary() {
		case stTime:
			return p.ssa3(seqtime, left, right, p.And(p.mask(left), p.mask(right)))
		}
		fallthrough
	default:
		return p.errorf("cannot compare %s and %s", left, right)
	}
}

// EqualStr computes equality between strings
func (p *prog) EqualStr(left, right *value, caseSensitive bool) *value {
	if (left.op == sliteral) && (right.op == sliteral) {
		if caseSensitive {
			return p.Constant(left.imm == right.imm)
		}
		leftStr, _ := left.imm.(string)
		rightStr, _ := right.imm.(string)
		return p.Constant(strings.EqualFold(stringext.NormalizeString(leftStr), stringext.NormalizeString(rightStr)))
	}

	if left.op == sliteral { // swap literal to the right
		left, right = right, left
	}

	if right.op == sliteral { // ideally, we can compare against an immediate
		if caseSensitive {
			return p.ssa2imm(sStrCmpEqCs, left, left, right.imm)
		}
		rightStr, _ := right.imm.(string)
		right = p.Constant(stringext.NormalizeString(rightStr))

		if stringext.HasNtnString(rightStr) {
			rightExt := p.Constant(stringext.GenNeedleExt(rightStr, false))
			return p.ssa2imm(sStrCmpEqUTF8Ci, left, left, rightExt.imm)
		}
		return p.ssa2imm(sStrCmpEqCi, left, left, right.imm)
	}
	v := p.val()
	v.errf("not yet supported comparison %v", ssainfo[left.op].rettype)
	return v
}

// CharLength returns the number of unicode code-points in v
func (p *prog) CharLength(v *value) *value {
	v = p.toStr(v)
	return p.ssa2(sCharLength, v, v)
}

// Substring returns a substring at the provided startIndex with length
func (p *prog) Substring(v, substrOffset, substrLength *value) *value {
	offsetInt, offsetMask := p.coerceInt(substrOffset)
	lengthInt, lengthMask := p.coerceInt(substrLength)
	mask := p.And(v, p.And(offsetMask, lengthMask))
	return p.ssa4(sSubStr, v, offsetInt, lengthInt, mask)
}

// SplitPart splits string on delimiter and returns the field index. Field indexes start with 1.
func (p *prog) SplitPart(v *value, delimiter byte, index *value) *value {
	delimiterStr := string(delimiter)
	indexInt, indexMask := p.coerceInt(index)
	mask := p.And(v, indexMask)
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
		return p.ValidLanes() // TRUE
	}
	return p.ssa2(sisnonnull, v, p.mask(v))
}

// round an immediate to an integer
//
// if the input is floating-point, 'dir'
// determines the rounding mode:
//  +1: up, 0: nearest, -1: down
func roundi(imm interface{}, dir int) uint64 {
	switch i := imm.(type) {
	case int:
		return uint64(i)
	case int64:
		return uint64(i)
	case uint:
		return uint64(i)
	case uint64:
		return uint64(i)
	case float32:
		v := int64(i)
		if dir > 0 && float32(v) < i {
			v++
		} else if dir < 0 && float32(v) > i {
			v--
		}
		return uint64(v)
	case float64:
		v := int64(i)
		if dir > 0 && float64(v) < i {
			v++
		} else if dir < 0 && float64(v) > i {
			v--
		}
		return uint64(v)
	default:
		panic("invalid immediate for rounding")
	}
}

func isIntImmediate(imm interface{}) bool {
	switch imm.(type) {
	case int, int64, uint, uint64:
		return true
	default:
		return false
	}
}

func tof64(imm interface{}) float64 {
	switch i := imm.(type) {
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

// General strategy for immediate comparisons:
//
// We're given an immediate integer or float to
// compare lanes against, and it's possible that
// lanes are integers are floats themselves.
//
// Therefore, we compute this approximately as
//
//   intcmp(toint(val), imm) OR floatcmp(tofp(val), imm)
//
// ... but taking care to use the appropriate rounding when
// converting floating-point immediates.
func (p *prog) cmpimm(intop, fpop ssaop, dir int, left *value, imm interface{}) *value {
	rt := left.primary()
	if rt == stFloat {
		// just do fp compare
		return p.ssa2imm(fpop, left, p.mask(left), tof64(imm))
	}
	if rt == stInt {
		return p.ssa2imm(intop, left, p.mask(left), roundi(imm, dir))
	}
	// at this point we know we need to do
	// a complete unboxing of a value
	if rt != stValue {
		v := p.val()
		v.errf("invalid immediate comparison between %s and %v", left, imm)
		return v
	}
	i := p.ssa3(stoint, p.undef(), left, p.mask(left))
	icmp := p.ssa2imm(intop, i, i, roundi(imm, dir))
	f := p.ssa3(stofloat, p.undef(), left, p.ssa2(snand, i, p.mask(left)))
	fcmp := p.ssa2imm(fpop, f, f, tof64(imm))
	v := p.Or(fcmp, icmp)
	// the result of this OR has non-standard
	// NOT MISSING behavior, because we don't
	// care if one of the comparisons didn't
	// convert correctly; we only care if they *both* did
	v.notMissing = p.Or(i, f)
	return v
}

// coerce a value to floating point,
// taking care to promote integers appropriately
func (p *prog) coercefp(arg *value) (*value, *value) {
	if arg.op == sliteral {
		return p.ssa0imm(sbroadcastf, arg.imm), p.ValidLanes()
	}
	if arg.primary() == stFloat {
		return arg, p.mask(arg)
	}
	if arg.primary() == stInt {
		ret := p.ssa2(sinttofp, arg, p.mask(arg))
		return ret, p.mask(arg)
	}
	// TODO: emit slightly less optimized code here
	// so that CSE could choose to hoist either the
	// float or integer conversion ops here...
	easy := p.ssa3(stofloat, p.undef(), arg, p.mask(arg))
	intv := p.ssa3(stoint, easy, arg, p.ssa2(snand, easy, p.mask(arg)))
	conv := p.ssa2(sinttofp, intv, intv)
	return conv, p.Or(conv, easy)
}

// coerceInt coerces a value to integer
func (p *prog) coerceInt(v *value) (*value, *value) {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcasti, v.imm), p.ValidLanes()
	}
	switch v.primary() {
	case stInt:
		return v, p.mask(v)
	case stFloat:
		return p.ssa2(sfptoint, v, p.mask(v)), p.mask(v)
	case stValue:
		ret := p.ssa3(stoint, p.undef(), v, p.mask(v))
		return ret, ret
	default:
		err := p.val()
		err.errf("cannot convert %s to an integer", v)
		return err, err
	}
}

// for a current FP value 'into', a value argument 'arg',
// and a predicate 'when', parse arg and use the predicate
// when to blend the floating-point-converted results
// into 'into'
func (p *prog) blendv2fp(into, arg, when *value) (*value, *value) {
	if arg.op == sliteral {
		return p.ssa0imm(sbroadcastf, arg.imm), p.ValidLanes()
	}
	easy := p.ssa3(stofloat, into, arg, when)
	intv := p.ssa3(stoint, easy, arg, when)
	conv := p.ssa2(sinttofp, intv, intv)
	return conv, p.Or(easy, conv)
}

func (p *prog) cmp(fpop ssaop, left, right *value) *value {
	lhs, lhk := p.coercefp(left)
	rhs, rhk := p.coercefp(right)
	return p.ssa3(fpop, lhs, rhs, p.ssa2(sand, lhk, rhk))
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
		return p.ssa2(sfptoint, v, p.mask(v))
	default:
		v := p.val()
		v.errf("cannot convert %s to int", v.String())
		return v
	}
}

func (p *prog) toStr(str *value) *value {
	switch str.primary() {
	case stString:
		return str // no need to parse
	case stValue:
		return p.ssa2(stostr, str, p.mask(str))
	default:
		v := p.val()
		v.errf("internal error: unsupported value %v", str.String())
		return v
	}
}

// TrimWhitespace trim chars: ' ', '\t', '\n', '\v', '\f', '\r'
func (p *prog) TrimWhitespace(str *value, left, right bool) *value {
	str = p.toStr(str)
	if left {
		str = p.ssa2(sStrTrimWsLeft, str, p.mask(str))
	}
	if right {
		str = p.ssa2(sStrTrimWsRight, str, p.mask(str))
	}
	return str
}

// TrimSpace trim char: ' '
func (p *prog) TrimSpace(str *value, left, right bool) *value {
	return p.TrimChar(str, " ", left, right)
}

// TrimChar trim provided chars
func (p *prog) TrimChar(str *value, chars string, left, right bool) *value {
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
	if left {
		str = p.ssa2imm(sStrTrimCharLeft, str, p.mask(str), preparedChars)
	}
	if right {
		str = p.ssa2imm(sStrTrimCharRight, str, p.mask(str), preparedChars)
	}
	return str
}

// TrimPrefix trim the provided prefix
func (p *prog) TrimPrefix(str *value, prefix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if prefix == "" {
		return str
	}
	if caseSensitive {
		return p.ssa2imm(sStrTrimPrefixCs, str, p.mask(str), prefix)
	}
	prefix = stringext.NormalizeString(prefix)
	if stringext.HasNtnString(prefix) {
		//TODO sStrTrimPrefixUTF8Ci is not implemented yet
		//return p.ssa2imm(sStrTrimPrefixUTF8Ci, str, p.mask(str), p.Constant(prefix).imm)
		return p.ssa2imm(sStrTrimPrefixCi, str, p.mask(str), p.Constant(prefix).imm)
	}
	return p.ssa2imm(sStrTrimPrefixCi, str, p.mask(str), p.Constant(prefix).imm)
}

// TrimSuffix trim the provided suffix
func (p *prog) TrimSuffix(str *value, suffix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if suffix == "" {
		return str
	}
	if caseSensitive {
		return p.ssa2imm(sStrTrimSuffixCs, str, p.mask(str), suffix)
	}
	suffix = stringext.NormalizeString(suffix)
	if stringext.HasNtnString(suffix) {
		//TODO sStrTrimSuffixUTFCi is not implemented yet
		//return p.ssa2imm(sStrTrimSuffixUTFCi, str, p.mask(str), p.Constant(suffix).imm)
		return p.ssa2imm(sStrTrimSuffixCi, str, p.mask(str), p.Constant(suffix).imm)
	}
	return p.ssa2imm(sStrTrimSuffixCi, str, p.mask(str), p.Constant(suffix).imm)
}

// HasPrefix returns true when str contains the provided prefix; false otherwise
func (p *prog) HasPrefix(str *value, prefix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if prefix == "" {
		return str
	}
	if caseSensitive {
		return p.ssa2imm(sStrContainsPrefixCs, str, p.mask(str), p.Constant(prefix).imm)
	}
	prefix = stringext.NormalizeString(prefix)
	if stringext.HasNtnString(prefix) {
		prefixExt := p.Constant(stringext.GenNeedleExt(prefix, false))
		return p.ssa2imm(sStrContainsPrefixUTF8Ci, str, p.mask(str), prefixExt.imm)
	}
	return p.ssa2imm(sStrContainsPrefixCi, str, p.mask(str), p.Constant(prefix).imm)
}

// HasSuffix returns true when str contains the provided suffix; false otherwise
func (p *prog) HasSuffix(str *value, suffix string, caseSensitive bool) *value {
	str = p.toStr(str)
	if suffix == "" {
		return str
	}
	if caseSensitive {
		return p.ssa2imm(sStrContainsSuffixCs, str, p.mask(str), p.Constant(suffix).imm)
	}
	suffix = stringext.NormalizeString(suffix)
	if stringext.HasNtnString(suffix) {
		suffixExt := p.Constant(stringext.GenNeedleExt(suffix, true))
		return p.ssa2imm(sStrContainsSuffixUTF8Ci, str, p.mask(str), suffixExt.imm)
	}
	return p.ssa2imm(sStrContainsSuffixCi, str, p.mask(str), p.Constant(suffix).imm)
}

// Contains returns whether the given value
// is a string containing 'needle' as a substring.
// (The return value is always 'true' if 'str' is
// a string and 'needle' is the empty string.)
func (p *prog) Contains(str *value, needle string, caseSensitive bool) *value {
	// n.b. the 'contains' code doesn't actually
	// handle the empty string; just return whether
	// this value is a string
	str = p.toStr(str)
	if needle == "" {
		return str
	}
	enc := string(byte(len(needle))) + needle
	if caseSensitive {
		return p.ssa2imm(sStrMatchPatternCs, str, p.mask(str), p.Constant(enc).imm)
	}
	enc = stringext.NormalizeString(enc)
	if stringext.HasNtnString(needle) {
		segments := make([]string, 0)
		segments = append(segments, needle) // only one single segment
		patternExt := p.Constant(stringext.GenPatternExt(segments))
		return p.ssa2imm(sStrMatchPatternUTF8Ci, str, p.mask(str), patternExt.imm)
	}
	//return p.ssa2imm(sStrMatchPatternCi, str, p.mask(str), enc)
	//TODO HJ alternative code in sStrContainsSubstrCi but this seems to be slower then sStrMatchPatternCi for regular payloads
	return p.ssa2imm(sStrContainsSubstrCi, str, p.mask(str), p.Constant(enc).imm)
}

// IsSubnetOfIP4 returns whether the give value is an IPv4 address between (and including) min and max
func (p *prog) IsSubnetOfIP4(str *value, min, max net.IP) *value {
	str = p.toStr(str)

	// Create an encoding of an IP4 as 16 bytes that is convenient. eg., string "192.1.2.3" becomes byte sequence 2,9,1,0, 1,0,0,0, 2,0,0,0, 3,0,0,0
	ipBCD := make([]byte, 16)
	min = min.To4()
	max = max.To4()
	minStr := []byte(fmt.Sprintf("%04d%04d%04d%04d", min[0], min[1], min[2], min[3]))
	maxStr := []byte(fmt.Sprintf("%04d%04d%04d%04d", max[0], max[1], max[2], max[3]))
	for i := 0; i < 16; i += 4 {
		ipBCD[0+i] = (minStr[3+i] & 0b1111) | ((maxStr[3+i] & 0b1111) << 4) // keep only the lower nibble from ascii '0'-'9' gives byte 0-9
		ipBCD[1+i] = (minStr[2+i] & 0b1111) | ((maxStr[2+i] & 0b1111) << 4)
		ipBCD[2+i] = (minStr[1+i] & 0b1111) | ((maxStr[1+i] & 0b1111) << 4)
		ipBCD[3+i] = (minStr[0+i] & 0b1111) | ((maxStr[0+i] & 0b1111) << 4)
	}
	return p.ssa2imm(sIsSubnetOfIP4, str, p.mask(str), string(ipBCD))
}

// SkipCharLeft skips a variable number of UTF-8 code-points from the left side of a string
func (p *prog) SkipCharLeft(str, nChars *value) *value {
	str = p.toStr(str)
	return p.ssa3(sStrSkipNCharLeft, str, nChars, p.And(p.mask(str), p.mask(nChars)))
}

// SkipCharRight skips a variable number of UTF-8 code-points from the right side of a string
func (p *prog) SkipCharRight(str, nChars *value) *value {
	str = p.toStr(str)
	return p.ssa3(sStrSkipNCharRight, str, nChars, p.And(p.mask(str), p.mask(nChars)))
}

// SkipCharLeftConst skips a constant number of UTF-8 code-points from the left side of a string
func (p *prog) SkipCharLeftConst(str *value, nChars int) *value {
	str = p.toStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharLeft, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceInt(p.Constant(int64(nChars)))
		return p.ssa3(sStrSkipNCharLeft, str, nCharsInt, p.And(p.mask(str), nCharsMask))
	}
}

// SkipCharRightConst skips a constant number of UTF-8 code-points from the right side of a string
func (p *prog) SkipCharRightConst(str *value, nChars int) *value {
	str = p.toStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharRight, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceInt(p.Constant(int64(nChars)))
		return p.ssa3(sStrSkipNCharRight, str, nCharsInt, p.And(p.mask(str), nCharsMask))
	}
}

// encode the appropriate immediate for a
// pattern-matching operation
//
// each segment of the pattern is encoded
// as a 1-byte length, and implicitly each
// segment is delimited with a '?' operation
func (p *prog) patmatch(str *value, pat string, wc byte, caseSensitive bool) *value {
	str = p.toStr(str)
	if len(pat) == 0 {
		return str
	}
	// we can't pass a wildcard as the first
	// or last segment to the assembly code;
	// that needs to be handled at a higher level
	if pat[0] == wc || pat[len(pat)-1] == wc {
		panic("internal error: bad pattern-matching string")
	}

	enc := ""
	hasNtn := false // segment has non-trivial normalization
	segments := make([]string, 0)

	for len(pat) > 0 {
		i := strings.IndexByte(pat, wc)
		if i == -1 {
			i = len(pat)
		}
		if i > 255 {
			// NOTE: will be fixed in separate MR
			panic("pattern too long")
		}
		segment := pat[:i]
		if !caseSensitive {
			segment = stringext.NormalizeString(segment)
			hasNtn = hasNtn || stringext.HasNtnString(segment)
		}
		enc += string(byte(i)) + segment
		segments = append(segments, segment)

		if i == len(pat) {
			break
		}
		pat = pat[i+1:] // plus 1 to skip the '?'; it is implied
	}

	if caseSensitive {
		return p.ssa2imm(sStrMatchPatternCs, str, p.mask(str), p.Constant(enc).imm)
	}
	if hasNtn { // segment has non-trivial normalization
		patternExt := p.Constant(stringext.GenPatternExt(segments))
		return p.ssa2imm(sStrMatchPatternUTF8Ci, str, p.mask(str), patternExt.imm)
	}
	return p.ssa2imm(sStrMatchPatternCi, str, p.mask(str), p.Constant(enc).imm)
}

// Like matches 'str' as a string against
// a SQL 'LIKE' pattern
//
// The '%' character will match zero or more
// unicode points, and the '_' character will
// match exactly one unicode point.
func (p *prog) Like(str *value, expr string, caseSensitive bool) *value {
	return p.glob(str, expr, '_', '%', caseSensitive)
}

// Glob matches 'str' as a string against
// a simple glob pattern.
//
// The '*' character will match zero or more
// unicode points, and the '?' character will
// match exactly one unicode point.
func (p *prog) Glob(str *value, expr string, caseSensitive bool) *value {
	return p.glob(str, expr, '?', '*', caseSensitive)
}

// match a pattern using
func (p *prog) glob(str *value, expr string, wc, ks byte, caseSensitive bool) *value {
	if !caseSensitive { // Bytecode for case-insensitive comparing expects that needles and patterns are in normalized (UPPER) case
		expr = stringext.NormalizeString(expr)
	}
	lefti := strings.IndexByte(expr, ks)
	if lefti == -1 {
		return p.pattern(str, expr, nil, "", wc, caseSensitive)
	}
	left := expr[:lefti]
	expr = expr[lefti+1:]

	var middle []string
	for len(expr) > 0 {
		segi := strings.IndexByte(expr, ks)
		if segi == -1 {
			return p.pattern(str, left, middle, expr, wc, caseSensitive)
		}
		seg := expr[:segi]
		expr = expr[segi+1:]
		if len(seg) == 0 {
			continue
		}
		middle = append(middle, seg)
	}
	return p.pattern(str, left, middle, "", wc, caseSensitive)
}

// matches '<start>*<middle0>...*<middleN>*<end>'
func (p *prog) pattern(str *value, startStr string, middle []string, endStr string, wc byte, caseSensitive bool) *value {

	start := []rune(startStr)
	end := []rune(endStr)

	str = p.toStr(str)
	// match pattern anchored at start;
	// match forwards by repeatedly trimming literal prefixes or single characters with '?'
	for len(start) > 0 {
		// skip all leading code-points with '?'
		nCharsToSkip := 0
		for (len(start) > 0) && (start[0] == rune(wc)) {
			start = start[1:] // skip the first code-point
			nCharsToSkip++
		}
		str = p.SkipCharLeftConst(str, nCharsToSkip)

		// if anything remaining, match with prefix
		if len(start) > 0 {
			qi := strings.IndexByte(string(start), wc)
			if qi == -1 {
				qi = len(start)
			}
			str = p.HasPrefix(str, string(start[:qi]), caseSensitive)
			start = start[qi:]
		}
	}
	// match pattern anchored at end;
	// we match this pattern backwards by trimming matching suffixes off of the string or single characters with '?'
	for len(end) > 0 {
		// skip all trailing code-points with '?'
		nCharsToSkip := 0
		for (len(end) > 0) && (end[len(end)-1] == rune(wc)) {
			end = end[:len(end)-1] // skip the last code-point
			nCharsToSkip++
		}
		str = p.SkipCharRightConst(str, nCharsToSkip)

		// if anything remaining, match with suffix
		if len(end) > 0 {
			var seg []rune
			si := strings.LastIndexByte(string(end), wc)
			if si == -1 {
				seg = end
				end = make([]rune, 0)
			} else {
				seg = end[si+1:]
				end = end[:si+1]
			}
			str = p.HasSuffix(str, string(seg), caseSensitive)
			end = end[:si+1]
		}
	}

	for i := range middle {
		// any '?' at the beginning of an unanchored match simply becomes a 'skipchar'
		mid := []rune(middle[i])

		nCharsToSkip := 0
		for len(mid) > 0 && mid[0] == rune(wc) {
			mid = mid[1:]
			nCharsToSkip++
		}
		str = p.SkipCharLeftConst(str, nCharsToSkip)

		// similarly, and '?' at the end of an unanchored match becomes a 'skipchar' after the inner match
		nCharsToChomp := 0
		for len(mid) > 0 && mid[len(mid)-1] == rune(wc) {
			mid = mid[:len(mid)-1]
			nCharsToChomp++
		}

		// do the difficult matching
		if len(mid) > 0 {
			str = p.patmatch(str, string(mid), wc, caseSensitive)
		}
		str = p.SkipCharLeftConst(str, nCharsToChomp)
	}
	return str
}

//#endregion

// Less computes 'left < right'
func (p *prog) Less(left, right *value) *value {
	if left.op == sliteral {
		if right.op == sliteral {
			panic("missed constprop opportunity")
		}
		return p.GreaterEqual(right, left)
	}
	if right.op == sliteral {
		return p.cmpimm(scmpltimmi, scmpltimmf, 1, left, right.imm)
	}
	return p.cmp(scmpltf, left, right)
}

// Greater computes 'left > right'
func (p *prog) Greater(left, right *value) *value {
	if left.op == sliteral {
		if right.op == sliteral {
			panic("TODO: constprop")
		}
		return p.LessEqual(right, left)
	}
	if right.op == sliteral {
		return p.cmpimm(scmpgtimmi, scmpgtimmf, -1, left, right.imm)
	}
	return p.cmp(scmpgtf, left, right)
}

// GreaterEqual computes 'left >= right'
func (p *prog) GreaterEqual(left, right *value) *value {
	if left.op == sliteral {
		if right.op == sliteral {
			panic("TODO: constprop")
		}
		return p.Less(right, left)
	}
	if right.op == sliteral {
		return p.cmpimm(scmpgeimmi, scmpgeimmf, 1, left, right.imm)
	}
	return p.cmp(scmpgef, left, right)
}

// LessEqual computes 'left <= right'
func (p *prog) LessEqual(left, right *value) *value {
	if left.op == sliteral {
		if right.op == sliteral {
			panic("TODO: constprop")
		}
		return p.Greater(right, left)
	}
	if right.op == sliteral {
		return p.cmpimm(scmpleimmi, scmpleimmf, -1, left, right.imm)
	}
	return p.cmp(scmplef, left, right)
}

// And computes 'left AND right'
func (p *prog) And(left, right *value) *value {
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
		return p.And(left, right)
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
		return p.ValidLanes()
	}
	return p.ssa2(sxnor, left, right)
}

// Or computes 'left OR right'
func (p *prog) Or(left, right *value) *value {
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
func (p *prog) Not(v *value) *value {
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
	return p.nand(v, p.ValidLanes())
}

func (p *prog) makeBroadcastOp(child *value) *value {
	if child.op != sliteral {
		panic(fmt.Sprintf("BroadcastOp requires a literal value, not %s", child.op.String()))
	}

	return p.ssa0imm(sbroadcastf, child.imm)
}

func isIntValue(v *value) bool {
	if v.op == sliteral {
		return isIntImmediate(v.imm)
	}

	return v.primary() == stInt
}

// Unary arithmetic operators and functions
func (p *prog) makeUnaryArithmeticOp(regOpF, regOpI ssaop, child *value) *value {
	if isIntValue(child) && child.op != sliteral {
		s, k := p.coerceInt(child)
		return p.ssa2(regOpI, s, k)
	}

	return p.makeUnaryArithmeticOpFp(regOpF, child)
}

func (p *prog) makeUnaryArithmeticOpFp(op ssaop, child *value) *value {
	if child.op == sliteral {
		child = p.makeBroadcastOp(child)
	}

	s, k := p.coercefp(child)
	return p.ssa2(op, s, k)
}

func (p *prog) Neg(child *value) *value {
	return p.makeUnaryArithmeticOp(snegf, snegi, child)
}

func (p *prog) Abs(child *value) *value {
	return p.makeUnaryArithmeticOp(sabsf, sabsi, child)
}

func (p *prog) Sign(child *value) *value {
	return p.makeUnaryArithmeticOp(ssignf, ssigni, child)
}

func (p *prog) Round(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sroundf, child)
}

func (p *prog) RoundEven(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sroundevenf, child)
}

func (p *prog) Trunc(child *value) *value {
	return p.makeUnaryArithmeticOpFp(struncf, child)
}

func (p *prog) Floor(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sfloorf, child)
}

func (p *prog) Ceil(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sceilf, child)
}

func (p *prog) Sqrt(child *value) *value {
	return p.makeUnaryArithmeticOpFp(ssqrtf, child)
}

func (p *prog) Cbrt(child *value) *value {
	return p.makeUnaryArithmeticOpFp(scbrtf, child)
}

func (p *prog) Exp(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexpf, child)
}

func (p *prog) ExpM1(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexpm1f, child)
}

func (p *prog) Exp2(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexp2f, child)
}

func (p *prog) Exp10(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sexp10f, child)
}

func (p *prog) Ln(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slnf, child)
}

func (p *prog) Ln1p(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sln1pf, child)
}

func (p *prog) Log2(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slog2f, child)
}

func (p *prog) Log10(child *value) *value {
	return p.makeUnaryArithmeticOpFp(slog10f, child)
}

func (p *prog) Sin(child *value) *value {
	return p.makeUnaryArithmeticOpFp(ssinf, child)
}

func (p *prog) Cos(child *value) *value {
	return p.makeUnaryArithmeticOpFp(scosf, child)
}

func (p *prog) Tan(child *value) *value {
	return p.makeUnaryArithmeticOpFp(stanf, child)
}

func (p *prog) Asin(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sasinf, child)
}

func (p *prog) Acos(child *value) *value {
	return p.makeUnaryArithmeticOpFp(sacosf, child)
}

func (p *prog) Atan(child *value) *value {
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
		return p.ssa3(regOpI, left, right, p.And(p.mask(left), p.mask(right)))
	}

	lhs, lhk := p.coercefp(left)
	rhs, rhk := p.coercefp(right)
	return p.ssa3(regOpF, lhs, rhs, p.And(lhk, rhk))
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
	return p.ssa3(op, lhs, rhs, p.And(lhk, rhk))
}

func (p *prog) Add(left, right *value) *value {
	if left == right {
		return p.makeBinaryArithmeticOpImm(smulimmf, smulimmi, left, 2)
	}
	return p.makeBinaryArithmeticOp(saddf, saddi, saddimmf, saddimmi, saddimmf, saddimmi, left, right)
}

func (p *prog) Sub(left, right *value) *value {
	if left == right {
		return p.makeBinaryArithmeticOpImm(smulimmf, smulimmi, left, 0)
	}
	return p.makeBinaryArithmeticOp(ssubf, ssubi, ssubimmf, ssubimmi, srsubimmf, srsubimmi, left, right)
}

func (p *prog) Mul(left, right *value) *value {
	if left == right {
		return p.makeUnaryArithmeticOp(ssquaref, ssquarei, left)
	}
	return p.makeBinaryArithmeticOp(smulf, smuli, smulimmf, smulimmi, smulimmf, smulimmi, left, right)
}

func (p *prog) Div(left, right *value) *value {
	return p.makeBinaryArithmeticOp(sdivf, sdivi, sdivimmf, sdivimmi, srdivimmf, srdivimmi, left, right)
}

func (p *prog) Mod(left, right *value) *value {
	return p.makeBinaryArithmeticOp(smodf, smodi, smodimmf, smodimmi, srmodimmf, srmodimmi, left, right)
}

func (p *prog) MinValue(left, right *value) *value {
	if left == right {
		return left
	}
	return p.makeBinaryArithmeticOp(sminvaluef, sminvaluei, sminvalueimmf, sminvalueimmi, sminvalueimmf, sminvalueimmi, left, right)
}

func (p *prog) MaxValue(left, right *value) *value {
	if left == right {
		return left
	}
	return p.makeBinaryArithmeticOp(smaxvaluef, smaxvaluei, smaxvalueimmf, smaxvalueimmi, smaxvalueimmf, smaxvalueimmi, left, right)
}

func (p *prog) Hypot(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(shypotf, left, right)
}

func (p *prog) Pow(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(spowf, left, right)
}

func (p *prog) Atan2(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(satan2f, left, right)
}

func (p *prog) WidthBucket(val, min, max, bucketCount *value) *value {
	if isIntValue(val) && isIntValue(min) && isIntValue(max) {
		vali, valk := p.coerceInt(val)
		mini, mink := p.coerceInt(min)
		maxi, maxk := p.coerceInt(max)
		cnti, cntk := p.coerceInt(bucketCount)

		mask := p.And(valk, p.And(cntk, p.And(mink, maxk)))
		return p.ssa5(swidthbucketi, vali, mini, maxi, cnti, mask)
	}

	valf, valk := p.coercefp(val)
	minf, mink := p.coercefp(min)
	maxf, maxk := p.coercefp(max)
	cntf, cntk := p.coercefp(bucketCount)

	mask := p.And(valk, p.And(cntk, p.And(mink, maxk)))
	return p.ssa5(swidthbucketf, valf, minf, maxf, cntf, mask)
}

func (p *prog) coerceTimestamp(v *value) (*value, *value) {
	if v.op == sliteral {
		ts, ok := v.imm.(date.Time)
		if !ok {
			return p.errorf("cannot use result of %T as TIMESTAMP", v.imm), p.ValidLanes()
		}
		return p.ssa0imm(sbroadcastts, ts.UnixMicro()), p.ValidLanes()
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
		return p.errorf("cannot use result of %s as TIMESTAMP", v), p.ValidLanes()
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
}

func (p *prog) DateAdd(part expr.Timepart, arg0, arg1 *value) *value {
	arg1Time, arg1Mask := p.coerceTimestamp(arg1)
	if arg0.op == sliteral && isIntImmediate(arg0.imm) {
		i64Imm := toi64(arg0.imm)
		if int(part) < len(timePartMultiplier) {
			i64Imm *= timePartMultiplier[part]
			return p.ssa2imm(sdateaddimm, arg1Time, arg1Mask, i64Imm)
		}

		if part == expr.Month {
			return p.ssa2imm(sdateaddmonthimm, arg1Time, arg1Mask, i64Imm)
		}

		if part == expr.Year {
			return p.ssa2imm(sdateaddmonthimm, arg1Time, arg1Mask, i64Imm*12)
		}
	} else {
		arg0Int, arg0Mask := p.coerceInt(arg0)

		// Microseconds need no multiplication of the input, thus use the simplest operation available.
		if part == expr.Microsecond {
			return p.ssa3(sdateadd, arg1Time, arg0Int, p.And(arg1Mask, arg0Mask))
		}

		// If the part is lesser than Month, we can just use addmulimm operation with the required scale.
		if int(part) < len(timePartMultiplier) {
			return p.ssa3imm(sdateaddmulimm, arg1Time, arg0Int, p.And(arg1Mask, arg0Mask), timePartMultiplier[part])
		}

		if part == expr.Month {
			return p.ssa3(sdateaddmonth, arg1Time, arg0Int, p.And(arg1Mask, arg0Mask))
		}

		if part == expr.Year {
			return p.ssa3(sdateaddyear, arg1Time, arg0Int, p.And(arg1Mask, arg0Mask))
		}
	}

	return p.errorf("unhandled date part in DateAdd()")
}

func (p *prog) DateDiff(part expr.Timepart, arg0, arg1 *value) *value {
	t0, m0 := p.coerceTimestamp(arg0)
	t1, m1 := p.coerceTimestamp(arg1)

	if part == expr.Microsecond {
		return p.ssa3(sdatediffmicro, t0, t1, p.And(m0, m1))
	}

	if int(part) < len(timePartMultiplier) {
		imm := timePartMultiplier[part]
		return p.ssa3imm(sdatediffparam, t0, t1, p.And(m0, m1), imm)
	}

	if part == expr.Month {
		return p.ssa3(sdatediffmonth, t0, t1, p.And(m0, m1))
	}

	if part == expr.Year {
		return p.ssa3(sdatediffyear, t0, t1, p.And(m0, m1))
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
		panic("Sub-second precision is invalid here")
	}
}

func (p *prog) DateExtract(part expr.Timepart, val *value) *value {
	if val.primary() == stTimeInt || part < expr.Second {
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
		case expr.Month:
			return p.ssa2(sdateextractmonth, v, m)
		case expr.Year:
			return p.ssa2(sdateextractyear, v, m)
		default:
			return p.errorf("unhandled date part in DateExtract()")
		}
	}

	v := p.toTime(val)
	return p.ssa2imm(stmextract, v, p.mask(v), immediateForBoxedDateInstruction(part))
}

func (p *prog) DateToUnixEpoch(val *value) *value {
	v, m := p.coerceTimestamp(val)
	return p.ssa2(sdatetounixepoch, v, m)
}

func (p *prog) DateToUnixMicro(val *value) *value {
	v, m := p.coerceTimestamp(val)
	return p.ssa2(sdatetounixmicro, v, m)
}

func (p *prog) DateTrunc(part expr.Timepart, val *value) *value {
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
	case expr.Year:
		return p.ssa2(sdatetruncyear, v, m)
	default:
		return p.errorf("unhandled date part in DateTrunc()")
	}
}

func (p *prog) TimeBucket(timestamp, interval *value) *value {
	tv := p.DateToUnixEpoch(timestamp)
	iv, im := p.coerceInt(interval)
	return p.ssa3(stimebucketts, tv, iv, p.And(p.mask(tv), im))
}

func (p *prog) GeoHash(latitude, longitude, numChars *value) *value {
	latV, latM := p.coercefp(latitude)
	lonV, lonM := p.coercefp(longitude)

	if numChars.op == sliteral && isIntImmediate(numChars.imm) {
		return p.ssa3imm(sgeohashimm, latV, lonV, p.And(latM, lonM), numChars.imm)
	}

	charsV, charsM := p.coerceInt(numChars)
	mask := p.And(p.And(latM, lonM), charsM)
	return p.ssa4(sgeohash, latV, lonV, charsV, mask)
}

func (p *prog) GeoTileX(longitude, precision *value) *value {
	lonV, lonM := p.coercefp(longitude)
	precV, precM := p.coerceInt(precision)
	mask := p.And(lonM, precM)
	return p.ssa3(sgeotilex, lonV, precV, mask)
}

func (p *prog) GeoTileY(latitude, precision *value) *value {
	latV, latM := p.coercefp(latitude)
	precV, precM := p.coerceInt(precision)
	mask := p.And(latM, precM)
	return p.ssa3(sgeotiley, latV, precV, mask)
}

func (p *prog) GeoTileES(latitude, longitude, precision *value) *value {
	latV, latM := p.coercefp(latitude)
	lonV, lonM := p.coercefp(longitude)

	if precision.op == sliteral && isIntImmediate(precision.imm) {
		return p.ssa3imm(sgeotileesimm, latV, lonV, p.And(latM, lonM), precision.imm)
	}

	charsV, charsM := p.coerceInt(precision)
	mask := p.And(p.And(latM, lonM), charsM)
	return p.ssa4(sgeotilees, latV, lonV, charsV, mask)
}

func (p *prog) GeoDistance(latitude1, longitude1, latitude2, longitude2 *value) *value {
	lat1V, lat1M := p.coercefp(latitude1)
	lon1V, lon1M := p.coercefp(longitude1)
	lat2V, lat2M := p.coercefp(latitude2)
	lon2V, lon2M := p.coercefp(longitude2)

	mask := p.And(p.And(lat1M, lon1M), p.And(lat2M, lon2M))
	return p.ssa5(sgeodistance, lat1V, lon1V, lat2V, lon2V, mask)
}

func emitAddMulImmI(v *value, c *compilestate) {
	arg0 := v.args[0]                            // t0
	arg1Slot := c.forceStackRef(v.args[1], regS) // t1
	mask := v.args[2]                            // predicate
	imm64 := v.imm.(uint64)

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16u64(v, bc, arg1Slot, imm64)
}

func emitBinaryOp(v *value, c *compilestate) {
	arg0 := v.args[0]                            // t0
	arg1Slot := c.forceStackRef(v.args[1], regS) // t1
	mask := v.args[2]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16(v, bc, arg1Slot)
}

func emitGeoHash(v *value, c *compilestate) {
	arg0 := v.args[0]                            // latitude
	arg1Slot := c.forceStackRef(v.args[1], regS) // longitude
	arg2Slot := c.forceStackRef(v.args[2], regS) // precision
	mask := v.args[3]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16s16(v, bc, arg1Slot, arg2Slot)
}

func emitGeoHashImm(v *value, c *compilestate) {
	arg0 := v.args[0]                            // latitude
	arg1Slot := c.forceStackRef(v.args[1], regS) // longitude
	mask := v.args[2]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16u16(v, bc, arg1Slot, uint16(toi64(v.imm)))
}

func emitGeoTileXY(v *value, c *compilestate) {
	arg0 := v.args[0]                            // coordinate
	arg1Slot := c.forceStackRef(v.args[1], regS) // precision
	mask := v.args[2]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16(v, bc, arg1Slot)
}

func emitGeoDistance(v *value, c *compilestate) {
	arg0 := v.args[0]                            // lat1
	arg1Slot := c.forceStackRef(v.args[1], regS) // lon1
	arg2Slot := c.forceStackRef(v.args[2], regS) // lat2
	arg3Slot := c.forceStackRef(v.args[3], regS) // lon2
	mask := v.args[4]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16s16s16(v, bc, arg1Slot, arg2Slot, arg3Slot)
}

func emitdatediffparam(v *value, c *compilestate) {
	arg0 := v.args[0]                            // t0
	arg1Slot := c.forceStackRef(v.args[1], regS) // t1
	mask := v.args[2]                            // predicate
	imm64 := v.imm.(uint64)                      // parameter

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.ops16u64(v, bc, arg1Slot, imm64)
}

func emitdatediffmonthyear(v *value, c *compilestate) {
	arg0 := v.args[0]                            // t0
	arg1Slot := c.forceStackRef(v.args[1], regS) // t1
	mask := v.args[2]                            // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)

	isYearImm := uint16(0)
	if v.op == sdatediffyear {
		isYearImm = 1
	}

	c.ops16u16(v, bc, arg1Slot, isYearImm)
}

func emitdateextract(v *value, c *compilestate) {
	arg0 := v.args[0] // t0
	mask := v.args[1] // predicate

	info := ssainfo[v.op]
	bc := info.bc

	c.loadk(v, mask)
	c.loads(v, arg0)
	c.clobbers(v)
	c.op(v, bc)
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
func (p *prog) makeAggregateOp(opF, opI ssaop, child *value, slot int) (v *value, fp bool) {
	if isIntValue(child) {
		scalar, mask := p.coerceInt(child)
		mem := p.InitMem()
		return p.ssa3imm(opI, mem, scalar, mask, slot), false
	}

	scalar, mask := p.coercefp(child)
	mem := p.InitMem()
	return p.ssa3imm(opF, mem, scalar, mask, slot), true
}

func (p *prog) makeTimeAggregateOp(op ssaop, child *value, slot int) *value {
	scalar, mask := p.coerceTimestamp(child)
	mem := p.InitMem()
	return p.ssa3imm(op, mem, scalar, mask, slot)
}

func (p *prog) AggregateSumInt(child *value, slot int) *value {
	child = p.toint(child)
	return p.ssa3imm(saggsumi, p.InitMem(), child, p.mask(child), slot)
}

func (p *prog) AggregateSum(child *value, slot int) (v *value, fp bool) {
	return p.makeAggregateOp(saggsumf, saggsumi, child, slot)
}

func (p *prog) AggregateAvg(child *value, slot int) (v *value, fp bool) {
	return p.makeAggregateOp(saggavgf, saggavgi, child, slot)
}

func (p *prog) AggregateMin(child *value, slot int) (v *value, fp bool) {
	return p.makeAggregateOp(saggminf, saggmini, child, slot)
}

func (p *prog) AggregateMax(child *value, slot int) (v *value, fp bool) {
	return p.makeAggregateOp(saggmaxf, saggmaxi, child, slot)
}

func (p *prog) AggregateEarliest(child *value, slot int) *value {
	return p.makeTimeAggregateOp(saggmints, child, slot)
}

func (p *prog) AggregateLatest(child *value, slot int) *value {
	return p.makeTimeAggregateOp(saggmaxts, child, slot)
}

func (p *prog) AggregateCount(child *value, slot int) *value {
	return p.ssa2imm(saggcount, p.InitMem(), p.notMissing(child), slot)
}

// Slot aggregate operations
func (p *prog) makeAggregateSlotOp(opF, opI ssaop, mem, bucket, v, mask *value, offset int) (rv *value, fp bool) {
	if isIntValue(v) {
		scalar, m := p.coerceInt(v)
		if mask != nil {
			m = p.And(m, mask)
		}
		return p.ssa4imm(opI, mem, bucket, scalar, m, offset), false
	}

	scalar, m := p.coercefp(v)
	if mask != nil {
		m = p.And(m, mask)
	}
	return p.ssa4imm(opF, mem, bucket, scalar, m, offset), true
}

func (p *prog) makeTimeAggregateSlotOp(op ssaop, mem, bucket, v, mask *value, offset int) *value {
	scalar, m := p.coerceTimestamp(v)
	if mask != nil {
		m = p.And(m, mask)
	}
	return p.ssa4imm(op, mem, bucket, scalar, m, offset)
}

func (p *prog) AggregateSlotSum(mem, bucket, value, mask *value, offset int) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotsumf, saggslotsumi, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotSumInt(mem, bucket, value, mask *value, offset int) *value {
	scalar, m := p.coerceInt(value)
	if mask != nil {
		m = p.And(m, mask)
	}
	return p.ssa4imm(saggslotsumi, mem, bucket, scalar, m, offset)
}

func (p *prog) AggregateSlotAvg(mem, bucket, value, mask *value, offset int) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotavgf, saggslotavgi, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotMin(mem, bucket, value, mask *value, offset int) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotminf, saggslotmini, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotMax(mem, bucket, value, mask *value, offset int) (v *value, fp bool) {
	return p.makeAggregateSlotOp(saggslotmaxf, saggslotmaxi, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotEarliest(mem, bucket, value, mask *value, offset int) *value {
	return p.makeTimeAggregateSlotOp(saggslotmints, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotLatest(mem, bucket, value, mask *value, offset int) *value {
	return p.makeTimeAggregateSlotOp(saggslotmaxts, mem, bucket, value, mask, offset)
}

func (p *prog) AggregateSlotCount(mem, bucket, mask *value, offset int) *value {
	return p.ssa3imm(saggslotcount, mem, bucket, mask, offset)
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
	switch v.primary() {
	case stValue:
		return p.ssa2(shashvalue, v, p.mask(v))
	default:
		return p.errorf("bad value %v passed to prog.hash()", v)
	}
}

func (p *prog) hashplus(h *value, v *value) *value {
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
	argtypes := ssainfo[v.op].argtypes
	for i := range v.args {
		if len(argtypes) == 0 {
			str += " m" + strconv.Itoa(v.args[i].id)
			continue
		}
		str += " " + string(argtypes[i].char()) + strconv.Itoa(v.args[i].id)
	}
	if v.imm != nil {
		str += fmt.Sprintf(" $%v", v.imm)
	}
	return str
}

func (p *prog) WriteTo(w io.Writer) (int64, error) {
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
func (p *prog) Graphviz(w io.Writer) {
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
	return p.ValidLanes()
}

type treenode struct {
	child, sibling, parent *value
	// lo and hi are pre- and post-order
	// numbers of this node in the tree
	lo, hi int
}

// tree is a tree of values indexed by value IDs
type tree []treenode

// does value 'v' strictly postdominate 'sub' ?
//
// in plain English:
// there are no users of 'sub' (direct or indirect)
// that are not also used by 'v'
func (t tree) postdom(v, sub *value) bool {
	// v dominates sub if all uses of 'sub'
	// occur 'inside' the sub-expression range
	// delimited by 'v'
	outer := &t[v.id]
	inner := &t[sub.id]
	return outer.lo < inner.lo && inner.hi < outer.hi
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

// unordered returns whether or not the order
// of two values can be freely interchanged when
// taking into account their respective dependencies
// domtree computes the (post)dominator tree
// and puts the result in 'dom' such that
// the immediate postdominator of a value
// 'v' is dom[v.id]
//
// ord must be a valid exeuction ordering
// (all definitions come before uses)
func domtree(root *value, num []int, ord, dom []*value) []*value {
	// by walking this DAG in reverse-postorder,
	// we're guaranteed that we can compute the
	// relaxation of the predecessors into the
	// dominator tree in a single pass
	// (we are guaranteed to see all uses before definition)

	// compute order numbering so we can
	// quickly compute intersections
	for i := range ord {
		num[ord[i].id] = i
	}
	for i := len(ord) - 1; i >= 0; i-- {
		v := ord[i]
	argloop:
		for _, arg := range v.args {
			adom := dom[arg.id]
			if adom == v {
				continue argloop
			}
			if adom == nil {
				dom[arg.id] = v
				continue argloop
			}
			// compute intersect(v, adom)
			// by walking up the dominator tree
			// to the common ancestor that dominates
			// both v and its argument
			common := v
			count := 0
			for adom.id != common.id {
				if num[common.id] < num[adom.id] {
					common = dom[common.id]
				}
				if num[adom.id] < num[common.id] {
					adom = dom[adom.id]
				}
				count++
				if count > 50 {
					panic("???")
				}
			}
			dom[arg.id] = common
		}
	}
	dom[root.id] = nil
	return ord
}

// proginfo caches data structures computed
// during optimization passes; we can use
// it to avoid repeatedly allocating slices
// for dominator trees, etc.
type proginfo struct {
	num  []int    // execution numbering for next bit
	rpo  []*value // valid execution ordering
	pdom []*value // valid postdominator tree
	span tree     // valid postdominator tree, sparse
}

func (i *proginfo) invalidate() {
	i.rpo = i.rpo[:0]
	i.pdom = i.pdom[:0]
	i.span = i.span[:0]
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

// compute the dominator tree from proginfo
// (returns a cached value if present)
func (p *prog) domtree(pi *proginfo) []*value {
	if len(pi.pdom) != 0 {
		return pi.pdom
	}
	ord := p.order(pi)
	if cap(pi.pdom) >= len(p.values) {
		pi.pdom = pi.pdom[:len(p.values)]
		for i := range pi.pdom {
			pi.pdom[i] = nil
		}
		pi.num = pi.num[:len(p.values)]
		for i := range pi.num {
			pi.num[i] = 0
		}
	} else {
		pi.pdom = make([]*value, len(p.values))
		pi.num = make([]int, len(p.values))
	}
	domtree(p.ret, pi.num, ord, pi.pdom)
	return pi.pdom
}

// write the spanning tree of the expression graph
// to 'dst'
func (p *prog) spantree(pi *proginfo) tree {
	if len(pi.span) != 0 {
		return pi.span
	}
	pdom := p.domtree(pi)
	// use old memory if we have it
	if cap(pi.span) >= len(p.values) {
		pi.span = pi.span[:len(p.values)]
		for i := range pi.span {
			pi.span[i].child = nil
			pi.span[i].parent = nil
			pi.span[i].sibling = nil
			pi.span[i].lo = -1
			pi.span[i].hi = -1
		}
	} else {
		pi.span = make(tree, len(p.values))
		for i := range pi.span {
			pi.span[i].lo = -1
			pi.span[i].hi = -1
		}
	}
	for i := range p.values {
		v := p.values[i]
		dom := pdom[v.id]
		if dom == nil {
			continue
		}
		if dom == v {
			panic("dom == v?")
		}
		sp := &pi.span[v.id]
		if sp.parent != nil || sp.sibling != nil {
			panic("duplicate value id?")
		}
		domsp := &pi.span[dom.id]
		sp.parent = dom
		sp.sibling, domsp.child = domsp.child, v
	}
	pi.span.number(p.ret, 0)
	return pi.span
}

// compute pre- and post-order numbers of the tree
func (t tree) number(v *value, n int) int {
	node := &t[v.id]
	if node.lo != -1 {
		panic("re-numbering???")
	}
	node.lo = n
	n++
	for child := node.child; child != nil; child = t[child.id].sibling {
		n = t.number(child, n)
	}
	node.hi = n
	return n + 1
}

// find the articulation point of this sub-expression
//
// returns the edge (v, arg) that is the articulation point
func (p *prog) articulation(v *value, tr tree) (*value, *value) {
	parent := v
	child := parent.maskarg()
	for child.op != sinit && tr.postdom(v, child) {
		// if we find an OR, we have to
		// postdominate both sides
		if child.op == sor {
			if !tr.postdom(v, child.args[0]) || !tr.postdom(v, child.args[1]) {
				break
			}
		}
		if child.op == sxor || child.op == sxnor {
			break
		}
		parent = child
		child = parent.maskarg()
	}
	return parent, child
}

func depends(ponum []int, a, b *value) bool {
	if a == b {
		return true
	}
	// a definitely cannot depend on b
	// if it has a smaller post-order numbering
	// TODO: would performing a similar check
	// with pre-order numbering provide an
	// improvement in time complexity on average?
	if ponum[a.id] < ponum[b.id] {
		return false
	}
	for _, arg := range a.args {
		if depends(ponum, arg, b) {
			return true
		}
	}
	return false
}

// depends computes whether 'a' depends on 'b' transitively
//
// TODO: the time-complexity here is quadratic;
// are there better space/time tradeoffs?
// (AFAIK there are no linear-time + linear-space solutions...)
func (p *prog) depends(pi *proginfo, a, b *value) bool {
	num := p.numbering(pi)
	return depends(num, a, b)
}

func (p *prog) androtate(v, left, right *value, tr tree, pi *proginfo) *value {
	conjunctive := func(v *value) bool {
		return !(v.op == sxor || v.op == sxnor)
	}

	// if the value is the terminal use of 'left'
	// and 'right' does not not depend on 'left',
	// set 'left' to depend on 'right'
	if conjunctive(left) && tr.postdom(v, left) {
		lparent, lchild := p.articulation(left, tr)
		if lchild == right {
			return left
		}
		if !p.depends(pi, right, lparent) {
			if lchild.op == sinit || lchild.op == stuples {
				lparent.setmask(right)
				return left
			}
		}
	}

	// simply the reverse of the above
	if conjunctive(right) && tr.postdom(v, right) {
		rparent, rchild := p.articulation(right, tr)
		if rchild == left {
			return right
		}
		if !p.depends(pi, left, rparent) {
			if rchild.op == sinit || rchild.op == stuples {
				rparent.setmask(left)
				return right
			}
		}
	}
	return nil
}

// perform a series of boolean reductions
// so that we simplify expressions that
// depend on values we have determined
// to be constantly false
func (p *prog) falseprop(pi *proginfo) {
	var rewrite []*value
	opt := true
	for opt {
		opt = false
		// walking in order means we always examine
		// values before their uses, so every argument
		// to each instruction should be rewritten by
		// the time we get to it...
		ord := p.order(pi)
		for i := range ord {
			v := ord[i]
			if rewrite != nil {
				for j, arg := range v.args {
					for rewrite[arg.id] != nil {
						v.args[j] = rewrite[arg.id]
						arg = v.args[j]
						opt = true
					}
				}
			}

			switch v.op {
			case sand:
				// x AND false -> false,
				// false AND x -> false
				if v.args[0].op == skfalse || v.args[1].op == skfalse {
					v.op = skfalse
					v.args = nil
					v.imm = nil
					opt = true
				} else if v.args[0].op == sinit {
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					// TRUE AND x -> x
					rewrite[v.id] = v.args[1]
					opt = true
				} else if v.args[1].op == sinit {
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					// x AND TRUE -> x
					rewrite[v.id] = v.args[0]
					opt = true
				}
			case sor:
				if v.args[0].op == skfalse {
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[1]
					opt = true
				} else if v.args[1].op == skfalse {
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[0]
					opt = true
				}
			case sxor:
				// x ^ x = false
				if v.args[0] == v.args[1] {
					v.op = skfalse
					v.args = nil
					opt = true
				} else if v.args[0].op == sinit {
					// true ^ x -> !x
					v.op = snand
					v.args[0], v.args[1] = v.args[1], v.args[0]
					opt = true
				} else if v.args[1].op == sinit {
					// x ^ true -> !x
					v.op = snand
					opt = true
				} else if v.args[0].op == skfalse {
					// x ^ false -> x
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[1]
					opt = true
				} else if v.args[1].op == skfalse {
					// false ^ x -> x
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[0]
					opt = true
				}
			case sxnor:
				if v.args[0] == v.args[1] {
					// x xnor x -> true
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = p.values[0] // sinit = true
					opt = true
				} else if v.args[0].op == sinit {
					// true xnor x -> x
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[1]
					opt = true
				} else if v.args[1].op == sinit {
					// x xnor true -> x
					if rewrite == nil {
						rewrite = make([]*value, len(p.values))
					}
					rewrite[v.id] = v.args[0]
					opt = true
				} else if v.args[0].op == skfalse {
					// false xnor x -> !x
					v.op = snand
					v.args[0] = v.args[1]
					v.args[1] = p.values[0] // sinit
					opt = true
				} else if v.args[1].op == skfalse {
					// x xnor false -> !x
					v.op = snand
					v.args[1] = p.values[0]
					opt = true
				}
			case snand:
				if v.args[0] == v.args[1] {
					v.op = skfalse
					v.args = nil
					opt = true
					break
				}
				fallthrough
			default:
				if m := v.maskarg(); m != nil &&
					m.op == skfalse &&
					ssainfo[v.op].rettype&stMem == 0 {
					if ssainfo[v.op].blend {
						if rewrite == nil {
							rewrite = make([]*value, len(p.values))
						}
						rewrite[v.id] = v.args[0]
					} else {
						v.op = skfalse
						v.args = nil
						v.imm = nil
						opt = true
					}
				}
			}
		}
		if opt {
			pi.invalidate()
			if rewrite != nil && rewrite[p.ret.id] != nil {
				p.ret = rewrite[p.ret.id]
			}
		}
	}
}

func (p *prog) anyDepends(pi *proginfo, any []*value, val *value) bool {
	for i := range any {
		if p.depends(pi, any[i], val) {
			return true
		}
	}
	return false
}

// rotate AND expressions so that the mask on one side
// of the expression is passed to the beginning of the
// second expression
//
// concretely: (AND (exprA ops... maskA) (exprB ops... maskB))
// becomes:    (exprB ops... (AND maskB (exprA ops... maskA)))
// (typically the inner AND can be elided as well)
func (p *prog) andprop(pi *proginfo) {
	var tr tree
	opt := true

	for opt {
		opt = false

	inner:
		for i := range p.values {
			// find values that have mask arguments
			// that are computed as 'AND', and where
			// this use postdominates the calculation
			// of the AND (and therefore its arguments)
			v := p.values[i]
			and := v.maskarg()
			if and == nil || and.op != sand {
				continue
			}
			// super simple opt:
			//  (and k0 _) -> k0
			//  (and _ k0) -> k0
			if and.args[0].op == sinit {
				v.setmask(and.args[1])
				opt = true
				pi.invalidate()
				continue
			}
			if and.args[1].op == sinit {
				v.setmask(and.args[0])
				opt = true
				pi.invalidate()
				continue
			}
			if v.op == sxor || v.op == sxnor {
				continue
			}

			if tr == nil {
				tr = p.spantree(pi)
			}
			if !tr.postdom(v, and) {
				continue
			}

			lhs, rhs := and.args[0], and.args[1]
			rewrite := p.androtate(v, lhs, rhs, tr, pi)
			if rewrite == nil {
				// let's perform a non-obvious
				// rotation and see if we can optimize
				// using more aggressive hoisting:
				//  (op ... (and x y)) -> (and (op ... x) y)
				// puts the 'and' higher in the dominator tree,
				// which means we can reach back further into
				// the program to find an articulation
				//
				// this is only profitable if v is very dominant,
				// and it is only possible if v does not depend
				// on the 'and' result except for its mask input
				if p.ret.maskarg() != v || p.anyDepends(pi, v.args[:len(v.args)-1], and) {
					continue inner
				}

				and.args[0] = v
				v.setmask(lhs)
				p.ret.setmask(and)
				pi.invalidate()
				tr = p.spantree(pi)
				rewrite := p.androtate(and, and.args[0], and.args[1], tr, pi)
				if rewrite == nil {
					// ... and try right-pivot
					pi.invalidate()
					and.args[0] = lhs
					and.args[1] = v
					v.setmask(rhs)
					tr = p.spantree(pi)
					rewrite = p.androtate(and, and.args[0], and.args[1], tr, pi)
				}
				if rewrite == nil {
					// restore original state
					// from the right-pivot that failed
					p.ret.setmask(v)
					and.args[1] = rhs
					v.setmask(and)
				} else {
					p.ret = rewrite
					opt = true
				}
				pi.invalidate()
				tr = nil
				continue inner
			}
			v.setmask(rewrite)
			// must only occur in the subsequent instructions
			// re-compute spanning tree; we've dirtied it
			pi.invalidate()
			tr = nil
			opt = true
		}
	}
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

func (p *prog) GraphvizDomtree(pi *proginfo, dst io.Writer) {
	dt := p.domtree(pi)
	fmt.Fprintln(dst, "digraph domtree {")
	for i := range p.values {
		v := p.values[i]
		fmt.Fprintf(dst, "\t%q [label=%q];\n", v.Name(), v.String())
		dom := dt[v.id]
		if dom != nil {
			fmt.Fprintf(dst, "\t%q -> %q;\n", dom.Name(), v.Name())
		}
	}
	fmt.Fprintln(dst, "}")
}

// optimize the program and set
// p.values to the values in program order
func (p *prog) optimize() {
	// Two renumbering passes here;
	// each effectively acts as dead
	// code elimination
	var pi proginfo
	order := p.order(&pi)
	for i := range order {
		order[i].id = i
	}
	// p.exprs is invalidated if we re-number
	p.exprs = nil
	p.values = p.values[:copy(p.values, order)]
	// optimization passes
	p.falseprop(&pi)
	p.ordersyms(&pi)
	p.andprop(&pi)

	// final dead code elimination and scheduling
	order = p.finalorder(p.order(&pi), p.numbering(&pi))
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
	for i := range p.values {
		v := p.values[i]
		if v.id != i {
			panic("liveranges() before re-numbering")
		}
		op := v.op
		if op == smergemem {
			// variadic, and only
			// memory args anyway...
			continue
		}
		types := ssainfo[op].argtypes
		args := p.values[i].args
		for j := range args {
			switch types[j] {
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

	trees       []*radixTree64
	instrs      []byte
	dict        []string
	litbuf      []byte // output datum literals
	needscratch bool   // need the scratch buffer to be allocated
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
	c.instrs = append(c.instrs, byte(op), byte(op>>8))
}

func (c *compilestate) opu8(v *value, op bcop, imm uint8) {
	checkImmediateBeforeEmit1(op, 1)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm))
}

func (c *compilestate) opu16(v *value, op bcop, imm0 uint16) {
	checkImmediateBeforeEmit1(op, 2)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8))
}

func (c *compilestate) opu32(v *value, op bcop, imm0 uint32) {
	checkImmediateBeforeEmit1(op, 4)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8), byte(imm0>>16), byte(imm0>>24))
}

func (c *compilestate) opu64(v *value, op bcop, imm0 uint64) {
	checkImmediateBeforeEmit1(op, 8)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8), byte(imm0>>16), byte(imm0>>24), byte(imm0>>32), byte(imm0>>40), byte(imm0>>48), byte(imm0>>56))
}

func (c *compilestate) opu16u16(v *value, op bcop, imm0, imm1 uint16) {
	checkImmediateBeforeEmit2(op, 2, 2)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8),
		byte(imm1), byte(imm1>>8))
}

func (c *compilestate) opu16u32(v *value, op bcop, imm0 uint16, imm1 uint32) {
	checkImmediateBeforeEmit2(op, 2, 4)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8),
		byte(imm1), byte(imm1>>8), byte(imm1>>16), byte(imm1>>24))
}

func (c *compilestate) opu16u64(v *value, op bcop, imm0 uint16, imm1 uint64) {
	checkImmediateBeforeEmit2(op, 2, 8)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8),
		byte(imm1), byte(imm1>>8), byte(imm1>>16), byte(imm1>>24), byte(imm1>>32), byte(imm1>>40), byte(imm1>>48), byte(imm1>>56))
}

func (c *compilestate) opu32u32(v *value, op bcop, imm0 uint32, imm1 uint32) {
	checkImmediateBeforeEmit2(op, 4, 4)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8), byte(imm0>>16), byte(imm0>>24),
		byte(imm1), byte(imm1>>8), byte(imm1>>16), byte(imm1>>24))
}

func (c *compilestate) opu16u16u16(v *value, op bcop, imm0, imm1, imm2 uint16) {
	checkImmediateBeforeEmit3(op, 2, 2, 2)
	c.instrs = append(
		c.instrs,
		byte(op), byte(op>>8),
		byte(imm0), byte(imm0>>8),
		byte(imm1), byte(imm1>>8),
		byte(imm2), byte(imm2>>8))
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

func (c *compilestate) ops16u64(v *value, op bcop, imm0 stackslot, imm1 uint64) {
	c.opu16u64(v, op, uint16(imm0), imm1)
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
			d = ion.Float(float32(i)) // TODO: maybe don't convert here...
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
	types := info.argtypes
	for i := range v.args {
		arg := v.args[i]
		argType := types[i]
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
	c.needscratch = true
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
	if v.op != snand {
		panic("?")
	}

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
		c.opu32(v, opfindsym3, uint32(sym))
		return
	}
	if c.regs.cur[regK] == addmask.id {
		c.loadk(v, addmask)
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
	var bits uint8 // bits = ION descriptor tag (https://amzn.github.io/ion-docs/docs/binary.html)
	switch v.op {
	case stostr:
		bits = 0x08 //ION typed value format for string is 8
	case stolist:
		bits = 0x0b
	case stotime:
		bits = 0x06
	default:
		panic("unrecognized op for emitslice")
	}
	val := v.args[0]
	mask := v.args[1]
	c.loadk(v, mask)
	c.loadv(v, val)
	c.clobbers(v)
	c.opu8(v, opunpack, bits)
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

func emitBinaryArithmeticOp(v *value, c *compilestate) {
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

func emitWidthBucket(v *value, c *compilestate) {
	val := v.args[0]
	minSlot := c.forceStackRef(v.args[1], regS)
	maxSlot := c.forceStackRef(v.args[2], regS)
	cntSlot := c.forceStackRef(v.args[3], regS)
	msk := v.args[4]

	c.loadk(v, msk)
	c.loads(v, val)
	c.clobbers(v)
	c.ops16s16s16(v, ssainfo[v.op].bc, minSlot, maxSlot, cntSlot)
}

func emitboxmask(v *value, c *compilestate) {
	truefalse := v.args[0]
	output := v.args[1]

	// we must have scratch space available
	// during program execution
	c.needscratch = true
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

func emitstorevblend(v *value, c *compilestate) {
	_ = v.args[0] // mem
	arg := v.args[1]
	mask := v.args[2]
	slot := v.imm.(int)
	if mask.op == skfalse {
		// the only observable side-effect
		// is updating lanes, so no lanes set
		// means this instruction is entirely dead
		return
	}
	c.loadk(v, mask)
	c.loadv(v, arg)
	c.ops16(v, opsaveblendv, stackslot(slot))
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
	k := v.args[0]
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
	c.needscratch = c.needscratch || info.scratch
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

func (p *prog) emit1(v *value, c *compilestate) {
	defer func() {
		if err := recover(); err != nil {
			println(fmt.Sprintf("Error emitting %v: %v", v.String(), err))
			p.WriteTo(os.Stderr)
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

func (p *prog) compile(dst *bytecode) error {
	var c compilestate

	if err := p.compileinto(&c); err != nil {
		return err
	}

	dst.vstacksize = c.regs.stack.stackSize(stackTypeV)
	dst.hstacksize = c.regs.stack.stackSize(stackTypeH)

	dst.allocStacks()
	dst.trees = c.trees
	dst.dict = c.dict
	dst.compiled = c.instrs
	dst.scratchreserve = 0

	// try to reserve scratch space destructively:
	// if we already have a buffer, just copy the right
	// amount of data into it and reserve it; otherwise
	// just force the buffer to be allocated with a reasonable
	// amount of space
	if c.litbuf != nil {
		if !dst.setlit(c.litbuf) {
			return fmt.Errorf("literal buffer (len=%d) too large", len(c.litbuf))
		}
	} else if c.needscratch && dst.scratch == nil {
		// zero-length, large-capacity buffer
		dst.scratch = Malloc()[:0]
		dst.scratchoff, _ = vmdispl(dst.scratch[:1])
	}
	return dst.finalize()
}

// append a second program to 'dst'
func (p *prog) appendcode(dst *bytecode) error {
	var c compilestate
	c.dict = dst.dict

	if err := p.compileinto(&c); err != nil {
		return err
	}

	dst.ensureVStackSize(c.regs.stack.stackSize(stackTypeV))
	dst.ensureHStackSize(c.regs.stack.stackSize(stackTypeH))
	dst.allocStacks()

	dst.dict = c.dict
	dst.compiled = append(dst.compiled, c.instrs...)
	if c.litbuf != nil || c.needscratch {
		return fmt.Errorf("scratch buffer not handled in appendcode (yet)")
	}
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

// Renumber performs some simple dead-code elimination
// and re-orders and re-numbers each value in prog.
//
// Renumber must be called before prog.Symbolize.
func (p *prog) Renumber() {
	var pi proginfo
	ord := p.order(&pi)
	for i := range ord {
		ord[i].id = i
	}
	p.values = ord
}

// Symbolize applies the symbol table from 'st'
// to the program by copying the old program
// to 'dst' and applying rewrites to findsym operations.
func (p *prog) Symbolize(st *ion.Symtab, dst *prog) error {
	p.clone(dst)
	return dst.symbolize(st)
}

func recompile(st *ion.Symtab, src, dst *prog, final *bytecode) error {
	if !dst.IsStale(st) {
		return nil
	}
	err := src.Symbolize(st, dst)
	if err != nil {
		return err
	}

	return dst.compile(final)
}

// IsStale returns whether the symbolized program
// (see prog.Symbolize) is stale with respect to
// the provided symbol table.
func (p *prog) IsStale(st *ion.Symtab) bool {
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

func (p *prog) symbolize(st *ion.Symtab) error {
	p.resolved = p.resolved[:0]
	for i := range p.values {
		v := p.values[i]
		if v.op == shashmember {
			p.literals = true
			v.imm = p.mktree(st, v.imm)
		} else if v.op == shashlookup {
			p.literals = true
			v.imm = p.mkhash(st, v.imm)
		} else if v.op == sliteral {
			if d, ok := v.imm.(ion.Datum); ok {
				p.literals = true
				var tmp ion.Buffer
				d.Encode(&tmp, st)
				v.imm = rawDatum(tmp.Bytes())
			}
		}
		if v.op != sdot {
			continue
		}
		str := v.imm.(string)
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
	}
	p.symbolized = true
	return nil
}
