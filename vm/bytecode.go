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
	"math"
	"os"
	"strings"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

//go:generate go run _generate/genops.go
//go:generate gofmt -w ops_gen.go
//go:generate go run _generate/genconst.go -i evalbc_amd64.s -o bc_constant_gen.h

// --- How to Add an Instruction ---
//  - define a new TEXT label in evalbc_{arch}.s
//    that begins with 'bc'
//  - run 'go generate'
//  - add opcode information below

const bcSlotSize = 2                    // stack slot size in bytes
const bcLaneCount = 16                  // number of lanes processed per iteration
const bcLaneCountMask = bcLaneCount - 1 // number of lanes as mask

// actual bytecode constants are generated automatically
// by reading the assembly source and generating a named
// constant for each bytecode function
type bcop uint16

// bcArgType is a type of a single BC instruction argument that follows its opcode
// identifier.
type bcArgType uint8

const (
	bcK        bcArgType = iota // bool stack-slot, shown as k[imm]
	bcS                         // scalar stack-slot, shown as s[imm]
	bcV                         // value stack-slot, shown as v[imm]
	bcB                         // struct pointer, shown as b[imm]
	bcH                         // hash-slot, shown as h[imm]
	bcL                         // bucket-slot, shown as l[imm]
	bcDictSlot                  // 16-bit dictionary reference, shown as dict[imm]
	bcAuxSlot                   // 32-bit aux value slot
	bcAggSlot                   // 32-bit aggregation slot
	bcSymbolID                  // 32-bit symbol identifier
	bcLitRef                    // 64-bit value reference
	bcImmI8                     // signed 8-bit int immediate argument
	bcImmI16                    // signed 16-bit int immediate argument
	bcImmI32                    // signed 32-bit int immediate argument
	bcImmI64                    // signed 64-bit int immediate argument
	bcImmU8                     // unsigned 8-bit int immediate argument
	bcImmU16                    // unsigned 16-bit int immediate argument
	bcImmU32                    // unsigned 32-bit int immediate argument
	bcImmU64                    // unsigned 64-bit int immediate argument
	bcImmF64                    // 64-bit float immediate argument
)

func (a bcArgType) String() string {
	switch a {
	case bcK:
		return "K"
	case bcS:
		return "S"
	case bcV:
		return "V"
	case bcB:
		return "B"
	case bcH:
		return "H"
	case bcL:
		return "L"
	case bcDictSlot:
		return "DictSlot"
	case bcAuxSlot:
		return "AuxSlot"
	case bcAggSlot:
		return "AggSlot"
	case bcSymbolID:
		return "SymbolID"
	case bcLitRef:
		return "LitRef"
	case bcImmI8:
		return "ImmI8"
	case bcImmI16:
		return "ImmI16"
	case bcImmI32:
		return "ImmI32"
	case bcImmI64:
		return "ImmI64"
	case bcImmU8:
		return "ImmU8"
	case bcImmU16:
		return "ImmU16"
	case bcImmU32:
		return "ImmU32"
	case bcImmU64:
		return "ImmU64"
	case bcImmF64:
		return "ImmF64"
	default:
		return "<Unknown>"
	}
}

// Maps each bcArgType into a width of the immediate in bytes.
var bcImmWidth = [...]uint8{
	bcK:        bcSlotSize,
	bcS:        bcSlotSize,
	bcV:        bcSlotSize,
	bcH:        bcSlotSize,
	bcL:        bcSlotSize,
	bcB:        bcSlotSize,
	bcDictSlot: 2,
	bcAuxSlot:  2,
	bcAggSlot:  4,
	bcSymbolID: 4,
	bcLitRef:   10,
	bcImmI8:    1,
	bcImmI16:   2,
	bcImmI32:   4,
	bcImmI64:   8,
	bcImmU8:    1,
	bcImmU16:   2,
	bcImmU32:   4,
	bcImmU64:   8,
	bcImmF64:   8,
}

func (a bcArgType) immWidth() int {
	return int(bcImmWidth[a])
}

type bcopinfo struct {
	text string // opcode name
	// out and in are the output and input argument specs, respectively;
	// the serialized argument layout for non-variadic ops is
	//
	//   out ... in ...
	//
	// and for variadic ops it is
	//
	//   out ... in ... uint32(chunks) chunks...
	//
	// where each chunk is len(va) arguments consecutively
	out, in []bcArgType
	va      []bcArgType
	scratch int // desired scratch space (up to PageSize)
}

func bcmakeopinfo() [_maxbcop]bcopinfo {
	sharedArgs := make(map[string][]bcArgType)

	// r -> register-from-string
	r := func(s string) []bcArgType {
		if ret, ok := sharedArgs[s]; ok {
			return ret
		}
		ret := make([]bcArgType, len(s))
		for i, c := range s {
			var t bcArgType
			switch c {
			case 'k':
				t = bcK
			case 's':
				t = bcS
			case 'v':
				t = bcV
			case 'b':
				t = bcB
			case 'h':
				t = bcH
			case 'l':
				t = bcL
			case 'i': // Integer
				t = bcImmI64
			case 'f': // Float
				t = bcImmF64
			case 'd': // Datum
				t = bcLitRef
			case '2': // 2-byte immediate
				t = bcImmU16
			case '4': // 4-byte immediate
				t = bcImmU32
			case '8': // 8-byte immediate
				t = bcImmU64
			case 'y': // sYmbol
				t = bcSymbolID
			case 'a': // Aggregate
				t = bcAggSlot
			case 'p': // Param
				t = bcAuxSlot
			case 'x':
				t = bcDictSlot
			default:
				panic("bad char:" + string(c))
			}
			ret[i] = t
		}
		sharedArgs[s] = ret
		return ret
	}

	return [_maxbcop]bcopinfo{
		// When adding a new entry, please read the following rules:
		//   - Opcode 'text' represents the opcode name, use dots to separate type(s) the instruction operates on
		//   - Opcode 'args' represents opcode arguments, use makeArgs() to define them
		//   - Opcode 'va' field represents variable arguments that follow regular `args`

		// Control flow instructions:
		//   - ret  - terminates execution; returns current mask
		opret:    {text: "ret"},
		opretk:   {text: "ret.k", in: r("k")},
		opretsk:  {text: "ret.s.k", in: r("sk")},
		opretbk:  {text: "ret.b.k", in: r("bk")},
		opretbhk: {text: "ret.b.h.k", in: r("bhk")},

		opinit: {text: "init", out: r("bk")},

		// Mask instructions:
		//   - false - sets predicate to FALSE
		//   - others - mask-only operations
		opbroadcast0k: {text: "broadcast0.k", out: r("k")},        // k[0] = 0
		opbroadcast1k: {text: "broadcast1.k", out: r("k")},        // k[0] = 1              & ValidLanes
		opnotk:        {text: "not.k", out: r("k"), in: r("k")},   // k[0] = !k[1]          & ValidLanes
		opandk:        {text: "and.k", out: r("k"), in: r("kk")},  // k[0] = k[1] & k[2]    & ValidLanes
		opandnk:       {text: "andn.k", out: r("k"), in: r("kk")}, // k[0] = !k[1] & k[2]   & ValidLanes
		opork:         {text: "or.k", out: r("k"), in: r("kk")},   // k[0] = k[1] | k[2]    & ValidLanes
		opxork:        {text: "xor.k", out: r("k"), in: r("kk")},  // k[0] = k[1] ^ k[2]    & ValidLanes
		opxnork:       {text: "xnor.k", out: r("k"), in: r("kk")}, // k[0] = !(k[1] ^ k[2]) & ValidLanes
		opfalse:       {text: "false.k", out: r("vk")},

		// Integer math
		opbroadcasti64:   {text: "broadcast.i64", out: r("s"), in: r("i")},
		opabsi64:         {text: "abs.i64", out: r("sk"), in: r("sk")},
		opnegi64:         {text: "neg.i64", out: r("sk"), in: r("sk")},
		opsigni64:        {text: "sign.i64", out: r("sk"), in: r("sk")},
		opsquarei64:      {text: "square.i64", out: r("sk"), in: r("sk")},
		opbitnoti64:      {text: "bitnot.i64", out: r("s"), in: r("sk")},
		opbitcounti64:    {text: "bitcount.i64", out: r("s"), in: r("sk")},
		opaddi64:         {text: "add.i64", out: r("sk"), in: r("ssk")},
		opaddi64imm:      {text: "add.i64@imm", out: r("sk"), in: r("sik")},
		opsubi64:         {text: "sub.i64", out: r("sk"), in: r("ssk")},
		opsubi64imm:      {text: "sub.i64@imm", out: r("sk"), in: r("sik")},
		oprsubi64imm:     {text: "rsub.i64@imm", out: r("sk"), in: r("sik")},
		opmuli64:         {text: "mul.i64", out: r("sk"), in: r("ssk")},
		opmuli64imm:      {text: "mul.i64@imm", out: r("sk"), in: r("sik")},
		opdivi64:         {text: "div.i64", out: r("sk"), in: r("ssk")},
		opdivi64imm:      {text: "div.i64@imm", out: r("sk"), in: r("sik")},
		oprdivi64imm:     {text: "rdiv.i64@imm", out: r("sk"), in: r("sik")},
		opmodi64:         {text: "mod.i64", out: r("sk"), in: r("ssk")},
		opmodi64imm:      {text: "mod.i64@imm", out: r("sk"), in: r("sik")},
		oprmodi64imm:     {text: "rmod.i64@imm", out: r("sk"), in: r("sik")},
		opaddmuli64imm:   {text: "addmul.i64@imm", out: r("sk"), in: r("ssik")},
		opminvaluei64:    {text: "minvalue.i64", out: r("s"), in: r("ssk")},
		opminvaluei64imm: {text: "minvalue.i64@imm", out: r("s"), in: r("sik")},
		opmaxvaluei64:    {text: "maxvalue.i64", out: r("s"), in: r("ssk")},
		opmaxvaluei64imm: {text: "maxvalue.i64@imm", out: r("s"), in: r("sik")},
		opandi64:         {text: "and.i64", out: r("s"), in: r("ssk")},
		opandi64imm:      {text: "and.i64@imm", out: r("s"), in: r("sik")},
		opori64:          {text: "or.i64", out: r("s"), in: r("ssk")},
		opori64imm:       {text: "or.i64@imm", out: r("s"), in: r("sik")},
		opxori64:         {text: "xor.i64", out: r("s"), in: r("ssk")},
		opxori64imm:      {text: "xor.i64@imm", out: r("s"), in: r("sik")},
		opslli64:         {text: "sll.i64", out: r("s"), in: r("ssk")},
		opslli64imm:      {text: "sll.i64@imm", out: r("s"), in: r("sik")},
		opsrai64:         {text: "sra.i64", out: r("s"), in: r("ssk")},
		opsrai64imm:      {text: "sra.i64@imm", out: r("s"), in: r("sik")},
		opsrli64:         {text: "srl.i64", out: r("s"), in: r("ssk")},
		opsrli64imm:      {text: "srl.i64@imm", out: r("s"), in: r("sik")},

		// Floating point math
		opbroadcastf64:   {text: "broadcast.f64", out: r("s"), in: r("f")},
		opabsf64:         {text: "abs.f64", out: r("sk"), in: r("sk")},
		opnegf64:         {text: "neg.f64", out: r("sk"), in: r("sk")},
		opsignf64:        {text: "sign.f64", out: r("sk"), in: r("sk")},
		opsquaref64:      {text: "square.f64", out: r("s"), in: r("sk")},
		opsqrtf64:        {text: "sqrt.f64", out: r("sk"), in: r("sk")},
		opcbrtf64:        {text: "cbrt.f64", out: r("sk"), in: r("sk")},
		oproundf64:       {text: "round.f64", out: r("s"), in: r("sk")},
		oproundevenf64:   {text: "roundeven.f64", out: r("s"), in: r("sk")},
		optruncf64:       {text: "trunc.f64", out: r("s"), in: r("sk")},
		opfloorf64:       {text: "floor.f64", out: r("s"), in: r("sk")},
		opceilf64:        {text: "ceil.f64", out: r("s"), in: r("sk")},
		opaddf64:         {text: "add.f64", out: r("sk"), in: r("ssk")},
		opaddf64imm:      {text: "add.f64@imm", out: r("sk"), in: r("sfk")},
		opsubf64:         {text: "sub.f64", out: r("sk"), in: r("ssk")},
		opsubf64imm:      {text: "sub.f64@imm", out: r("sk"), in: r("sfk")},
		oprsubf64imm:     {text: "rsub.f64@imm", out: r("sk"), in: r("sfk")},
		opmulf64:         {text: "mul.f64", out: r("sk"), in: r("ssk")},
		opmulf64imm:      {text: "mul.f64@imm", out: r("sk"), in: r("sfk")},
		opdivf64:         {text: "div.f64", out: r("sk"), in: r("ssk")},
		opdivf64imm:      {text: "div.f64@imm", out: r("sk"), in: r("sfk")},
		oprdivf64imm:     {text: "rdiv.f64@imm", out: r("sk"), in: r("sfk")},
		opmodf64:         {text: "mod.f64", out: r("sk"), in: r("ssk")},
		opmodf64imm:      {text: "mod.f64@imm", out: r("sk"), in: r("sfk")},
		oprmodf64imm:     {text: "rmod.f64@imm", out: r("sk"), in: r("sfk")},
		opminvaluef64:    {text: "minvalue.f64", out: r("s"), in: r("ssk")},
		opminvaluef64imm: {text: "minvalue.f64@imm", out: r("s"), in: r("sfk")},
		opmaxvaluef64:    {text: "maxvalue.f64", out: r("s"), in: r("ssk")},
		opmaxvaluef64imm: {text: "maxvalue.f64@imm", out: r("s"), in: r("sfk")},
		opexpf64:         {text: "exp.f64", out: r("sk"), in: r("sk")},
		opexpm1f64:       {text: "expm1.f64", out: r("sk"), in: r("sk")},
		opexp2f64:        {text: "exp2.f64", out: r("sk"), in: r("sk")},
		opexp10f64:       {text: "exp10.f64", out: r("sk"), in: r("sk")},
		oplnf64:          {text: "ln.f64", out: r("sk"), in: r("sk")},
		opln1pf64:        {text: "ln1p.f64", out: r("sk"), in: r("sk")},
		oplog2f64:        {text: "log2.f64", out: r("sk"), in: r("sk")},
		oplog10f64:       {text: "log10.f64", out: r("sk"), in: r("sk")},
		opsinf64:         {text: "sin.f64", out: r("sk"), in: r("sk")},
		opcosf64:         {text: "cos.f64", out: r("sk"), in: r("sk")},
		optanf64:         {text: "tan.f64", out: r("sk"), in: r("sk")},
		opasinf64:        {text: "asin.f64", out: r("sk"), in: r("sk")},
		opacosf64:        {text: "acos.f64", out: r("sk"), in: r("sk")},
		opatanf64:        {text: "atan.f64", out: r("sk"), in: r("sk")},
		opatan2f64:       {text: "atan2.f64", out: r("sk"), in: r("ssk")},
		ophypotf64:       {text: "hypot.f64", out: r("sk"), in: r("ssk")},
		oppowf64:         {text: "pow.f64", out: r("sk"), in: r("ssk")},
		oppowuintf64:     {text: "powuint.f64", out: r("s"), in: r("sik")},

		// Conversion instructions
		opcvtktof64:        {text: "cvt.ktof64", out: r("s"), in: r("k")},
		opcvtktoi64:        {text: "cvt.ktoi64", out: r("s"), in: r("k")},
		opcvti64tok:        {text: "cvt.i64tok", out: r("k"), in: r("sk")},
		opcvtf64tok:        {text: "cvt.f64tok", out: r("k"), in: r("sk")},
		opcvti64tof64:      {text: "cvt.i64tof64", out: r("sk"), in: r("sk")},
		opcvttruncf64toi64: {text: "cvttrunc.f64toi64", out: r("sk"), in: r("sk")},
		opcvtfloorf64toi64: {text: "cvtfloor.f64toi64", out: r("sk"), in: r("sk")},
		opcvtceilf64toi64:  {text: "cvtceil.f64toi64", out: r("sk"), in: r("sk")},
		opcvti64tostr:      {text: "cvt.i64tostr", out: r("sk"), in: r("sk"), scratch: 20 * 16},

		// Comparison instructions
		opsortcmpvnf:  {text: "sortcmpv@nf", out: r("sk"), in: r("vvk")},
		opsortcmpvnl:  {text: "sortcmpv@nl", out: r("sk"), in: r("vvk")},
		opcmpv:        {text: "cmpv", out: r("sk"), in: r("vvk")},
		opcmpvk:       {text: "cmpv.k", out: r("sk"), in: r("vkk")},
		opcmpvkimm:    {text: "cmpv.k@imm", out: r("sk"), in: r("v2k")},
		opcmpvi64:     {text: "cmpv.i64", out: r("sk"), in: r("vsk")},
		opcmpvi64imm:  {text: "cmpv.i64@imm", out: r("sk"), in: r("vik")},
		opcmpvf64:     {text: "cmpv.f64", out: r("sk"), in: r("vsk")},
		opcmpvf64imm:  {text: "cmpv.f64@imm", out: r("sk"), in: r("vfk")},
		opcmpltstr:    {text: "cmplt.str", out: r("k"), in: r("ssk")},
		opcmplestr:    {text: "cmple.str", out: r("k"), in: r("ssk")},
		opcmpgtstr:    {text: "cmpgt.str", out: r("k"), in: r("ssk")},
		opcmpgestr:    {text: "cmpge.str", out: r("k"), in: r("ssk")},
		opcmpltk:      {text: "cmplt.k", out: r("k"), in: r("kkk")},
		opcmpltkimm:   {text: "cmplt.k@imm", out: r("k"), in: r("k2k")},
		opcmplek:      {text: "cmple.k", out: r("k"), in: r("kkk")},
		opcmplekimm:   {text: "cmple.k@imm", out: r("k"), in: r("k2k")},
		opcmpgtk:      {text: "cmpgt.k", out: r("k"), in: r("kkk")},
		opcmpgtkimm:   {text: "cmpgt.k@imm", out: r("k"), in: r("k2k")},
		opcmpgek:      {text: "cmpge.k", out: r("k"), in: r("kkk")},
		opcmpgekimm:   {text: "cmpge.k@imm", out: r("k"), in: r("k2k")},
		opcmpeqf64:    {text: "cmpeq.f64", out: r("k"), in: r("ssk")},
		opcmpeqf64imm: {text: "cmpeq.f64@imm", out: r("k"), in: r("sfk")},
		opcmpeqi64:    {text: "cmpeq.i64", out: r("k"), in: r("ssk")},
		opcmpeqi64imm: {text: "cmpeq.i64@imm", out: r("k"), in: r("sik")},
		opcmpltf64:    {text: "cmplt.f64", out: r("k"), in: r("ssk")},
		opcmpltf64imm: {text: "cmplt.f64@imm", out: r("k"), in: r("sfk")},
		opcmplti64:    {text: "cmplt.i64", out: r("k"), in: r("ssk")},
		opcmplti64imm: {text: "cmplt.i64@imm", out: r("k"), in: r("sik")},
		opcmplef64:    {text: "cmple.f64", out: r("k"), in: r("ssk")},
		opcmplef64imm: {text: "cmple.f64@imm", out: r("k"), in: r("sfk")},
		opcmplei64:    {text: "cmple.i64", out: r("k"), in: r("ssk")},
		opcmplei64imm: {text: "cmple.i64@imm", out: r("k"), in: r("sik")},
		opcmpgtf64:    {text: "cmpgt.f64", out: r("k"), in: r("ssk")},
		opcmpgtf64imm: {text: "cmpgt.f64@imm", out: r("k"), in: r("sfk")},
		opcmpgti64:    {text: "cmpgt.i64", out: r("k"), in: r("ssk")},
		opcmpgti64imm: {text: "cmpgt.i64@imm", out: r("k"), in: r("sik")},
		opcmpgef64:    {text: "cmpge.f64", out: r("k"), in: r("ssk")},
		opcmpgef64imm: {text: "cmpge.f64@imm", out: r("k"), in: r("sfk")},
		opcmpgei64:    {text: "cmpge.i64", out: r("k"), in: r("ssk")},
		opcmpgei64imm: {text: "cmpge.i64@imm", out: r("k"), in: r("sik")},

		// Test instructions:
		//   - null checks - each of these evaluates mask &= is{not}{false,true}(current value)
		opisnullv:    {text: "isnull.v", out: r("k"), in: r("vk")},
		opisnotnullv: {text: "isnotnull.v", out: r("k"), in: r("vk")},
		opisfalsev:   {text: "isfalse.v", out: r("k"), in: r("vk")},
		opistruev:    {text: "istrue.v", out: r("k"), in: r("vk")},
		opisnanf:     {text: "isnan.f", out: r("k"), in: r("sk")},
		opchecktag:   {text: "checktag", out: r("vk"), in: r("v2k")},   // checks that an ion tag is one of the set bits in the uint16 immediate
		opcmpeqslice: {text: "cmpeq.slice", out: r("k"), in: r("ssk")}, // compare timestamp or string slices
		opcmpeqv:     {text: "cmpeq.v", out: r("k"), in: r("vvk")},
		opcmpeqvimm:  {text: "cmpeq.v@imm", out: r("k"), in: r("vdk")},

		// Timestamp instructions
		opdateaddmonth:           {text: "dateaddmonth", out: r("sk"), in: r("ssk")},
		opdateaddmonthimm:        {text: "dateaddmonth.imm", out: r("sk"), in: r("sik")},
		opdateaddquarter:         {text: "dateaddquarter", out: r("sk"), in: r("ssk")},
		opdateaddyear:            {text: "dateaddyear", out: r("sk"), in: r("ssk")},
		opdatediffmicrosecond:    {text: "datediffmicrosecond", out: r("sk"), in: r("ssk")},
		opdatediffparam:          {text: "datediffparam", out: r("sk"), in: r("ss8k")},
		opdatediffmqy:            {text: "datediffmqy", out: r("sk"), in: r("ss2k")},
		opdateextractmicrosecond: {text: "dateextractmicrosecond", out: r("s"), in: r("sk")},
		opdateextractmillisecond: {text: "dateextractmillisecond", out: r("s"), in: r("sk")},
		opdateextractsecond:      {text: "dateextractsecond", out: r("s"), in: r("sk")},
		opdateextractminute:      {text: "dateextractminute", out: r("s"), in: r("sk")},
		opdateextracthour:        {text: "dateextracthour", out: r("s"), in: r("sk")},
		opdateextractday:         {text: "dateextractday", out: r("s"), in: r("sk")},
		opdateextractdow:         {text: "dateextractdow", out: r("s"), in: r("sk")},
		opdateextractdoy:         {text: "dateextractdoy", out: r("s"), in: r("sk")},
		opdateextractmonth:       {text: "dateextractmonth", out: r("s"), in: r("sk")},
		opdateextractquarter:     {text: "dateextractquarter", out: r("s"), in: r("sk")},
		opdateextractyear:        {text: "dateextractyear", out: r("s"), in: r("sk")},
		opdatetounixepoch:        {text: "datetounixepoch", out: r("s"), in: r("sk")},
		opdatetounixmicro:        {text: "datetounixmicro", out: r("s"), in: r("sk")},
		opdatetruncmillisecond:   {text: "datetruncmillisecond", out: r("s"), in: r("sk")},
		opdatetruncsecond:        {text: "datetruncsecond", out: r("s"), in: r("sk")},
		opdatetruncminute:        {text: "datetruncminute", out: r("s"), in: r("sk")},
		opdatetrunchour:          {text: "datetrunchour", out: r("s"), in: r("sk")},
		opdatetruncday:           {text: "datetruncday", out: r("s"), in: r("sk")},
		opdatetruncdow:           {text: "datetruncdow", out: r("s"), in: r("s2k")},
		opdatetruncmonth:         {text: "datetruncmonth", out: r("s"), in: r("sk")},
		opdatetruncquarter:       {text: "datetruncquarter", out: r("s"), in: r("sk")},
		opdatetruncyear:          {text: "datetruncyear", out: r("s"), in: r("sk")},
		opunboxts:                {text: "unboxts", out: r("sk"), in: r("vk")},
		opboxts:                  {text: "boxts", out: r("v"), in: r("sk"), scratch: 16 * 16},

		// Bucket instructions
		opwidthbucketf64: {text: "widthbucket.f64", out: r("s"), in: r("ssssk")},
		opwidthbucketi64: {text: "widthbucket.i64", out: r("s"), in: r("ssssk")},
		optimebucketts:   {text: "timebucket.ts", out: r("s"), in: r("ssk")},

		// Geo instructions
		opgeohash:      {text: "geohash", out: r("s"), in: r("sssk"), scratch: 16 * 16},
		opgeohashimm:   {text: "geohashimm", out: r("s"), in: r("ss2k"), scratch: 16 * 16},
		opgeotilex:     {text: "geotilex", out: r("s"), in: r("ssk")},
		opgeotiley:     {text: "geotiley", out: r("s"), in: r("ssk")},
		opgeotilees:    {text: "geotilees", out: r("s"), in: r("sssk"), scratch: 32 * 16},
		opgeotileesimm: {text: "geotilees.imm", out: r("s"), in: r("ss2k"), scratch: 32 * 16},
		opgeodistance:  {text: "geodistance", out: r("sk"), in: r("ssssk")},

		// String instructions
		opalloc:     {text: "alloc", out: r("sk"), in: r("sk"), scratch: PageSize},
		opconcatstr: {text: "concatstr", out: r("sk"), va: r("sk"), scratch: PageSize},

		// Find Symbol instructions
		//   - findsym - computes 'current struct' . 'symbol'
		opfindsym:  {text: "findsym", out: r("vk"), in: r("byk")},
		opfindsym2: {text: "findsym2", out: r("vk"), in: r("bvkyk")},

		// Blend instructions:
		opblendv:   {text: "blend.v", out: r("vk"), in: r("vkvk")},
		opblendf64: {text: "blend.f64", out: r("sk"), in: r("sksk")},

		// Unboxing instructions:
		opunboxktoi64:    {text: "unbox.k@i64", out: r("sk"), in: r("vk")},
		opunboxcoercef64: {text: "unbox.coerce.f64", out: r("sk"), in: r("vk")},
		opunboxcoercei64: {text: "unbox.coerce.i64", out: r("sk"), in: r("vk")},
		opunboxcvtf64:    {text: "unbox.cvt.f64", out: r("sk"), in: r("vk")},
		opunboxcvti64:    {text: "unbox.cvt.i64", out: r("sk"), in: r("vk")},

		// unpack a slice type (string/array/timestamp/etc.)
		opunpack: {text: "unpack", out: r("sk"), in: r("v2k")},

		opunsymbolize: {text: "unsymbolize", out: r("v"), in: r("vk")},

		// Boxing instructions
		opboxk:    {text: "box.k", out: r("v"), in: r("kk"), scratch: 16},
		opboxi64:  {text: "box.i64", out: r("v"), in: r("sk"), scratch: 9 * 16},
		opboxf64:  {text: "box.f64", out: r("v"), in: r("sk"), scratch: 9 * 16},
		opboxstr:  {text: "box.str", out: r("v"), in: r("sk"), scratch: PageSize},
		opboxlist: {text: "box.list", out: r("v"), in: r("sk"), scratch: PageSize},

		// Make instructions
		opmakelist:   {text: "makelist", out: r("vk"), in: r("k"), va: r("vk"), scratch: PageSize},
		opmakestruct: {text: "makestruct", out: r("vk"), in: r("k"), va: r("yvk"), scratch: PageSize},

		// Hash instructions
		ophashvalue:     {text: "hashvalue", out: r("h"), in: r("vk")},
		ophashvalueplus: {text: "hashvalue+", out: r("h"), in: r("hvk")},
		ophashmember:    {text: "hashmember", out: r("k"), in: r("h2k")},
		ophashlookup:    {text: "hashlookup", out: r("vk"), in: r("h2k")},

		// Simple aggregate operations
		opaggandk:  {text: "aggand.k", in: r("akk")},
		opaggork:   {text: "aggor.k", in: r("akk")},
		opaggsumf:  {text: "aggsum.f64", in: r("ask")},
		opaggsumi:  {text: "aggsum.i64", in: r("ask")},
		opaggminf:  {text: "aggmin.f64", in: r("ask")},
		opaggmini:  {text: "aggmin.i64", in: r("ask")},
		opaggmaxf:  {text: "aggmax.f64", in: r("ask")},
		opaggmaxi:  {text: "aggmax.i64", in: r("ask")},
		opaggandi:  {text: "aggand.i64", in: r("ask")},
		opaggori:   {text: "aggor.i64", in: r("ask")},
		opaggxori:  {text: "aggxor.i64", in: r("ask")},
		opaggcount: {text: "aggcount", in: r("ak")},

		// Slot aggregate operations
		opaggbucket:    {text: "aggbucket", out: r("l"), in: r("hk")},
		opaggslotandk:  {text: "aggslotand.k", in: r("alkk")},
		opaggslotork:   {text: "aggslotor.k", in: r("alkk")},
		opaggslotsumf:  {text: "aggslotsum.f64", in: r("alsk")},
		opaggslotsumi:  {text: "aggslotsum.i64", in: r("alsk")},
		opaggslotavgf:  {text: "aggslotavg.f64", in: r("alsk")},
		opaggslotavgi:  {text: "aggslotavg.i64", in: r("alsk")},
		opaggslotmaxf:  {text: "aggslotmax.f64", in: r("alsk")},
		opaggslotmaxi:  {text: "aggslotmax.i64", in: r("alsk")},
		opaggslotminf:  {text: "aggslotmin.f64", in: r("alsk")},
		opaggslotmini:  {text: "aggslotmin.i64", in: r("alsk")},
		opaggslotandi:  {text: "aggslotand.i64", in: r("alsk")},
		opaggslotori:   {text: "aggslotor.i64", in: r("alsk")},
		opaggslotxori:  {text: "aggslotxor.i64", in: r("alsk")},
		opaggslotcount: {text: "aggslotcount", in: r("alk")},

		// Uncategorized instructions
		oplitref: {text: "litref", out: r("v"), in: r("d")},
		opauxval: {text: "auxval", out: r("vk"), in: r("p")},
		opsplit:  {text: "split", out: r("vsk"), in: r("sk")}, // split a list into head and tail components
		optuple:  {text: "tuple", out: r("bk"), in: r("vk")},
		opmovk:   {text: "mov.k", out: r("k"), in: r("k")},     // duplicates a mask
		opzerov:  {text: "zero.v", out: r("v")},                // zeroes a value
		opmovv:   {text: "mov.v", out: r("v"), in: r("vk")},    // duplicates a value
		opmovvk:  {text: "mov.v.k", out: r("vk"), in: r("vk")}, // duplicates a value + mask
		opmovf64: {text: "mov.f64", out: r("s"), in: r("sk")},  // duplicates f64
		opmovi64: {text: "mov.i64", out: r("s"), in: r("sk")},  // duplicates i64

		opobjectsize:    {text: "objectsize", out: r("sk"), in: r("vk")},
		oparraysize:     {text: "arraysize", out: r("s"), in: r("sk")},
		oparrayposition: {text: "arrayposition", out: r("sk"), in: r("svk")},

		// string comparing operations
		opCmpStrEqCs:              {text: "cmp_str_eq_cs", out: r("k"), in: r("sxk")},
		opCmpStrEqCi:              {text: "cmp_str_eq_ci", out: r("k"), in: r("sxk")},
		opCmpStrEqUTF8Ci:          {text: "cmp_str_eq_utf8_ci", out: r("k"), in: r("sxk")},
		opEqPatternCs:             {text: "eq_pattern_cs", out: r("sk"), in: r("sxk")},
		opEqPatternCi:             {text: "eq_pattern_ci", out: r("sk"), in: r("sxk")},
		opEqPatternUTF8Ci:         {text: "eq_pattern_utf8_ci", out: r("sk"), in: r("sxk")},
		opCmpStrFuzzyA3:           {text: "cmp_str_fuzzy_A3", out: r("k"), in: r("ssxk")},
		opCmpStrFuzzyUnicodeA3:    {text: "cmp_str_fuzzy_unicode_A3", out: r("k"), in: r("ssxk")},
		opHasSubstrFuzzyA3:        {text: "contains_fuzzy_A3", out: r("k"), in: r("ssxk")},
		opHasSubstrFuzzyUnicodeA3: {text: "contains_fuzzy_unicode_A3", out: r("k"), in: r("ssxk")},
		// TODO: op_cmp_less_str, op_cmp_neq_str, op_cmp_between_str

		// string trim operations
		opTrimWsLeft:     {text: "trim_ws_left", out: r("s"), in: r("sk")},
		opTrimWsRight:    {text: "trim_ws_right", out: r("s"), in: r("sk")},
		opTrim4charLeft:  {text: "trim_char_left", out: r("s"), in: r("sxk")},
		opTrim4charRight: {text: "trim_char_right", out: r("s"), in: r("sxk")},

		// string prefix/suffix matching operations
		opContainsPrefixCs:      {text: "contains_prefix_cs", out: r("sk"), in: r("sxk")},
		opContainsPrefixUTF8Ci:  {text: "contains_prefix_utf8_ci", out: r("sk"), in: r("sxk")},
		opContainsPrefixCi:      {text: "contains_prefix_ci", out: r("sk"), in: r("sxk")},
		opContainsSuffixCs:      {text: "contains_suffix_cs", out: r("sk"), in: r("sxk")},
		opContainsSuffixCi:      {text: "contains_suffix_ci", out: r("sk"), in: r("sxk")},
		opContainsSuffixUTF8Ci:  {text: "contains_suffix_utf8_ci", out: r("sk"), in: r("sxk")},
		opContainsSubstrCs:      {text: "contains_substr_cs", out: r("sk"), in: r("sxk")},
		opContainsSubstrCi:      {text: "contains_substr_ci", out: r("sk"), in: r("sxk")},
		opContainsSubstrUTF8Ci:  {text: "contains_substr_utf8_ci", out: r("sk"), in: r("sxk")},
		opContainsPatternCs:     {text: "contains_pattern_cs", out: r("sk"), in: r("sxk")},
		opContainsPatternCi:     {text: "contains_pattern_ci", out: r("sk"), in: r("sxk")},
		opContainsPatternUTF8Ci: {text: "contains_pattern_utf8_ci", out: r("sk"), in: r("sxk")},

		// ip matching operations
		opIsSubnetOfIP4: {text: "is_subnet_of_ip4", out: r("k"), in: r("sxk")},

		// char skipping
		opSkip1charLeft:  {text: "skip_1char_left", out: r("sk"), in: r("sk")},
		opSkip1charRight: {text: "skip_1char_right", out: r("sk"), in: r("sk")},
		opSkipNcharLeft:  {text: "skip_nchar_left", out: r("sk"), in: r("ssk")},
		opSkipNcharRight: {text: "skip_nchar_right", out: r("sk"), in: r("ssk")},

		opoctetlength: {text: "octetlength", out: r("s"), in: r("sk")},
		opcharlength:  {text: "characterlength", out: r("s"), in: r("sk")},
		opSubstr:      {text: "substr", out: r("s"), in: r("sssk")},
		opSplitPart:   {text: "split_part", out: r("sk"), in: r("sxsk")},

		opDfaT6:  {text: "dfa_tiny6", out: r("k"), in: r("sxk")},
		opDfaT7:  {text: "dfa_tiny7", out: r("k"), in: r("sxk")},
		opDfaT8:  {text: "dfa_tiny8", out: r("k"), in: r("sxk")},
		opDfaT6Z: {text: "dfa_tiny6Z", out: r("k"), in: r("sxk")},
		opDfaT7Z: {text: "dfa_tiny7Z", out: r("k"), in: r("sxk")},
		opDfaT8Z: {text: "dfa_tiny8Z", out: r("k"), in: r("sxk")},
		opDfaLZ:  {text: "dfa_largeZ", out: r("k"), in: r("sxk")},

		opslower: {text: "slower", out: r("sk"), in: r("sk"), scratch: PageSize},
		opsupper: {text: "supper", out: r("sk"), in: r("sk"), scratch: PageSize},

		optypebits: {text: "typebits", out: r("s"), in: r("vk")},

		opaggapproxcount:          {text: "aggapproxcount", in: r("ah2k")},
		opaggapproxcountmerge:     {text: "aggapproxcountmerge", in: r("as2k")},
		opaggslotapproxcount:      {text: "aggslotapproxcount", in: r("alh2k")},
		opaggslotapproxcountmerge: {text: "aggslotapproxcountmerge", in: r("als2k")},

		optrap: {text: "trap"},
	}
}

var opinfo [_maxbcop]bcopinfo = bcmakeopinfo()

func init() {
	// Verify that new ops have been added to the opinfo table
	for _, r := range patchAVX512Level2 {
		opinfo[r.to] = opinfo[r.from]
	}

	for i := 0; i < _maxbcop; i++ {
		info := &opinfo[i]
		if info.text == "" {
			panic(fmt.Sprintf("missing opinfo for bcop %v", i))
		}
	}
}

func (op bcop) scratch() int { return opinfo[op].scratch }

// bcerr is an error code returned
// from the bytecode execution engine
type bcerr int32

const (
	// MoreScratch means that there was
	// not enough space in the scratch buffer
	// to re-box an unboxed value
	bcerrMoreScratch bcerr = iota + 1
	// NeedRadix means that there was a failed
	// radix tree lookup, which may be solved
	// by performing radix tree updates and
	// re-trying the operation
	//
	// the errinfo field will be set to the
	// hash slot containing the computed hashes
	bcerrNeedRadix
	// Corrupt is returned when the bytecode
	// fails a bounds check or some other
	// piece of sanity-checking
	bcerrCorrupt
	// TreeCorrupt is returned when the
	// bytecode fails a bounds check
	// in a radix tree lookup
	bcerrTreeCorrupt
	// NullSymbolTable is returned when unsymbolize
	// found symbols, but couldn't process them as
	// there was no symbol table
	bcerrNullSymbolTable
)

func (b bcerr) Error() string {
	switch b {
	case bcerrMoreScratch:
		return "insufficient scratch space"
	case bcerrNeedRadix:
		return "missing radix tree entry"
	case bcerrCorrupt:
		return "internal assertion failed"
	case bcerrTreeCorrupt:
		return "radix tree bounds-check failed"
	case bcerrNullSymbolTable:
		return "null symbol table"
	default:
		return "unknown bytecode error"
	}
}

func (b *bytecode) prepare(rp *rowParams) {
	b.auxvals = rp.auxbound
	b.auxpos = 0
}

type bytecode struct {
	// XXX struct offsets known to assembly!
	compiled []byte   // actual opcodes
	vstack   []uint64 // value scratch space
	dict     []string // string dictionary
	symtab   []vmref  // symtab[id] -> boxed string
	auxvals  [][]vmref
	auxpos   int

	trees []*radixTree64 // trees used for hashmember, etc.

	// scratch buffer used for projection
	scratch []byte
	// number of bytes to reserve for scratch, total
	scratchtotal int
	// allocation epoch; see symtab.epoch
	epoch int
	// relative displacment of scratch relative to vmm
	scratchoff uint32

	// savedlit is the saved literal contents
	// that are copied into scratch[:] before execution
	savedlit []byte

	//lint:ignore U1000 not unused; used in assembly
	// Area that is used by bytecode instructions to temporarily spill registers.
	// 512 bytes can be used to spill up to 8 ZMM registers (or more registers of
	// any choice). Note that spill area is designed to be used only by a single
	// bytecode instruction at a time, it should not be used to persist any data
	// during the execution of bytecode.
	spillArea [512]byte

	vstacksize int

	// set from abort handlers
	err   bcerr
	errpc int32
	// additional error information;
	// error-specific
	errinfo int
}

type bcFormatFlags uint

const (
	// Redacted mode (the default).
	//
	// This turns off all formatting features.
	bcFormatRedacted = 0

	// Formatter will try to output the string representation
	// of each symbol found in the compiled BC program.
	//
	// NOTE: That this flag cannot be used in production!
	bcFormatSymbols bcFormatFlags = (1 << iota)

	//lint:ignore U1000 can be used during debugging
	//
	// Enable all format features, which can be useful during debugging
	bcFormatAll = bcFormatSymbols
)

func readUIntFromBC(buf []byte) uint64 {
	value := uint64(0)
	for i := 0; i < len(buf); i++ {
		value |= uint64(buf[i]) << (i * 8)
	}
	return value
}

func formatArgs(bc *bytecode, dst *strings.Builder, compiled []byte, args []bcArgType, flags bcFormatFlags) int {
	offset := 0
	size := len(compiled)

	for i, argType := range args {
		width := argType.immWidth()
		if size-offset < width {
			return -1
		}

		if i != 0 {
			dst.WriteString(", ")
		}

		switch argType {
		case bcK:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "k[%d]", value)
		case bcS:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "s[%d]", value)
		case bcV:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "v[%d]", value)
		case bcB:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "b[%d]", value)
		case bcH:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "h[%d]", value)
		case bcL:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "l[%d]", value)
		case bcAuxSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "aux[%d]", value)
		case bcAggSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "agg[%d]", value)
		case bcDictSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "dict[%d]", value)
		case bcSymbolID:
			value := readUIntFromBC(compiled[offset : offset+width])
			if (flags & bcFormatSymbols) != 0 {
				decoded := decodeSymbolID(uint32(value))
				if uint64(decoded) < uint64(len(bc.symtab)) {
					encodedSymbolValue := bc.symtab[decoded].mem()
					str, _, err := ion.ReadString(encodedSymbolValue)
					if err == nil {
						fmt.Fprintf(dst, "sym(%d, %q)", decoded, str)
					} else {
						fmt.Fprintf(dst, "sym(%d, <%v>)", decoded, err)
					}
					continue
				}
			}
			fmt.Fprintf(dst, "sym(%d)", value)

		case bcImmI8:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i8(%d)", int8(value))
		case bcImmI16:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i16(%d)", int16(value))
		case bcImmI32:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i32(%d)", int32(value))
		case bcImmI64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "i64(%d)", int64(value))
		case bcImmU8:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u8(%d)", value)
		case bcImmU16:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u16(%d)", value)
		case bcImmU32:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u32(%d)", value)
		case bcImmU64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "u64(%d)", value)
		case bcImmF64:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "f64(%g)", math.Float64frombits(value))

		case bcLitRef:
			litOff := readUIntFromBC(compiled[offset : offset+4])
			litLen := readUIntFromBC(compiled[offset+4 : offset+8])
			litTLV := compiled[offset+8]
			litHLen := compiled[offset+9]
			fmt.Fprintf(dst, "litref(%d, %d, tlv=0x%02X, hLen=%d)", litOff, litLen, litTLV, litHLen)

		default:
			panic(fmt.Sprintf("Unhandled immediate type: %v", argType))
		}
		offset += width
	}

	return offset
}

func visitBytecode(bc *bytecode, fn func(offset int, op bcop, info *bcopinfo) error) error {
	compiled := bc.compiled
	size := len(compiled)
	offset := int(0)

	for offset < size {
		if size-offset < 8 {
			return fmt.Errorf("cannot decode opcode of size %d while there is only %d bytes left", 8, size-offset)
		}

		opaddr := uintptr(binary.LittleEndian.Uint64(compiled[offset:]))
		offset += 8

		op, ok := opcodeID(opaddr)
		if !ok {
			fmt.Fprintf(os.Stderr, "bytedode: %x\n", bc.compiled)
			return fmt.Errorf("failed to translate opcode address 0x%x", opaddr)
		}

		info := &opinfo[op]
		startoff := offset

		for _, argtype := range info.out {
			offset += argtype.immWidth()
		}
		for _, argtype := range info.in {
			offset += argtype.immWidth()
		}

		if len(info.va) != 0 {
			if size-offset < 4 {
				return fmt.Errorf("cannot decode va-length consisting of %d bytes while there is only %d bytes left", 4, size-offset)
			}

			vaLength := int(binary.LittleEndian.Uint32(compiled[offset:]))
			offset += 4

			width := 0
			for _, argtype := range info.va {
				width += argtype.immWidth()
			}

			offset += width * vaLength
		}

		// at this point the current opcode and its arguments are valid
		err := fn(startoff, op, info)
		if err != nil {
			return err
		}
	}

	return nil
}

func formatBytecode(bc *bytecode, flags bcFormatFlags) string {
	var b strings.Builder
	compiled := bc.compiled

	err := visitBytecode(bc, func(offset int, op bcop, info *bcopinfo) error {
		if len(info.out) != 0 {
			immSize := formatArgs(bc, &b, compiled[offset:], info.out, flags)
			if immSize == -1 {
				return nil
			}
			offset += immSize
			b.WriteString(" = ")
		}
		b.WriteString(info.text)

		if len(info.in) != 0 {
			b.WriteString(" ")
			immSize := formatArgs(bc, &b, compiled[offset:], info.in, flags)
			if immSize == -1 {
				return nil
			}
			offset += immSize
		}

		if len(info.va) != 0 {
			vaLength := uint(binary.LittleEndian.Uint32(compiled[offset:]))
			offset += 4

			if len(info.in) != 0 {
				b.WriteString(", ")
			} else {
				b.WriteString(" ")
			}

			fmt.Fprintf(&b, "va(%d)", vaLength)
			for vaIndex := 0; vaIndex < int(vaLength); vaIndex++ {
				b.WriteString(", {")
				immSize := formatArgs(bc, &b, compiled[offset:], info.va, flags)
				if immSize == -1 {
					return nil
				}
				offset += immSize
				b.WriteString("}")
			}
		}

		b.WriteString("\n")
		return nil
	})

	if err != nil {
		fmt.Fprintf(&b, "<bytecode error: %s>", err)
	}

	return b.String()
}

func (b *bytecode) String() string {
	return formatBytecode(b, bcFormatRedacted)
}

// finalize append the final 'return' instruction
// to the bytecode buffer and checks that the stack
// depth is sane
func (b *bytecode) finalize() error {
	return nil
}

// Makes sure that the virtual stack size is at least `size` (in bytes).
func (b *bytecode) ensureVStackSize(size int) {
	if b.vstacksize < size {
		b.vstacksize = size
	}
}

func alignVStackBuffer(buf []uint64) []uint64 {
	alignmentInU64Units := uintptr(bcStackAlignment >> 3)

	addr := uintptr(unsafe.Pointer(&buf[0]))
	alignDiff := (alignmentInU64Units - (addr >> 3)) & (alignmentInU64Units - 1)
	return buf[int(alignDiff):cap(buf)]
}

// Allocates all stacks that are needed to execute the bytecode program.
func (b *bytecode) allocStacks() {
	vSize := (b.vstacksize + 7) >> 3
	if cap(b.vstack) < vSize {
		b.vstack = alignVStackBuffer(make([]uint64, vSize+((bcStackAlignment-1)>>3)))
	}
}

func (b *bytecode) dropScratch() {
	b.epoch = -1
	b.scratch = nil
	// this will trigger a fault if it is used:
	b.scratchoff = 0x80000000
}

// restoreScratch updates the scratch state in b
// so that it has the correct number of bytes allocated
// from the symbol table's spare pages
func (b *bytecode) restoreScratch(st *symtab) {
	b.symtab = st.symrefs
	if b.scratchtotal == 0 {
		// this will trigger a fault if it is used:
		b.scratchoff = 0x80000000
		return
	}
	if b.epoch != st.epoch || cap(b.scratch) < b.scratchtotal {
		b.scratch = st.slab.malloc((b.scratchtotal + 15) &^ 15)
	}
	b.scratch = b.scratch[:copy(b.scratch, b.savedlit)]
	b.scratchoff, _ = vmdispl(b.scratch[:1])
}

func (b *bytecode) reset() {
	*b = bytecode{}
}
