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
	"strings"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

//go:generate go run _generate/genops.go
//go:generate gofmt -w ops_gen.go
//go:generate go run _generate/genconst.go -o bc_constant_gen.h

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
	bcReadK      bcArgType = iota // read-only bool stack-slot, shown as k[imm]
	bcWriteK                      // write-only bool stack-slot, shown as w:k[imm]
	bcReadS                       // read-only scalar stack-slot, shown as s[imm]
	bcWriteS                      // write-only scalar stack-slot, shown as w:s[imm]
	bcReadV                       // read-only value stack-slot, shown as v[imm]
	bcWriteV                      // write-only value stack-slot, shown as w:v[imm]
	bcReadWriteV                  // read-write value stack-slot, shown as x:v[imm] (not exposed to SSA)
	bcReadB                       // read-only struct pointer, shown as b[imm]
	bcWriteB                      // write-only struct pointer, shown as w:b[imm]
	bcReadH                       // read-only hash-slot, shown as h[imm]
	bcWriteH                      // write-only hash-slot, shown as w:h[imm]
	bcDictSlot                    // 16-bit dictionary reference, shown as dict[imm]
	bcAuxSlot                     // 32-bit aux value slot
	bcAggSlot                     // 32-bit aggregation slot
	bcHashSlot                    // 32-bit aggregation hash slot
	bcSymbolID                    // 32-bit symbol identifier
	bcLitRef                      // 64-bit value reference
	bcImmI8                       // signed 8-bit int immediate argument
	bcImmI16                      // signed 16-bit int immediate argument
	bcImmI32                      // signed 32-bit int immediate argument
	bcImmI64                      // signed 64-bit int immediate argument
	bcImmU8                       // unsigned 8-bit int immediate argument
	bcImmU16                      // unsigned 16-bit int immediate argument
	bcImmU32                      // unsigned 32-bit int immediate argument
	bcImmU64                      // unsigned 64-bit int immediate argument
	bcImmF64                      // 64-bit float immediate argument

	bcPredicate = bcReadK
)

func (a bcArgType) String() string {
	switch a {
	case bcReadK:
		return "ReadK"
	case bcWriteK:
		return "WriteK"
	case bcReadS:
		return "ReadS"
	case bcWriteS:
		return "WriteS"
	case bcReadV:
		return "ReadV"
	case bcWriteV:
		return "WriteV"
	case bcReadB:
		return "ReadB"
	case bcWriteB:
		return "WriteB"
	case bcReadH:
		return "ReadH"
	case bcWriteH:
		return "WriteH"
	case bcDictSlot:
		return "DictSlot"
	case bcAuxSlot:
		return "AuxSlot"
	case bcAggSlot:
		return "AggSlot"
	case bcHashSlot:
		return "HashSlot"
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
	bcReadK:    bcSlotSize,
	bcWriteK:   bcSlotSize,
	bcReadS:    bcSlotSize,
	bcWriteS:   bcSlotSize,
	bcReadV:    bcSlotSize,
	bcWriteV:   bcSlotSize,
	bcReadB:    bcSlotSize,
	bcWriteB:   bcSlotSize,
	bcReadH:    bcSlotSize,
	bcWriteH:   bcSlotSize,
	bcDictSlot: 2,
	bcAuxSlot:  2,
	bcAggSlot:  4,
	bcHashSlot: 4,
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
	text    string
	args    []bcArgType
	va      []bcArgType
	scratch int // desired scratch space (up to PageSize)
}

func bcmakeopinfo() [_maxbcop]bcopinfo {
	sharedArgs := make(map[string][]bcArgType)

	makeArgs := func(args ...bcArgType) []bcArgType {
		key := fmt.Sprint(args)
		if val, ok := sharedArgs[key]; ok {
			return val
		}
		sharedArgs[key] = args
		return args
	}

	return [_maxbcop]bcopinfo{
		// When adding a new entry, please read the following rules:
		//   - Opcode 'text' represents the opcode name, use dots to separate type(s) the instruction operates on
		//   - Opcode 'args' represents opcode arguments, use makeArgs() to define them
		//   - Opcode 'va' field represents variable arguments that follow regular `args`

		// Control flow instructions:
		//   - ret  - terminates execution; returns current mask
		opret:    {text: "ret"},
		opretk:   {text: "ret.k", args: makeArgs(bcReadK)},
		opretsk:  {text: "ret.s.k", args: makeArgs(bcReadS, bcReadK)},
		opretbk:  {text: "ret.b.k", args: makeArgs(bcReadB, bcReadK)},
		opretbhk: {text: "ret.b.h.k", args: makeArgs(bcReadB, bcReadH, bcReadK)},

		opinit: {text: "init", args: makeArgs(bcWriteB, bcWriteK)},

		// Mask instructions:
		//   - false - sets predicate to FALSE
		//   - others - mask-only operations
		opbroadcast0k: {text: "broadcast0.k", args: makeArgs(bcWriteK)},             // k[0] = 0
		opbroadcast1k: {text: "broadcast1.k", args: makeArgs(bcWriteK)},             // k[0] = 1              & ValidLanes
		opnotk:        {text: "not.k", args: makeArgs(bcWriteK, bcReadK)},           // k[0] = !k[1]          & ValidLanes
		opandk:        {text: "and.k", args: makeArgs(bcWriteK, bcReadK, bcReadK)},  // k[0] = k[1] & k[2]    & ValidLanes
		opandnk:       {text: "andn.k", args: makeArgs(bcWriteK, bcReadK, bcReadK)}, // k[0] = !k[1] & k[2]   & ValidLanes
		opork:         {text: "or.k", args: makeArgs(bcWriteK, bcReadK, bcReadK)},   // k[0] = k[1] | k[2]    & ValidLanes
		opxork:        {text: "xor.k", args: makeArgs(bcWriteK, bcReadK, bcReadK)},  // k[0] = k[1] ^ k[2]    & ValidLanes
		opxnork:       {text: "xnor.k", args: makeArgs(bcWriteK, bcReadK, bcReadK)}, // k[0] = !(k[1] ^ k[2]) & ValidLanes
		opfalse:       {text: "false.k", args: makeArgs(bcWriteV, bcWriteK)},

		// Integer math
		opbroadcasti64:   {text: "broadcast.i64", args: makeArgs(bcWriteS, bcImmI64)},
		opabsi64:         {text: "abs.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opnegi64:         {text: "neg.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opsigni64:        {text: "sign.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opsquarei64:      {text: "square.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opbitnoti64:      {text: "bitnot.i64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opbitcounti64:    {text: "bitcount.i64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opaddi64:         {text: "add.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opaddi64imm:      {text: "add.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opsubi64:         {text: "sub.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opsubi64imm:      {text: "sub.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		oprsubi64imm:     {text: "rsub.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opmuli64:         {text: "mul.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opmuli64imm:      {text: "mul.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opdivi64:         {text: "div.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdivi64imm:      {text: "div.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		oprdivi64imm:     {text: "rdiv.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opmodi64:         {text: "mod.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opmodi64imm:      {text: "mod.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		oprmodi64imm:     {text: "rmod.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opaddmuli64imm:   {text: "addmul.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcImmI64, bcPredicate)},
		opminvaluei64:    {text: "minvalue.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opminvaluei64imm: {text: "minvalue.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opmaxvaluei64:    {text: "maxvalue.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opmaxvaluei64imm: {text: "maxvalue.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opandi64:         {text: "and.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opandi64imm:      {text: "and.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opori64:          {text: "or.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opori64imm:       {text: "or.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opxori64:         {text: "xor.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opxori64imm:      {text: "xor.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opslli64:         {text: "sll.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opslli64imm:      {text: "sll.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opsrai64:         {text: "sra.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opsrai64imm:      {text: "sra.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},
		opsrli64:         {text: "srl.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opsrli64imm:      {text: "srl.i64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},

		// Floating point math
		opbroadcastf64:   {text: "broadcast.f64", args: makeArgs(bcWriteS, bcImmF64)},
		opabsf64:         {text: "abs.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opnegf64:         {text: "neg.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opsignf64:        {text: "sign.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opsquaref64:      {text: "square.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opsqrtf64:        {text: "sqrt.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcbrtf64:        {text: "cbrt.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		oproundf64:       {text: "round.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		oproundevenf64:   {text: "roundeven.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		optruncf64:       {text: "trunc.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opfloorf64:       {text: "floor.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opceilf64:        {text: "ceil.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opaddf64:         {text: "add.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opaddf64imm:      {text: "add.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opsubf64:         {text: "sub.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opsubf64imm:      {text: "sub.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		oprsubf64imm:     {text: "rsub.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opmulf64:         {text: "mul.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opmulf64imm:      {text: "mul.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opdivf64:         {text: "div.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdivf64imm:      {text: "div.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		oprdivf64imm:     {text: "rdiv.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opmodf64:         {text: "mod.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opmodf64imm:      {text: "mod.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		oprmodf64imm:     {text: "rmod.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opminvaluef64:    {text: "minvalue.f64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opminvaluef64imm: {text: "minvalue.f64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmF64, bcPredicate)},
		opmaxvaluef64:    {text: "maxvalue.f64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opmaxvaluef64imm: {text: "maxvalue.f64@imm", args: makeArgs(bcWriteS, bcReadS, bcImmF64, bcPredicate)},
		opexpf64:         {text: "exp.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opexpm1f64:       {text: "expm1.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opexp2f64:        {text: "exp2.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opexp10f64:       {text: "exp10.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		oplnf64:          {text: "ln.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opln1pf64:        {text: "ln1p.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		oplog2f64:        {text: "log2.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		oplog10f64:       {text: "log10.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opsinf64:         {text: "sin.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcosf64:         {text: "cos.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		optanf64:         {text: "tan.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opasinf64:        {text: "asin.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opacosf64:        {text: "acos.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opatanf64:        {text: "atan.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opatan2f64:       {text: "atan2.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		ophypotf64:       {text: "hypot.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		oppowf64:         {text: "pow.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		oppowuintf64:     {text: "powuint.f64", args: makeArgs(bcWriteS, bcReadS, bcImmI64, bcPredicate)},

		// Conversion instructions
		opcvtktof64:        {text: "cvt.ktof64", args: makeArgs(bcWriteS, bcReadK)},
		opcvtktoi64:        {text: "cvt.ktoi64", args: makeArgs(bcWriteS, bcReadK)},
		opcvti64tok:        {text: "cvt.i64tok", args: makeArgs(bcWriteK, bcReadS, bcPredicate)},
		opcvtf64tok:        {text: "cvt.f64tok", args: makeArgs(bcWriteK, bcReadS, bcPredicate)},
		opcvti64tof64:      {text: "cvt.i64tof64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcvttruncf64toi64: {text: "cvttrunc.f64toi64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcvtfloorf64toi64: {text: "cvtfloor.f64toi64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcvtceilf64toi64:  {text: "cvtceil.f64toi64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opcvti64tostr:      {text: "cvt.i64tostr", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate), scratch: 20 * 16},

		// Comparison instructions
		opsortcmpvnf:  {text: "sortcmpv@nf", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadV, bcPredicate)},
		opsortcmpvnl:  {text: "sortcmpv@nl", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadV, bcPredicate)},
		opcmpv:        {text: "cmpv", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadV, bcPredicate)},
		opcmpvk:       {text: "cmpv.k", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadK, bcPredicate)},
		opcmpvkimm:    {text: "cmpv.k@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcImmU16, bcPredicate)},
		opcmpvi64:     {text: "cmpv.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadS, bcPredicate)},
		opcmpvi64imm:  {text: "cmpv.i64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcImmI64, bcPredicate)},
		opcmpvf64:     {text: "cmpv.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcReadS, bcPredicate)},
		opcmpvf64imm:  {text: "cmpv.f64@imm", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcImmF64, bcPredicate)},
		opcmpltstr:    {text: "cmplt.str", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmplestr:    {text: "cmple.str", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgtstr:    {text: "cmpgt.str", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgestr:    {text: "cmpge.str", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpltk:      {text: "cmplt.k", args: makeArgs(bcWriteK, bcReadK, bcReadK, bcPredicate)},
		opcmpltkimm:   {text: "cmplt.k@imm", args: makeArgs(bcWriteK, bcReadK, bcImmU16, bcPredicate)},
		opcmplek:      {text: "cmple.k", args: makeArgs(bcWriteK, bcReadK, bcReadK, bcPredicate)},
		opcmplekimm:   {text: "cmple.k@imm", args: makeArgs(bcWriteK, bcReadK, bcImmU16, bcPredicate)},
		opcmpgtk:      {text: "cmpgt.k", args: makeArgs(bcWriteK, bcReadK, bcReadK, bcPredicate)},
		opcmpgtkimm:   {text: "cmpgt.k@imm", args: makeArgs(bcWriteK, bcReadK, bcImmU16, bcPredicate)},
		opcmpgek:      {text: "cmpge.k", args: makeArgs(bcWriteK, bcReadK, bcReadK, bcPredicate)},
		opcmpgekimm:   {text: "cmpge.k@imm", args: makeArgs(bcWriteK, bcReadK, bcImmU16, bcPredicate)},
		opcmpeqf64:    {text: "cmpeq.f64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpeqf64imm: {text: "cmpeq.f64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opcmpeqi64:    {text: "cmpeq.i64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpeqi64imm: {text: "cmpeq.i64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opcmpltf64:    {text: "cmplt.f64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpltf64imm: {text: "cmplt.f64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opcmplti64:    {text: "cmplt.i64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmplti64imm: {text: "cmplt.i64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opcmplef64:    {text: "cmple.f64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmplef64imm: {text: "cmple.f64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opcmplei64:    {text: "cmple.i64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmplei64imm: {text: "cmple.i64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opcmpgtf64:    {text: "cmpgt.f64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgtf64imm: {text: "cmpgt.f64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opcmpgti64:    {text: "cmpgt.i64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgti64imm: {text: "cmpgt.i64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opcmpgef64:    {text: "cmpge.f64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgef64imm: {text: "cmpge.f64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmF64, bcPredicate)},
		opcmpgei64:    {text: "cmpge.i64", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opcmpgei64imm: {text: "cmpge.i64@imm", args: makeArgs(bcWriteK, bcReadS, bcImmI64, bcPredicate)},

		// Test instructions:
		//   - null checks - each of these evaluates mask &= is{not}{false,true}(current value)
		opisnullv:    {text: "isnull.v", args: makeArgs(bcWriteK, bcReadV, bcPredicate)},
		opisnotnullv: {text: "isnotnull.v", args: makeArgs(bcWriteK, bcReadV, bcPredicate)},
		opisfalsev:   {text: "isfalse.v", args: makeArgs(bcWriteK, bcReadV, bcPredicate)},
		opistruev:    {text: "istrue.v", args: makeArgs(bcWriteK, bcReadV, bcPredicate)},
		opisnanf:     {text: "isnan.f", args: makeArgs(bcWriteK, bcReadS, bcPredicate)},
		opchecktag:   {text: "checktag", args: makeArgs(bcWriteV, bcWriteK, bcReadV, bcImmU16, bcPredicate)}, // checks that an ion tag is one of the set bits in the uint16 immediate
		opcmpeqslice: {text: "cmpeq.slice", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcPredicate)},         // compare timestamp or string slices
		opcmpeqv:     {text: "cmpeq.v", args: makeArgs(bcWriteK, bcReadV, bcReadV, bcPredicate)},
		opcmpeqvimm:  {text: "cmpeq.v@imm", args: makeArgs(bcWriteK, bcReadV, bcLitRef, bcPredicate)},

		// Timestamp instructions
		opdateaddmonth:           {text: "dateaddmonth", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdateaddmonthimm:        {text: "dateaddmonth.imm", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcImmI64, bcPredicate)},
		opdateaddquarter:         {text: "dateaddquarter", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdateaddyear:            {text: "dateaddyear", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdatediffmicrosecond:    {text: "datediffmicrosecond", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opdatediffparam:          {text: "datediffparam", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcImmU64, bcPredicate)},
		opdatediffmqy:            {text: "datediffmqy", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcImmU16, bcPredicate)},
		opdateextractmicrosecond: {text: "dateextractmicrosecond", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractmillisecond: {text: "dateextractmillisecond", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractsecond:      {text: "dateextractsecond", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractminute:      {text: "dateextractminute", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextracthour:        {text: "dateextracthour", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractday:         {text: "dateextractday", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractdow:         {text: "dateextractdow", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractdoy:         {text: "dateextractdoy", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractmonth:       {text: "dateextractmonth", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractquarter:     {text: "dateextractquarter", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdateextractyear:        {text: "dateextractyear", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetounixepoch:        {text: "datetounixepoch", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetounixmicro:        {text: "datetounixmicro", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncmillisecond:   {text: "datetruncmillisecond", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncsecond:        {text: "datetruncsecond", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncminute:        {text: "datetruncminute", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetrunchour:          {text: "datetrunchour", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncday:           {text: "datetruncday", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncdow:           {text: "datetruncdow", args: makeArgs(bcWriteS, bcReadS, bcImmU16, bcPredicate)},
		opdatetruncmonth:         {text: "datetruncmonth", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncquarter:       {text: "datetruncquarter", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opdatetruncyear:          {text: "datetruncyear", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opunboxts:                {text: "unboxts", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		opboxts:                  {text: "boxts", args: makeArgs(bcWriteV, bcReadS, bcPredicate), scratch: 16 * 16},

		// Bucket instructions
		opwidthbucketf64: {text: "widthbucket.f64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcReadS, bcReadS, bcPredicate)},
		opwidthbucketi64: {text: "widthbucket.i64", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcReadS, bcReadS, bcPredicate)},
		optimebucketts:   {text: "timebucket.ts", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},

		// Geo instructions
		opgeohash:      {text: "geohash", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcReadS, bcPredicate), scratch: 16 * 16},
		opgeohashimm:   {text: "geohashimm", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcImmU16, bcPredicate), scratch: 16 * 16},
		opgeotilex:     {text: "geotilex", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opgeotiley:     {text: "geotiley", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcPredicate)},
		opgeotilees:    {text: "geotilees", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcReadS, bcPredicate), scratch: 32 * 16},
		opgeotileesimm: {text: "geotilees.imm", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcImmU16, bcPredicate), scratch: 32 * 16},
		opgeodistance:  {text: "geodistance", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcReadS, bcReadS, bcPredicate)},

		// String instructions
		opalloc:     {text: "alloc", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate), scratch: PageSize},
		opconcatstr: {text: "concatstr", args: makeArgs(bcWriteS, bcWriteK), va: makeArgs(bcReadS, bcReadK), scratch: PageSize},

		// Find Symbol instructions
		//   - findsym - computes 'current struct' . 'symbol'
		opfindsym:  {text: "findsym", args: makeArgs(bcWriteV, bcWriteK, bcReadB, bcSymbolID, bcPredicate)},
		opfindsym2: {text: "findsym2", args: makeArgs(bcWriteV, bcWriteK, bcReadB, bcReadV, bcReadK, bcSymbolID, bcPredicate)},

		// Blend instructions:
		opblendv:     {text: "blend.v", args: makeArgs(bcWriteV, bcWriteK, bcReadV, bcReadK, bcReadV, bcReadK)},
		opblendk:     {text: "blend.k", args: makeArgs(bcWriteK, bcWriteK, bcReadK, bcReadK, bcReadK, bcReadK)},
		opblendi64:   {text: "blend.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadK, bcReadS, bcReadK)},
		opblendf64:   {text: "blend.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadK, bcReadS, bcReadK)},
		opblendslice: {text: "blend.slice", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadK, bcReadS, bcReadK)},

		// Unboxing instructions:
		opunboxktoi64:    {text: "unbox.k@i64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		opunboxcoercef64: {text: "unbox.coerce.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		opunboxcoercei64: {text: "unbox.coerce.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		opunboxcvtf64:    {text: "unbox.cvt.f64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		opunboxcvti64:    {text: "unbox.cvt.i64", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},

		// unpack a slice type (string/array/timestamp/etc.)
		opunpack: {text: "unpack", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcImmU16, bcPredicate)},

		opunsymbolize: {text: "unsymbolize", args: makeArgs(bcWriteV, bcReadV, bcPredicate)},

		// Boxing instructions
		opboxk:    {text: "box.k", args: makeArgs(bcWriteV, bcReadK, bcPredicate), scratch: 16},
		opboxi64:  {text: "box.i64", args: makeArgs(bcWriteV, bcReadS, bcPredicate), scratch: 9 * 16},
		opboxf64:  {text: "box.f64", args: makeArgs(bcWriteV, bcReadS, bcPredicate), scratch: 9 * 16},
		opboxstr:  {text: "box.str", args: makeArgs(bcWriteV, bcReadS, bcPredicate), scratch: PageSize},
		opboxlist: {text: "box.list", args: makeArgs(bcWriteV, bcReadS, bcPredicate), scratch: PageSize},

		// Make instructions
		opmakelist:   {text: "makelist", args: makeArgs(bcWriteV, bcWriteK, bcPredicate), va: makeArgs(bcReadV, bcReadK), scratch: PageSize},
		opmakestruct: {text: "makestruct", args: makeArgs(bcWriteV, bcWriteK, bcPredicate), va: makeArgs(bcSymbolID, bcReadV, bcReadK), scratch: PageSize},

		// Hash instructions
		ophashvalue:     {text: "hashvalue", args: makeArgs(bcWriteH, bcReadV, bcPredicate)},
		ophashvalueplus: {text: "hashvalue+", args: makeArgs(bcWriteH, bcReadH, bcReadV, bcPredicate)},
		ophashmember:    {text: "hashmember", args: makeArgs(bcWriteK, bcReadH, bcImmU16, bcPredicate)},
		ophashlookup:    {text: "hashlookup", args: makeArgs(bcWriteV, bcWriteK, bcReadH, bcImmU16, bcPredicate)},

		// Simple aggregate operations
		opaggandk:  {text: "aggand.k", args: makeArgs(bcAggSlot, bcReadK, bcPredicate)},
		opaggork:   {text: "aggor.k", args: makeArgs(bcAggSlot, bcReadK, bcPredicate)},
		opaggsumf:  {text: "aggsum.f64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggsumi:  {text: "aggsum.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggminf:  {text: "aggmin.f64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggmini:  {text: "aggmin.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggmaxf:  {text: "aggmax.f64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggmaxi:  {text: "aggmax.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggandi:  {text: "aggand.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggori:   {text: "aggor.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggxori:  {text: "aggxor.i64", args: makeArgs(bcAggSlot, bcReadS, bcPredicate)},
		opaggcount: {text: "aggcount", args: makeArgs(bcAggSlot, bcReadK)},

		// Slot aggregate operations
		opaggbucket:    {text: "aggbucket", args: makeArgs(bcReadH, bcPredicate)},
		opaggslotandk:  {text: "aggslotand.k", args: makeArgs(bcHashSlot, bcReadK, bcPredicate)},
		opaggslotork:   {text: "aggslotor.k", args: makeArgs(bcHashSlot, bcReadK, bcPredicate)},
		opaggslotsumf:  {text: "aggslotsum.f64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotsumi:  {text: "aggslotsum.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotavgf:  {text: "aggslotavg.f64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotavgi:  {text: "aggslotavg.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotmaxf:  {text: "aggslotmax.f64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotmaxi:  {text: "aggslotmax.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotminf:  {text: "aggslotmin.f64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotmini:  {text: "aggslotmin.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotandi:  {text: "aggslotand.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotori:   {text: "aggslotor.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotxori:  {text: "aggslotxor.i64", args: makeArgs(bcHashSlot, bcReadS, bcPredicate)},
		opaggslotcount: {text: "aggslotcount", args: makeArgs(bcHashSlot, bcReadK)},

		// Uncategorized instructions
		oplitref: {text: "litref", args: makeArgs(bcWriteV, bcLitRef)},
		opauxval: {text: "auxval", args: makeArgs(bcWriteV, bcWriteK, bcAuxSlot)},
		opsplit:  {text: "split", args: makeArgs(bcWriteV, bcWriteS, bcWriteK, bcReadS, bcPredicate)}, // split a list into head and tail components
		optuple:  {text: "tuple", args: makeArgs(bcWriteB, bcWriteK, bcReadV, bcPredicate)},
		opmovk:   {text: "mov.k", args: makeArgs(bcWriteK, bcReadK)},                          // duplicates a mask
		opzerov:  {text: "zero.v", args: makeArgs(bcWriteV)},                                  // zeroes a value
		opmovv:   {text: "mov.v", args: makeArgs(bcWriteV, bcReadV, bcPredicate)},             // duplicates a value
		opmovvk:  {text: "mov.v.k", args: makeArgs(bcWriteV, bcWriteK, bcReadV, bcPredicate)}, // duplicates a value + mask
		opmovf64: {text: "mov.f64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},           // duplicates f64
		opmovi64: {text: "mov.i64", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},           // duplicates i64

		opobjectsize:    {text: "objectsize", args: makeArgs(bcWriteS, bcWriteK, bcReadV, bcPredicate)},
		oparraysize:     {text: "arraysize", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		oparrayposition: {text: "arrayposition", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadV, bcPredicate)},

		// string comparing operations
		opCmpStrEqCs:              {text: "cmp_str_eq_cs", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opCmpStrEqCi:              {text: "cmp_str_eq_ci", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opCmpStrEqUTF8Ci:          {text: "cmp_str_eq_utf8_ci", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opEqPatternCs:             {text: "eq_pattern_cs", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opEqPatternCi:             {text: "eq_pattern_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opEqPatternUTF8Ci:         {text: "eq_pattern_utf8_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opCmpStrFuzzyA3:           {text: "cmp_str_fuzzy_A3", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcDictSlot, bcPredicate)},
		opCmpStrFuzzyUnicodeA3:    {text: "cmp_str_fuzzy_unicode_A3", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcDictSlot, bcPredicate)},
		opHasSubstrFuzzyA3:        {text: "contains_fuzzy_A3", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcDictSlot, bcPredicate)},
		opHasSubstrFuzzyUnicodeA3: {text: "contains_fuzzy_unicode_A3", args: makeArgs(bcWriteK, bcReadS, bcReadS, bcDictSlot, bcPredicate)},
		// TODO: op_cmp_less_str, op_cmp_neq_str, op_cmp_between_str

		// string trim operations
		opTrimWsLeft:     {text: "trim_ws_left", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opTrimWsRight:    {text: "trim_ws_right", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opTrim4charLeft:  {text: "trim_char_left", args: makeArgs(bcWriteS, bcReadS, bcDictSlot, bcPredicate)},
		opTrim4charRight: {text: "trim_char_right", args: makeArgs(bcWriteS, bcReadS, bcDictSlot, bcPredicate)},

		// string prefix/suffix matching operations
		opContainsPrefixCs:      {text: "contains_prefix_cs", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsPrefixUTF8Ci:  {text: "contains_prefix_utf8_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsPrefixCi:      {text: "contains_prefix_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSuffixCs:      {text: "contains_suffix_cs", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSuffixCi:      {text: "contains_suffix_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSuffixUTF8Ci:  {text: "contains_suffix_utf8_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSubstrCs:      {text: "contains_substr_cs", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSubstrCi:      {text: "contains_substr_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsSubstrUTF8Ci:  {text: "contains_substr_utf8_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsPatternCs:     {text: "contains_pattern_cs", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsPatternCi:     {text: "contains_pattern_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opContainsPatternUTF8Ci: {text: "contains_pattern_utf8_ci", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcPredicate)},

		// ip matching operations
		opIsSubnetOfIP4: {text: "is_subnet_of_ip4", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},

		// char skipping
		opSkip1charLeft:  {text: "skip_1char_left", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opSkip1charRight: {text: "skip_1char_right", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate)},
		opSkipNcharLeft:  {text: "skip_nchar_left", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},
		opSkipNcharRight: {text: "skip_nchar_right", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcReadS, bcPredicate)},

		opoctetlength: {text: "octetlength", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opcharlength:  {text: "characterlength", args: makeArgs(bcWriteS, bcReadS, bcPredicate)},
		opSubstr:      {text: "substr", args: makeArgs(bcWriteS, bcReadS, bcReadS, bcReadS, bcPredicate)},
		opSplitPart:   {text: "split_part", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcDictSlot, bcReadS, bcPredicate)},

		opDfaT6:  {text: "dfa_tiny6", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaT7:  {text: "dfa_tiny7", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaT8:  {text: "dfa_tiny8", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaT6Z: {text: "dfa_tiny6Z", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaT7Z: {text: "dfa_tiny7Z", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaT8Z: {text: "dfa_tiny8Z", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},
		opDfaLZ:  {text: "dfa_largeZ", args: makeArgs(bcWriteK, bcReadS, bcDictSlot, bcPredicate)},

		opslower: {text: "slower", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate), scratch: PageSize},
		opsupper: {text: "supper", args: makeArgs(bcWriteS, bcWriteK, bcReadS, bcPredicate), scratch: PageSize},

		optypebits: {text: "typebits", args: makeArgs(bcWriteS, bcReadV, bcPredicate)},

		opaggapproxcount:          {text: "aggapproxcount", args: makeArgs(bcAggSlot, bcReadH, bcImmU16, bcPredicate)},
		opaggapproxcountmerge:     {text: "aggapproxcountmerge", args: makeArgs(bcAggSlot, bcReadS, bcImmU16, bcPredicate)},
		opaggslotapproxcount:      {text: "aggslotapproxcount", args: makeArgs(bcAggSlot, bcReadH, bcImmU16, bcPredicate)},
		opaggslotapproxcountmerge: {text: "aggslotapproxcountmerge", args: makeArgs(bcAggSlot, bcReadS, bcImmU16, bcPredicate)},

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

	//lint:ignore U1000 not unused; used in assembly
	bucket [16]int32 // the L register (32 bits per lane)

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
			fmt.Fprintf(dst, "<bytecode error: cannot decode argument of size %d while there is only %d bytes left>", width, size-offset)
			return -1
		}

		if i != 0 {
			dst.WriteString(", ")
		}

		switch argType {
		case bcReadK:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "k[%d]", value)
		case bcWriteK:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "w:k[%d]", value)

		case bcReadS:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "s[%d]", value)
		case bcWriteS:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "w:s[%d]", value)

		case bcReadV:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "v[%d]", value)
		case bcWriteV:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "w:v[%d]", value)
		case bcReadWriteV:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "x:v[%d]", value)

		case bcReadB:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "b[%d]", value)
		case bcWriteB:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "w:b[%d]", value)

		case bcReadH:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "h[%d]", value)
		case bcWriteH:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "w:h[%d]", value)

		case bcAuxSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "aux[%d]", value)
		case bcAggSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "agg[%d]", value)
		case bcHashSlot:
			value := readUIntFromBC(compiled[offset : offset+width])
			fmt.Fprintf(dst, "hash[%d]", value)
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
			return fmt.Errorf("failed to translate opcode address 0x%x", opaddr)
		}

		info := &opinfo[op]
		startoff := offset

		for _, argtype := range info.args {
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
		b.WriteString(info.text)

		if len(info.args) != 0 {
			b.WriteString(" ")
			immSize := formatArgs(bc, &b, compiled[offset:], info.args, flags)
			if immSize == -1 {
				return nil
			}
			offset += immSize
		}

		if len(info.va) != 0 {
			vaLength := uint(binary.LittleEndian.Uint32(compiled[offset:]))
			offset += 4

			if len(info.args) != 0 {
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
