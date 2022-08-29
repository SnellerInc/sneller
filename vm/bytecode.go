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

	"golang.org/x/exp/slices"
)

//go:generate go run _generate/genops.go
//go:generate gofmt -w ops_gen.go

// --- VM machine model ---
// The WHERE bytecode VM uses a simple stack-based bytecode
// to perform vectorized row operations. The VM has the following
// pieces of implicit state:
//
//   - current struct
//   - current value
//   - current scalar
//   - current mask
//
// Additionally, there is a stack of values that
// can be fed to certain instructions
//
// --- How to Add an Instruction ---
//  - define a new TEXT label in evalbc_{arch}.s
//    that begins with 'bc'
//  - run 'go generate'
//  - add opcode information below

// actual bytecode constants are generated automatically
// by reading the assembly source and generating a named
// constant for each bytecode function
type bcop uint16

// bytecode operation flags
type bcflags uint32

const bcLaneCount = 16
const bcLaneCountMask = bcLaneCount - 1

const (
	bcReadK  bcflags = 1 << iota // opcode reads a mask register
	bcWriteK                     // opcode writes to mask register
	bcReadS                      // opcode reads a scalar
	bcWriteS                     // opcode writes to scalar register
	bcReadV                      // opcode reads value pointers
	bcWriteV                     // opcode writes to value register
	bcReadB                      // opcode reads structure pointers
	bcWriteB                     // opcode writes to structure register
	bcReadH                      // opcode reads hash register
	bcWriteH                     // opcode writes to hash register

	bcReadWriteK = bcReadK | bcWriteK
	bcReadWriteS = bcReadS | bcWriteS
	bcReadWriteV = bcReadV | bcWriteV
	bcReadWriteB = bcReadB | bcWriteB
	bcReadWriteH = bcReadH | bcWriteH
)

// Type of an immediate value that follows bcop in a compiled code
type bcImmType uint8

const (
	bcImmI8     bcImmType = iota // signed 8-bit int
	bcImmI16                     // signed 16-bit int
	bcImmI32                     // signed 32-bit int
	bcImmI64                     // signed 64-bit int
	bcImmU8                      // unsigned 8-bit int
	bcImmU16                     // unsigned 16-bit int
	bcImmU32                     // unsigned 32-bit int
	bcImmU64                     // unsigned 64-bit int
	bcImmU8Hex                   // unsigned 8-bit int (shown as hex)
	bcImmU16Hex                  // unsigned 16-bit int (shown as hex)
	bcImmU32Hex                  // unsigned 32-bit int (shown as hex)
	bcImmU64Hex                  // unsigned 64-bit int (shown as hex)
	bcImmF64                     // 64-bit float
	bcImmS16                     // 16-bit stack slot, shown as [imm]
	bcImmDict                    // 16-bit dictionary reference, shown as dict[imm]
)

// Maps each bcImmType into a width of the immediate in bytes.
var bcImmWidth = [...]uint8{
	bcImmI8:     1,
	bcImmI16:    2,
	bcImmI32:    4,
	bcImmI64:    8,
	bcImmU8:     1,
	bcImmU16:    2,
	bcImmU32:    4,
	bcImmU64:    8,
	bcImmU8Hex:  1,
	bcImmU16Hex: 2,
	bcImmU32Hex: 4,
	bcImmU64Hex: 8,
	bcImmF64:    8,
	bcImmS16:    2,
	bcImmDict:   2,
}

type bcopinfo struct {
	text     string
	imms     []bcImmType
	vaImms   []bcImmType
	flags    bcflags
	immwidth uint8 // immediate size
	inverse  bcop  // for comparisons, etc., the inverse operation
}

// Shared immediate combinations used in opinfo (to reduce the number of dynamic memory allocations on init).
var bcImmsS16 = []bcImmType{bcImmS16}
var bcImmsS16S16 = []bcImmType{bcImmS16, bcImmS16}
var bcImmsS16U8 = []bcImmType{bcImmS16, bcImmU8}
var bcImmsS16U16 = []bcImmType{bcImmS16, bcImmU16}
var bcImmsS16H32 = []bcImmType{bcImmS16, bcImmU32Hex}
var bcImmsS16I64 = []bcImmType{bcImmS16, bcImmI64}
var bcImmsS16U64 = []bcImmType{bcImmS16, bcImmU64}
var bcImmsS16S16S16 = []bcImmType{bcImmS16, bcImmS16, bcImmS16}
var bcImmsU8 = []bcImmType{bcImmU8}
var bcImmsU16 = []bcImmType{bcImmU16}
var bcImmsU32 = []bcImmType{bcImmU32}
var bcImmsU32H32 = []bcImmType{bcImmU32, bcImmU32Hex}
var bcImmsH32S16S16 = []bcImmType{bcImmU32Hex, bcImmS16, bcImmS16}
var bcImmsH32 = []bcImmType{bcImmU32Hex}
var bcImmsH32H32 = []bcImmType{bcImmU32Hex, bcImmU32Hex}
var bcImmsI64 = []bcImmType{bcImmI64}
var bcImmsU64 = []bcImmType{bcImmU64}
var bcImmsF64 = []bcImmType{bcImmF64}
var bcImmsDict = []bcImmType{bcImmDict}
var bcImmsDictS16 = []bcImmType{bcImmDict, bcImmS16}

var opinfo = [_maxbcop]bcopinfo{
	// When adding a new entry, please read the following rules:
	//   - Opcode 'text' should represent the opcode name, use dots to separate type(s) the instruction operates on
	//   - Opcode 'imms' combination should use the shared combinations as defined above
	//   - Opcode 'flags' order should follow their declaration order, for example K register is first, etc...

	// Control flow instructions:
	//   - ret  - terminates execution; returns current mask
	//   - jz N - adds 'N' to the virtual PC if K1 == 0
	opret: {text: "ret"},
	opjz:  {text: "jz", imms: bcImmsU64, flags: bcReadK},

	// Load/Save instructions:
	//   - Load instructions load a register from a stack slot
	//   - Save instructions save a register to a stack slot
	//   - Zero means zeroing (either during load/save)
	//   - Blend means blending (either during load/save)
	oploadk:         {text: "load.k", imms: bcImmsS16, flags: bcReadWriteK},
	opsavek:         {text: "save.k", imms: bcImmsS16, flags: bcReadK},
	opxchgk:         {text: "xchg.k", imms: bcImmsS16, flags: bcReadWriteK},
	oploadb:         {text: "load.b", imms: bcImmsS16, flags: bcWriteB},
	opsaveb:         {text: "save.b", imms: bcImmsS16, flags: bcReadB},
	oploadv:         {text: "load.v", imms: bcImmsS16, flags: bcWriteV},
	opsavev:         {text: "save.v", imms: bcImmsS16, flags: bcReadV},
	oploadzerov:     {text: "loadzero.v", imms: bcImmsS16, flags: bcWriteK | bcWriteV},
	opsavezerov:     {text: "savezero.v", imms: bcImmsS16, flags: bcReadK | bcReadV},
	oploadpermzerov: {text: "loadpermzero.v", imms: bcImmsS16, flags: bcWriteK | bcWriteV},
	opsaveblendv:    {text: "saveblend.v", imms: bcImmsS16, flags: bcReadK | bcReadV},
	oploads:         {text: "load.s", imms: bcImmsS16, flags: bcWriteS},
	opsaves:         {text: "save.s", imms: bcImmsS16, flags: bcReadS},
	oploadzeros:     {text: "loadzero.s", imms: bcImmsS16, flags: bcWriteK | bcWriteS},
	opsavezeros:     {text: "savezero.s", imms: bcImmsS16, flags: bcReadK | bcReadS},

	// Mask instructions:
	//   - false - sets predicate to FALSE
	//   - others - mask-only operations
	opbroadcastimmk: {text: "broadcast.imm.k", imms: bcImmsU16, flags: bcWriteK},
	opfalse:         {text: "false", flags: bcWriteK},
	opandk:          {text: "and.k", imms: bcImmsS16, flags: bcReadWriteK},
	opork:           {text: "or.k", imms: bcImmsS16, flags: bcReadWriteK},
	opandnotk:       {text: "andnot.k", imms: bcImmsS16, flags: bcReadWriteK}, // really 'and not'
	opnandk:         {text: "nand.k", imms: bcImmsS16, flags: bcReadWriteK},
	opxork:          {text: "xor.k", imms: bcImmsS16, flags: bcReadWriteK},
	opxnork:         {text: "xnor.k", imms: bcImmsS16, flags: bcReadWriteK},
	opnotk:          {text: "not.k", flags: bcReadWriteK},

	// Arithmetic and logical instructions
	opbroadcastimmf: {text: "broadcast.imm.f", imms: bcImmsF64, flags: bcWriteS},
	opbroadcastimmi: {text: "broadcast.imm.i", imms: bcImmsI64, flags: bcWriteS},
	opabsf:          {text: "abs.f", flags: bcReadK | bcReadWriteS},
	opabsi:          {text: "abs.i", flags: bcReadK | bcReadWriteS},
	opnegf:          {text: "neg.f", flags: bcReadK | bcReadWriteS},
	opnegi:          {text: "neg.i", flags: bcReadK | bcReadWriteS},
	opsignf:         {text: "sign.f", flags: bcReadK | bcReadWriteS},
	opsigni:         {text: "sign.i", flags: bcReadK | bcReadWriteS},
	opsquaref:       {text: "square.f", flags: bcReadK | bcReadWriteS},
	opsquarei:       {text: "square.i", flags: bcReadK | bcReadWriteS},
	opbitnoti:       {text: "bitnot.i", flags: bcReadK | bcReadWriteS},
	opbitcounti:     {text: "bitcount.i", flags: bcReadK | bcReadWriteS},
	opsqrtf:         {text: "sqrt.f", flags: bcReadK | bcReadWriteS},
	opcbrtf:         {text: "cbrt.f", flags: bcReadK | bcReadWriteS},
	oproundf:        {text: "round.f", flags: bcReadK | bcReadWriteS},
	oproundevenf:    {text: "roundeven.f", flags: bcReadK | bcReadWriteS},
	optruncf:        {text: "trunc.f", flags: bcReadK | bcReadWriteS},
	opfloorf:        {text: "floor.f", flags: bcReadK | bcReadWriteS},
	opceilf:         {text: "ceil.f", flags: bcReadK | bcReadWriteS},
	opaddf:          {text: "add.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opaddi:          {text: "add.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opaddimmf:       {text: "add.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opaddimmi:       {text: "add.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opsubf:          {text: "sub.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opsubi:          {text: "sub.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opsubimmf:       {text: "sub.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opsubimmi:       {text: "sub.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	oprsubf:         {text: "rsub.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprsubi:         {text: "rsub.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprsubimmf:      {text: "rsub.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	oprsubimmi:      {text: "rsub.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opmulf:          {text: "mul.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmuli:          {text: "mul.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmulimmf:       {text: "mul.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opmulimmi:       {text: "mul.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opdivf:          {text: "div.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opdivi:          {text: "div.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opdivimmf:       {text: "div.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opdivimmi:       {text: "div.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	oprdivf:         {text: "rdiv.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprdivi:         {text: "rdiv.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprdivimmf:      {text: "rdiv.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	oprdivimmi:      {text: "rdiv.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opmodf:          {text: "mod.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmodi:          {text: "mod.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmodimmf:       {text: "mod.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opmodimmi:       {text: "mod.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	oprmodf:         {text: "rmod.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprmodi:         {text: "rmod.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oprmodimmf:      {text: "rmod.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	oprmodimmi:      {text: "rmod.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opaddmulimmi:    {text: "addmul.imm.i", imms: bcImmsS16I64, flags: bcReadK | bcReadWriteS},
	opminvaluef:     {text: "minvalue.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opminvaluei:     {text: "minvalue.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opminvalueimmf:  {text: "minvalue.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opminvalueimmi:  {text: "minvalue.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opmaxvaluef:     {text: "maxvalue.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmaxvaluei:     {text: "maxvalue.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opmaxvalueimmf:  {text: "maxvalue.imm.f", imms: bcImmsF64, flags: bcReadK | bcReadWriteS},
	opmaxvalueimmi:  {text: "maxvalue.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opandi:          {text: "and.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opandimmi:       {text: "and.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opori:           {text: "or.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oporimmi:        {text: "or.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opxori:          {text: "xor.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opxorimmi:       {text: "xor.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opslli:          {text: "sll.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opsllimmi:       {text: "sll.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opsrai:          {text: "sra.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opsraimmi:       {text: "sra.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opsrli:          {text: "srl.i", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opsrlimmi:       {text: "srl.imm.i", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},

	// Math functions
	opexpf:   {text: "exp.f", flags: bcReadK | bcReadWriteS},
	opexpm1f: {text: "expm1.f", flags: bcReadK | bcReadWriteS},
	opexp2f:  {text: "exp2.f", flags: bcReadK | bcReadWriteS},
	opexp10f: {text: "exp10.f", flags: bcReadK | bcReadWriteS},
	oplnf:    {text: "ln.f", flags: bcReadK | bcReadWriteS},
	opln1pf:  {text: "ln1p.f", flags: bcReadK | bcReadWriteS},
	oplog2f:  {text: "log2.f", flags: bcReadK | bcReadWriteS},
	oplog10f: {text: "log10.f", flags: bcReadK | bcReadWriteS},
	opsinf:   {text: "sin.f", flags: bcReadK | bcReadWriteS},
	opcosf:   {text: "cos.f", flags: bcReadK | bcReadWriteS},
	optanf:   {text: "tan.f", flags: bcReadK | bcReadWriteS},
	opasinf:  {text: "asin.f", flags: bcReadK | bcReadWriteS},
	opacosf:  {text: "acos.f", flags: bcReadK | bcReadWriteS},
	opatanf:  {text: "atan.f", flags: bcReadK | bcReadWriteS},
	opatan2f: {text: "atan2.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	ophypotf: {text: "hypot.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	oppowf:   {text: "pow.f", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},

	// Conversion instructions
	opcvtktof64:   {text: "cvtktof64", flags: bcReadK | bcWriteS}, // convert mask -> floats
	opcvtktoi64:   {text: "cvtktoi64", flags: bcReadK | bcWriteS}, // convert mask -> ints
	opcvti64tok:   {text: "cvti64tok", flags: bcReadWriteK | bcReadS},
	opcvti64tof64: {text: "cvti64tof64", flags: bcReadWriteK | bcReadWriteS},
	opcvtf64toi64: {text: "cvtf64toi64", flags: bcReadWriteK | bcReadWriteS},
	opfproundd:    {text: "fproundd", flags: bcReadWriteK | bcReadWriteS},
	opfproundu:    {text: "fproundu", flags: bcReadWriteK | bcReadWriteS},
	opcvti64tostr: {text: "cvti64tostr", flags: bcReadK | bcReadWriteS},

	// Comparison instructions
	opsortcmpvnf: {text: "sortcmpv@nf", imms: bcImmsS16, flags: bcReadWriteK | bcReadV | bcWriteS},
	opsortcmpvnl: {text: "sortcmpv@nl", imms: bcImmsS16, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpv:       {text: "cmpv", imms: bcImmsS16, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpvk:      {text: "cmpv.k", imms: bcImmsS16, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpvimmk:   {text: "cmpv.imm.k", imms: bcImmsU8, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpvi64:    {text: "cmpv.i64", flags: bcReadWriteK | bcReadV | bcReadWriteS},
	opcmpvimmi64: {text: "cmpv.imm.i64", imms: bcImmsI64, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpvf64:    {text: "cmpv.f64", flags: bcReadWriteK | bcReadV | bcReadWriteS},
	opcmpvimmf64: {text: "cmpv.imm.f64", imms: bcImmsF64, flags: bcReadWriteK | bcReadV | bcWriteS},
	opcmpltstr:   {text: "cmplt.str", imms: bcImmsS16, flags: bcReadWriteK | bcReadS},
	opcmplestr:   {text: "cmple.str", imms: bcImmsS16, flags: bcReadWriteK | bcReadS},
	opcmpgtstr:   {text: "cmpgt.str", imms: bcImmsS16, flags: bcReadWriteK | bcReadS},
	opcmpgestr:   {text: "cmpge.str", imms: bcImmsS16, flags: bcReadWriteK | bcReadS},
	opcmpltk:     {text: "cmplt.k", imms: bcImmsS16S16, flags: bcReadWriteK, inverse: opcmpgtk},
	opcmpltimmk:  {text: "cmplt.imm.k", imms: bcImmsS16U8, flags: bcReadWriteK, inverse: opcmpgtimmk},
	opcmplek:     {text: "cmple.k", imms: bcImmsS16S16, flags: bcReadWriteK, inverse: opcmpgek},
	opcmpleimmk:  {text: "cmple.imm.k", imms: bcImmsS16U8, flags: bcReadWriteK, inverse: opcmpgeimmk},
	opcmpgtk:     {text: "cmpgt.k", imms: bcImmsS16S16, flags: bcReadWriteK, inverse: opcmpltk},
	opcmpgtimmk:  {text: "cmpgt.imm.k", imms: bcImmsS16U8, flags: bcReadWriteK, inverse: opcmpgtimmk},
	opcmpgek:     {text: "cmpge.k", imms: bcImmsS16S16, flags: bcReadWriteK, inverse: opcmplek},
	opcmpgeimmk:  {text: "cmpge.imm.k", imms: bcImmsS16U8, flags: bcReadWriteK, inverse: opcmpleimmk},
	opcmpeqf:     {text: "cmpeq.f", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpeqf},
	opcmpeqi:     {text: "cmpeq.i", imms: bcImmsS16, flags: bcReadWriteK | bcReadS},
	opcmpeqimmf:  {text: "cmpeq.imm.f", imms: bcImmsF64, flags: bcReadWriteK | bcReadS},
	opcmpeqimmi:  {text: "cmpeq.imm.i", imms: bcImmsI64, flags: bcReadWriteK | bcReadS},
	opcmpltf:     {text: "cmplt.f", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpgtf},
	opcmplti:     {text: "cmplt.i", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpgti},
	opcmpltimmf:  {text: "cmplt.imm.f", imms: bcImmsF64, flags: bcReadWriteK | bcReadS, inverse: opcmpgtimmf},
	opcmpltimmi:  {text: "cmplt.imm.i", imms: bcImmsI64, flags: bcReadWriteK | bcReadS, inverse: opcmpgtimmi},
	opcmplef:     {text: "cmple.f", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpgef},
	opcmplei:     {text: "cmple.i", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpgei},
	opcmpleimmf:  {text: "cmple.imm.f", imms: bcImmsF64, flags: bcReadWriteK | bcReadS, inverse: opcmpgeimmf},
	opcmpleimmi:  {text: "cmple.imm.i", imms: bcImmsI64, flags: bcReadWriteK | bcReadS, inverse: opcmpgeimmi},
	opcmpgtf:     {text: "cmpgt.f", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmpltf},
	opcmpgti:     {text: "cmpgt.i", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmplti},
	opcmpgtimmf:  {text: "cmpgt.imm.f", imms: bcImmsF64, flags: bcReadWriteK | bcReadS, inverse: opcmpltimmf},
	opcmpgtimmi:  {text: "cmpgt.imm.i", imms: bcImmsI64, flags: bcReadWriteK | bcReadS, inverse: opcmpltimmi},
	opcmpgef:     {text: "cmpge.f", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmplef},
	opcmpgei:     {text: "cmpge.i", imms: bcImmsS16, flags: bcReadWriteK | bcReadS, inverse: opcmplei},
	opcmpgeimmf:  {text: "cmpge.imm.f", imms: bcImmsF64, flags: bcReadWriteK | bcReadS, inverse: opcmpleimmf},
	opcmpgeimmi:  {text: "cmpge.imm.i", imms: bcImmsI64, flags: bcReadWriteK | bcReadS, inverse: opcmpleimmi},

	// Test instructions:
	//   - null checks - each of these evaluates mask &= is{not}{false,true}(current value)
	opisnanf:       {text: "isnan.f", flags: bcReadWriteK | bcReadS},
	opchecktag:     {text: "checktag", imms: bcImmsU16, flags: bcReadWriteK | bcReadV}, // checks that an ion tag is one of the set bits in the uint16 immediate
	opisnull:       {text: "isnull", flags: bcReadWriteK | bcReadV, inverse: opisnotnull},
	opisnotnull:    {text: "isnotnull", flags: bcReadWriteK | bcReadV, inverse: opisnull},
	opisfalse:      {text: "isfalse", flags: bcReadWriteK | bcReadV},
	opistrue:       {text: "istrue", flags: bcReadWriteK | bcReadV},
	opeqslice:      {text: "eqslice", imms: bcImmsS16, flags: bcReadWriteK | bcReadS}, // compare timestamp or string slices
	opequalv:       {text: "equalv", imms: bcImmsS16, flags: bcReadWriteK | bcReadV},
	opeqv4mask:     {text: "eqv4mask", imms: bcImmsU32H32, flags: bcReadWriteK | bcReadV},
	opeqv4maskplus: {text: "eqv4mask+", imms: bcImmsU32H32, flags: bcReadWriteK | bcReadV},
	opeqv8:         {text: "eqv8", imms: bcImmsU64, flags: bcReadWriteK | bcReadV},
	opeqv8plus:     {text: "eqv8+", imms: bcImmsU64, flags: bcReadWriteK | bcReadV},
	opleneq:        {text: "leneq", imms: bcImmsU32, flags: bcReadWriteK | bcReadV},

	// Timestamp instructions
	opdateaddmonth:           {text: "dateaddmonth", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opdateaddmonthimm:        {text: "dateaddmonthimm", imms: bcImmsI64, flags: bcReadK | bcReadWriteS},
	opdateaddyear:            {text: "dateaddyear", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opdatediffparam:          {text: "datediffparam", imms: bcImmsS16U64, flags: bcReadK | bcReadWriteS},
	opdatediffmonthyear:      {text: "datediffmonthyear", imms: bcImmsS16U16, flags: bcReadK | bcReadWriteS},
	opdateextractmicrosecond: {text: "dateextractmicrosecond", flags: bcReadK | bcReadWriteS},
	opdateextractmillisecond: {text: "dateextractmillisecond", flags: bcReadK | bcReadWriteS},
	opdateextractsecond:      {text: "dateextractsecond", flags: bcReadK | bcReadWriteS},
	opdateextractminute:      {text: "dateextractminute", flags: bcReadK | bcReadWriteS},
	opdateextracthour:        {text: "dateextracthour", flags: bcReadK | bcReadWriteS},
	opdateextractday:         {text: "dateextractday", flags: bcReadK | bcReadWriteS},
	opdateextractmonth:       {text: "dateextractmonth", flags: bcReadK | bcReadWriteS},
	opdateextractyear:        {text: "dateextractyear", flags: bcReadK | bcReadWriteS},
	opdatetounixepoch:        {text: "datetounixepoch", flags: bcReadK | bcReadWriteS},
	opdatetruncmillisecond:   {text: "datetruncmillisecond", flags: bcReadK | bcReadWriteS},
	opdatetruncsecond:        {text: "datetruncsecond", flags: bcReadK | bcReadWriteS},
	opdatetruncminute:        {text: "datetruncminute", flags: bcReadK | bcReadWriteS},
	opdatetrunchour:          {text: "datetrunchour", flags: bcReadK | bcReadWriteS},
	opdatetruncday:           {text: "datetruncday", flags: bcReadK | bcReadWriteS},
	opdatetruncmonth:         {text: "datetruncmonth", flags: bcReadK | bcReadWriteS},
	opdatetruncyear:          {text: "datetruncyear", flags: bcReadK | bcReadWriteS},
	opunboxts:                {text: "unboxts", flags: bcReadK | bcWriteS},
	opboxts:                  {text: "boxts", flags: bcReadK | bcReadS},
	opconsttm:                {text: "consttm", imms: bcImmsDict, flags: bcReadWriteS},
	optimelt:                 {text: "timelt", flags: bcReadWriteK | bcReadS},
	optimegt:                 {text: "timegt", flags: bcReadWriteK | bcReadS},
	optmextract:              {text: "tmextract", imms: []bcImmType{bcImmU8}, flags: bcReadWriteK | bcReadWriteS},

	// Bucket instructions
	opwidthbucketf: {text: "widthbucket.f", imms: bcImmsS16S16S16, flags: bcReadK | bcReadWriteS},
	opwidthbucketi: {text: "widthbucket.i", imms: bcImmsS16S16S16, flags: bcReadK | bcReadWriteS},
	optimebucketts: {text: "timebucket.ts", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},

	// Geo instructions
	opgeohash:      {text: "geohash", imms: bcImmsS16S16, flags: bcReadK | bcReadWriteS},
	opgeohashimm:   {text: "geohashimm", imms: bcImmsS16U16, flags: bcReadK | bcReadWriteS},
	opgeotilex:     {text: "geotilex", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opgeotiley:     {text: "geotiley", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opgeotilees:    {text: "geotilees", imms: bcImmsS16S16, flags: bcReadK | bcReadWriteS},
	opgeotileesimm: {text: "geotilees.imm", imms: bcImmsS16U16, flags: bcReadK | bcReadWriteS},
	opgeodistance:  {text: "geodistance", imms: bcImmsS16S16S16, flags: bcReadK | bcReadWriteS},

	opconcatlenget1: {text: "concatlenget1", flags: bcReadK | bcReadWriteS},
	opconcatlenget2: {text: "concatlenget2", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opconcatlenget3: {text: "concatlenget3", imms: bcImmsS16S16, flags: bcReadK | bcReadWriteS},
	opconcatlenget4: {text: "concatlenget4", imms: bcImmsS16S16S16, flags: bcReadK | bcReadWriteS},

	opconcatlenacc1: {text: "concatlenacc1", flags: bcReadK | bcReadWriteS},
	opconcatlenacc2: {text: "concatlenacc2", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opconcatlenacc3: {text: "concatlenacc3", imms: bcImmsS16S16, flags: bcReadK | bcReadWriteS},
	opconcatlenacc4: {text: "concatlenacc4", imms: bcImmsS16S16S16, flags: bcReadK | bcReadWriteS},

	opallocstr:  {text: "alloc.str", flags: bcReadWriteK | bcReadWriteS},
	opappendstr: {text: "append.str", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},

	// Find Symbol instructions
	//   - findsym - computes 'current struct' . 'symbol'
	opfindsym:     {text: "findsym", imms: bcImmsH32, flags: bcReadWriteK | bcReadB | bcWriteV},
	opfindsym2:    {text: "findsym2", imms: bcImmsS16H32, flags: bcReadWriteK | bcReadB | bcReadWriteV},
	opfindsym2rev: {text: "findsym2rev", imms: bcImmsS16H32, flags: bcReadWriteK | bcReadB | bcReadWriteV},
	opfindsym3:    {text: "findsym3", imms: bcImmsH32, flags: bcReadWriteK | bcReadB | bcReadWriteV},

	// Blend instructions
	opblendnum:      {text: "blendnum", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opblendnumrev:   {text: "blendnumrev", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opblendslice:    {text: "blendslice", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opblendslicerev: {text: "blendslicerev", imms: bcImmsS16, flags: bcReadK | bcReadWriteS},
	opblendv:        {text: "blend.v", imms: bcImmsS16, flags: bcReadK | bcReadWriteV},
	opblendrevv:     {text: "blendrev.v", imms: bcImmsS16, flags: bcReadK | bcReadWriteV},

	// Unboxing instructions:
	//   - current scalar = coerce(current value, type)
	opunboxktoi64: {text: "unboxktoi64", flags: bcReadWriteK | bcWriteS | bcReadV},
	optoint:       {text: "toint", flags: bcReadWriteK | bcWriteS | bcReadV},
	optof64:       {text: "tof64", flags: bcWriteS | bcReadV},
	// unpack a slice type (string/array/timestamp/etc.)
	opunpack: {text: "unpack", imms: []bcImmType{bcImmU8Hex}, flags: bcReadWriteK | bcWriteS | bcReadV},

	opunsymbolize: {text: "unsymbolize", flags: bcReadWriteV},

	// Boxing instructions
	opboxmask:   {text: "boxmask", imms: bcImmsS16, flags: bcReadK},
	opboxmask2:  {text: "boxmask2", imms: bcImmsS16, flags: bcReadK},
	opboxmask3:  {text: "boxmask3", flags: bcReadK},
	opboxint:    {text: "boxint", flags: bcReadK | bcReadS},
	opboxfloat:  {text: "boxfloat", flags: bcReadK | bcReadS},
	opboxstring: {text: "boxstring", flags: bcReadK | bcReadS},
	opboxlist:   {text: "boxlist", flags: bcReadK | bcReadS},

	// Make instructions
	opmakelist:   {text: "makelist", vaImms: bcImmsS16S16, flags: bcReadWriteK | bcWriteV},
	opmakestruct: {text: "makestruct", vaImms: bcImmsH32S16S16, flags: bcReadWriteK | bcWriteV},

	// Hash instructions
	ophashvalue:     {text: "hashvalue", imms: bcImmsS16, flags: bcReadK | bcReadV | bcWriteH},
	ophashvalueplus: {text: "hashvalue+", imms: bcImmsS16S16, flags: bcReadK | bcReadV | bcReadWriteH},
	ophashmember:    {text: "hashmember", imms: bcImmsS16U16, flags: bcReadWriteK | bcReadH},
	ophashlookup:    {text: "hashlookup", imms: bcImmsS16U16, flags: bcReadWriteK | bcWriteV | bcReadH},

	// Simple aggregate operations
	opaggandk:  {text: "aggand.k", imms: bcImmsS16S16, flags: bcReadK},
	opaggork:   {text: "aggor.k", imms: bcImmsS16S16, flags: bcReadK},
	opaggsumf:  {text: "aggsum.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggsumi:  {text: "aggsum.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggminf:  {text: "aggmin.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggmini:  {text: "aggmin.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggmaxf:  {text: "aggmax.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggmaxi:  {text: "aggmax.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggandi:  {text: "aggand.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggori:   {text: "aggor.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggxori:  {text: "aggxor.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggcount: {text: "aggcount", imms: bcImmsS16, flags: bcReadK},

	// Slot aggregate operations
	opaggbucket:    {text: "aggbucket", imms: bcImmsS16, flags: bcReadK | bcWriteS | bcReadH},
	opaggslotandk:  {text: "aggslotand.k", imms: bcImmsS16S16, flags: bcReadK},
	opaggslotork:   {text: "aggslotor.k", imms: bcImmsS16S16, flags: bcReadK},
	opaggslotaddf:  {text: "aggslotadd.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotaddi:  {text: "aggslotadd.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotavgf:  {text: "aggslotavg.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotavgi:  {text: "aggslotavg.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotmaxf:  {text: "aggslotmax.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotmaxi:  {text: "aggslotmax.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotminf:  {text: "aggslotmin.f", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotmini:  {text: "aggslotmin.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotandi:  {text: "aggslotand.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotori:   {text: "aggslotor.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotxori:  {text: "aggslotxor.i", imms: bcImmsS16, flags: bcReadK | bcReadS},
	opaggslotcount: {text: "aggslotcount", imms: bcImmsS16, flags: bcReadK},

	// Uncategorized instructions
	oplitref:     {text: "litref", imms: bcImmsH32H32, flags: bcWriteV},
	opauxval:     {text: "auxval", imms: bcImmsS16, flags: bcWriteV | bcWriteK},
	opsplit:      {text: "split", flags: bcReadWriteK | bcReadWriteS | bcWriteV}, // split a list into head and tail components
	optuple:      {text: "tuple", flags: bcReadV | bcWriteB},
	opdupv:       {text: "dup.v", imms: bcImmsS16S16, flags: 0}, // duplicates a saved stack slot
	opzerov:      {text: "zero.v", imms: bcImmsS16, flags: 0},   // zeroes all values in a slot
	opobjectsize: {text: "objectsize", flags: bcReadWriteK | bcWriteS | bcReadV},

	// string comparing operations
	opCmpStrEqCs:     {text: "cmp_str_eq_cs", imms: bcImmsDict, flags: bcReadS | bcReadWriteK},
	opCmpStrEqCi:     {text: "cmp_str_eq_ci", imms: bcImmsDict, flags: bcReadS | bcReadWriteK},
	opCmpStrEqUTF8Ci: {text: "cmp_str_eq_utf8_ci", imms: bcImmsDict, flags: bcReadS | bcReadWriteK},
	// TODO: op_cmp_less_str, op_cmp_neq_str, op_cmp_between_str

	// string trim operations
	opTrimWsLeft:     {text: "trim_ws_left", flags: bcReadK | bcReadWriteS},
	opTrimWsRight:    {text: "trim_ws_right", flags: bcReadK | bcReadWriteS},
	opTrim4charLeft:  {text: "trim_char_left", imms: bcImmsDict, flags: bcReadK | bcReadWriteS},
	opTrim4charRight: {text: "trim_char_right", imms: bcImmsDict, flags: bcReadK | bcReadWriteS},

	// string prefix/suffix/contains matching operations
	opContainsSubstrCs:     {text: "contains_substr_cs", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsSubstrCi:     {text: "contains_substr_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsPrefixCs:     {text: "contains_prefix_cs", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsPrefixUTF8Ci: {text: "contains_prefix_utf8_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsPrefixCi:     {text: "contains_prefix_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsSuffixCs:     {text: "contains_suffix_cs", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsSuffixCi:     {text: "contains_suffix_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opContainsSuffixUTF8Ci: {text: "contains_suffix_utf8_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},

	// string pattern matcher
	opMatchpatCs:     {text: "matchpat_cs", imms: bcImmsDict, flags: bcReadWriteK | bcReadWriteS},
	opMatchpatCi:     {text: "matchpat_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadWriteS},
	opMatchpatUTF8Ci: {text: "matchpat_utf8_ci", imms: bcImmsDict, flags: bcReadWriteK | bcReadWriteS},

	// ip matching operations
	opIsSubnetOfIP4: {text: "is_subnet_of_ip4", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},

	// char skipping
	opSkip1charLeft:  {text: "skip_1char_left", flags: bcReadWriteK | bcReadWriteS},
	opSkip1charRight: {text: "skip_1char_right", flags: bcReadWriteK | bcReadWriteS},
	opSkipNcharLeft:  {text: "skip_nchar_left", imms: bcImmsS16, flags: bcReadWriteK | bcReadWriteS},
	opSkipNcharRight: {text: "skip_nchar_right", imms: bcImmsS16, flags: bcReadWriteK | bcReadWriteS},

	opLengthStr: {text: "lengthstr", flags: bcReadK | bcReadWriteS},
	opSubstr:    {text: "substr", imms: bcImmsS16S16, flags: bcReadK | bcReadWriteS},
	opSplitPart: {text: "split_part", imms: bcImmsDictS16, flags: bcReadWriteK | bcReadWriteS},

	opDfaT6:  {text: "dfa_tiny6", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaT7:  {text: "dfa_tiny7", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaT8:  {text: "dfa_tiny8", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaT6Z: {text: "dfa_tiny6Z", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaT7Z: {text: "dfa_tiny7Z", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaT8Z: {text: "dfa_tiny8Z", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaL:   {text: "dfa_large", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},
	opDfaLZ:  {text: "dfa_largeZ", imms: bcImmsDict, flags: bcReadWriteK | bcReadS},

	opslower:      {text: "slower", imms: bcImmsS16, flags: bcReadWriteK | bcReadWriteS},
	opsupper:      {text: "supper", imms: bcImmsS16, flags: bcReadWriteK | bcReadWriteS},
	opsadjustsize: {text: "saddjustsize", flags: bcReadWriteS},

	optrap: {text: "trap"},
}

func init() {
	// Multiple purposes:
	//   - Verify that new ops have been added to the opinfo table
	//   - Automatically calculate the final immediate width from all immediates
	for i := 0; i < _maxbcop; i++ {
		info := &opinfo[i]
		if info.text == "" {
			panic(fmt.Sprintf("missing opinfo for bcop %v", i))
		}

		immw := uint(0)
		for j := 0; j < len(info.imms); j++ {
			immw += uint(bcImmWidth[info.imms[j]])
		}

		if len(info.vaImms) != 0 {
			immw += 4 // variable argument count is 4 bytes
		}

		if immw >= 256 {
			panic(fmt.Sprintf("%s immediate width too large: %d bytes", info.text, immw))
		}

		info.immwidth = uint8(immw)
	}
}

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

	hashmem []uint64 // the H virtual registers (128 bits per lane)

	trees []*radixTree64 // trees used for hashmember, etc.

	//lint:ignore U1000 not unused; used in assembly
	bucket [16]int32 // the L register (32 bits per lane)
	//lint:ignore U1000 not unused; used in assembly
	lbuf [16]int64 // location buffer

	// scratch buffer used for projection
	scratch []byte
	// number of bytes to reserve for literals
	scratchreserve int
	// relative displacment of scratch relative to vmm
	scratchoff uint32

	// this is a back-up copy if scratch[:scratchreserve]
	// that we use if we have decided to temporarily
	// de-allocate scratch[]
	scratchsave []byte

	//lint:ignore U1000 not unused; used in assembly
	outer *bytecode // outer variable bindings
	//lint:ignore U1000 not unused; used in assembly
	perm [16]int32 // permutation from outer to inner bindings

	//lint:ignore U1000 not unused; used in assembly
	// Area that is used by bytecode instructions to temporarily spill registers.
	// 512 bytes can be used to spill up to 8 ZMM registers (or more registers of
	// any choice). Note that spill area is designed to be used only by a single
	// bytecode instruction at a time, it should not be used to persist any data
	// during the execution of bytecode.
	spillArea [512]byte

	vstacksize int
	hstacksize int

	// set from abort handlers
	err   bcerr
	errpc int32
	// additional error information;
	// error-specific
	errinfo int
}

func formatImmediatesTo(b *strings.Builder, imms []bcImmType, bc []byte) int {
	i := 0
	size := len(bc)

	for immIndex := 0; immIndex < len(imms); immIndex++ {
		immType := imms[immIndex]
		immWidth := int(bcImmWidth[immType])

		if size-i < immWidth {
			fmt.Fprintf(b, "<bytecode is truncated, cannot decode immediate value of size %d while there is only %d bytes left>", immWidth, size-i)
			return -1
		}

		immValue := uint64(0)
		for immByte := 0; immByte < immWidth; immByte++ {
			immValue |= uint64(bc[i]) << (immByte * 8)
			i++
		}

		if immIndex != 0 {
			b.WriteString(", ")
		}

		switch immType {
		case bcImmI8:
			fmt.Fprintf(b, "i8(%d)", int8(immValue))
		case bcImmI16:
			fmt.Fprintf(b, "i16(%d)", int16(immValue))
		case bcImmI32:
			fmt.Fprintf(b, "i32(%d)", int32(immValue))
		case bcImmI64:
			fmt.Fprintf(b, "i64(%d)", int64(immValue))
		case bcImmU8:
			fmt.Fprintf(b, "u8(%d)", immValue)
		case bcImmU16:
			fmt.Fprintf(b, "u16(%d)", immValue)
		case bcImmU32:
			fmt.Fprintf(b, "u32(%d)", immValue)
		case bcImmU64:
			fmt.Fprintf(b, "u64(%d)", immValue)
		case bcImmU8Hex:
			fmt.Fprintf(b, "u8(0x%X)", immValue)
		case bcImmU16Hex:
			fmt.Fprintf(b, "u16(0x%X)", immValue)
		case bcImmU32Hex:
			fmt.Fprintf(b, "u32(0x%X)", immValue)
		case bcImmU64Hex:
			fmt.Fprintf(b, "u64(0x%X)", immValue)
		case bcImmF64:
			fmt.Fprintf(b, "f64(%g)", math.Float64frombits(immValue))
		case bcImmS16:
			fmt.Fprintf(b, "[%d]", immValue)
		case bcImmDict:
			fmt.Fprintf(b, "dict[%d]", immValue)
		default:
			panic(fmt.Sprintf("Unhandled immediate type %v", immType))
		}
	}

	return i
}

func formatBytecode(bc []byte) string {
	var b strings.Builder

	i := int(0)
	size := len(bc)

	for i < size {
		if size-i < 8 {
			fmt.Fprintf(&b, "<bytecode is truncated, cannot decode opcode of size %d while there is only %d bytes left>", 8, size-i)
			break
		}

		opaddr := uintptr(binary.LittleEndian.Uint64(bc[i:]))
		i += 8

		op, ok := opcodeID(opaddr)

		if !ok {
			fmt.Fprintf(&b, "<invalid:%x>\n", opaddr)
			continue
		}

		info := &opinfo[op]
		b.WriteString(info.text)

		if len(info.imms) != 0 {
			b.WriteString(" ")
			immSize := formatImmediatesTo(&b, info.imms, bc[i:])
			if immSize == -1 {
				break
			}
			i += immSize
		}

		if len(info.vaImms) != 0 {
			if size-i < 4 {
				fmt.Fprintf(&b, "<bytecode is truncated, cannot decode va-length consisting of %d bytes while there is only %d bytes left>", 4, size-i)
				break
			}

			vaLength := uint(binary.LittleEndian.Uint32(bc[i:]))
			i += 4

			if len(info.imms) != 0 {
				b.WriteString(", ")
			} else {
				b.WriteString(" ")
			}

			fmt.Fprintf(&b, "va(%d)", vaLength)
			for vaIndex := 0; vaIndex < int(vaLength); vaIndex++ {
				b.WriteString(", {")
				immSize := formatImmediatesTo(&b, info.vaImms, bc[i:])
				if immSize == -1 {
					break
				}
				i += immSize
				b.WriteString("}")
			}
		}

		b.WriteString("\n")
	}
	return b.String()
}

func (b *bytecode) String() string {
	return formatBytecode(b.compiled)
}

// finalize append the final 'return' instruction
// to the bytecode buffer and checks that the stack
// depth is sane
func (b *bytecode) finalize() error {
	b.compiled = append(b.compiled, opcodeToBytes(opret)...)
	return nil
}

// Makes sure that the virtual stack size is at least `size` (in bytes).
func (b *bytecode) ensureVStackSize(size int) {
	if b.vstacksize < size {
		b.vstacksize = size
	}
}

// Makes sure that the hash stack size is at least `size` (in bytes).
func (b *bytecode) ensureHStackSize(size int) {
	if b.hstacksize < size {
		b.hstacksize = size
	}
}

// Allocates all stacks that are needed to execute the bytecode program.
func (b *bytecode) allocStacks() {
	vSize := (b.vstacksize + 7) >> 3
	hSize := (b.hstacksize + 7) >> 3

	if cap(b.vstack) < vSize {
		b.vstack = make([]uint64, vSize)
	} else if len(b.vstack) != vSize {
		b.vstack = b.vstack[:vSize]
	}

	if cap(b.hashmem) < hSize {
		b.hashmem = make([]uint64, hSize)
	} else if len(b.hashmem) != hSize {
		b.hashmem = b.hashmem[:hSize]
	}
}

// dropScratch saves a copy of the current
// reserved memory in b.scratch (if any)
// and frees b.scratch (if it exists);
// this operation can be un-done with b.restoreScratch()
func (b *bytecode) dropScratch() {
	if b.scratch != nil {
		// note: this will often be a 0-byte slice,
		// but that's fine; we just need it to be
		// non-nil so that restoreScratch() will
		// see it and allocate b.scratch again
		b.scratchsave = slices.Clone(b.scratch[:b.scratchreserve])
		Free(b.scratch)
		b.scratch = nil
		// this will trigger a fault if it is used:
		b.scratchoff = 0x80000000
	}
}

// dropSaved drops a previously-saved scratch
// buffer saved in b.dropScratch()
func (b *bytecode) dropSaved() {
	b.scratchsave = nil
	b.scratchreserve = 0
}

func (b *bytecode) restoreScratch() {
	if b.scratch != nil || b.scratchsave == nil {
		return
	}
	b.scratch = Malloc()
	b.scratch = b.scratch[:copy(b.scratch, b.scratchsave)]
	b.scratchoff, _ = vmdispl(b.scratch[:1])
	b.scratchsave = nil
}

// called from ssa compilation;
// this sets up the initial space
// for literals that need to be projected
func (b *bytecode) setlit(buf []byte) bool {
	if len(buf) > defaultAlign {
		return false
	}
	if b.scratch == nil {
		b.scratch = Malloc()
	}
	b.scratchreserve = copy(b.scratch, buf)
	b.scratch = b.scratch[:b.scratchreserve]
	var ok bool
	b.scratchoff, ok = vmdispl(b.scratch[:1])
	if !ok {
		panic("buffer from malloc has bad displacement?")
	}
	return true
}

func (b *bytecode) reset() {
	if b.scratch != nil {
		Free(b.scratch)
	}
	*b = bytecode{}
}
