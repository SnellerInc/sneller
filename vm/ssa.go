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
	"io"
	"math"
	"math/bits"
	"os"
	"strconv"

	"golang.org/x/exp/slices"
	"golang.org/x/sys/cpu"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/heap"
	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
)

// MaxSymbolID is the largest symbol ID
// supported by the system.
const MaxSymbolID = (1 << 21) - 1

type value struct {
	id   int
	op   ssaop
	args []*value

	// if this value has non-standard
	// not-missing-ness, then that is set here
	notMissing *value

	imm any
}

type hashcode [6]uint64

type sympair struct {
	sym ion.Symbol
	val string
}

type symid struct {
	id  int
	val string
}

type prog struct {
	values []*value // all values in program
	ret    *value   // value actually yielded by program

	// used to find common expressions
	dict  []string            // common strings
	tmpSt ion.Symtab          // temporary symbol table we need to encode data for dict
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
	// similarly, resolvedAux is resolved[] but for aux bindings
	resolvedAux []symid

	// finalizers that must be run when this prog is GC'd
	finalize []func()
}

func (p *prog) reset() {
	for i := range p.finalize {
		p.finalize[i]()
	}
	p.finalize = p.finalize[:0]
	*p = prog{}
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

func (p *prog) binaryDataToBits(str string) uint64 {
	for i := range p.dict {
		if str == p.dict[i] {
			return uint64(i)
		}
	}

	p.dict = append(p.dict, pad(str))
	return uint64(len(p.dict) - 1)
}

// used to produce a consistent bit pattern
// for hashing common subexpressions
func (p *prog) tobits(imm any) uint64 {
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
		return p.binaryDataToBits(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case date.Time:
		var buf ion.Buffer
		buf.WriteTime(v)
		return p.binaryDataToBits(string(buf.Bytes()[1:]))
	case ion.Datum:
		buf := ion.Buffer{}
		v.Encode(&buf, &p.tmpSt)
		return p.binaryDataToBits(string(buf.Bytes()))
	default:
		panic(fmt.Sprintf("invalid immediate %+v with type %T", imm, imm))
	}
}

// overwrite a value with a message
// indicating why it is invalid
func (v *value) errf(f string, args ...any) {
	v.op = sinvalid
	v.args = nil
	v.imm = fmt.Sprintf(f, args...)
}

func (v *value) setimm(imm any) {
	if v.op != sinvalid && ssainfo[v.op].immfmt == fmtnone {
		v.errf("cannot assign immediate %v to op %s", imm, v.op)
		return
	}

	v.imm = imm
}

func (p *prog) errorf(f string, args ...any) *value {
	v := p.val()
	v.errf(f, args...)
	return v
}

func (p *prog) begin() {
	p.exprs = make(map[hashcode]*value)
	p.values = nil
	p.ret = nil
	p.dict = nil
	p.tmpSt.Reset()

	// op 0 is always 'init'
	v := p.val()
	v.op = sinit
	// op 1 is always 'undef'
	v = p.val()
	v.op = sundef

	p.symbolized = false
	p.resolved = p.resolved[:0]
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

func (p *prog) ssa0imm(op ssaop, imm any) *value {
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

func (p *prog) ssa1(op ssaop, arg *value) *value {
	var hc hashcode
	hc[0] = uint64(op)
	hc[1] = uint64(arg.id)
	if v := p.exprs[hc]; v != nil {
		return v
	}
	v := p.val()
	v.op = op
	v.args = []*value{arg}
	v.checkarg(arg, 0)
	if v.op != sinvalid {
		p.exprs[hc] = v
	}
	return v
}

func (p *prog) ssa2imm(op ssaop, arg0, arg1 *value, imm any) *value {
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

func (p *prog) ssa3imm(op ssaop, arg0, arg1, arg2 *value, imm any) *value {
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

func (p *prog) ssa4imm(op ssaop, arg0, arg1, arg2, arg3 *value, imm any) *value {
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
func (p *prog) setssa(v *value, op ssaop, imm any, args ...*value) *value {
	v.op = op
	v.notMissing = nil

	if imm == nil {
		v.imm = nil
	} else {
		v.setimm(imm)
	}

	v.args = shrink(v.args, len(args))
	copy(v.args, args)
	for i := range args {
		v.checkarg(args[i], i)
	}
	return v
}

func (p *prog) ssaimm(op ssaop, imm any, args ...*value) *value {
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

func (p *prog) constant(imm any) *value {
	v := p.val()
	v.op = sliteral
	v.imm = imm
	return v
}

// returnValue terminates the execution of the program and optionally fills
// output registers with values that are allocated on virtual stack.
func (p *prog) returnValue(v *value) {
	info := &ssainfo[v.op]

	// Return only accepts operations that actually return (terminate the execution).
	if info.returnOp {
		p.ret = v
		return
	}

	// If the input is stMem, we would wrap it in a void return - this
	// program doesn't return any value, it most likely only aggregates.
	if (info.rettype & stMem) != 0 {
		p.ret = p.ssa1(sretm, v)
		return
	}

	panic(fmt.Sprintf("invalid return operation %s", v.op.String()))
}

func (p *prog) returnBool(mem, pred *value) {
	p.returnValue(p.ssa2(sretmk, mem, pred))
}

func (p *prog) returnScalar(mem, scalar, pred *value) {
	p.returnValue(p.ssa3(sretmsk, mem, scalar, pred))
}

func (p *prog) returnBK(base, pred *value) {
	p.returnValue(p.ssa2(sretbk, base, pred))
}

func (p *prog) returnBHK(base, hash, pred *value) {
	p.returnValue(p.ssa3(sretbhk, base, hash, pred))
}

// initMem returns the memory token associated
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

func (p *prog) missing() *value {
	return p.ssa0(skfalse)
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
	// non-logical instructions
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
			return p.ssa1(snotmissing, v)
		default:
			// arithmetic or other op with no return mask;
			// the mask argument is implicitly the NOT MISSING value
			return p.mask(v)
		}
	}
	switch v.op {
	case skfalse, sinit:
		return v
	case sandn:
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

// makes a V+K tuple so `p.mask(v)` returns `k`
func (p *prog) makevk(v, k *value) *value {
	if v == k || p.mask(v) == k {
		return v
	}
	return p.ssa2(smakevk, v, k)
}

// float+K tuple
func (p *prog) floatk(f, k *value) *value {
	return p.ssa2(sfloatk, f, k)
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
		return p.ssa2(sandn, v, p.notMissing(v))
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
				return p.missing()
			}
			if b {
				// left = TRUE -> left mask
				return left
			}
			// left = FALSE -> !left and left is not missing
			return p.andn(left, p.notMissing(left))
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
			return p.ssa3(scmpeqv, left, right, p.ssa2(sand, p.mask(left), p.mask(right)))
		case stInt:
			lefti, k := p.coerceI64(left)
			return p.ssa3(scmpeqi, lefti, right, p.and(k, p.mask(right)))
		case stFloat:
			leftf, k := p.coerceF64(left)
			return p.ssa3(scmpeqf, leftf, right, p.and(k, p.mask(right)))
		case stString:
			leftstr := p.coerceStr(left)
			return p.ssa3(scmpeqstr, leftstr, right, p.and(p.mask(leftstr), p.mask(right)))
		case stTime:
			leftts, leftk := p.coerceTimestamp(left)
			return p.ssa3(scmpeqts, leftts, right, p.and(p.mask(leftk), p.mask(right)))
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
		left = p.ssa2(scvti64tof64, left, p.mask(left))
		fallthrough
	case stFloat:
		if right.op == sliteral {
			return p.ssa2imm(scmpeqimmf, left, p.mask(left), right.imm)
		}
		switch right.primary() {
		case stInt:
			right = p.ssa2(scvti64tof64, right, p.mask(right))
			fallthrough
		case stFloat:
			return p.ssa3(scmpeqf, left, right, p.and(p.mask(left), p.mask(right)))
		default:
			return p.missing() // FALSE/MISSING
		}
	case stString:
		if right.op == sliteral {
			return p.ssa2imm(sStrCmpEqCs, left, left, right.imm)
		}
		switch right.primary() {
		case stString:
			return p.ssa3(scmpeqstr, left, right, p.and(p.mask(left), p.mask(right)))
		default:
			return p.missing() // FALSE/MISSING
		}
	case stTime:
		switch right.primary() {
		case stTime:
			return p.ssa3(scmpeqts, left, right, p.and(p.mask(left), p.mask(right)))
		case stValue:
			rv, rm := p.coerceTimestamp(right)
			return p.ssa3(scmpeqts, left, rv, p.and(p.mask(left), rm))
		default:
			return p.missing() // FALSE/MISSING
		}
	default:
		return p.errorf("cannot compare %s and %s", left, right)
	}
}

// octetLength returns the number of bytes in v
func (p *prog) octetLength(v *value) *value {
	v = p.coerceStr(v)
	return p.ssa2(soctetlength, v, p.mask(v))
}

// charLength returns the number of unicode code-points in v
func (p *prog) charLength(v *value) *value {
	v = p.coerceStr(v)
	return p.ssa2(scharacterlength, v, p.mask(v))
}

// Substring returns a substring at the provided startIndex with length
func (p *prog) substring(v, substrOffset, substrLength *value) *value {
	offsetInt, offsetMask := p.coerceI64(substrOffset)
	lengthInt, lengthMask := p.coerceI64(substrLength)
	mask := p.and(v, p.and(offsetMask, lengthMask))
	return p.ssa4(sSubStr, v, offsetInt, lengthInt, mask)
}

// SplitPart splits string on delimiter and returns the field index. Field indexes start with 1.
func (p *prog) splitPart(v *value, delimiter byte, index *value) *value {
	delimiterStr := string(delimiter)
	indexInt, indexMask := p.coerceI64(index)
	mask := p.and(v, indexMask)
	return p.ssa3imm(sSplitPart, v, indexInt, mask, delimiterStr)
}

// is v an ion null value?
func (p *prog) isnull(v *value) *value {
	if v.primary() != stValue {
		return p.missing()
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

func isBoolImmediate(imm any) bool {
	switch imm.(type) {
	case bool:
		return true
	default:
		return false
	}
}

func isIntImmediate(imm any) bool {
	switch v := imm.(type) {
	case int, int64, uint, uint64:
		return true
	case float64:
		return float64(int64(v)) == v
	default:
		return false
	}
}

func isFloatImmediate(imm any) bool {
	switch imm.(type) {
	case float64:
		return true
	default:
		return false
	}
}

func isNumericImmediate(imm any) bool {
	return isFloatImmediate(imm) || isIntImmediate(imm)
}

func isStringImmediate(imm any) bool {
	switch imm.(type) {
	case string:
		return true
	default:
		return false
	}
}

func isTimestampImmediate(imm any) bool {
	switch imm.(type) {
	case date.Time:
		return true
	default:
		return false
	}
}

func tobool(imm any) bool {
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

func tof64(imm any) float64 {
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

func toi64(imm any) uint64 {
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
	case uint16:
		return uint64(i)
	case uint32:
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
		op := sbroadcast0k
		if toi64(arg.imm) != 0 {
			op = sbroadcast1k
		}
		return p.ssa0(op), p.validLanes()
	}

	if arg.primary() == stBool {
		return arg, p.notMissing(arg)
	}

	if arg.primary() == stValue {
		k := p.mask(arg)
		i := p.ssa2(sunboxktoi64, arg, k)
		return p.ssa2(scvti64tok, i, p.mask(i)), p.mask(i)
	}

	err := p.val()
	err.errf("cannot convert %s to BOOL", arg)
	return err, err
}

// coerce a value to floating point,
// taking care to promote integers appropriately
func (p *prog) coerceF64(v *value) (*value, *value) {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcastf, v.imm), p.validLanes()
	}
	switch v.primary() {
	case stFloat:
		return v, p.mask(v)
	case stInt:
		ret := p.ssa2(scvti64tof64, v, p.mask(v))
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

// coerceI64 coerces a value to integer
func (p *prog) coerceI64(v *value) (*value, *value) {
	if v.op == sliteral {
		return p.ssa0imm(sbroadcasti, v.imm), p.validLanes()
	}
	switch v.primary() {
	case stInt:
		return v, p.mask(v)
	case stFloat:
		return p.ssa2(scvtf64toi64, v, p.mask(v)), p.mask(v)
	case stValue:
		ret := p.ssa2(sunboxcoercei64, v, p.mask(v))
		return ret, ret
	default:
		err := p.errf("cannot convert %s to an integer", v)
		return err, err
	}
}

func (p *prog) coerceStr(str *value) *value {
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
		v = p.ssa2(sunboxtime, v, p.mask(v))
		fallthrough
	case stTime:
		return v, p.mask(v)
	default:
		return p.errorf("cannot use result of %s as TIMESTAMP", v), p.validLanes()
	}
}

func (p *prog) concat(args ...*value) *value {
	if len(args) == 0 {
		panic("Concat() requires at least 1 argument")
	}

	if len(args) == 1 {
		return p.coerceStr(args[0])
	}

	var values []*value = make([]*value, 0, len(args)*2)
	for _, arg := range args {
		s := p.coerceStr(arg)
		values = append(values, s, p.mask(s))
	}
	return p.ssava(sstrconcat, values)
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
	str = p.coerceStr(str)
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
	str = p.coerceStr(str)
	numberOfChars := len(chars)
	if numberOfChars == 0 {
		return str
	}
	if numberOfChars > 4 {
		v := p.val()
		v.errf("only 4 chars are supported in TrimChar, %v char(s) provided in %v", numberOfChars, chars)
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

// EqualsStr returns true when needle equals the provided string; false otherwise
func (p *prog) equalsStr(str *value, needle stringext.Needle, caseSensitive bool) *value {
	if !caseSensitive && !stringext.HasCaseSensitiveChar(needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodeNeedle(needle, sStrCmpEqCs)
		return p.ssa2imm(sStrCmpEqCs, str, str, enc)
	}
	if stringext.HasNtnString(needle) { // needle has non-trivial normalization
		enc := encodeNeedle(needle, sStrCmpEqUTF8Ci)
		return p.ssa2imm(sStrCmpEqUTF8Ci, str, str, enc)
	}
	enc := encodeNeedle(needle, sStrCmpEqCi)
	return p.ssa2imm(sStrCmpEqCi, str, str, enc)
}

// EqualsPattern returns true when pattern equals the provided string; false otherwise
func (p *prog) equalsPattern(str *value, pattern *stringext.Pattern, caseSensitive bool) *value {
	if !pattern.HasWildcard {
		return p.equalsStr(str, pattern.Needle, caseSensitive)
	}
	str = p.coerceStr(str)
	if !caseSensitive && !stringext.HasCaseSensitiveChar(pattern.Needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodePattern(pattern, sEqPatternCs)
		return p.ssa2imm(sEqPatternCs, str, str, enc)
	}
	if stringext.HasNtnString(pattern.Needle) { // needle has non-trivial normalization
		enc := encodePattern(pattern, sEqPatternUTF8Ci)
		return p.ssa2imm(sEqPatternUTF8Ci, str, str, enc)
	}
	enc := encodePattern(pattern, sEqPatternCi)
	return p.ssa2imm(sEqPatternCi, str, str, enc)
}

// HasPrefix returns true when str has the provided prefix; false otherwise
func (p *prog) hasPrefix(str *value, prefix stringext.Needle, caseSensitive bool) *value {
	str = p.coerceStr(str)
	if prefix == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(prefix) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodeNeedle(prefix, sStrContainsPrefixCs)
		return p.ssa2imm(sStrContainsPrefixCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(prefix) { // prefix has non-trivial normalization
		enc := encodeNeedle(prefix, sStrContainsPrefixUTF8Ci)
		return p.ssa2imm(sStrContainsPrefixUTF8Ci, str, p.mask(str), enc)
	}
	enc := encodeNeedle(prefix, sStrContainsPrefixCi)
	return p.ssa2imm(sStrContainsPrefixCi, str, p.mask(str), enc)
}

// HasPrefixPattern returns true when str has the provided pattern as prefix; false otherwise
func (p *prog) hasPrefixPattern(str *value, pattern *stringext.Pattern, caseSensitive bool) *value {
	if !caseSensitive && !stringext.HasCaseSensitiveChar(pattern.Needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	// split the pattern based on the wildcard and issue skip and prefix calls.
	needles, wildcards := pattern.SplitWC()
	for i, needle := range needles {
		wildcard := wildcards[i]
		if wildcard[0] { // NOTE: elements of slice wildcard are always either only true or false
			str = p.skipCharLeftConst(str, len(wildcard))
		} else {
			str = p.hasPrefix(str, needle, caseSensitive)
		}
	}
	return str
}

// HasSuffix returns true when str has the provided suffix; false otherwise
func (p *prog) hasSuffix(str *value, suffix stringext.Needle, caseSensitive bool) *value {
	str = p.coerceStr(str)
	if suffix == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(suffix) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodeNeedle(suffix, sStrContainsSuffixCs)
		return p.ssa2imm(sStrContainsSuffixCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(suffix) { // suffix has non-trivial normalization
		enc := encodeNeedle(suffix, sStrContainsSuffixUTF8Ci)
		return p.ssa2imm(sStrContainsSuffixUTF8Ci, str, p.mask(str), enc)
	}
	enc := encodeNeedle(suffix, sStrContainsSuffixCi)
	return p.ssa2imm(sStrContainsSuffixCi, str, p.mask(str), enc)
}

// hasSuffixPattern returns true when str has the provided pattern as suffix; false otherwise
func (p *prog) hasSuffixPattern(str *value, pattern *stringext.Pattern, caseSensitive bool) *value {
	if !caseSensitive && !stringext.HasCaseSensitiveChar(pattern.Needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	// split the pattern based on the wildcard and issue skip and suffix calls.
	needles, wildcards := pattern.SplitWC()
	for i := len(needles) - 1; i >= 0; i-- {
		wildcard := wildcards[i]
		if wildcard[0] { // NOTE: elements of slice wildcard are always either only true or false
			str = p.skipCharRightConst(str, len(wildcard))
		} else {
			str = p.hasSuffix(str, needles[i], caseSensitive)
		}
	}
	return str
}

// Contains returns whether the given value
// is a string containing 'needle' as a substring.
// (The return value is always 'true' if 'str' is
// a string and 'needle' is the empty string.)
func (p *prog) contains(str *value, needle stringext.Needle, caseSensitive bool) *value {
	// n.b. the 'contains' code doesn't actually
	// handle the empty string; just return whether
	// this value is a string
	str = p.coerceStr(str)
	if needle == "" {
		return str
	}
	if !caseSensitive && !stringext.HasCaseSensitiveChar(needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodeNeedle(needle, sStrContainsSubstrCs)
		return p.ssa2imm(sStrContainsSubstrCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(needle) { // needle has non-trivial normalization
		enc := encodeNeedle(needle, sStrContainsSubstrUTF8Ci)
		return p.ssa2imm(sStrContainsSubstrUTF8Ci, str, p.mask(str), enc)
	}
	enc := encodeNeedle(needle, sStrContainsSubstrCi)
	return p.ssa2imm(sStrContainsSubstrCi, str, p.mask(str), enc)
}

// containsPattern returns whether the given value
// is a string containing a matching substring
// that matches 'pattern'.
func (p *prog) containsPattern(str *value, pattern *stringext.Pattern, caseSensitive bool) *value {
	if !pattern.HasWildcard {
		return p.contains(str, pattern.Needle, caseSensitive)
	}
	str = p.coerceStr(str)
	if !caseSensitive && !stringext.HasCaseSensitiveChar(pattern.Needle) {
		// we are requested to do case-insensitive compare, but there are no case-sensitive characters.
		caseSensitive = true
	}
	if caseSensitive {
		enc := encodePattern(pattern, sStrContainsPatternCs)
		return p.ssa2imm(sStrContainsPatternCs, str, p.mask(str), enc)
	}
	if stringext.HasNtnString(pattern.Needle) { // needle has non-trivial normalization
		enc := encodePattern(pattern, sStrContainsPatternUTF8Ci)
		return p.ssa2imm(sStrContainsPatternUTF8Ci, str, p.mask(str), enc)
	}
	enc := encodePattern(pattern, sStrContainsPatternCi)
	return p.ssa2imm(sStrContainsPatternCi, str, p.mask(str), enc)
}

// IsSubnetOfIP4 returns whether the give value is an IPv4 address between (and including) min and max
func (p *prog) isSubnetOfIP4(str *value, min, max [4]byte) *value {
	str = p.coerceStr(str)
	return p.ssa2imm(sIsSubnetOfIP4, str, p.mask(str), stringext.ToBCD(&min, &max))
}

// SkipCharLeftConst skips a constant number of UTF-8 code-points from the left side of a string
func (p *prog) skipCharLeftConst(str *value, nChars int) *value {
	str = p.coerceStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharLeft, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceI64(p.constant(int64(nChars)))
		return p.ssa3(sStrSkipNCharLeft, str, nCharsInt, p.and(p.mask(str), nCharsMask))
	}
}

// SkipCharRightConst skips a constant number of UTF-8 code-points from the right side of a string
func (p *prog) skipCharRightConst(str *value, nChars int) *value {
	str = p.coerceStr(str)
	switch nChars {
	case 0:
		return str
	case 1:
		return p.ssa2(sStrSkip1CharRight, str, p.mask(str))
	default:
		nCharsInt, nCharsMask := p.coerceI64(p.constant(int64(nChars)))
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
	const wc = '_' // wildcard character
	const ks = '%'

	str = p.coerceStr(str)
	if !caseSensitive { // Bytecode for case-insensitive comparing expects that needles and patterns are in normalized (UPPER) case
		expr = stringext.NormalizeString(expr)
	}
	likeSegments := stringext.SimplifyLikeExpr(expr, wc, ks, escape)
	nSegments := len(likeSegments)

	// special situation when expr only contains '_' and '%'
	if nSegments == 1 {
		first := likeSegments[0]
		charLen := p.charLength(str)
		if first.SkipMax == -1 { // skip at-most inf chars
			if first.SkipMin == 0 { // skip at-least 0 chars
				return p.mask(str) // thus skip any number of chars
			}
			return p.lessEqual(p.constant(first.SkipMin), charLen)
		}
		if first.SkipMax == first.SkipMin {
			// e.g. LIKE '____' gives 1 segment `[4~4:]` which means
			// "skip at-least and at-most 4 chars, and match with ''"
			return p.equals(charLen, p.constant(first.SkipMin))
		}
		// not sure if this situation can be constructed in a LIKE expression
		min := p.lessEqual(charLen, p.constant(first.SkipMin))
		max := p.less(p.constant(first.SkipMax), charLen)
		return p.and(min, max)
	}

	// special situation when expr does not contain ks '%', the equals pattern applies
	if nSegments == 2 {
		first := likeSegments[0]
		second := likeSegments[1]
		if (first.SkipMax == first.SkipMin) && (second.SkipMax == second.SkipMin) {
			str = p.skipCharLeftConst(str, first.SkipMax)
			str = p.skipCharRightConst(str, second.SkipMax)
			return p.equalsPattern(str, &first.Pattern, caseSensitive)
		}
	}

	// if the first likeSegment is a prefix
	first := likeSegments[0]
	if first.SkipMax == first.SkipMin {
		str = p.skipCharLeftConst(str, first.SkipMax)
		str = p.hasPrefixPattern(str, &first.Pattern, caseSensitive)
		likeSegments = likeSegments[1:] // remove the first segment
		nSegments--
	}

	// if the last likeElement is a suffix
	last := likeSegments[nSegments-1]
	if (last.SkipMax != -1) && (last.Pattern.Needle == "") && (nSegments > 1) {
		secondLast := likeSegments[nSegments-2]
		str = p.skipCharLeftConst(str, secondLast.SkipMin)
		str = p.skipCharRightConst(str, last.SkipMax)
		str = p.hasSuffixPattern(str, &secondLast.Pattern, caseSensitive)
		likeSegments = likeSegments[:nSegments-2] // remove the last two segments
	}

	// the remaining likeElements are `contains patterns'
	for _, seg := range likeSegments {
		str = p.skipCharLeftConst(str, seg.SkipMin)
		str = p.containsPattern(str, &seg.Pattern, caseSensitive)
	}
	return str
}

// RegexMatch matches 'str' as a string against regex
func (p *prog) regexMatch(str *value, store *regexp2.DFAStore) (*value, error) {
	if trivial, accepting := store.IsTrivial(); trivial {
		if accepting {
			return p.mask(str), nil
		}
		return p.ssa0(skfalse), nil
	}
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

// equalsFuzzy does a fuzzy string equality of 'str' as a string against needle.
// Equality is computed with Damerau–Levenshtein distance estimation based on three
// character horizon. If the distance exceeds the provided threshold, the match is
// rejected; that is, str and needle are considered unequal.
func (p *prog) equalsFuzzy(str *value, needle stringext.Needle, threshold *value, ascii bool) *value {
	thresholdInt, thresholdMask := p.coerceI64(threshold)
	mask := p.and(str, thresholdMask)
	if ascii {
		enc := encodeNeedle(needle, sCmpFuzzyA3)
		return p.ssa3imm(sCmpFuzzyA3, str, thresholdInt, mask, enc)
	}
	enc := encodeNeedle(needle, sCmpFuzzyUnicodeA3)
	return p.ssa3imm(sCmpFuzzyUnicodeA3, str, thresholdInt, mask, enc)
}

// containsFuzzy does a fuzzy string contains of needle in 'str'.
// Equality is computed with Damerau–Levenshtein distance estimation based on three
// character horizon. If the distance exceeds the provided threshold, the match is
// rejected; that is, str and needle are considered unequal.
func (p *prog) containsFuzzy(str *value, needle stringext.Needle, threshold *value, ascii bool) *value {
	thresholdInt, thresholdMask := p.coerceI64(threshold)
	mask := p.and(str, thresholdMask)
	if ascii {
		enc := encodeNeedle(needle, sHasSubstrFuzzyA3)
		return p.ssa3imm(sHasSubstrFuzzyA3, str, thresholdInt, mask, enc)
	}
	enc := encodeNeedle(needle, sHasSubstrFuzzyUnicodeA3)
	return p.ssa3imm(sHasSubstrFuzzyUnicodeA3, str, thresholdInt, mask, enc)
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
	cmpkimm ssaop
	cmpi    ssaop
	cmpiimm ssaop
	cmpf    ssaop
	cmpfimm ssaop
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
	comparelt: {cmpk: scmpltk, cmpkimm: scmpltimmk, cmpi: scmplti, cmpiimm: scmpltimmi, cmpf: scmpltf, cmpfimm: scmpltimmf, cmps: scmpltstr, cmpts: scmpltts},
	comparele: {cmpk: scmplek, cmpkimm: scmpleimmk, cmpi: scmplei, cmpiimm: scmpleimmi, cmpf: scmplef, cmpfimm: scmpleimmf, cmps: scmplestr, cmpts: scmplets},
	comparegt: {cmpk: scmpgtk, cmpkimm: scmpgtimmk, cmpi: scmpgti, cmpiimm: scmpgtimmi, cmpf: scmpgtf, cmpfimm: scmpgtimmf, cmps: scmpgtstr, cmpts: scmpgtts},
	comparege: {cmpk: scmpgek, cmpkimm: scmpgeimmk, cmpi: scmpgei, cmpiimm: scmpgeimmi, cmpf: scmpgef, cmpfimm: scmpgeimmf, cmps: scmpgestr, cmpts: scmpgets},
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
			return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
		}
		if isIntImmediate(imm) {
			cmpv := p.ssa2imm(scmpvimmi64, left, p.mask(left), toi64(imm))
			return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
		}
		if isFloatImmediate(imm) {
			cmpv := p.ssa2imm(scmpvimmf64, left, p.mask(left), tof64(imm))
			return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
		}
		if isStringImmediate(imm) {
			left = p.coerceStr(left)
			right = p.coerceStr(right)
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
		return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stInt {
		cmpv := p.ssa3(scmpvi64, left, right, p.and(p.mask(left), p.mask(right)))
		return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stFloat {
		cmpv := p.ssa3(scmpvf64, left, right, p.and(p.mask(left), p.mask(right)))
		return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
	}
	if rType == stString {
		left = p.coerceStr(left)
		return p.ssa3(info.cmps, left, right, p.and(p.mask(left), p.mask(right)))
	}
	if rType == stTime {
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
			return p.ssa2imm(info.cmpkimm, left, p.mask(left), tobool(right.imm))
		}
		return p.missing()
	}

	// compare immediate vs bool
	if lLiteral && rType == stBool {
		if isBoolImmediate(left.imm) {
			return p.ssa2imm(revInfo.cmpkimm, right, p.mask(right), tobool(left.imm))
		}
		return p.missing()
	}

	// compare bool vs bool
	if lType == stBool && rType == stBool {
		return p.ssa3(info.cmpk, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// compare int/float vs immediate
	if lType == stInt && rLiteral {
		if isIntImmediate(right.imm) {
			return p.ssa2imm(info.cmpiimm, left, p.mask(left), toi64(right.imm))
		}

		lhs, lhk := p.coerceF64(left)
		return p.ssa2imm(info.cmpfimm, lhs, lhk, tof64(right.imm))
	}

	if lType == stFloat && rLiteral {
		return p.ssa2imm(info.cmpfimm, left, p.mask(left), tof64(right.imm))
	}

	// compare immediate vs int/float
	if lLiteral && rType == stInt {
		if isIntImmediate(left.imm) {
			return p.ssa2imm(revInfo.cmpiimm, right, p.mask(right), toi64(left.imm))
		}

		rhs, rhk := p.coerceF64(right)
		return p.ssa2imm(info.cmpfimm, rhs, rhk, tof64(left.imm))
	}

	if lLiteral && rType == stFloat {
		return p.ssa2imm(revInfo.cmpfimm, right, p.mask(right), tof64(left.imm))
	}

	// compare int/float vs int/float (if the types are mixed, int is coerced to float)
	if lType == stInt && rType == stInt {
		return p.ssa3(info.cmpi, left, right, p.and(p.mask(left), p.mask(right)))
	}

	if lType == stInt && rType == stFloat {
		lhs, lhk := p.coerceF64(left)
		return p.ssa3(info.cmpi, lhs, right, p.and(lhk, p.mask(right)))
	}

	if lType == stFloat && rType == stInt {
		rhs, rhk := p.coerceF64(right)
		return p.ssa3(info.cmpi, left, rhs, p.and(p.mask(left), rhk))
	}

	if lType == stFloat && rType == stFloat {
		return p.ssa3(info.cmpf, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// compare timestamp vs timestamp
	lTimeCompat := lType == stTime || (lLiteral && isTimestampImmediate(left.imm))
	rTimeCompat := rType == stTime || (rLiteral && isTimestampImmediate(right.imm))

	if lTimeCompat && rTimeCompat {
		lhs, lhk := p.coerceTimestamp(left)
		rhs, rhk := p.coerceTimestamp(right)
		return p.ssa3(info.cmpts, lhs, rhs, p.and(p.mask(lhk), p.mask(rhk)))
	}

	// Compare string vs string
	lStringCompat := lType == stString || (lLiteral && isStringImmediate(left.imm))
	rStringCompat := rType == stString || (rLiteral && isStringImmediate(right.imm))

	if lStringCompat && rStringCompat {
		left = p.coerceStr(left)
		right = p.coerceStr(right)
		return p.ssa3(info.cmps, left, right, p.and(p.mask(left), p.mask(right)))
	}

	// Compare value vs value
	if lType == stValue && rType == stValue {
		mask := p.and(p.mask(left), p.mask(right))
		cmpv := p.ssa3(scmpv, left, right, mask)
		return p.ssa2imm(info.cmpiimm, cmpv, p.mask(cmpv), int64(0))
	}

	// Uncomparable...
	return p.missing()
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

	if left.op == skfalse {
		return left
	}

	if right.op == skfalse {
		return right
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
func (p *prog) andn(left, right *value) *value {
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
	// as it would show up as (andn (andn x true) true)
	if left.op == sandn && left.args[1] == right {
		return p.and(left, right)
	}
	return p.ssa2(sandn, left, right)
}

// xor computes 'left != right' for boolean values
func (p *prog) xor(left, right *value) *value {
	if left == right {
		return p.ssa0(skfalse)
	}
	// true ^ x -> !x
	if left.op == sinit {
		return p.andn(right, left)
	}
	if right.op == sinit {
		return p.andn(left, right)
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
	return p.andn(v, p.validLanes())
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
		s, k := p.coerceI64(child)
		return p.ssa2(regOpI, s, k)
	}

	return p.makeUnaryArithmeticOpFp(regOpF, child)
}

func (p *prog) makeUnaryArithmeticOpInt(op ssaop, child *value) *value {
	s, k := p.coerceI64(child)
	return p.ssa2(op, s, k)
}

func (p *prog) makeUnaryArithmeticOpFp(op ssaop, child *value) *value {
	if child.op == sliteral {
		child = p.makeBroadcastOp(child)
	}

	s, k := p.coerceF64(child)
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
func (p *prog) makeBinaryArithmeticOpImm(regOpF, regOpI ssaop, v *value, imm any) *value {
	if isIntValue(v) && isIntImmediate(imm) {
		s, k := p.coerceI64(v)
		i64Imm := toi64(imm)
		return p.ssa2imm(regOpI, s, k, i64Imm)
	}

	s, k := p.coerceF64(v)
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

	lhs, lhk := p.coerceF64(left)
	rhs, rhk := p.coerceF64(right)
	return p.ssa3(regOpF, lhs, rhs, p.and(lhk, rhk))
}

func (p *prog) makeBinaryArithmeticOpFp(op ssaop, left *value, right *value) *value {
	if left.op == sliteral {
		left = p.makeBroadcastOp(left)
	}

	if right.op == sliteral {
		right = p.makeBroadcastOp(right)
	}

	lhs, lhk := p.coerceF64(left)
	rhs, rhk := p.coerceF64(right)
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

	lhs, lhk := p.coerceI64(left)
	if right.op == sliteral {
		i64Imm := toi64(right.imm)
		return p.ssa2imm(immOp, lhs, lhk, i64Imm)
	}

	rhs, rhk := p.coerceI64(right)
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

func (p *prog) powuint(arg *value, exp int64) *value {
	x, m := p.coerceF64(arg)
	return p.ssa2imm(spowuintf, x, m, exp)
}

func (p *prog) atan2(left, right *value) *value {
	return p.makeBinaryArithmeticOpFp(satan2f, left, right)
}

func (p *prog) widthBucket(val, min, max, bucketCount *value) *value {
	if isIntValue(val) && isIntValue(min) && isIntValue(max) {
		vali, valk := p.coerceI64(val)
		mini, mink := p.coerceI64(min)
		maxi, maxk := p.coerceI64(max)
		cnti, cntk := p.coerceI64(bucketCount)

		mask := p.and(valk, p.and(cntk, p.and(mink, maxk)))
		return p.ssa5(swidthbucketi, vali, mini, maxi, cnti, mask)
	}

	valf, valk := p.coerceF64(val)
	minf, mink := p.coerceF64(min)
	maxf, maxk := p.coerceF64(max)
	cntf, cntk := p.coerceF64(bucketCount)

	mask := p.and(valk, p.and(cntk, p.and(mink, maxk)))
	return p.ssa5(swidthbucketf, valf, minf, maxf, cntf, mask)
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
		arg0Int, arg0Mask := p.coerceI64(arg0)

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

func (p *prog) dateExtract(part expr.Timepart, val *value) *value {
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
		return p.errorf("unhandled date part in dateExtract()")
	}
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
	iv, im := p.coerceI64(interval)
	return p.ssa3(stimebucketts, tv, iv, p.and(p.mask(tv), im))
}

func (p *prog) geoHash(latitude, longitude, numChars *value) *value {
	latV, latM := p.coerceF64(latitude)
	lonV, lonM := p.coerceF64(longitude)

	if numChars.op == sliteral && isIntImmediate(numChars.imm) {
		return p.ssa3imm(sgeohashimm, latV, lonV, p.and(latM, lonM), numChars.imm)
	}

	charsV, charsM := p.coerceI64(numChars)
	mask := p.and(p.and(latM, lonM), charsM)
	return p.ssa4(sgeohash, latV, lonV, charsV, mask)
}

func (p *prog) geoTileX(longitude, precision *value) *value {
	lonV, lonM := p.coerceF64(longitude)
	precV, precM := p.coerceI64(precision)
	mask := p.and(lonM, precM)
	return p.ssa3(sgeotilex, lonV, precV, mask)
}

func (p *prog) geoTileY(latitude, precision *value) *value {
	latV, latM := p.coerceF64(latitude)
	precV, precM := p.coerceI64(precision)
	mask := p.and(latM, precM)
	return p.ssa3(sgeotiley, latV, precV, mask)
}

func (p *prog) geoTileES(latitude, longitude, precision *value) *value {
	latV, latM := p.coerceF64(latitude)
	lonV, lonM := p.coerceF64(longitude)

	if precision.op == sliteral && isIntImmediate(precision.imm) {
		return p.ssa3imm(sgeotileesimm, latV, lonV, p.and(latM, lonM), precision.imm)
	}

	charsV, charsM := p.coerceI64(precision)
	mask := p.and(p.and(latM, lonM), charsM)
	return p.ssa4(sgeotilees, latV, lonV, charsV, mask)
}

func (p *prog) geoDistance(latitude1, longitude1, latitude2, longitude2 *value) *value {
	lat1V, lat1M := p.coerceF64(latitude1)
	lon1V, lon1M := p.coerceF64(longitude1)
	lat2V, lat2M := p.coerceF64(latitude2)
	lon2V, lon2M := p.coerceF64(longitude2)

	mask := p.and(p.and(lat1M, lon1M), p.and(lat2M, lon2M))
	return p.ssa5(sgeodistance, lat1V, lon1V, lat2V, lon2V, mask)
}

func (p *prog) lower(s *value) *value {
	return p.ssa2(slowerstr, s, p.mask(s))
}

func (p *prog) upper(s *value) *value {
	return p.ssa2(supperstr, s, p.mask(s))
}

func (p *prog) objectSize(v *value) *value {
	return p.ssa2(sobjectsize, v, p.mask(v))
}

func (p *prog) arraySize(array *value) *value {
	array = p.tolist(array)
	mask := p.mask(array)

	return p.ssa2(sarraysize, array, mask)
}

func (p *prog) arrayContains(array, item *value) *value {
	array = p.tolist(array)
	item = p.unsymbolized(item)
	mask := p.and(p.mask(array), p.mask(item))

	out := p.ssa1(snotmissing, p.ssa3(sarrayposition, array, item, mask))
	out.notMissing = mask
	return out
}

func (p *prog) arrayPosition(array, item *value) *value {
	array = p.tolist(array)
	item = p.unsymbolized(item)
	mask := p.and(p.mask(array), p.mask(item))

	return p.ssa3(sarrayposition, array, item, mask)
}

func emitNone(v *value, c *compilestate) {
	// does nothing...
}

// TODO: Maybe bcop would be better contextually here?
func dateDiffMQYImm(op ssaop) uint16 {
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
	info := &ssainfo[v.op]
	bc := info.bc
	c.emit(v, bc,
		c.slotOf(v.args[0], regS),
		c.slotOf(v.args[1], regS),
		dateDiffMQYImm(v.op),
		c.slotOf(v.args[2], regK),
	)
}

// Simple aggregate operations
func (p *prog) makeAggregateBoolOp(aggBoolOp, aggIntOp ssaop, v, filter *value, slot aggregateslot) *value {
	mem := p.initMem()

	// In general we have to coerce to BOOL, however, if the input is a boxed value we
	// will just unbox BOOL to INT64 and use INT64 aggregation instead of converting such
	// INT64 to BOOL. This saves us some instructions.
	if v.primary() == stValue {
		k := p.mask(v)
		intVal := p.ssa2(sunboxktoi64, v, k)
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
		scalar, mask := p.coerceI64(child)
		if filter != nil {
			mask = p.and(mask, filter)
		}
		mem := p.initMem()
		return p.ssa3imm(opI, mem, scalar, mask, slot), false
	}

	scalar, mask := p.coerceF64(child)
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
	v, m := p.coerceI64(child)
	if filter != nil {
		m = p.and(m, filter)
	}
	return p.ssa3imm(saggsumi, p.initMem(), v, m, slot)
}

func (p *prog) aggregateSum(child, filter *value, slot aggregateslot) (v *value, fp bool) {
	return p.makeAggregateOp(saggsumf, saggsumi, child, filter, slot)
}

func (p *prog) aggregateTDigest(child, filter *value, slot aggregateslot) *value {
	v, m := p.coerceF64(child)
	if filter != nil {
		m = p.and(m, filter)
	}
	return p.ssa3imm(sAggTDigest, p.initMem(), v, m, slot)
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
		scalar, m := p.coerceI64(v)
		if mask != nil {
			m = p.and(m, mask)
		}
		return p.ssa4imm(opI, mem, bucket, scalar, m, offset), false
	}

	scalar, m := p.coerceF64(v)
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
	scalar, m := p.coerceI64(value)
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

// writeTo writes bytecode as text to io.Writer w
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

// writeToDot writes bytecode as dot graph to io.Writer w
func (p *prog) writeToDot(w io.Writer, name string) (int64, error) {
	getResults := func(v *value) []string {
		var results []string
		rt := ssainfo[v.op].rettype
		value := rt &^ stBool
		if value != 0 {
			results = append(results, string(value.char())+strconv.Itoa(v.id))
		}
		if rt&stBool != 0 {
			results = append(results, "k"+strconv.Itoa(v.id))
		}
		return results
	}

	getArgs := func(v *value) []string {
		var args []string
		info := &ssainfo[v.op]
		for i := range v.args {
			arg := string(info.argType(i).char()) + strconv.Itoa(v.args[i].id)
			args = append(args, arg)
		}
		return args
	}

	var nn int64
	values := p.values
	nodeID := make(map[string]int)
	n, _ := io.WriteString(w, fmt.Sprintf("digraph a {\n\tnode [shape=\"rectangle\"];\n\trankdir=TD;\n\tlabelloc=\"t\";\n\tlabel=\"%v\";\n", name))
	nn += int64(n)

	for idCurr, v := range values {
		for _, id := range getResults(v) {
			nodeID[id] = idCurr
		}
		args := getArgs(v)
		n, _ := io.WriteString(w, fmt.Sprintf("\ts%v [label=\"%v\"];\n", idCurr, v.String()))
		nn += int64(n)

		for _, id := range args {
			n, _ := io.WriteString(w, fmt.Sprintf("\ts%v -> s%v [label=\"%v\"];\n", nodeID[id], idCurr, id))
			nn += int64(n)
		}
	}
	n, _ = io.WriteString(w, "}\n")
	nn += int64(n)
	return nn, nil
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

func (r regset) contains(class regclass) bool {
	return (r & (1 << class)) != 0
}

func (r *regset) add(class regclass) {
	*r |= (1 << class)
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

type compilestate struct {
	lr    lranges  // variable live ranges
	stack stackmap // stack map

	trees  []*radixTree64
	asm    assembler
	dict   []string
	litbuf []byte // output datum literals

	symtab *ion.Symtab // current symtab
	buf    ion.Buffer  // temporary buffer
}

func (c *compilestate) emit(v *value, op bcop, args ...any) {
	// de-allocate argument slots
	c.final(v)

	clobbers := v.ret().vregs()
	// allocate return value slots:
	ret := opinfo[op].out
	final := make([]any, len(ret), len(args)+len(ret))
	for i := len(ret) - 1; i >= 0; i-- {
		switch ret[i] {
		case bcK:
			if !clobbers.contains(regK) {
				panic(fmt.Sprintf("error emitting %s: bcK doesn't correspond to a clobbered regK", opinfo[op].text))
			}
			final[i] = c.slotOf(v, regK)
		case bcS:
			if !clobbers.contains(regS) {
				panic(fmt.Sprintf("error emitting %s: bcS doesn't correspond to a clobbered regS", opinfo[op].text))
			}
			final[i] = c.slotOf(v, regS)
		case bcL:
			if !clobbers.contains(regL) {
				panic(fmt.Sprintf("error emitting %s: bcL doesn't correspond to a clobbered regL", opinfo[op].text))
			}
			final[i] = c.slotOf(v, regL)
		case bcH:
			if !clobbers.contains(regH) {
				panic(fmt.Sprintf("error emitting %s: bcH doesn't correspond to a clobbered regH", opinfo[op].text))
			}
			slot := c.slotOf(v, regH)
			if v.imm == nil {
				v.imm = int(slot)
			}
			final[i] = slot
		case bcV:
			if !clobbers.contains(regV) {
				panic(fmt.Sprintf("error emitting %s: bcV doesn't correspond to a clobbered regV", opinfo[op].text))
			}
			final[i] = c.slotOf(v, regV)
		case bcB:
			if !clobbers.contains(regB) {
				panic(fmt.Sprintf("error emitting %s: bcB doesn't correspond to a clobbered regB", opinfo[op].text))
			}
			final[i] = c.slotOf(v, regB)
		default:
			panic("cannot handle " + ret[i].String())
		}
	}
	all := append(final, args...)
	if len(opinfo[op].va) == 0 {
		c.asm.emitOpcode(op, all...)
	} else {
		c.asm.emitOpcodeVA(op, all)
	}
}

func (r regclass) String() string {
	switch r {
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

func ionType(imm any) ion.Type {
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
		return
	}
	info := &ssainfo[v.op]
	for i := range v.args {
		arg := v.args[i]
		argType := info.argType(i)
		rng := c.lr.vrange // normal range info
		var rc regclass
		switch argType {
		case stMem, stList | stFloat | stInt | stString | stTime:
			continue // not stack-allocated
		case stBool:
			rng = c.lr.krange
			rc = regK
		case stValue:
			rc = regV
		case stBucket:
			rc = regL
		case stList, stFloat, stInt, stString, stTime:
			rc = regS
		case stBase:
			rc = regB
		case stHash:
			rc = regH
		default:
			panic("unexpected return type " + argType.String())
		}
		if rng[arg.id] < v.id {
			panic("arg not live up to use?")
		}
		// anything live only up to here is now dead
		if rng[arg.id] == v.id && c.stack.hasFreeableSlot(rc, arg.id) {
			c.stack.freeValue(rc, arg.id)
		}
	}
}

// Returns a stack slot id of the given value. It would
// lazily create the slot if it was not created previously.
func (c *compilestate) slotOf(v *value, rc regclass) stackslot {
	slot := c.stack.slotOf(rc, v.id)
	if slot != invalidstackslot {
		return slot
	}
	return c.stack.allocValue(rc, v.id)
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
		panic(fmt.Sprintf("dictionary reference (offset=%d) exceeds 65535 limit", n))
	}
	return uint16(n)
}

type rawDatum []byte

func encodeLitRef(off int, buf []byte) litref {
	return litref{
		offset: uint32(off),
		length: uint32(len(buf)),
		tlv:    buf[0],
		hLen:   uint8(ion.HeaderSizeOf(buf)),
	}
}

func (c *compilestate) storeLitRef(imm any) litref {
	b := &c.buf
	b.Reset()
	switch t := imm.(type) {
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
	return encodeLitRef(off, b.Bytes())
}

func emithashlookup(v *value, c *compilestate) {
	h := v.args[0]
	k := v.args[1]

	tSlot := len(c.trees)
	tree := v.imm.(*radixTree64)
	c.trees = append(c.trees, tree)
	c.emit(v, ssainfo[v.op].bc,
		c.slotOf(h, regH),
		uint64(tSlot),
		c.slotOf(k, regK),
	)
}

func emithashmember(v *value, c *compilestate) {
	h := v.args[0]
	k := v.args[1]

	tSlot := uint16(len(c.trees))
	c.trees = append(c.trees, v.imm.(*radixTree64))

	c.emit(v, ssainfo[v.op].bc,
		c.slotOf(h, regH),
		uint64(tSlot),
		c.slotOf(k, regK),
	)
}

func emitslice(v *value, c *compilestate) {
	info := &ssainfo[v.op]
	bc := info.bc

	var t ion.Type
	switch v.op {
	case stostr:
		t = ion.StringType
	case stolist:
		t = ion.ListType
	case stoblob:
		t = ion.BlobType
	default:
		panic("unrecognized op for emitslice")
	}

	c.emit(v, bc,
		c.slotOf(v.args[0], regV),
		uint64(t),
		c.slotOf(v.args[1], regK),
	)
}

// emit constant comparison
// reads value & immediate & mask, writes to mask
func emitconstcmp(v *value, c *compilestate) {
	imm := v.imm
	val := v.args[0]
	msk := v.args[1]

	if b, ok := imm.(bool); ok {
		// we have built-in ops for these!
		if b {
			c.emit(v, opistruev,
				c.slotOf(val, regV),
				c.slotOf(msk, regK),
			)
		} else {
			c.emit(v, opisfalsev,
				c.slotOf(val, regV),
				c.slotOf(msk, regK),
			)
		}
		return
	}

	raw, ok := imm.(rawDatum)
	if !ok {
		// if we get a datum object,
		// then encode it verbatim;
		// otherwise try to convert
		// to a datum...
		d, ok := imm.(ion.Datum)
		if !ok {
			switch imm := imm.(type) {
			case float64:
				d = ion.Float(imm)
			case float32:
				d = ion.Float(float64(imm)) // TODO: maybe don't convert here...
			case int64:
				d = ion.Int(imm)
			case int:
				d = ion.Int(int64(imm))
			case uint64:
				d = ion.Uint(imm)
			case string:
				d = ion.String(imm)
			case []byte:
				d = ion.Blob(imm)
			case date.Time:
				d = ion.Timestamp(imm)
			default:
				panic("type not supported for literal comparison")
			}
		}

		c.buf.Reset()
		d.Encode(&c.buf, c.symtab)
		raw = c.buf.Bytes()
	}

	off := len(c.litbuf)
	c.litbuf = append(c.litbuf, raw...)

	c.emit(v, opcmpeqvimm,
		c.slotOf(val, regV),
		encodeLitRef(off, raw),
		c.slotOf(msk, regK),
	)
}

func emitstorev(v *value, c *compilestate) {
	_ = v.args[0] // mem
	arg := v.args[1]
	mask := v.args[2]
	slot := v.imm.(int)

	if mask.op == skfalse {
		// don't care what is in the V register;
		// we are just zeroing the memory
		c.asm.emitOpcode(opzerov, stackslot(slot))
		c.final(v) // deallocate argument slots
		return
	}

	c.asm.emitOpcode(opmovv, stackslot(slot), c.slotOf(arg, regV), c.slotOf(mask, regK))
	c.final(v) // deallocate argument slots
}

func emitauto(v *value, c *compilestate) {
	info := &ssainfo[v.op]
	bc := info.bc
	bcInfo := opinfo[bc]
	ssaArgCount := len(v.args)

	if bc == 0 {
		panic("cannot emit instruction that doesn't have associated bcop")
	}

	if ssaArgCount != len(info.argtypes) {
		panic(fmt.Sprintf("error emitting %v: the instruction requires %d arguments, not %d", info.bc, len(v.args), len(info.argtypes)))
	}

	args := make([]any, len(bcInfo.in))

	ssaImmDone := false
	ssaArgBegin := int(0)
	ssaArgIndex := ssaArgCount

	if ssaArgCount > 0 && (info.argtypes[0]&stMem) != 0 {
		// skip stMem argument (if first) - it's just for ordering, it has no effect here
		ssaArgBegin++
	}

	for i := len(bcInfo.in) - 1; i >= 0; i-- {
		switch bcInfo.in[i] {
		case bcK:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadK) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & stBool) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadK) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}
			args[i] = c.slotOf(v.args[ssaArgIndex], regK)

		case bcS:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadS) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & (stFloat | stInt | stString | stList | stTime)) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadS) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}
			args[i] = c.slotOf(v.args[ssaArgIndex], regS)

		case bcV:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadV) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & stValue) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadV) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}
			args[i] = c.slotOf(v.args[ssaArgIndex], regV)

		case bcB:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadB) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & stBase) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadB) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}
			args[i] = c.slotOf(v.args[ssaArgIndex], regB)

		case bcL:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadB) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & stBucket) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadB) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}
			args[i] = c.slotOf(v.args[ssaArgIndex], regL)
		case bcH:
			ssaArgIndex--
			if ssaArgIndex < ssaArgBegin {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadH) doesn't have a corresponding SSA argument", bcInfo.text, i))
			}
			argType := info.argtypes[ssaArgIndex]
			if (argType & stHash) == 0 {
				panic(fmt.Sprintf("error emitting %s: bytecode argument %d (bcReadH) is not compatible with SSA arg type %s", bcInfo.text, i, argType.String()))
			}

			if v.imm != nil {
				panic(fmt.Sprintf("error emitting %s: cannot handle hash input when value.imm is already non-null", bcInfo.text))
			}

			// the immediate is the H register number of the hash argument
			slot := c.slotOf(v.args[ssaArgIndex], regH)
			v.imm = int(slot)
			args[i] = stackslot(slot)

		case bcDictSlot:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %v: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			dictSlot := c.dictimm(v.imm.(string))
			args[i] = uint64(dictSlot)
			ssaImmDone = true

		case bcAuxSlot:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %v: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			slot := v.imm.(int)
			args[i] = uint64(slot)
			ssaImmDone = true

		case bcAggSlot:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %s: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			aggSlot := v.imm.(aggregateslot)
			args[i] = aggSlot
			ssaImmDone = true

		case bcSymbolID:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %s: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			args[i] = encodeSymbolID(v.imm.(ion.Symbol))
			ssaImmDone = true

		case bcImmI8, bcImmI16, bcImmI32, bcImmI64, bcImmU8, bcImmU16, bcImmU32, bcImmU64, bcImmF64:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %s: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			args[i] = v.imm
			ssaImmDone = true

		case bcLitRef:
			if ssaImmDone {
				panic(fmt.Sprintf("error emitting %s: only one immediate can be encoded, found second immediate at #%d", bcInfo.text, i))
			}
			args[i] = c.storeLitRef(v.imm)
			ssaImmDone = true
		default:
			panic(fmt.Sprintf("bad bc arg type %s", bcInfo.in[i]))
		}
	}
	c.emit(v, bc, args...)
}

func emitBoolConv(v *value, c *compilestate) {
	info := &ssainfo[v.op]
	c.emit(v, info.bc, c.slotOf(v.args[0], regK))
}

func emitConcatStr(v *value, c *compilestate) {
	if len(v.args)&1 != 0 {
		panic(fmt.Sprintf("The number of arguments to emitConcatStr() must be even, not %d", len(v.args)))
	}

	args := make([]any, len(v.args))
	for i := 0; i < len(args); i += 2 {
		args[i+0] = c.slotOf(v.args[i+0], regS)
		args[i+1] = c.slotOf(v.args[i+1], regK)
	}

	info := &ssainfo[v.op]
	c.emit(v, info.bc, args...)
}

func emitMakeList(v *value, c *compilestate) {
	args := make([]any, 0, 1+len(v.args)-1)
	args = append(args, c.slotOf(v.args[0], regK))
	for i := 1; i < len(v.args); i += 2 {
		args = append(args, c.slotOf(v.args[i], regV), c.slotOf(v.args[i+1], regK))
	}

	info := &ssainfo[v.op]
	c.emit(v, info.bc, args...)
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

	args := make([]any, 0, 1+len(v.args)-1)
	args = append(args, c.slotOf(v.args[0], regK))

	for _, orderedSymbol := range orderedSymbols {
		i := int(orderedSymbol & 0xFFFFFFFF)
		sym := ion.Symbol(orderedSymbol >> 32)

		val := v.args[i+1]
		mask := v.args[i+2]
		args = append(args, encodeSymbolID(sym), c.slotOf(val, regV), c.slotOf(mask, regK))
	}

	info := &ssainfo[v.op]
	op := info.bc
	c.emit(v, op, args...)
}

func emitaggapproxcount(v *value, c *compilestate) {
	op := ssainfo[v.op].bc
	hash := v.args[0]
	mask := v.args[1]

	if hash.op == skfalse {
		v.setfalse()
		return
	}

	imm := v.imm.(uint64)
	aggSlot := aggregateslot(imm >> 8)
	precision := imm & 0xFF

	c.emit(v, op,
		aggSlot,
		c.slotOf(hash, regH),
		precision,
		c.slotOf(mask, regK),
	)
}

func emitaggapproxcountmerge(v *value, c *compilestate) {
	op := ssainfo[v.op].bc
	blob := v.args[0]
	mask := v.args[1]

	imm := v.imm.(uint64)
	aggSlot := aggregateslot(imm >> 8)
	precision := imm & 0xFF

	c.emit(v, op,
		aggSlot,
		c.slotOf(blob, regS),
		precision,
		c.slotOf(mask, regK),
	)
}

func emitaggslotapproxcount(v *value, c *compilestate) {
	bucket := v.args[1]
	hash := v.args[2]
	mask := v.args[3]

	if hash.op == skfalse {
		v.setfalse()
		return
	}

	imm := v.imm.(uint64)
	aggSlot := aggregateslot(imm >> 8)
	precision := imm & 0xFF

	c.emit(v, ssainfo[v.op].bc,
		aggSlot,
		c.slotOf(bucket, regL),
		c.slotOf(hash, regH),
		precision,
		c.slotOf(mask, regK),
	)
}

func emitaggslotapproxcountmerge(v *value, c *compilestate) {
	bucket := v.args[1]
	blob := v.args[2]
	mask := v.args[3]

	imm := v.imm.(uint64)
	aggSlot := aggregateslot(imm >> 8)
	precision := imm & 0xFF

	c.emit(v, ssainfo[v.op].bc,
		aggSlot,
		c.slotOf(bucket, regL),
		c.slotOf(blob, regS),
		precision,
		c.slotOf(mask, regK),
	)
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
}

// reserve stack slots for any stores that
// are explicitly performed
func (p *prog) reserveslots(c *compilestate) {
	for i := range p.reserved {
		c.stack.reserveSlot(regV, p.reserved[i])
	}
}

// eliminateOutputMoves eliminates moves to reserved stack slots in cases in
// which a value could be stored to such slots directly by the operation that
// creates the final value. Note that it's not always possible and only SSA
// ops that have safeValueMask flag enabled can store the result directly to
// a reserved stack slot.
func (p *prog) eliminateOutputMoves(c *compilestate) {
	out := 0
	for _, v := range p.values {
		if v.op == sstorev {
			src := v.args[1]
			msk := v.args[2]
			if p.mask(src) == msk && ssainfo[src.op].safeValueMask {
				c.stack.assignPermanentSlot(regV, src.id, stackslot(v.imm.(int)))
				continue
			}
		}
		p.values[out] = v
		out++
	}
	p.values = p.values[:out]
}

func (p *prog) compile(dst *bytecode, st *symtab, callerName string) error {
	c := compilestate{symtab: &st.Symtab}

	if err := p.compileinto(&c); err != nil {
		return err
	}

	if flags := TraceFlags(tracing.Load()); flags != 0 {
		if flags&TraceSSADot != 0 {
			trace(func(w io.Writer) {
				p.writeToDot(w, callerName)
			})
		}
		if flags&TraceSSAText != 0 {
			trace(func(w io.Writer) {
				io.WriteString(w, callerName+":\n")
				p.writeTo(w)
				io.WriteString(w, "----------------\n")
			})
		}
	}

	dst.vstacksize = c.stack.stackSize()
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
	err := dst.finalize()
	if err != nil {
		return err
	}
	if enabled(TraceBytecodeText) {
		trace(func(w io.Writer) {
			io.WriteString(w, dst.String())
		})
	}
	return nil
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
	p.reserveslots(c)
	p.eliminateOutputMoves(c)

	for _, v := range p.values {
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
func (p *prog) cloneSymbolize(st *symtab, dst *prog, aux *auxbindings) error {
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
func recompile(st *symtab, src, dst *prog, final *bytecode, aux *auxbindings, callerName string) error {
	final.symtab = st.symrefs
	if !dst.isStale(st, aux) {
		// the scratch buffer may be invalid,
		// so ensure that it is populated correctly:
		final.restoreScratch(st)
		return nil
	}
	err := src.cloneSymbolize(st, dst, aux)
	if err != nil {
		return err
	}
	return dst.compile(final, st, "recompile "+callerName)
}

// IsStale returns whether the symbolized program
// (see prog.Symbolize) is stale with respect to
// the provided symbol table.
func (p *prog) isStale(st *symtab, aux *auxbindings) bool {
	if !p.symbolized || p.literals {
		return true
	}
	for i := range p.resolvedAux {
		if p.resolvedAux[i].id >= len(aux.bound) ||
			aux.bound[p.resolvedAux[i].id] != p.resolvedAux[i].val {
			return true
		}
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

func (p *prog) recordAux(str string, id int) {
	for i := range p.resolvedAux {
		if p.resolvedAux[i].id == id {
			return
		}
	}
	p.resolvedAux = append(p.resolvedAux, symid{id: id, val: str})
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

func (p *prog) symbolize(st *symtab, aux *auxbindings) error {
	defer st.build() // make sure symtab is up-to-date
	p.resolved = p.resolved[:0]
	p.resolvedAux = p.resolvedAux[:0]
	for i := range p.values {
		v := p.values[i]
		switch v.op {
		case shashmember:
			v.imm = p.mktree(st, v.imm)
		case shashlookup:
			v.imm = p.mkhash(st, v.imm)
		case sdot:
			str := v.imm.(string)

			// for top-level "dot" operations,
			// check the auxilliary values first:
			if v.args[1].op == sinit {
				if id, ok := aux.id(str); ok {
					v.op = sauxval
					v.args = v.args[:0]
					v.imm = id
					p.recordAux(str, id)
					continue
				}
			}

			sym, ok := st.Symbolize(str)
			if !ok {
				// if a symbol isn't present, the
				// search will always fail (and this
				// will cause the optimizer to eliminate
				// any code that depends on this value)
				v.setfalse()
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
		default:
			if d, ok := v.imm.(ion.Datum); ok {
				if !isHashConst(d) {
					p.literals = true
				}

				var bag ion.Bag
				var tmp ion.Buffer

				bag.AddDatum(d)
				bag.Encode(&tmp, &st.Symtab)
				v.imm = rawDatum(tmp.Bytes())
			}
		}
	}
	p.symbolized = true
	return nil
}

func encodeNeedle(needle stringext.Needle, op ssaop) string {
	return encodeNeedleOp(needle, ssainfo[op].bc)
}

func encodeNeedleOp(needle stringext.Needle, op bcop) string {
	switch op {
	case opCmpStrEqCs:
		return stringext.EncodeEqualStringCS(needle)
	case opCmpStrEqCi:
		return stringext.EncodeEqualStringCI(needle)
	case opCmpStrEqUTF8Ci:
		return stringext.EncodeEqualStringUTF8CI(needle)
	case opContainsPrefixCs:
		return stringext.EncodeContainsPrefixCS(needle)
	case opContainsPrefixCi:
		return stringext.EncodeContainsPrefixCI(needle)
	case opContainsPrefixUTF8Ci:
		return stringext.EncodeContainsPrefixUTF8CI(needle)
	case opContainsSuffixCs:
		return stringext.EncodeContainsSuffixCS(needle)
	case opContainsSuffixCi:
		return stringext.EncodeContainsSuffixCI(needle)
	case opContainsSuffixUTF8Ci:
		return stringext.EncodeContainsSuffixUTF8CI(needle)
	case opContainsSubstrCs:
		return stringext.EncodeContainsSubstrCS(needle)
	case opContainsSubstrCi:
		return stringext.EncodeContainsSubstrCI(needle)
	case opContainsSubstrUTF8Ci:
		return stringext.EncodeContainsSubstrUTF8CI(needle)
	case opCmpStrFuzzyA3, opHasSubstrFuzzyA3:
		return stringext.EncodeFuzzyNeedleASCII(needle)
	case opCmpStrFuzzyUnicodeA3, opHasSubstrFuzzyUnicodeA3:
		return stringext.EncodeFuzzyNeedleUnicode(needle)
	default:
		panic("unsupported op")
	}
}

func encodePattern(pattern *stringext.Pattern, op ssaop) string {
	return encodePatternOp(pattern, ssainfo[op].bc)
}

func encodePatternOp(pattern *stringext.Pattern, op bcop) string {
	switch op {
	case opEqPatternCs, opContainsPatternCs:
		return stringext.EncodeContainsPatternCS(pattern)
	case opEqPatternCi, opContainsPatternCi:
		return stringext.EncodeContainsPatternCI(pattern)
	case opEqPatternUTF8Ci, opContainsPatternUTF8Ci:
		return stringext.EncodeContainsPatternUTF8CI(pattern)
	default:
		panic("unsupported op")
	}
}
