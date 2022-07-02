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
	"sort"

	"github.com/SnellerInc/sneller/expr"
)

// UnnestProjection unnests rows on a field
// and projects the outer and inner row elements,
// optionally with a filter that restricts the
// inner rows that are cross-joined with the outer rows.
type UnnestProjection struct {
	dst          QuerySink
	field        *expr.Path
	outer, inner Selection
	filter       expr.Node
}

// NewUnnest creates a new UnnestProjection.
// The provided 'field' expression is the slice
// to be unnested, and the outer and inner selections
// are the fields from outside and inside the slice, respectively,
// that need to be projected.
// The filter expression is applied to slice fields
// in order to limit the number of output cross-joined rows.
func NewUnnest(dst QuerySink, field *expr.Path, outer, inner Selection, filter expr.Node) *UnnestProjection {
	return &UnnestProjection{
		dst:    dst,
		field:  field,
		outer:  outer.outputSorted(),
		inner:  inner.outputSorted(),
		filter: filter,
	}
}

func (u *UnnestProjection) Open() (io.WriteCloser, error) {
	dst, err := u.dst.Open()
	if err != nil {
		return nil, err
	}
	rc, _ := dst.(rowConsumer)
	unnest := &unnesting{parent: u, out: dst, dstrc: rc}
	unnest.aw.out = dst
	return splitter(unnest), nil
}

func (u *UnnestProjection) Close() error {
	return u.dst.Close()
}

type unnesting struct {
	parent  *UnnestProjection
	outsel  []syminfo
	outord  []int
	outerbc bytecode
	innerbc bytecode
	aw      alignedWriter
	delims  []vmref
	perms   []int32
	dstrc   rowConsumer
	out     io.WriteCloser
}

type uByID unnesting

func (u *uByID) Len() int {
	return len(u.outord)
}

func (u *uByID) Swap(i, j int) {
	u.outord[i], u.outord[j] = u.outord[j], u.outord[i]
	u.outsel[i], u.outsel[j] = u.outsel[j], u.outsel[i]
}

func (u *uByID) Less(i, j int) bool {
	return u.outsel[i].value < u.outsel[j].value
}

func (u *unnesting) symbolize(st *symtab) error {
	if len(u.outsel) != len(u.parent.outer)+len(u.parent.inner) {
		u.outsel = make([]syminfo, len(u.parent.outer)+len(u.parent.inner))
	}
	for i := range u.parent.outer {
		bind := u.parent.outer[i].Result()
		sym := st.Intern(bind)
		u.outsel[i].value = sym
		u.outsel[i].encoded, u.outsel[i].mask, u.outsel[i].size = encoded(sym)
	}
	for i := range u.parent.inner {
		j := len(u.parent.outer) + i
		bind := u.parent.inner[i].Result()
		sym := st.Intern(bind)
		u.outsel[j].value = sym
		u.outsel[j].encoded, u.outsel[j].mask, u.outsel[j].size = encoded(sym)
	}
	if len(u.outord) != len(u.outsel) {
		u.outord = make([]int, len(u.outsel))
	}
	for i := range u.outord {
		u.outord[i] = i
	}
	sort.Stable((*uByID)(u))
	var p prog
	var err error
	p.Begin()
	mem0 := p.InitMem()
	mem := make([]*value, len(u.parent.outer))
	for i := range u.parent.outer {
		_, ok := u.parent.outer[i].Expr.(*expr.Path)
		if !ok {
			return fmt.Errorf("cannot handle non-path-expression %q", u.parent.outer[i].Expr)
		}
		mem[i], err = p.compileStore(mem0, u.parent.outer[i].Expr, stackSlotFromIndex(regV, i), false)
		if err != nil {
			panic(err)
		}
	}
	// the list slice storage is past the end
	// of the slots used for projection
	listv := p.walk(u.parent.field)
	list := p.ssa2(stolist, listv, listv)
	p.Return(p.msk(p.MergeMem(mem...), list, list))
	p.symbolize(st)
	err = p.compile(&u.outerbc)
	if err != nil {
		return err
	}

	// second sub-program
	p.Begin()
	mem0 = p.InitMem()
	// reserve input slots
	// so that the register allocator
	// does not clobber them
	for i := range u.outord[:len(u.parent.outer)] {
		p.ReserveSlot(stackSlotFromIndex(regV, u.outord[i]))
	}
	mem = make([]*value, len(u.parent.inner))
	for i := range u.parent.inner {
		j := len(u.parent.outer) + i
		_, ok := u.parent.inner[i].Expr.(*expr.Path)
		if !ok {
			return fmt.Errorf("cannot handle non-path expression %q", u.parent.inner[i].Expr)
		}
		mem[i], err = p.compileStore(mem0, u.parent.inner[i].Expr, stackSlotFromIndex(regV, u.outord[j]), false)
		if err != nil {
			panic(err)
		}
	}
	outk := p.ValidLanes()
	if u.parent.filter != nil {
		outk, err = compile(&p, u.parent.filter)
		if err != nil {
			return fmt.Errorf("Unnest: compiling inner filter: %w", err)
		}
	}
	p.Return(p.mk(p.MergeMem(mem...), outk))
	p.symbolize(st)

	// generate permutation at the beginning
	// of the program so that the values in the stack slots
	// appear as if they were bound correctly in advance
	//
	// FIXME: just do this in the SSA instead
	maskSlot := stackSlotFromIndex(regV, len(u.parent.outer))
	u.innerbc.outer = &u.outerbc
	u.innerbc.compiled = append(u.innerbc.compiled[:0],
		// save.k [0]
		byte(opsavek&0xFF), byte(opsavek>>8), byte(maskSlot), byte(maskSlot>>8),
		// tuple (parse structure in Z30:Z31; set Z0:Z1)
		byte(optuple&0xFF), byte(optuple>>8),
	)
	for i := range u.parent.outer {
		outSlot := stackSlotFromIndex(regV, u.outord[i])
		u.innerbc.compiled = append(u.innerbc.compiled,
			// load upvalue [i]
			byte(oploadpermzerov), byte(oploadpermzerov>>8), byte(i), byte(i>>8),
			// store locally in slot outord[i]
			byte(opsavezerov), byte(opsavezerov>>8), byte(outSlot), byte(outSlot>>8),
		)
	}
	u.innerbc.compiled = append(u.innerbc.compiled, byte(oploadk), byte(oploadk>>8), byte(maskSlot), byte(maskSlot>>8))
	err = p.appendcode(&u.innerbc)
	if err != nil {
		return err
	}
	u.innerbc.ensureVStackSize(len(u.outsel)*int(vRegSize) + 8)
	u.innerbc.allocStacks()
	if u.dstrc != nil {
		err := u.dstrc.symbolize(st)
		if err != nil {
			return err
		}
	}

	if u.aw.buf == nil {
		u.aw.init(u.out, nil, defaultAlign)
	}
	return u.aw.setpre(st)
}

//go:noescape
func evalsplat(bc *bytecode, indelims, outdelims []vmref, perm []int32) (int, int)

//go:noescape
func evalunnest(bc *bytecode, delims []vmref, perm []int32, dst []byte, symbols []syminfo) (int, int)

//go:noescape
func compress(delims []vmref) int

func (u *unnesting) writeRows(delims []vmref) error {
	if len(delims) == 0 {
		return nil
	}
	if u.aw.space() < (4 + 7) {
		_, err := u.aw.flush()
		if err != nil {
			return err
		}
	}
	if len(u.delims) == 0 {
		u.delims = make([]vmref, 1023)
		u.perms = make([]int32, 1023)
	}
	if u.outerbc.compiled == nil {
		return fmt.Errorf("unnesting.writeRows() before symbolize()")
	}

	for len(delims) > 0 {
		shouldgrow := false
		in, out := evalsplat(&u.outerbc, delims, u.delims, u.perms)
		if u.outerbc.err != 0 {
			return fmt.Errorf("unnest: splatting arrays: %w", u.outerbc.err)
		}
		if in == 0 {
			// there wasn't enough room to splat a single
			// lane's array members! we need more space
			u.delims = make([]vmref, 2*len(u.delims))
			u.perms = make([]int32, 2*len(u.perms))
			continue
		}
		if in < 16 && in < len(delims) {
			shouldgrow = true
		}
		delims = delims[in:]

		inner := u.delims[:out] // absolute addresses
		innerperm := u.perms[:out]
		for len(inner) > 0 {
			wrote, consumed := evalunnest(&u.innerbc, inner, innerperm, u.aw.buf[u.aw.off:], u.outsel)
			if u.innerbc.err != 0 {
				return fmt.Errorf("unnest inner bytecode: %w", u.innerbc.err)
			}
			if wrote > u.aw.space() {
				panic("memory corruption")
			}
			if u.dstrc != nil {
				subrows := inner[:consumed]
				if u.parent.filter != nil {
					subrows = subrows[:compress(subrows)]
				}
				err := u.dstrc.writeRows(subrows)
				if err != nil {
					return err
				}
			} else {
				if consumed != 0 {
					u.aw.off += wrote
				}
				if consumed < len(inner) {
					_, err := u.aw.flush()
					if err != nil {
						return err
					}
				}
			}
			inner = inner[consumed:]
			innerperm = innerperm[consumed:]
		}
		if shouldgrow {
			// there wasn't enough space to splat sixteen
			// lanes worth of data at once, so lets allocate
			// more space for the next go-around to improve
			// lane utilization
			u.delims = make([]vmref, 2*len(u.delims))
			u.perms = make([]int32, 2*len(u.perms))
		}
	}
	return nil
}

func (u *unnesting) Close() error {
	u.outerbc.reset()
	u.innerbc.reset()
	return u.aw.Close()
}
