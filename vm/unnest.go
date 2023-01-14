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
	"io"

	"github.com/SnellerInc/sneller/expr"
	"golang.org/x/exp/slices"
)

// Unnest un-nests an array and produces rows
// that have their contents cross-joined with
// the array contents as an auxilliary binding
type Unnest struct {
	dst    QuerySink
	field  expr.Node
	result string
}

// NewUnnest creates an Unnest QuerySink that cross-joins
// the given field (which should be an array) into the
// input stream as an auxilliary binding with the given name.
func NewUnnest(dst QuerySink, field expr.Node, result string) *Unnest {
	return &Unnest{
		dst:    dst,
		field:  field,
		result: result,
	}
}

func (u *Unnest) Open() (io.WriteCloser, error) {
	dst, err := u.dst.Open()
	if err != nil {
		return nil, err
	}
	unnest := &unnesting{parent: u, dstrc: asRowConsumer(dst)}
	return splitter(unnest), nil
}

func (u *Unnest) Close() error {
	return u.dst.Close()
}

type unnesting struct {
	parent *Unnest
	splat  bytecode
	perms  []int32
	dstrc  rowConsumer
	params rowParams
	auxnum int

	// cached buffers for inner and outer refs
	inner, outer []vmref
}

func (u *unnesting) next() rowConsumer { return u.dstrc }

func (u *unnesting) EndSegment() {
	u.splat.dropScratch()
}

func (u *unnesting) symbolize(st *symtab, aux *auxbindings) error {
	var p prog
	var err error
	p.begin()
	// produce a program that sticks the slice
	// to be splat-ed into Z2:Z3
	v, err := compile(&p, u.parent.field)
	if err != nil {
		return err
	}
	list := p.ssa2(stolist, v, p.mask(v))
	p.returnScalar(p.initMem(), list, p.mask(list))
	p.symbolize(st, aux)
	err = p.compile(&u.splat, st)
	if err != nil {
		return err
	}
	u.auxnum = aux.push(u.parent.result)
	return u.dstrc.symbolize(st, aux)
}

//go:noescape
func evalsplat(bc *bytecode, indelims, outdelims []vmref, perm []int32) (int, int)

func shrink[T any](s []T, c int) []T {
	if cap(s) < c {
		s = make([]T, c)
	} else {
		s = s[:c]
	}
	return s
}

// sanitizeAux returns aux[:size] such that the lanes
// in aux[size:] up to the next multiple of bcLaneCount
// are a) valid memory, and b) zeroed
func sanitizeAux(aux []vmref, size int) []vmref {
	// always pad to lane multiple with zeros
	wantcap := (size + bcLaneCountMask) &^ bcLaneCountMask
	if cap(aux) < wantcap {
		ret := make([]vmref, size, wantcap)
		copy(ret, aux)
		return ret
	}
	aux = aux[:size]
	tail := aux[size:wantcap]
	for i := range tail {
		tail[i] = vmref{}
	}
	return aux
}

func (u *unnesting) splatParams(in *rowParams, consumed int, perm []int32, inner []vmref) *rowParams {
	if len(perm) != len(inner) {
		panic("???")
	}
	if len(in.auxbound) != u.auxnum {
		panic("unexpected auxilliary inputs")
	}
	// splat existing row-oriented bindings
	u.params.auxbound = shrink(u.params.auxbound, u.auxnum+1)
	for i := range in.auxbound {
		u.params.auxbound[i] = sanitizeAux(u.params.auxbound[i], len(inner))
		for j, n := range perm {
			u.params.auxbound[i][j] = in.auxbound[i][consumed+int(n)]
		}
	}
	// add new bindings
	u.params.auxbound[u.auxnum] = inner
	return &u.params
}

func (u *unnesting) writeRows(delims []vmref, rp *rowParams) error {
	if len(delims) == 0 {
		return nil
	}
	if cap(u.outer) == 0 {
		u.outer = make([]vmref, 1024)
		u.perms = make([]int32, 1024)
	}
	if u.splat.compiled == nil {
		panic("WriteRows() called before Symbolize()")
	}

	u.splat.prepare(rp)
	consumed := 0
	for consumed < len(delims) {
		// provide as much space as possible:
		u.outer = u.outer[:cap(u.outer)]
		u.perms = u.perms[:cap(u.perms)]
		in, out := evalsplat(&u.splat, delims[consumed:], u.outer, u.perms)
		if u.splat.err != 0 {
			return bytecodeerror("unnest", &u.splat)
		}
		// adjust this to take into account the fact
		// that we may not have actually handled all the lanes
		u.splat.auxpos = consumed + in
		if in == 0 {
			// there wasn't enough room to splat a single
			// lane's array members! we need more space,
			// so double the slice sizes here
			u.outer = slices.Grow(u.outer, len(u.outer))
			u.perms = slices.Grow(u.perms, len(u.perms))
			continue
		}
		if out == 0 {
			consumed += in
			continue
		}
		// incorporate inner and outer values
		// in two slices adjacent to one another:
		outer := u.outer[:out]
		u.inner = shrink(u.inner, len(outer))
		innerperm := u.perms[:out]
		// permute delimiters into unrolled delimiters:
		for i, n := range innerperm {
			u.inner[i] = delims[consumed+int(n)]
		}
		err := u.dstrc.writeRows(u.inner, u.splatParams(rp, consumed, innerperm, outer))
		if err != nil {
			return err
		}
		consumed += in
	}
	return nil
}

func (u *unnesting) Close() error {
	u.splat.reset()
	return u.dstrc.Close()
}
