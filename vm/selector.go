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
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

// Selection represents a set of
// columns with optional re-naming
//
// i.e. 'x, y, z' or 'x AS a, y AS b, z AS c'
type Selection []expr.Binding

func (s Selection) String() string {
	sub := make([]string, len(s))
	for i := range s {
		sub[i] = expr.ToString(&s[i])
	}
	return strings.Join(sub, ", ")
}

// short-hand for testing, etc.
func selection(spec string) Selection {
	if spec == "" {
		return Selection{}
	}
	bind, err := expr.ParseBindings(spec)
	if err != nil {
		panic(err)
	}
	return bind
}

type syminfo struct {
	value         ion.Symbol
	encoded, mask uint32
	size          int8
}

//go:noescape
func evalfindbc(w *bytecode, delims []vmref, stride int)

func evalfind(w *bytecode, delims []vmref, stride int) error {
	evalfindbc(w, delims, stride*vRegSize)
	if w.err != 0 {
		return w.err
	}

	return nil
}

type Projection struct {
	dst QuerySink
	sel Selection // selection w/ renaming

	// constexpr, if non-nil, indicates
	// that this projection is actually
	// a constant structure
	constexpr *ion.Struct
}

func (s Selection) toConst() (ion.Struct, bool) {
	var fields []ion.Field
	for i := range s {
		c, ok := s[i].Expr.(expr.Constant)
		if !ok {
			return ion.Struct{}, false
		}
		fields = append(fields, ion.Field{
			Label: s[i].Result(),
			Datum: c.Datum(),
		})
	}
	return ion.NewStruct(nil, fields), true
}

// NewProjection implements simple column projection from
// one set of values to a subset of those values,
// possibly re-named.
func NewProjection(sel Selection, dst QuerySink) *Projection {
	p := &Projection{
		dst: dst,
		sel: sel,
	}
	constexpr, ok := sel.toConst()
	if ok {
		p.constexpr = &constexpr
	}
	return p
}

// goroutine-local component of Select(...)
type projector struct {
	parent *Projection
	prog   prog
	bc     bytecode
	aw     alignedWriter
	dst    io.WriteCloser
	outsel []syminfo   // output symbol IDs (sorted)
	inslot []int       // parent.sel[p.inslot[i]] = outsel[i]
	params rowParams   // always starts empty
	aux    auxbindings // always starts empty

	// sometimes we're projecting into a sub-query
	// that wants to perform additional row operations;
	// in that case we should preserve the delimiters
	// as we compute them
	dstrc rowConsumer // if dst is a RowConsumer, this is set
}

// implements sort.Interface for outsel + outslot
type byID projector

func (b *byID) Len() int {
	return len(b.outsel)
}

func (b *byID) Swap(i, j int) {
	b.outsel[i], b.outsel[j] = b.outsel[j], b.outsel[i]
	b.inslot[i], b.inslot[j] = b.inslot[j], b.inslot[i]
}

func (b *byID) Less(i, j int) bool {
	return b.outsel[i].value < b.outsel[j].value
}

func (p *Projection) Open() (io.WriteCloser, error) {
	dst, err := p.dst.Open()
	if err != nil {
		return nil, err
	}
	rc, ok := dst.(rowConsumer)
	if !ok && p.constexpr != nil {
		cp := &constproject{datum: p.constexpr, dst: dst}
		return splitter(cp), nil
	}
	pj := &projector{parent: p, dst: dst, dstrc: rc}

	// set alignedWriter.out so that even if the
	// projection goroutine receives zero rows of
	// input, it still calls Close() on the destination
	pj.aw.out = pj.dst
	return splitter(pj), nil
}

func (p *Projection) Close() error {
	return p.dst.Close()
}

func (p *projector) update(st *symtab, aux *auxbindings) error {
	if p.aw.buf == nil {
		p.aw.init(p.dst)
	}
	err := p.aw.setpre(st)
	if err != nil {
		return err
	}
	if p.dstrc != nil {
		p.aux.reset()
		return p.dstrc.symbolize(st, &p.aux)
	}
	return nil
}

func (p *projector) symbolize(st *symtab, aux *auxbindings) error {
	p.bc.restoreScratch(st) // see EndSegment
	sel := p.parent.sel
	// output symbol table is the union of the
	// input symbol table plus the output bindings
	if len(p.outsel) != len(sel) {
		p.outsel = make([]syminfo, len(sel))
	}
	allsame := true
	for i := range sel {
		bind := sel[i].Result()
		sym := st.Intern(bind)
		if p.outsel[i].value == sym {
			continue
		}
		allsame = false
		p.outsel[i].value = sym
		p.outsel[i].encoded, p.outsel[i].mask, p.outsel[i].size = encoded(sym)
	}
	p.bc.symtab = st.symrefs
	// if the output slot order is the same
	// *and* the input symbol table has not changed
	// in a meaningful way, we don't need to recompile
	// the bytecode
	if allsame && !p.prog.isStale(st) {
		return p.update(st, aux)
	}

	// re-order the output symbols + slots
	// so that they are ordered
	if len(p.inslot) != len(p.outsel) {
		p.inslot = make([]int, len(p.outsel))
	}
	for i := range p.inslot {
		p.inslot[i] = i
	}
	sort.Sort((*byID)(p))

	var err error
	prg := &p.prog
	prg.begin()
	mem0 := prg.initMem()
	mem := make([]*value, len(sel))
	for i := range sel {
		mem[i], err = prg.compileStore(mem0, sel[p.inslot[i]].Expr, stackSlotFromIndex(regV, i), false)
		if err != nil {
			return err
		}
	}
	// preserve the initial predicate mask
	// so that we can use it for projection
	prg.returnBool(prg.mergeMem(mem...), prg.validLanes())

	if err := prg.symbolize(st, aux); err != nil {
		return fmt.Errorf("projector.symbolize(): %w", err)
	}
	if err := prg.compile(&p.bc, st, "projector"); err != nil {
		return fmt.Errorf("projector.compile(): %w", err)
	}

	return p.update(st, aux)
}

func (p *projector) Close() error {
	p.bc.reset()
	return p.aw.Close()
}

func (p *projector) flush() error {
	_, err := p.aw.flush()
	return err
}

func (p *projector) bcproject(delims []vmref, dst []byte, out []syminfo) (int, int) {
	if len(p.bc.compiled) == 0 {
		panic("projector.bcproject() before symbolize()")
	}
	if len(p.parent.sel) != len(out) {
		panic("len(selector.symbols) != len(outsymbols)")
	}

	p.bc.ensureVStackSize(len(p.parent.sel) * int(vRegSize))
	p.bc.allocStacks()

	return evalproject(&p.bc, delims, dst, out)
}

func (p *projector) next() rowConsumer { return p.dstrc }

func (p *projector) EndSegment() {
	// if we do not have any buffered data,
	// then do not eat up vm memory
	p.aw.maybeDrop()
	p.bc.dropScratch()
}

func (p *projector) writeRows(delims []vmref, rp *rowParams) error {
	if len(delims) == 0 {
		return nil
	}
	if p.aw.buf == nil {
		panic("WriteRows() called before Symbolize()")
	}
	// if the first iteration of the projection
	// loop would fail due to not enough space,
	// flush preemptively
	if p.aw.space() < (7 + 4) {
		err := p.flush()
		if err != nil {
			return err
		}
	}

	// for each subsequent invocation, we know that
	// any call to bcproject() that doesn't consume
	// all of the input delimiters must need more buffer space
	lc := 0

	p.bc.prepare(rp)
	for len(delims) > 0 {
		off, rewrote := p.bcproject(delims, p.aw.buf[p.aw.off:], p.outsel)
		if p.bc.err != 0 {
			// we don't expect to encounter
			// any errors...
			return fmt.Errorf("projection: bytecode error: %w", p.bc.err)
		}
		if rewrote == 0 && lc > 0 {
			// output projection is larger than the output buffer:
			return fmt.Errorf("Projection: no progress writing %d delimiters into buf len=%d",
				len(delims), p.aw.space())
		}
		if off > p.aw.space() {
			panic("memory corruption")
		}
		if p.dstrc != nil && rewrote > 0 {
			err := p.dstrc.writeRows(delims[:rewrote], &p.params)
			if err != nil {
				return fmt.Errorf("Projection.dst.WriteRows: %w", err)
			}
		} else {
			p.aw.off += off
		}
		delims = delims[rewrote:]

		// if we didn't process all of the delimiters,
		// it was because we didn't have enough space,
		// so we need to flush at each loop iteration
		if len(delims) > 0 && p.dstrc == nil {
			err := p.flush()
			if err != nil {
				return fmt.Errorf("Projection.flush(): %w", err)
			}
		}
		lc++
	}
	return nil
}

//go:noescape
func evalproject(bc *bytecode, delims []vmref, dst []byte, symbols []syminfo) (int, int)

// constproject is a specialization that we use
// when the output is known to be a constant structure;
// we pre-encode the output and just emit it on each input row
type constproject struct {
	datum *ion.Struct // constant row
	data  ion.Buffer  // scratch buffer for parent.constexpr
	dst   io.WriteCloser
	aw    alignedWriter
}

func (p *constproject) next() rowConsumer { return nil }

func (p *constproject) symbolize(st *symtab, aux *auxbindings) error {
	p.data.Reset()
	p.datum.Encode(&p.data, &st.Symtab)

	if p.aw.buf == nil {
		p.aw.init(p.dst)
	}

	return p.aw.setpre(st)
}

func (p *constproject) writeRows(delims []vmref, rp *rowParams) error {
	row := p.data.Bytes()
	n := len(row)
	for range delims {
		if p.aw.space() < n {
			_, err := p.aw.flush()
			if err != nil {
				return err
			}
		}

		if n > p.aw.space() {
			return fmt.Errorf("row too big (%d bytes > buffer size %d bytes)", n, len(p.aw.buf))
		}

		copy(p.aw.buf[p.aw.off:], row)
		p.aw.off += n
	}
	return nil
}

func (p *constproject) Close() error { return p.aw.Close() }

func (p *constproject) EndSegment() {
	p.aw.maybeDrop()
}
