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
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
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
	slot          uint16
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
	dst  QuerySink
	sel  Selection // selection w/ renaming
	prog prog

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
func NewProjection(sel Selection, dst QuerySink) (*Projection, error) {
	p := &Projection{
		dst: dst,
		sel: sel,
	}
	constexpr, ok := sel.toConst()
	if ok {
		p.constexpr = &constexpr
	}

	prg := &p.prog
	prg.begin()
	mem0 := prg.initMem()
	mem := make([]*value, len(sel))
	var err error
	for i := range sel {
		mem[i], err = prg.compileStore(mem0, sel[i].Expr, stackSlotFromIndex(regV, i), false)
		if err != nil {
			return nil, err
		}
	}
	// preserve the initial predicate mask
	// so that we can use it for projection
	prg.returnBool(prg.mergeMem(mem...), prg.validLanes())
	return p, nil
}

// goroutine-local component of Select(...)
type projector struct {
	parent *Projection
	st     *symtab // most recent symbol table
	prog   prog
	bc     bytecode
	aw     alignedWriter
	prep   bool // aw contains current symbol table
	dst    io.WriteCloser
	outsel []syminfo   // output symbol IDs (sorted)
	params rowParams   // always starts empty
	aux    auxbindings // always starts empty

	// sometimes we're projecting into a sub-query
	// that wants to perform additional row operations;
	// in that case we should preserve the delimiters
	// as we compute them
	dstrc rowConsumer // if dst is a RowConsumer, this is set
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
	p.prog.reset()
	return p.dst.Close()
}

func (p *projector) symbolize(st *symtab, aux *auxbindings) error {
	err := recompile(st, &p.parent.prog, &p.prog, &p.bc, aux, "projector")
	if err != nil {
		return err
	}
	sel := p.parent.sel
	if len(p.outsel) != len(sel) {
		p.outsel = make([]syminfo, len(sel))
	}
	for i := range sel {
		sym := st.Intern(sel[i].Result())
		p.outsel[i].slot = uint16(stackSlotFromIndex(regV, i))
		p.outsel[i].value = sym
		p.outsel[i].encoded, p.outsel[i].mask, p.outsel[i].size = encoded(sym)
	}
	slices.SortFunc(p.outsel, func(x, y syminfo) bool {
		return x.value < y.value
	})
	p.st = st
	p.prep = false // p.aw.setpre() on next writeRows call
	if p.dstrc != nil {
		p.aux.reset()
		return p.dstrc.symbolize(st, &p.aux)
	}
	return nil
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
	if p.st == nil {
		panic("WriteRows() called before Symbolize()")
	}
	if p.aw.buf == nil {
		p.aw.init(p.dst)
	}
	// if we haven't prepared the current symbol table,
	// then prepare it now:
	if !p.prep {
		err := p.aw.setpre(p.st)
		if err != nil {
			return err
		}
		p.prep = true
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
