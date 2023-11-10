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
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
)

// Filter is a concrete implementation
// of QuerySink that applies a filter to
// incoming rows.
type Filter struct {
	prog *prog
	rest QuerySink // rest of sub-query
}

// NewFilter constructs a Filter from a boolean expression.
// The returned Filter will write rows for which e evaluates
// to TRUE to rest.
func NewFilter(e expr.Node, rest QuerySink) (*Filter, error) {
	prog, err := compileLogical(e)
	if err != nil {
		return nil, err
	}
	return where(prog, rest), nil
}

func where(p *prog, rest QuerySink) *Filter {
	return &Filter{prog: p, rest: rest}
}

// Open implements QuerySink.Open
func (r *Filter) Open() (io.WriteCloser, error) {
	q, err := r.rest.Open()
	if err != nil {
		return nil, err
	}
	// we know we'd like to write to a RowConsumer,
	// so determine if we have one already or if we
	// need to create one with a rematerializer
	return splitter(&wherebc{parent: r, dst: asRowConsumer(q)}), nil
}

// Close implements io.Closer
func (r *Filter) Close() error {
	r.prog.reset()
	return r.rest.Close()
}

// Count is a utility QuerySink
// that simply counts the number
// of rows that it receives.
type Count struct {
	val int64
}

func (c *Count) Open() (io.WriteCloser, error) {
	return splitter(c), nil
}

func (c *Count) Close() error {
	return nil
}

var _ zionConsumer = &Count{}

// zion is allowed unconditionally
func (c *Count) zionOk(fields []string) bool { return true }

func (c *Count) writeZion(state *zionState) error {
	n, err := state.shape.Count()
	if err != nil {
		return err
	}
	atomic.AddInt64(&c.val, int64(n))
	return nil
}

func (c *Count) writeRows(delims []vmref, _ *rowParams) error {
	atomic.AddInt64(&c.val, int64(len(delims)))
	return nil
}

func (c *Count) symbolize(st *symtab, _ *auxbindings) error {
	return nil
}

func (c *Count) next() rowConsumer { return nil }

func (c *Count) Value() int64 { return c.val }

const (
	constFalse = 1
	constTrue  = 2
)

type wherebc struct {
	parent      *Filter
	ssa         prog
	bc          bytecode
	dst         rowConsumer
	params      rowParams
	constResult int // indicates the result of compiled program
}

//go:noescape
func evalfilterbc(w *bytecode, delims []vmref) int

// progresult returns whether the program got simplified to true or false.
func progresult(p *prog) int {
	ret := p.ret
	if ret.op != sretbk {
		return 0
	}

	if len(ret.args) < 2 {
		return 0
	}

	switch ret.args[1].op {
	case skfalse:
		return constFalse

	case sinit:
		return constTrue
	}

	return 0
}

func (w *wherebc) symbolize(st *symtab, aux *auxbindings) error {
	err := recompile(st, w.parent.prog, &w.ssa, &w.bc, aux, "wherebc")
	if err != nil {
		return err
	}
	w.params.auxbound = shrink(w.params.auxbound, len(aux.bound))
	w.constResult = progresult(&w.ssa)

	// pass on same aux bindings:
	return w.dst.symbolize(st, aux)
}

func (w *wherebc) next() rowConsumer { return w.dst }

func (w *wherebc) EndSegment() {
	w.bc.dropScratch() // restored in recompile()
}

func (w *wherebc) writeRows(delims []vmref, rp *rowParams) error {
	if w.bc.compiled == nil {
		panic("WriteRows() called before symbolize()")
	}
	if len(rp.auxbound) != len(w.params.auxbound) {
		println(len(rp.auxbound), "!=", len(w.params.auxbound))
		panic("mismatched auxbound len")
	}

	switch w.constResult {
	case constFalse:
		return nil

	case constTrue:
		return w.dst.writeRows(delims, &w.params)
	}

	w.bc.prepare(rp)
	var valid int
	if portable.Load() {
		valid = evalfiltergo(&w.bc, delims)
	} else {
		valid = evalfilterbc(&w.bc, delims)
	}
	if w.bc.err != 0 {
		return bytecodeerror("filter", &w.bc)
	}
	if valid > 0 {
		// the assembly already did the compression for us:
		for i := range w.params.auxbound {
			w.params.auxbound[i] = sanitizeAux(rp.auxbound[i], valid) // ensure rp.auxbound[i][valid:] is zeroed up to the lane multiple
		}
		return w.dst.writeRows(delims[:valid], &w.params)
	}
	return nil
}

func (w *wherebc) Close() error {
	w.bc.reset()
	return w.dst.Close()
}
