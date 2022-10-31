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
	prog.Renumber()
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

func (c *Count) writeRows(delims []vmref, _ *rowParams) error {
	atomic.AddInt64(&c.val, int64(len(delims)))
	return nil
}

func (c *Count) symbolize(st *symtab, _ *auxbindings) error {
	return nil
}

func (c *Count) next() rowConsumer { return nil }

func (c *Count) Value() int64 { return c.val }

type wherebc struct {
	parent *Filter
	ssa    prog
	bc     bytecode
	dst    rowConsumer
	params rowParams
}

//go:noescape
func evalfilterbc(w *bytecode, delims []vmref) int

func (w *wherebc) symbolize(st *symtab, aux *auxbindings) error {
	err := recompile(st, w.parent.prog, &w.ssa, &w.bc, aux)
	if err != nil {
		return err
	}
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
	w.bc.prepare(rp)
	valid := evalfilterbc(&w.bc, delims)
	if w.bc.err != 0 {
		return bytecodeerror("filter", &w.bc)
	}
	if valid > 0 {
		// the assembly already did the compression for us:
		w.params.auxbound = shrink(w.params.auxbound, len(rp.auxbound))
		for i := range w.params.auxbound {
			w.params.auxbound[i] = rp.auxbound[i][:valid]
		}
		return w.dst.writeRows(delims[:valid], &w.params)
	}
	return nil
}

func (w *wherebc) Close() error {
	w.bc.reset()
	return w.dst.Close()
}
