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
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
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
func NewFilter(e expr.Node, rest QuerySink) *Filter {
	prog, err := compileLogical(e)
	if err != nil {
		panic(err)
	}
	prog.Renumber()
	return where(prog, rest)
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
	return Splitter(&wherebc{parent: r, dst: AsRowConsumer(q)}), nil
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
	return Splitter(c), nil
}

func (c *Count) Close() error {
	return nil
}

func (c *Count) writeRows(delims []vmref) error {
	atomic.AddInt64(&c.val, int64(len(delims)))
	return nil
}

func (c *Count) symbolize(st *ion.Symtab) error {
	return nil
}

func (c *Count) Value() int64 { return c.val }

type wherebc struct {
	parent *Filter
	ssa    prog
	bc     bytecode
	dst    RowConsumer
}

//go:noescape
func evalfilterbc(w *bytecode, delims []vmref) int

func (w *wherebc) symbolize(st *ion.Symtab) error {
	err := recompile(st, w.parent.prog, &w.ssa, &w.bc)
	if err != nil {
		return err
	}
	return w.dst.symbolize(st)
}

func (w *wherebc) writeRows(delims []vmref) error {
	if w.bc.compiled == nil {
		panic("bytecode writeRows() before Symbolize()")
	}
	valid := evalfilterbc(&w.bc, delims)
	if w.bc.err != 0 {
		return fmt.Errorf("filter: bytecode error: %w", w.bc.err)
	}
	if valid > 0 {
		// delims are absolute now, so use vmm as base
		return w.dst.writeRows(delims[:valid])
	}
	return nil
}

func (w *wherebc) Close() error {
	w.bc.reset()
	return w.dst.Close()
}
