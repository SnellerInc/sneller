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

package plan

import (
	"io"
	"runtime"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// Exec executes a plan and writes the
// results of the query execution to dst.
func Exec(t *Tree, dst io.Writer, stats *ExecStats) error {
	return (&LocalTransport{}).Exec(t, nil, dst, stats)
}

// LocalTransport is a Transport
// that executes queries locally.
type LocalTransport struct {
	// Threads is the number of threads
	// used for query evaluation.
	// If Threads is <= 0, then runtime.GOMAXPROCS
	// is used.
	Threads int
}

func (l *LocalTransport) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype("local", dst, st)
	if l.Threads > 0 {
		dst.BeginField(st.Intern("threads"))
		dst.WriteInt(int64(l.Threads))
	}
	dst.EndStruct()
}

// Exec implements Transport.Exec
func (l *LocalTransport) Exec(t *Tree, rw TableRewrite, dst io.Writer, stats *ExecStats) error {
	s := vm.LockedSink(dst)
	parallel := l.Threads
	if parallel <= 0 {
		parallel = runtime.GOMAXPROCS(0)
	}
	if rw != nil {
		var err error
		t, err = t.Rewrite(rw)
		if err != nil {
			return err
		}
	}
	return t.exec(s, parallel, stats)
}

func (f *fakeenv) rewrite(in *expr.Table, h TableHandle) (*expr.Table, TableHandle) {
	in, h = f.rw(in, h)
	out := &expr.Table{
		Binding: expr.Bind(expr.Integer(int64(len(f.tables))), ""),
		Value:   in.Value,
	}
	f.tables = append(f.tables, in)
	f.handles = append(f.handles, h)
	return out, h
}

func (f *fakeenv) Schema(e *expr.Table) expr.Hint { return nil }

func (f *fakeenv) Stat(e *expr.Table) (TableHandle, error) {
	iv := int64(e.Expr.(expr.Integer))
	e.Value = f.tables[iv].Value
	return f.handles[iv], nil
}

// Rewrite returns a copy of a tree
// with tables rewritten according
// to the supplied rewrite rule.
func (t *Tree) Rewrite(rw TableRewrite) (*Tree, error) {
	fe := &fakeenv{rw: rw}
	var st ion.Symtab
	var buf ion.Buffer
	err := t.EncodePart(&buf, &st, fe.rewrite)
	if err != nil {
		return nil, err
	}
	return Decode(fe, &st, buf.Bytes())
}

// fakeenv is a TableRewriter and an Env
// for performing deep copies of a Plan
// with a table rewrite
type fakeenv struct {
	rw      TableRewrite
	tables  []*expr.Table
	handles []TableHandle
}

// Transport models the exection environment
// of a query plan.
//
// See LocalTransport for executing queries locally.
// See Client for executing queries remotely.
type Transport interface {
	// Exec executes the provided query plan,
	// streaming the output of the query to dst.
	// Each call to dst.Write should contain exactly
	// one "chunk" of ion-encoded data, which will
	// begin with an ion BVM and be followed by zero
	// or more ion structures.
	//
	// The TableRewrite provided to Exec, if non-nil,
	// determines how table expressions are re-written
	// before they are provided to Transport.
	Exec(t *Tree, rw TableRewrite, dst io.Writer, stats *ExecStats) error
}
