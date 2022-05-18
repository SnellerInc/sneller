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
	return t.exec(s, parallel, stats, rw)
}

type wrappedHandle int

func (w wrappedHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteInt(int64(w))
	return nil
}

func (w wrappedHandle) Open() (vm.Table, error) {
	panic("wrappedHandle.Open")
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
