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
	"context"
	"io"
	"runtime"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// ExecParams is a collection of all the
// runtime parameters for a query.
type ExecParams struct {
	// Output is the destination of the query output.
	Output io.Writer
	// Stats are stats that are collected
	// during query execution.
	Stats ExecStats
	// Rewrite is a function that is applied
	// to tables before expanding them in queries.
	// Rewrite may be nil.
	Rewrite TableRewrite
	// Parallel determines the (local) parallelism
	// of plan execution. If Parallel is unset, then
	// runtime.GOMAXPROCS(0) is used instead.
	Parallel int
	// Context indicates the cancellation scope
	// of the query. Transports are expected to
	// stop processing queries after Context is canceled.
	Context context.Context
}

// Exec executes a plan and writes the
// results of the query execution to dst.
func Exec(t *Tree, dst io.Writer, stats *ExecStats) error {
	ep := ExecParams{
		Output:   dst,
		Parallel: runtime.GOMAXPROCS(0),
		Context:  context.Background(),
	}
	err := (&LocalTransport{}).Exec(t, &ep)
	stats.atomicAdd(&ep.Stats)
	return err
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
func (l *LocalTransport) Exec(t *Tree, ep *ExecParams) error {
	s := vm.LockedSink(ep.Output)
	if ep.Parallel == 0 {
		ep.Parallel = l.Threads
	}
	if ep.Parallel == 0 {
		ep.Parallel = runtime.GOMAXPROCS(0)
	}
	return t.exec(s, ep)
}

// Transport models the exection environment
// of a query plan.
//
// See LocalTransport for executing queries locally.
// See Client for executing queries remotely.
type Transport interface {
	// Exec executes the provided query plan,
	// streaming the output of the query to ep.Output
	// (ep.Output may not be nil).
	// Each call to ep.Output.Write should contain exactly
	// one "chunk" of ion-encoded data, which will
	// begin with an ion BVM and be followed by zero
	// or more ion structures.
	//
	// The ep.Rewrite provided via ExecParams, if non-nil,
	// determines how table expressions are re-written
	// before they are provided to Transport.
	Exec(t *Tree, ep *ExecParams) error
}
