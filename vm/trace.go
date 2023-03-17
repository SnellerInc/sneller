// Copyright (C) 2023 Sneller, Inc.
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
	"sync"
	"sync/atomic"
)

var (
	tracing   atomic.Uint32
	tracelock sync.Mutex
	traceout  io.Writer
)

// TraceFlags is set of tracing options
// that can be passed to Trace.
type TraceFlags uint

const (
	// TraceSSAText causes all compiled SSA programs
	// to be dumped in their textual representation.
	TraceSSAText = 1 << iota
	// TraceSSADot causes all compiled SSA programs
	// to be dumped in a format suitable for processing
	// with graphviz(1).
	TraceSSADot
	// TraceBytecodeText causes all compiled bytecode
	// programs to be dumped in a text-based format.
	TraceBytecodeText
)

// Trace enables or disables tracing of bytecode
// program compilation.
//
// To enable tracing, Trace should be called with
// a non-nil io.Writer and non-zero flags.
// To disable tracing, Trace should be called
// with a nil io.Writer and flags equal to zero.
func Trace(w io.Writer, flags TraceFlags) {
	if (w == nil) != (flags == 0) {
		panic("invalid arguments for vm.Trace")
	}
	tracelock.Lock()
	defer tracelock.Unlock()
	traceout = w
	tracing.Store(uint32(flags))
}

func enabled(flags TraceFlags) bool {
	return TraceFlags(tracing.Load())&flags != 0
}

func trace(body func(io.Writer)) {
	tracelock.Lock()
	defer tracelock.Unlock()
	if traceout == nil {
		return // could have raced if we inspected flags before locking
	}
	body(traceout)
}
