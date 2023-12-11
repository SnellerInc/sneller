// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
