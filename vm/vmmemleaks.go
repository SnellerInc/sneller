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

//go:build vmmemleaks

package vm

import (
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"
)

var (
	// filed when vmdebugleaksEnabled is set via -tags=vmmemleaks
	vmdebugleaksActive atomic.Bool
	vmdebugleaksLock   sync.Mutex
	vmdebugleaksTraces = map[int]string{}
)

func leakstart(i int) {
	if vmdebugleaksActive.Load() {
		stack := string(debug.Stack())
		vmdebugleaksLock.Lock()
		vmdebugleaksTraces[i] = stack
		vmdebugleaksLock.Unlock()
	}
}

func leakend(i int) {
	if vmdebugleaksActive.Load() {
		vmdebugleaksLock.Lock()
		delete(vmdebugleaksTraces, i)
		vmdebugleaksLock.Unlock()
	}
}

// LeakCheck runs fn and writes the stack traces
// of all the page allocation sites to w for each
// page that was allocated within fn and was not freed.
//
// Note that LeakCheck *just* runs fn() unless -tags=vmemleaks is set.
func LeakCheck(w io.Writer, fn func()) {
	if vmdebugleaksActive.Swap(true) {
		panic("concurrent vm.LeakCheck calls")
	}
	fn()
	vmdebugleaksLock.Lock()
	defer vmdebugleaksLock.Unlock()
	i := 1
	for page, stacktrace := range vmdebugleaksTraces {
		fmt.Fprintf(w, "\n#%d. page %x allocated at\n%s\n", i, page, stacktrace)
		i += 1
	}
	maps.Clear(vmdebugleaksTraces)
	vmdebugleaksActive.Store(false)
}
