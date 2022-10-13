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
