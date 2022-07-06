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
	"runtime"
)

// LeakCheckHook is a hook that can be
// set in test code to look for leaked RowConsumers.
// LeakCheckHook should not be set in production code.
var LeakCheckHook func(stack []byte, obj any)

func leakCheck(x any) {
	if LeakCheckHook == nil {
		return
	}
	hook := LeakCheckHook
	stk := make([]byte, 1024)
	runtime.Stack(stk, false)
	runtime.SetFinalizer(x, func(x any) {
		hook(stk, x)
	})
}

func noLeakCheck(x any) {
	if LeakCheckHook == nil {
		return
	}
	runtime.SetFinalizer(x, nil)
}
