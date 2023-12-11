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

//go:build !vmmemleaks

package vm

import "io"

func leakstart(i int) {}
func leakend(i int)   {}

// LeakCheck runs fn and writes the stack traces
// of all the page allocation sites to w for each
// page that was allocated within fn and was not freed.
// LeakCheck is not reentrancy-safe.
//
// Note that LeakCheck *just* runs fn() unless -tags=vmemleaks is set.
func LeakCheck(w io.Writer, fn func()) {
	fn()
}
