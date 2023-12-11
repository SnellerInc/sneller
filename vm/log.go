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
	"fmt"
)

// Errorf is a global diagnostic function
// that can be set during init() to capture
// additional diagnostic information from
// the vm.
var Errorf func(f string, args ...any)

func errorf(f string, args ...any) {
	if Errorf != nil {
		Errorf(f, args...)
	}
}

// bytecodeerror reports bytecode errors in a consistent way
func bytecodeerror(ctx string, bc *bytecode) error {
	if bc.err == 0 {
		return nil
	}

	errorf("error pc %d", bc.errpc)
	errorf("bytecode:\n%s\n", bc.String())
	return fmt.Errorf("%s: bytecode error: errpc %d: %w", ctx, bc.errpc, bc.err)
}
