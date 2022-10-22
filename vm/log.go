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
