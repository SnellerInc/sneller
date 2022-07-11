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
	"sync"
)

// The Unsafe variants assume all the parameters are valid. If pre-validation is required, it should be provided by the respective wrappers.

// Takes a single uint16 parameter denoting opcode ID and returns the address of the associated handler.
//
//go:noescape
//go:norace
//go:nosplit
func opcodeAddressUnsafe(op uint16) uintptr

func (op bcop) address() uintptr {
	if op >= _maxbcop {
		op = optrap
	}
	return opcodeAddressUnsafe(uint16(op))
}

// Reverse mapping from opcode adresses to opcode IDs. For pretty printers, serializers etc.

var opcodeToIdMapOnce sync.Once
var opcodeToIdMap map[uintptr]bcop

func opcodeID(addr uintptr) (bcop, bool) {

	// Lazy initialization of the map

	opcodeToIdMapOnce.Do(func() {

		opcodeToIdMap = make(map[uintptr]bcop)

		for i := 0; i != _maxbcop; i++ {

			id := bcop(i)
			addr := id.address()
			opcodeToIdMap[addr] = id
		}
	})

	val, present := opcodeToIdMap[addr]
	return val, present
}
