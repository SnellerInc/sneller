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

func bcbroadcast0kgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	kdst.mask = 0
	return pc + 2
}

func bcbroadcast1kgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	kdst.mask = bc.vmState.validLanes.mask
	return pc + 2
}

func bcfalsego(bc *bytecode, pc int) int {
	vdst := argptr[vRegData](bc, pc)
	kdst := argptr[kRegData](bc, pc+2)

	*vdst = vRegData{}
	kdst.mask = 0
	return pc + 4
}

func bcnotkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	kdst.mask = ^k0.mask & bc.vmState.validLanes.mask
	return pc + 4
}

func bcandkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = k0.mask & k1.mask
	return pc + 6
}

func bcandnkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = ^k0.mask & k1.mask
	return pc + 6
}

func bcorkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = k0.mask | k1.mask
	return pc + 6
}

func bcxorkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = (k0.mask ^ k1.mask)
	return pc + 6
}

func bcxnorkgo(bc *bytecode, pc int) int {
	kdst := argptr[kRegData](bc, pc)
	k0 := argptr[kRegData](bc, pc+2)
	k1 := argptr[kRegData](bc, pc+4)
	kdst.mask = (k0.mask ^ (^k1.mask)) & bc.vmState.validLanes.mask
	return pc + 6
}
