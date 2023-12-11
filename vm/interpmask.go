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
