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

//go:build darwin

package vm

import (
	"syscall"
)

// darwin implementation of vmm area

func mapVM() *[vmUse]byte {
	// reserve 4GiB of memory
	buf, err := syscall.Mmap(0, 0, vmReserve, syscall.PROT_NONE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		panic("couldn't map vmm region: " + err.Error())
	}
	// map some usable memory in the middle of the region;
	// this means that any reference to vmm +/- 2GiB must
	// hit the region we mapped above
	//
	// we add 1 to the usable memory region so that the
	// user of the final page can have a multi-byte load
	// extend past the final page boundary
	// (mprotect will round up to the next page)
	//
	// (we do this b/c AVX-512 VPGATHER*D sign-extends
	// the per-lane offset, so a gather that uses vmm
	// as the base is guaranteed to reference only the
	// mapping that we picked above)
	err = syscall.Mprotect(buf[vmStart:vmStart+vmUse+1], syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		panic("couldn't map unused vmm region as PROT_NONE: " + err.Error())
	}
	guard(buf[vmStart : vmStart+vmUse])
	return (*[vmUse]byte)(buf[vmStart:])
}

func hintUnused(mem []byte) {}
