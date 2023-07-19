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
