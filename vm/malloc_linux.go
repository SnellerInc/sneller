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

//go:build linux

package vm

import (
	"syscall"
)

// linux implementation of vmm area

func mapVM() *[vmReserve]byte {
	buf, err := syscall.Mmap(0, 0, vmReserve, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	if err != nil {
		panic("couldn't map vmm region: " + err.Error())
	}
	if vmUse < vmReserve {
		err = syscall.Mprotect(buf[vmUse:], syscall.PROT_NONE)
		if err != nil {
			panic("couldn't map unused vmm region as PROT_NONE: " + err.Error())
		}
	}
	return (*[vmReserve]byte)(buf)
}

func hintUnused(mem []byte) {
	err := syscall.Madvise(mem, 8) // MADV_FREE
	if err != nil {
		panic("madvise: " + err.Error())
	}
}
