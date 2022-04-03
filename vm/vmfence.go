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

//go:build vmfence && linux

package vm

import (
	"syscall"
)

func guard(mem []byte) {
	err := syscall.Mprotect(mem[:4096], syscall.PROT_READ)
	if err != nil {
		println("mprotect:", err.Error())
	}
	err = syscall.Mprotect(mem[4096:], syscall.PROT_NONE)
	if err != nil {
		println("mprotect:", err.Error())
	}
}

func unguard(mem []byte) {
	err := syscall.Mprotect(mem, syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		println("mprotect:", err.Error())
	}
}
