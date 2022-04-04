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

//go:build windows

package vm

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func mapVM() *[vmUse]byte {
	base, err := windows.VirtualAlloc(0, vmReserve, windows.MEM_RESERVE, windows.PAGE_NOACCESS)
	if err != nil {
		panic("VirtualAlloc(reserve): " + err.Error())
	}
	base, err = windows.VirtualAlloc(base+vmStart, vmUse, windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		panic("VirtualAlloc(commit): " + err.Error())
	}
	return (*[vmUse]byte)(unsafe.Pointer(base + vmStart))
}

func hintUnused(mem []byte) {
	// implement me!
	// I believe this is VirtualAlloc(base, length, MEM_RESET, 0)
}
