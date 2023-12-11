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

//go:build windows

package vm

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func mapVM() *[vmUse]byte {
	base, err := windows.VirtualAlloc(0, vmReserve, windows.MEM_RESERVE, windows.PAGE_NOACCESS)
	if err != nil {
		panic("VirtualAlloc(reserve) failed: " + err.Error())
	}
	_, err = windows.VirtualAlloc(base+vmStart, vmUse+1, windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		panic("VirtualAlloc(commit) failed: " + err.Error())
	}
	return (*[vmUse]byte)(unsafe.Pointer(base + vmStart))
}

func hintUnused(mem []byte) {}
