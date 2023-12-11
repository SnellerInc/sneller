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
