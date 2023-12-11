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

//go:build amd64
// +build amd64

package aes

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

const offsX86HasAVX512VAES = unsafe.Offsetof(cpu.X86.HasAVX512VAES) //lint:ignore U1000, used in asm

//go:noescape
//go:nosplit
func aesHash64(quad *ExpandedKey128Quad, p *byte, n int) uint64

//go:noescape
//go:nosplit
func aesHashWide(quad *ExpandedKey128Quad, p *byte, n int) WideHash
