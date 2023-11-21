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
