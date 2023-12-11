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

import (
	"github.com/SnellerInc/sneller/ion"
)

// scan scans any buffer and produces
// relative displacments for all of
// the structures relative to &buf[0]
// beginning at offset start
//
// NOTE: these are *not* vmref slices;
// those are produced by scanvmm
//
//go:noescape
func scan(buf []byte, start int32, dst [][2]uint32) (int, int32)

// scanvmm scans a vmm-allocated buffer
// and produces absolute displacements
// relative to vmm for all of the structures
// present in buf, up to either len(buf) or
// the maximum number of records that fit in dst
//
//go:noescape
func scanvmm(buf []byte, dst []vmref) (int, int32)

// encoded returns a Symbol as its UVarInt
// encoded bytes (up to 4 bytes) and the mask
// necessary to examine just those bytes
//
// NOTE: only symbols up to 2^28 are supported
func encoded(sym ion.Symbol) (uint32, uint32, int8) {
	mask := uint32(0)
	out := uint32(0)
	size := int8(0)
	for sym != 0 {
		size++
		mask <<= 8
		out <<= 8
		if out == 0 {
			out |= 0x80
		}
		mask |= 0xff
		out |= uint32(sym) & 0x7f
		sym >>= 7
	}
	return out, mask, size
}
