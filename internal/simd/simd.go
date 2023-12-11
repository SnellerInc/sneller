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

// Package simd provides selected intrinsics for the AVX512 SIMD extension emulation
package simd

import (
	"encoding/binary"
	"fmt"
)

type Vec8x64 [64]uint8
type Vec32x16 [16]uint32
type Vec64x8 [8]uint64
type Vec8x16 [16]uint8
type Vec32x4 [4]uint32
type Vec64x2 [2]uint64

func (v Vec8x16) ToVec64x2() Vec64x2 {
	return Vec64x2{
		binary.LittleEndian.Uint64(v[0:8]),
		binary.LittleEndian.Uint64(v[8:16]),
	}
}

func (v Vec8x16) ToVec32x4() Vec32x4 {
	return Vec32x4{
		binary.LittleEndian.Uint32(v[0:4]),
		binary.LittleEndian.Uint32(v[4:8]),
		binary.LittleEndian.Uint32(v[8:12]),
		binary.LittleEndian.Uint32(v[12:16]),
	}
}

func (v Vec64x2) ToVec8x16() Vec8x16 {
	return Vec8x16{
		uint8(v[0] >> 0), uint8(v[0] >> 8), uint8(v[0] >> 16), uint8(v[0] >> 24),
		uint8(v[0] >> 32), uint8(v[0] >> 40), uint8(v[0] >> 48), uint8(v[0] >> 56),
		uint8(v[1] >> 0), uint8(v[1] >> 8), uint8(v[1] >> 16), uint8(v[1] >> 24),
		uint8(v[1] >> 32), uint8(v[1] >> 40), uint8(v[1] >> 48), uint8(v[1] >> 56),
	}
}

func (v Vec8x64) ToVec64x8() Vec64x8 {
	return Vec64x8{
		binary.LittleEndian.Uint64(v[0:8]),
		binary.LittleEndian.Uint64(v[8:16]),
		binary.LittleEndian.Uint64(v[16:24]),
		binary.LittleEndian.Uint64(v[24:32]),
		binary.LittleEndian.Uint64(v[32:40]),
		binary.LittleEndian.Uint64(v[40:48]),
		binary.LittleEndian.Uint64(v[48:56]),
		binary.LittleEndian.Uint64(v[56:64]),
	}
}

func (v Vec8x64) ToVec32x16() Vec32x16 {
	return Vec32x16{
		binary.LittleEndian.Uint32(v[0:4]),
		binary.LittleEndian.Uint32(v[4:8]),
		binary.LittleEndian.Uint32(v[8:12]),
		binary.LittleEndian.Uint32(v[12:16]),
		binary.LittleEndian.Uint32(v[16:20]),
		binary.LittleEndian.Uint32(v[20:24]),
		binary.LittleEndian.Uint32(v[24:32]),
		binary.LittleEndian.Uint32(v[32:36]),
		binary.LittleEndian.Uint32(v[36:40]),
		binary.LittleEndian.Uint32(v[40:44]),
		binary.LittleEndian.Uint32(v[44:48]),
		binary.LittleEndian.Uint32(v[48:52]),
		binary.LittleEndian.Uint32(v[52:56]),
		binary.LittleEndian.Uint32(v[56:60]),
		binary.LittleEndian.Uint32(v[60:64]),
	}
}

func (v Vec32x16) ToVec64x8() Vec64x8 {
	return Vec64x8{
		uint64(v[0]) | (uint64(v[1]) << 32),
		uint64(v[2]) | (uint64(v[3]) << 32),
		uint64(v[4]) | (uint64(v[5]) << 32),
		uint64(v[6]) | (uint64(v[7]) << 32),
		uint64(v[8]) | (uint64(v[9]) << 32),
		uint64(v[10]) | (uint64(v[11]) << 32),
		uint64(v[12]) | (uint64(v[13]) << 32),
		uint64(v[14]) | (uint64(v[15]) << 32),
	}
}

func (v Vec64x8) String() string {
	return fmt.Sprintf("{%016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x}",
		v[7], v[6], v[5], v[4], v[3], v[2], v[1], v[0])
}

func (v Vec32x16) String() string {
	return fmt.Sprintf("{%08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x, %08x}",
		v[15], v[14], v[13], v[12], v[11], v[10], v[9], v[8],
		v[7], v[6], v[5], v[4], v[3], v[2], v[1], v[0])
}

func (v Vec8x64) String() string {
	return fmt.Sprintf("{%02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x}",
		v[63], v[62], v[61], v[60], v[59], v[58], v[57], v[56],
		v[55], v[54], v[53], v[52], v[51], v[50], v[49], v[48],
		v[47], v[46], v[45], v[44], v[43], v[42], v[41], v[40],
		v[39], v[38], v[37], v[36], v[35], v[34], v[33], v[32],
		v[31], v[30], v[29], v[28], v[27], v[26], v[25], v[24],
		v[23], v[22], v[21], v[20], v[19], v[18], v[17], v[16],
		v[15], v[14], v[13], v[12], v[11], v[10], v[9], v[8],
		v[7], v[6], v[5], v[4], v[3], v[2], v[1], v[0])
}

func (v Vec8x16) String() string {
	return fmt.Sprintf("{%02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x, %02x}",
		v[15], v[14], v[13], v[12], v[11], v[10], v[9], v[8],
		v[7], v[6], v[5], v[4], v[3], v[2], v[1], v[0])
}

func (v Vec32x4) String() string {
	return fmt.Sprintf("{%08x, %08x, %08x, %08x}", v[3], v[2], v[1], v[0])
}

func (v Vec64x2) String() string {
	return fmt.Sprintf("{%016x, %016x}", v[1], v[0])
}
