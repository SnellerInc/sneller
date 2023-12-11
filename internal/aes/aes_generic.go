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

//go:build !amd64
// +build !amd64

package aes

import (
	"math/bits"

	"github.com/SnellerInc/sneller/internal/simd"
)

func aesSubWord(x uint32) uint32 {
	b0 := byte(x & 0xff)
	b1 := byte((x >> 8) & 0xff)
	b2 := byte((x >> 16) & 0xff)
	b3 := byte((x >> 24) & 0xff)
	s0 := simd.AESSBox[b0]
	s1 := simd.AESSBox[b1]
	s2 := simd.AESSBox[b2]
	s3 := simd.AESSBox[b3]
	return (uint32(s3) << 24) | (uint32(s2) << 16) | (uint32(s1) << 8) | uint32(s0)
}

func aesRotWord(x uint32) uint32 {
	return bits.RotateLeft32(x, -8)
}

func auxExpandFromKey128(p *ExpandedKey128, key Key128) {
	p[0] = key
	for i := 4; i < 44; i++ {
		t := p[(i-1)/4][(i-1)%4]
		if i%4 == 0 {
			t = aesSubWord(aesRotWord(t)) ^ roundConstant[(i/4)-1]
		}
		p[i/4][i%4] = p[(i-4)/4][(i-4)%4] ^ t
	}
}

func auxExpandFromKey128Quad(p *ExpandedKey128Quad, quad Key128Quad) {
	expanded := [4]ExpandedKey128{}
	auxExpandFromKey128(&expanded[0], quad[0])
	auxExpandFromKey128(&expanded[1], quad[1])
	auxExpandFromKey128(&expanded[2], quad[2])
	auxExpandFromKey128(&expanded[3], quad[3])
	for i := range *p {
		p[i][0] = expanded[0][i]
		p[i][1] = expanded[1][i]
		p[i][2] = expanded[2][i]
		p[i][3] = expanded[3][i]
	}
}

var roundConstant [10]uint32 = [10]uint32{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36}
