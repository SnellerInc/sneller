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

//go:build !amd64
// +build !amd64

package aes

import (
	"github.com/SnellerInc/sneller/internal/simd"
)

func aesHash64(quad *ExpandedKey128Quad, p *byte, n int) uint64 {
	return aesHashWide(quad, p, n)[0]
}

func (quad Key128Quad) toVec32x16() simd.Vec32x16 {
	return simd.Vec32x16{
		quad[0][0], quad[0][1], quad[0][2], quad[0][3],
		quad[1][0], quad[1][1], quad[1][2], quad[1][3],
		quad[2][0], quad[2][1], quad[2][2], quad[2][3],
		quad[3][0], quad[3][1], quad[3][2], quad[3][3],
	}
}

func (quad Key128Quad) toVec64x8() simd.Vec64x8 {
	return quad.toVec32x16().ToVec64x8()
}

func aesHashWide(quad *ExpandedKey128Quad, p *byte, n int) WideHash {
	if n == 0 {
		return WideHash{ // A hash value for an empty input
			uint64(quad[10][0][0]) | (uint64(quad[10][0][1]) << 32),
			uint64(quad[10][0][2]) | (uint64(quad[10][0][3]) << 32),
			uint64(quad[10][1][0]) | (uint64(quad[10][1][1]) << 32),
			uint64(quad[10][1][2]) | (uint64(quad[10][1][3]) << 32),
			uint64(quad[10][2][0]) | (uint64(quad[10][2][1]) << 32),
			uint64(quad[10][2][2]) | (uint64(quad[10][2][3]) << 32),
			uint64(quad[10][3][0]) | (uint64(quad[10][3][1]) << 32),
			uint64(quad[10][3][2]) | (uint64(quad[10][3][3]) << 32),
		}
	}

	var z0, z1, z2, z3, z6, z7, z8, z16, z17, z18, z19, z20, z21 simd.Vec64x8

	// BX := the base address of the expanded 128-bit key quad
	// CX := the number of bytes to process
	// SI := the base address of the input data block

	var si int64
	cx := uint64(-n)

	{ // Observe the actual shift is (cx & 0x3f), so ax is 0xff..ff for every multiple of 64.
		// The n=0 case has been already excluded by the test above, so a full 64-byte chunk will
		// be fetched correctly with the 0xff..ff mask. That leaves only a possibly empty set of
		// the preceeding full chunks for further processing. Due to the chunks being either full
		// (the preceeding ones) or zero-padded to full (the last one), no lane masking is needed.
		k1 := ^uint64(0) >> (cx & 0x3f)
		cx = cx & ^uint64(0x3f)
		si = -int64(cx)
		z0 = simd.VMOVDQU8Z(p, -0x40+si, k1).ToVec64x8()
	}

	z6 = quad[0].toVec64x8() // The round 1 key quad
	z7 = quad[1].toVec64x8() // The round 2 key quad
	keyIdx := 2

	// cx := the negative byte count - 64, multiple of 64
	// si := input read cursor, 64 bytes past the beginning of the last chunk
	// z0 := the zero-padded last chunk of input data
	// z6 := the round 1 key quad
	// z7 := the round 2 key quad

	for {
		// From "The Design of Rijndael", page 41, section 3.5:
		//
		// Two rounds of Rijndael provide 'full diffusion' in the following
		// sense: every state bit depends on all state bits two rounds ago,
		// or a change in one state bit is likely to affect half of the state
		// bits after two rounds.

		simd.VAESENC(&z6, &z0, &z0)
		simd.VAESENC(&z7, &z0, &z0)
		z6 = quad[keyIdx].toVec64x8()   // fetch the round N+1 key quad
		z8 = quad[keyIdx+1].toVec64x8() // fetch the round N+2 key quad. Z8 is the next Z7

		keyIdx += 2
		if keyIdx == 10 {
			// move the ring cursor to the next pair of round keys (or wrap-around to the beginning)
			keyIdx = 0
		}

		// move the input read cursor to the preceding chunk
		si -= 0x40
		cx += 0x40

		if cx == 0 {
			break
		}

		t := simd.VMOVDQU8(p, -0x40+si).ToVec64x8()
		simd.VPXORQ(&t, &z0, &z0)
		simd.VMOVDQA64(&z8, &z7)
	}

	// No more chunks to process. The in-lane diffusion should be quite good per
	// the Rijndael design, but there is absolutley no inter-lane diffusion so far!
	// This will be fixed by the non-linear mixing step that follows.
	//
	// BX := the AES keys
	// Z0 := the partial hash result in the ABCD lane order
	// Z6 := the round (n+3) key quad
	// Z8 := the round (n+4) key quad
	//
	// The "A" means the 128-bit chunk from lane 0, "B" from lane 1, etc. Inter-lane mixing
	// requires that the chunks from every other lane influence the chunk from a given lane.
	// For every pair (x, y) compute x^y and encrypt it with the i-th lane key K_i to ensure
	// diffusion. Then xor all the encrypted pairs obtained from K_i to form the final result
	// for the i-th lane. The code block below takes the full advantage of the 5-cycle latency,
	// 1-cycle throughput pipeline characteristics of VAESenc: there are 6 partial results to
	// compute, each in 2 encryption steps, so the mixing takes just ~12 cycles instead of ~60.
	// The remaining operations are expected to slide under the VAESenc latency, barely
	// contributing to the total execution time of the mixer. enc(x)=enc2(enc1(x)).

	simd.VSHUFI64X2(((1 << 6) | (1 << 4) | (1 << 2) | (1 << 0)), &z0, &z0, &z1) // z1 := BBBB
	simd.VSHUFI64X2(((2 << 6) | (2 << 4) | (2 << 2) | (2 << 0)), &z0, &z0, &z2) // z2 := CCCC
	simd.VSHUFI64X2(((3 << 6) | (3 << 4) | (3 << 2) | (3 << 0)), &z0, &z0, &z3) // z3 := DDDD
	simd.VSHUFI64X2(((0 << 6) | (0 << 4) | (0 << 2) | (0 << 0)), &z0, &z0, &z0) // z0 := AAAA
	simd.VPXORQ(&z1, &z2, &z16)                                                 // z16 := B^C
	simd.VAESENC(&z6, &z16, &z16)                                               // z16 := enc1(B^C,k_1), enc1(B^C,k_2), enc1(B^C,k_3), enc1(B^C,k_4)
	simd.VPXORQ(&z1, &z3, &z17)                                                 // z17 := B^D
	simd.VAESENC(&z6, &z17, &z17)                                               // z17 := enc1(B^D,k_1), enc1(B^D,k_2), enc1(B^D,k_3), enc1(B^D,k_4)
	simd.VPXORQ(&z2, &z3, &z18)                                                 // z18 := C^D
	simd.VAESENC(&z6, &z18, &z18)                                               // z18 := enc1(C^D,k_1), enc1(C^D,k_2), enc1(C^D,k_3), enc1(C^D,k_4)
	simd.VPXORQ(&z0, &z1, &z19)                                                 // z19 := A^B
	simd.VAESENC(&z6, &z19, &z19)                                               // z19 := enc1(A^B,k_1), enc1(A^B,k_2), enc1(A^B,k_3), enc1(A^B,k_4)
	simd.VPXORQ(&z0, &z2, &z20)                                                 // z20 := A^C
	simd.VAESENC(&z6, &z20, &z20)                                               // z20 := enc1(A^C,k_1), enc1(A^C,k_2), enc1(A^C,k_3), enc1(A^C,k_4)
	simd.VPXORQ(&z0, &z3, &z21)                                                 // z21 := A^D
	simd.VAESENC(&z6, &z21, &z21)                                               // z21 := enc1(A^D,k_1), enc1(A^D,k_2), enc1(A^D,k_3), enc1(A^D,k_4)
	simd.VAESENC(&z8, &z16, &z16)                                               // z16 := enc2(z16)
	simd.VAESENC(&z8, &z17, &z17)                                               // z17 := enc2(z17)
	simd.VAESENC(&z8, &z18, &z18)                                               // z18 := enc2(z18)
	simd.VAESENC(&z8, &z19, &z19)                                               // z19 := enc2(z19)
	simd.VAESENC(&z8, &z20, &z20)                                               // z20 := enc2(z20)
	simd.VAESENC(&z8, &z21, &z21)                                               // z21 := enc2(z21)
	simd.VPTERNLOGQ(0x96, &z18, &z17, &z16)                                     // z16 := enc(B^C)^enc(B^D)^enc(C^D)
	simd.VPTERNLOGQ(0x96, &z21, &z20, &z19)                                     // z19 := enc(A^B)^enc(A^C)^enc(A^D)
	simd.VPXORQ(&z16, &z19, &z0)                                                // z0  := enc(A^B)^enc(A^C)^enc(A^D)^enc(B^C)^enc(B^D)^enc(C^D)
	// Z0 contains the final result
	return WideHash{z0[0], z0[1], z0[2], z0[3], z0[4], z0[5], z0[6], z0[7]}
}
