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

package vm

import (
	"math/bits"
	"unsafe"
)

// see chacha-20080128.pdf section 2.2
func qround(a, b, c, d int, state *[16]uint32) {
	state[a] += state[b]
	state[d] = bits.RotateLeft32(state[d]^state[a], 16)
	state[c] += state[d]
	state[b] = bits.RotateLeft32(state[b]^state[c], 12)
	state[a] += state[b]
	state[d] = bits.RotateLeft32(state[d]^state[a], 8)
	state[c] += state[d]
	state[b] = bits.RotateLeft32(state[b]^state[c], 7)
}

func chachainit(state *[16]uint32, l uint32) {
	state[0] = 0x9722F977 ^ l
	state[1] = 0x3320646e
	state[2] = 0x79622d32
	state[3] = 0x6b206574
	state[4] = 0x058A60F5
	state[5] = 0xB25F6FB1
	state[6] = 0x1FEFA3D9
	state[7] = 0xB9D8F520
	state[8] = 0xB415DBCC
	state[9] = 0x34B70366
	state[10] = 0x3F4DBB4D
	state[11] = 0xCBB67392
	state[12] = 0x61707865
	state[13] = 0x143BE9F6
	state[14] = 0xDA97A1A8
	state[15] = 0x6F0E9495
}

// 2 rounds of core chacha
// https://cr.yp.to/chacha/chacha-20080128.pdf
// see section 3.2 for the state matrix
func chacha2(state *[16]uint32) {
	// note the straightforward vectorization:
	//  v1 = [0, 1, 2, 3], v2 = [4, 5, 6, 7], ...
	qround(0, 4, 8, 12, state)
	qround(1, 5, 9, 13, state)
	qround(2, 6, 10, 14, state)
	qround(3, 7, 11, 15, state)
	qround(0, 5, 10, 15, state)
	qround(1, 6, 11, 12, state)
	qround(2, 7, 8, 13, state)
	qround(3, 4, 9, 14, state)
}

func chacha8(state *[16]uint32, input []byte) int {
	var inner [16]uint32
	copy(inner[:], state[:])
	bstate := (*[64]byte)(unsafe.Pointer(&inner[0]))
	l := xorcopy(bstate[16:], input)
	rounds8(&inner)
	for i := 0; i < 16; i++ {
		state[i] += inner[i]
	}
	return l
}

func rounds8(state *[16]uint32) {
	for i := 0; i < 4; i++ {
		chacha2(state)
	}
}

// dst ^= src
func xorcopy(dst, src []byte) int {
	l := len(dst)
	if len(src) < l {
		l = len(src)
	}
	for i := 0; i < l; i++ {
		dst[i] ^= src[i]
	}
	return l
}

//go:noescape
func chacha8x4(base *byte, ends [4]uint32) [4][16]byte

// Contrive a hash function by creating a sponge
// from chacha8 as out core permutation function.
// If we treat chacha8 as a 512-bit permutation,
// we can use capacity=128=16b and rate=384=48b.
// (Note that typically the capacity can't be less than
// the implied security margin of the construction.
// We don't imply any security here, but we want a full
// 128 bit PRF, so we'll stick to those guidelines.)
//
// This is pretty similar to the HChaCha20 KDF, except
// we are using only 8 rounds and we use the input as
// the key + nonce.
//
// see https://keccak.team/sponge_duplex.html
// for details on sponge capacity + rate tradeoffs

// chacha8Hash uses Chacha8 as a permutation function
// to construct a 128-bit hash of 'buf'
func chacha8Hash(buf []byte, out []byte) {
	var state [16]uint32
	chachainit(&state, uint32(len(buf)))

	bstate := (*[64]byte)(unsafe.Pointer(&state[0]))
	for len(buf) > 0 {
		buf = buf[chacha8(&state, buf):]
	}
	copy(out[:16], bstate[:])
}

// chacha8HashSeed is like Chacha8 but uses a 16-byte seed
// to populate part of the IV for the hash.
func chacha8HashSeed(buf []byte, out []byte, seed []byte) {
	if len(seed) != 16 {
		panic("bad len(seed)")
	}
	var state [16]uint32
	chachainit(&state, uint32(len(buf)))
	bstate := (*[64]byte)(unsafe.Pointer(&state[0]))
	xorcopy(bstate[:], seed)

	for len(buf) > 0 {
		buf = buf[chacha8(&state, buf):]
	}
	copy(out[:16], bstate[:])
}
