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

// This file provides a family of efficient hardened hash functions based on
// the accelerated AES primitives. These functions have been designed solely
// with high performance and excellent diffusion characteristics in mind.
// While the functions should be exceedingly resistant to preimage attacks
// due to the non-linear AES mixing properties, no cryptographic strenght
// should be assumed. Every function comes in two flavours: one returns
// a 64-bit hash value, the other exposes the full 64-byte WideHash value.
// The latter can be used in places where more than one hash function should
// be computed for a given input: Bloom filters, cuckoo hash tables, etc.
// Computing a single wide hash function and splitting the result into up to
// eight 64-bit chunks is technically equivalent to computing that many
// individual 64-bit functions, so the performance gain can be substantial.
// Both the narrow and wide variants are Level 2 hash functions per the
// http://nohatcoder.dk/2019-05-19-1.html#level3 taxonomy: a key is a part
// of the input. While a user can create an arbitrary number of the context-setting
// keys (henceforth referred to as HashEngines), two hash engines have been
// provided out of the box: Stable and Volatile. Stable uses hard-coded
// initialization values, which effectively reduces it to a Level-1 jellybean
// hash function: for a given input, the resulting hash value is guaranteed
// to remain stable across multiple runs of the program, both on the same and different
// machines. This enables distributed computing applications and consistent
// debugging, but a chosen plaintext attack can be trivially crafted for the
// same reason. The Volatile HashEngine has the opposite properties: at the
// start of the program it is initialized with random values produced by
// a FIPS 140-2-compliant random number generator. The resulting unpredictability
// of the hash value of a given input mitigates the vulnerability of an adversary
// tampering with input data, but can make debugging of hashing-based data structures
// more difficult if a repeatable shape is expected by the programmer. Observe that
// all the hash variants (narrow/wide, Stable/Volatile/user supplied) are
// internally produced by a single unified engine, so the performance should be
// the same in all cases. The choice should thus be based solely on the expected
// characteristics of the hash function. The Volatile variant is suggested as
// a default due to its superior security properties that come at no cost.

package aes

import (
	"reflect"
	"unsafe"

	"golang.org/x/exp/constraints"
	"golang.org/x/sys/cpu"
)

type HashEngine ExpandedKey128Quad
type WideHash [2 * 4]uint64

type Hashable interface {
	constraints.Integer
}

//go:nosplit
func Hash[T Hashable](e *HashEngine, v T) uint64 {
	return aesHash64((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(&v)), int(unsafe.Sizeof(v)))
}

//go:nosplit
func HashWide[T Hashable](e *HashEngine, v T) WideHash {
	return aesHashWide((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(&v)), int(unsafe.Sizeof(v)))
}

//go:nosplit
func HashSlice[T Hashable](e *HashEngine, in []T) uint64 {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&in)) // Let the empty slice case be handled in assembly
	return aesHash64((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(hdr.Data)), hdr.Len*int(unsafe.Sizeof(in[0])))
}

//go:nosplit
func HashWideSlice[T Hashable](e *HashEngine, in []T) WideHash {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&in)) // Let the empty slice case be handled in assembly
	return aesHashWide((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(hdr.Data)), hdr.Len*int(unsafe.Sizeof(in[0])))
}

// Initialize initializes the hash engine e according to the provided initialization key quad
func (e *HashEngine) Initialize(quad Key128Quad) {
	(*ExpandedKey128Quad)(e).ExpandFrom(quad)
}

// InitializeRandom initializes the hash engine e with a randomly generated initialization vector
func (e *HashEngine) InitializeRandom() error {
	quad, err := RandomKey128Quad()
	if err != nil {
		return err
	}
	e.Initialize(quad)
	return err
}

func init() {
	aesInitHashEngine()

	if err := Volatile.InitializeRandom(); err != nil {
		panic(err) // The crypto/rand RNG can fail in theory and this is the best I can do with err at the init() level.
	}
}

const offsX86HasAVX512VAES = unsafe.Offsetof(cpu.X86.HasAVX512VAES) //lint:ignore U1000, used in asm

//go:noescape
//go:nosplit
func aesInitHashEngine()

//go:noescape
//go:nosplit
func aesHash64(quad *ExpandedKey128Quad, p *byte, n int) uint64

//go:noescape
//go:nosplit
func aesHashWide(quad *ExpandedKey128Quad, p *byte, n int) WideHash

// Volatile is an out-of-the-box HashEngine initialized with a FIPS 140-2-compliant random number generator
var Volatile HashEngine

// Stable is an out-of-the-box HashEngine initialized with hard-coded values. DO NOT ALTER!
var Stable HashEngine = HashEngine{ // Pre-expanded 128-bit key quads
	Key128Quad{
		Key128{0x71032c93abc264e6, 0x6eb0443e5ccb9956},
		Key128{0x3b63ebd9b4c73073, 0x67d7f51bcedc356c},
		Key128{0xed641267974d4f29, 0x572f117e2c0507e8},
		Key128{0x673e49f947b42d12, 0xdff6ea711dbc8d0f},
	},
	Key128Quad{
		Key128{0x685eaf6f195d83fc, 0x5a25720734953639},
		Key128{0x2021d54d1b423e94, 0x892a153aeefde021},
		Key128{0x897248cd64165aaa, 0xf2585e5ba5774f25},
		Key128{0x8314266de42a6f94, 0x415e41139ea8ab62},
	},
	Key128Quad{
		Key128{0xb4bd13d1dce3bcbe, 0xda0d57ef802825e8},
		Key128{0xbbc40e829be5dbcf, 0xdc13fb995539eea3},
		Key128{0xd4ed783d5d9f30f0, 0x83c26943719a3718},
		Key128{0x1abd117899a93715, 0xc54bfb098415ba1a},
	},
	Key128Quad{
		Key128{0xb709783003b46be1, 0xed2c0a3737215dd8},
		Key128{0xcea7a8467563a6c4, 0x478dbd7c9b9e46e5},
		Key128{0x939e6d304773150d, 0x61c6336be2045a28},
		Key128{0x82b29566980f841e, 0xc3ecd47506a72f7c},
	},
	Key128Quad{
		Key128{0x2ee862be99e11a8e, 0xf4e5355119c93f66},
		Key128{0xab6453f065c3fbb6, 0x7777a86930fa1515},
		Key128{0xab02ccf6389ca1c6, 0x28c0a5b5490696de},
		Key128{0x8793df3805214a5e, 0x42d824318134f044},
	},
	Key128Quad{
		Key128{0x66b6a1b6485ec308, 0x8b9aab817f7f9ed0},
		Key128{0x37525d949c360e64, 0x70dfe0e807a84881},
		Key128{0x46aad726eda81bd0, 0x276ce44d0fac41f8},
		Key128{0x459ef440c20d2b78, 0x86722035c4aa0404},
	},
	Key128Quad{
		Key128{0x22d5dafc44637b4a, 0xd630efad5daa442c},
		Key128{0x3035cd31076790a5, 0x47426558379d85b0},
		Key128{0x48ce9cbf0e644b99, 0x600e390a4762dd47},
		Key128{0x11d79faf54496bef, 0x530fbb9ed57d9bab},
	},
	Key128Quad{
		Key128{0xf340a529d1957fd5, 0x78da0ea8aeeae105},
		Key128{0x5df271996dc7bca8, 0x2d2d91716a6ff429},
		Key128{0x217a7c7469b4e0cb, 0x061698396618a133},
		Key128{0x4e7382ea5fa41d45, 0xc801a2df9b0e1941},
	},
	Key128Quad{
		Key128{0xe0698dd7132928fe, 0x3659627a4e836cd2},
		Key128{0x93ed1530ce1f64a9, 0xd4af7068f982e119},
		Key128{0x5aa1db797bdba70d, 0x3aafe2733cb97a4a},
		Key128{0x8f3fe315c14c61ff, 0xdc30588b1431fa54},
	},
	Key128Quad{
		Key128{0x29456e98c92ce34f, 0x519f603067c6024a},
		Key128{0x18ba08d38b571de3, 0x359799a2e138e9ca},
		Key128{0xaefa05f7f45bde8e, 0xa8ec9dce92437fbd},
		Key128{0x73f5869bfcca658e, 0xbbf4244467c47ccf},
	},
	Key128Quad{
		Key128{0xe4b85631cdfd38a9, 0xd2e1344b837e547b},
		Key128{0xa97b9de8b1c1953b, 0x7dd4ed8048437422},
		Key128{0xd16315117f9910e6, 0xebccf76243206aac},
		Key128{0x94d55c15e720da8e, 0x48e5049ef31120da},
	},
}
