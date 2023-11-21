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
	"unsafe"

	"golang.org/x/exp/constraints"
)

type HashEngine ExpandedKey128Quad
type WideHash [2 * 4]uint64

type Hashable interface {
	constraints.Integer | WideHash
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
	return aesHash64((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(unsafe.SliceData(in))), len(in)*int(unsafe.Sizeof(in[0])))
}

//go:nosplit
func HashWideSlice[T Hashable](e *HashEngine, in []T) WideHash {
	return aesHashWide((*ExpandedKey128Quad)(e), (*byte)(unsafe.Pointer(unsafe.SliceData(in))), len(in)*int(unsafe.Sizeof(in[0])))
}

// HashCombine combines multiple uint64 hash values into a new one
func HashCombine(e *HashEngine, hashes ...uint64) uint64 {
	return HashSlice(e, hashes)
}

// HashCombineWide combines multiple WideHash values into a new one
func HashCombineWide(e *HashEngine, hashes ...WideHash) WideHash {
	return HashWideSlice(e, hashes)
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
	if err := Volatile.InitializeRandom(); err != nil {
		panic(err) // The crypto/rand RNG can fail in theory and this is the best I can do with err at the init() level.
	}
}

// Volatile is an out-of-the-box HashEngine initialized with a FIPS 140-2-compliant random number generator
var Volatile HashEngine

// Stable is an out-of-the-box HashEngine initialized with hard-coded values. DO NOT ALTER!
var Stable HashEngine = HashEngine{ // Pre-expanded 128-bit key quads
	Key128Quad{
		Key128{0x71032c93, 0xabc264e6, 0x6eb0443e, 0x5ccb9956},
		Key128{0x3b63ebd9, 0xb4c73073, 0x67d7f51b, 0xcedc356c},
		Key128{0xed641267, 0x974d4f29, 0x572f117e, 0x2c0507e8},
		Key128{0x673e49f9, 0x47b42d12, 0xdff6ea71, 0x1dbc8d0f},
	},
	Key128Quad{
		Key128{0x685eaf6f, 0x195d83fc, 0x5a257207, 0x34953639},
		Key128{0x2021d54d, 0x1b423e94, 0x892a153a, 0xeefde021},
		Key128{0x897248cd, 0x64165aaa, 0xf2585e5b, 0xa5774f25},
		Key128{0x8314266d, 0xe42a6f94, 0x415e4113, 0x9ea8ab62},
	},
	Key128Quad{
		Key128{0xb4bd13d1, 0xdce3bcbe, 0xda0d57ef, 0x802825e8},
		Key128{0xbbc40e82, 0x9be5dbcf, 0xdc13fb99, 0x5539eea3},
		Key128{0xd4ed783d, 0x5d9f30f0, 0x83c26943, 0x719a3718},
		Key128{0x1abd1178, 0x99a93715, 0xc54bfb09, 0x8415ba1a},
	},
	Key128Quad{
		Key128{0xb7097830, 0x03b46be1, 0xed2c0a37, 0x37215dd8},
		Key128{0xcea7a846, 0x7563a6c4, 0x478dbd7c, 0x9b9e46e5},
		Key128{0x939e6d30, 0x4773150d, 0x61c6336b, 0xe2045a28},
		Key128{0x82b29566, 0x980f841e, 0xc3ecd475, 0x06a72f7c},
	},
	Key128Quad{
		Key128{0x2ee862be, 0x99e11a8e, 0xf4e53551, 0x19c93f66},
		Key128{0xab6453f0, 0x65c3fbb6, 0x7777a869, 0x30fa1515},
		Key128{0xab02ccf6, 0x389ca1c6, 0x28c0a5b5, 0x490696de},
		Key128{0x8793df38, 0x05214a5e, 0x42d82431, 0x8134f044},
	},
	Key128Quad{
		Key128{0x66b6a1b6, 0x485ec308, 0x8b9aab81, 0x7f7f9ed0},
		Key128{0x37525d94, 0x9c360e64, 0x70dfe0e8, 0x07a84881},
		Key128{0x46aad726, 0xeda81bd0, 0x276ce44d, 0x0fac41f8},
		Key128{0x459ef440, 0xc20d2b78, 0x86722035, 0xc4aa0404},
	},
	Key128Quad{
		Key128{0x22d5dafc, 0x44637b4a, 0xd630efad, 0x5daa442c},
		Key128{0x3035cd31, 0x076790a5, 0x47426558, 0x379d85b0},
		Key128{0x48ce9cbf, 0x0e644b99, 0x600e390a, 0x4762dd47},
		Key128{0x11d79faf, 0x54496bef, 0x530fbb9e, 0xd57d9bab},
	},
	Key128Quad{
		Key128{0xf340a529, 0xd1957fd5, 0x78da0ea8, 0xaeeae105},
		Key128{0x5df27199, 0x6dc7bca8, 0x2d2d9171, 0x6a6ff429},
		Key128{0x217a7c74, 0x69b4e0cb, 0x06169839, 0x6618a133},
		Key128{0x4e7382ea, 0x5fa41d45, 0xc801a2df, 0x9b0e1941},
	},
	Key128Quad{
		Key128{0xe0698dd7, 0x132928fe, 0x3659627a, 0x4e836cd2},
		Key128{0x93ed1530, 0xce1f64a9, 0xd4af7068, 0xf982e119},
		Key128{0x5aa1db79, 0x7bdba70d, 0x3aafe273, 0x3cb97a4a},
		Key128{0x8f3fe315, 0xc14c61ff, 0xdc30588b, 0x1431fa54},
	},
	Key128Quad{
		Key128{0x29456e98, 0xc92ce34f, 0x519f6030, 0x67c6024a},
		Key128{0x18ba08d3, 0x8b571de3, 0x359799a2, 0xe138e9ca},
		Key128{0xaefa05f7, 0xf45bde8e, 0xa8ec9dce, 0x92437fbd},
		Key128{0x73f5869b, 0xfcca658e, 0xbbf42444, 0x67c47ccf},
	},
	Key128Quad{
		Key128{0xe4b85631, 0xcdfd38a9, 0xd2e1344b, 0x837e547b},
		Key128{0xa97b9de8, 0xb1c1953b, 0x7dd4ed80, 0x48437422},
		Key128{0xd1631511, 0x7f9910e6, 0xebccf762, 0x43206aac},
		Key128{0x94d55c15, 0xe720da8e, 0x48e5049e, 0xf31120da},
	},
}
