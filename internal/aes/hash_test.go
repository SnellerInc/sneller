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

package aes

import (
	"reflect"
	"testing"
)

func TestKeyExpand(t *testing.T) {
	key := Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00}
	var ek ExpandedKey128
	ek.ExpandFrom(key)

	refek := ExpandedKey128{
		Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
		Key128{0x72e31b53, 0x27856cdb, 0xbe2fd717, 0x63c12817},
		Key128{0x82186365, 0xa59d0fbe, 0x1bb2d8a9, 0x7873f0be},
		Key128{0x2ca4eced, 0x8939e353, 0x928b3bfa, 0xeaf8cb44},
		Key128{0x3723adfa, 0xbe1a4ea9, 0x2c917553, 0xc669be17},
		Key128{0xc7975444, 0x798d1aed, 0x551c6fbe, 0x9375d1a9},
		Key128{0x144bc95a, 0x6dc6d3b7, 0x38dabc09, 0xabaf6da0},
		Key128{0xf429b026, 0x99ef6391, 0xa135df98, 0x0a9ab238},
		Key128{0xf34e0891, 0x6aa16b00, 0xcb94b498, 0xc10e06a0},
		Key128{0x1336a3e5, 0x7997c8e5, 0xb2037c7d, 0x730d7add},
		Key128{0xd2b97409, 0xab2ebcec, 0x192dc091, 0x6a20ba4c},
	}

	if !reflect.DeepEqual(ek, refek) {
		t.Fatal("result mismatch")
	}
}

func TestKeyQuadExpand(t *testing.T) {
	quad := Key128Quad{
		Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
		Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
		Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
		Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
	}

	var eq ExpandedKey128Quad
	eq.ExpandFrom(quad)

	refeq := ExpandedKey128Quad{
		Key128Quad{
			Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
			Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
			Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
			Key128{0x11223344, 0x55667788, 0x99aabbcc, 0xddeeff00},
		},
		Key128Quad{
			Key128{0x72e31b53, 0x27856cdb, 0xbe2fd717, 0x63c12817},
			Key128{0x72e31b53, 0x27856cdb, 0xbe2fd717, 0x63c12817},
			Key128{0x72e31b53, 0x27856cdb, 0xbe2fd717, 0x63c12817},
			Key128{0x72e31b53, 0x27856cdb, 0xbe2fd717, 0x63c12817},
		},
		Key128Quad{
			Key128{0x82186365, 0xa59d0fbe, 0x1bb2d8a9, 0x7873f0be},
			Key128{0x82186365, 0xa59d0fbe, 0x1bb2d8a9, 0x7873f0be},
			Key128{0x82186365, 0xa59d0fbe, 0x1bb2d8a9, 0x7873f0be},
			Key128{0x82186365, 0xa59d0fbe, 0x1bb2d8a9, 0x7873f0be},
		},
		Key128Quad{
			Key128{0x2ca4eced, 0x8939e353, 0x928b3bfa, 0xeaf8cb44},
			Key128{0x2ca4eced, 0x8939e353, 0x928b3bfa, 0xeaf8cb44},
			Key128{0x2ca4eced, 0x8939e353, 0x928b3bfa, 0xeaf8cb44},
			Key128{0x2ca4eced, 0x8939e353, 0x928b3bfa, 0xeaf8cb44},
		},
		Key128Quad{
			Key128{0x3723adfa, 0xbe1a4ea9, 0x2c917553, 0xc669be17},
			Key128{0x3723adfa, 0xbe1a4ea9, 0x2c917553, 0xc669be17},
			Key128{0x3723adfa, 0xbe1a4ea9, 0x2c917553, 0xc669be17},
			Key128{0x3723adfa, 0xbe1a4ea9, 0x2c917553, 0xc669be17},
		},
		Key128Quad{
			Key128{0xc7975444, 0x798d1aed, 0x551c6fbe, 0x9375d1a9},
			Key128{0xc7975444, 0x798d1aed, 0x551c6fbe, 0x9375d1a9},
			Key128{0xc7975444, 0x798d1aed, 0x551c6fbe, 0x9375d1a9},
			Key128{0xc7975444, 0x798d1aed, 0x551c6fbe, 0x9375d1a9},
		},
		Key128Quad{
			Key128{0x144bc95a, 0x6dc6d3b7, 0x38dabc09, 0xabaf6da0},
			Key128{0x144bc95a, 0x6dc6d3b7, 0x38dabc09, 0xabaf6da0},
			Key128{0x144bc95a, 0x6dc6d3b7, 0x38dabc09, 0xabaf6da0},
			Key128{0x144bc95a, 0x6dc6d3b7, 0x38dabc09, 0xabaf6da0},
		},
		Key128Quad{
			Key128{0xf429b026, 0x99ef6391, 0xa135df98, 0x0a9ab238},
			Key128{0xf429b026, 0x99ef6391, 0xa135df98, 0x0a9ab238},
			Key128{0xf429b026, 0x99ef6391, 0xa135df98, 0x0a9ab238},
			Key128{0xf429b026, 0x99ef6391, 0xa135df98, 0x0a9ab238},
		},
		Key128Quad{
			Key128{0xf34e0891, 0x6aa16b00, 0xcb94b498, 0xc10e06a0},
			Key128{0xf34e0891, 0x6aa16b00, 0xcb94b498, 0xc10e06a0},
			Key128{0xf34e0891, 0x6aa16b00, 0xcb94b498, 0xc10e06a0},
			Key128{0xf34e0891, 0x6aa16b00, 0xcb94b498, 0xc10e06a0},
		},
		Key128Quad{
			Key128{0x1336a3e5, 0x7997c8e5, 0xb2037c7d, 0x730d7add},
			Key128{0x1336a3e5, 0x7997c8e5, 0xb2037c7d, 0x730d7add},
			Key128{0x1336a3e5, 0x7997c8e5, 0xb2037c7d, 0x730d7add},
			Key128{0x1336a3e5, 0x7997c8e5, 0xb2037c7d, 0x730d7add},
		},
		Key128Quad{
			Key128{0xd2b97409, 0xab2ebcec, 0x192dc091, 0x6a20ba4c},
			Key128{0xd2b97409, 0xab2ebcec, 0x192dc091, 0x6a20ba4c},
			Key128{0xd2b97409, 0xab2ebcec, 0x192dc091, 0x6a20ba4c},
			Key128{0xd2b97409, 0xab2ebcec, 0x192dc091, 0x6a20ba4c},
		},
	}

	if !reflect.DeepEqual(eq, refeq) {
		t.Fatal("result mismatch")
	}
}

func TestKeyHashWideSlice(t *testing.T) {
	{
		input := []uint8("")
		h := HashWideSlice(&Stable, input)
		refh := WideHash{
			0xcdfd38a9e4b85631, 0x837e547bd2e1344b,
			0xb1c1953ba97b9de8, 0x484374227dd4ed80,
			0x7f9910e6d1631511, 0x43206aacebccf762,
			0xe720da8e94d55c15, 0xf31120da48e5049e,
		}

		if !reflect.DeepEqual(h, refh) {
			t.Fatalf("mismatch:\nis:\n%016x\nshould be:\n%016x\n", h, refh)
		}
	}

	{
		input := []uint8("The quick brown fox jumps over the lazy dog")
		h := HashWideSlice(&Stable, input)
		refh := WideHash{
			0x7c4d79ce2d50e896, 0x4ff2cecca076e16f,
			0xeb421096d0c53799, 0x13f879fb39ff854e,
			0x4dc7aaa512cb3639, 0xbe0e9462889e5fce,
			0xb3b0818059f2b527, 0x4b86af261fe48207,
		}

		if !reflect.DeepEqual(h, refh) {
			t.Fatalf("mismatch:\nis:\n%016x\nshould be:\n%016x\n", h, refh)
		}
	}

	{
		input := []uint8("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")
		h := HashWideSlice(&Stable, input)
		refh := WideHash{
			0x74d971fb078528b3, 0xb2c00325ecd6ad95,
			0x661995291948b8c1, 0xa0dc94923b324fa2,
			0x726b92c3badad3ee, 0x21a0ffd8da11f2c3,
			0xfec831eec00d24dd, 0xfaf0ff1f5e95981e,
		}

		if !reflect.DeepEqual(h, refh) {
			t.Fatalf("mismatch:\nis:\n%016x\nshould be:\n%016x\n", h, refh)
		}
	}

	{
		longInput := make([]uint8, 1024*1024) // A really long input to ensure all the internal triggers have been activated
		h := HashWideSlice(&Stable, longInput)
		refh := WideHash{
			0x74b01d4f66f9ae67, 0x2423b332928532c5,
			0x38542f498c87eedb, 0xcb3ff46e0415bbe9,
			0x7715257cd7159b9c, 0xef4a95d7e0680396,
			0x28cf65689b94b428, 0x4e0b65e0ecd27a24,
		}

		if !reflect.DeepEqual(h, refh) {
			t.Fatalf("mismatch:\nis:\n%016x\nshould be:\n%016x\n", h, refh)
		}
	}
}
