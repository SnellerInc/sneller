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

// Package aes provides access to the hardware AES encryption/decryption accelerator
// and supports basic key expansion functionality. The low-level primitives are
// subsequently used to construct hardened building blocks like a family of hash functions.
package aes

import (
	"github.com/SnellerInc/sneller/ints"
)

// Key128 represents a 128-bit AES key
type Key128 [4]uint32

// Key128Quad represents a quad of 128-bit AES keys. Quads enable the full potential
// of the four-lane hardware AES encryption/decryption support; otherwise the operations
// on quads are fully equivalent to four independent regular AES operations, just more
// efficient. No assumption is made about the correlation of the keys: it is up to the
// user to decide if the keys should be the same or different in a given application.
type Key128Quad [4]Key128

// ExpandedKey128 stores the 11 round keys produced by the AES key expansion algorithm.
type ExpandedKey128 [11]Key128

// ExpandedKey128Quad stores a quad of the 11 round keys produced by the AES key expansion algorithm.
type ExpandedKey128Quad [11]Key128Quad

// ExpandFrom takes a Key128 key and expands it into 11 round keys
func (p *ExpandedKey128) ExpandFrom(key Key128) { auxExpandFromKey128(p, key) }

// ExpandFrom takes a Key128 quad and expands them into a quad of 11 round keys
func (p *ExpandedKey128Quad) ExpandFrom(quad Key128Quad) { auxExpandFromKey128Quad(p, quad) }

// ToQuad broadcast key to form a key quad
func (key Key128) ToQuad() Key128Quad { return Key128Quad{key, key, key, key} }

// RandomKey128 creates a 128-bit key with cryptographically strong RNG values
func RandomKey128() (Key128, error) {
	var key Key128
	err := ints.RandomFillSlice(key[:])
	return key, err
}

// RandomKey128Quad creates a 128-bit key quad with cryptographically strong RNG values
func RandomKey128Quad() (Key128Quad, error) {
	var quad Key128Quad
	var err error
	for i := range quad {
		quad[i], err = RandomKey128()
		if err != nil {
			break
		}
	}
	return quad, err
}
