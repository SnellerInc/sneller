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
