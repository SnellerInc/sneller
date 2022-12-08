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

package zll

import (
	"encoding/binary"

	"github.com/SnellerInc/sneller/ion"

	"github.com/dchest/siphash"
)

const (
	BucketBits = 4
	NumBuckets = 1 << BucketBits
	BucketMask = NumBuckets - 1
)

// Hash64 hashes a symbol using a 32-bit seed.
func Hash64(seed uint32, sym ion.Symbol) uint64 {
	var buf [9]byte
	size := binary.PutUvarint(buf[:], uint64(sym))
	return siphash.Hash(0, uint64(seed), buf[:size])
}

// SymbolBucket maps an ion symbol to a bucket
// using a specific hash seed and bit-selector.
func SymbolBucket(seed uint32, selector uint8, sym ion.Symbol) int {
	return int((Hash64(seed, sym) >> (selector * BucketBits)) & BucketMask)
}
