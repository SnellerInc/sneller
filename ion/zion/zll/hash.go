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
