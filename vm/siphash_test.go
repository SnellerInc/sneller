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

package vm

import (
	"crypto/rand"
	mrand "math/rand"
	"testing"

	"github.com/dchest/siphash"
)

func TestSiphash(t *testing.T) {
	buf := make([]byte, 256)
	rand.Read(buf)
	k0, k1 := uint64(mrand.Uint64()), uint64(mrand.Uint64())
	inner := func(t *testing.T, ends [8]uint32) {
		t.Helper()
		got := siphashx8(k0, k1, &buf[0], &ends)
		for i := 0; i < 8; i++ {
			off := 0
			if i > 0 {
				off = int(ends[i-1])
			}
			mem := buf[off:ends[i]]
			lo, hi := siphash.Hash128(k0, k1, mem)
			gotlo, gothi := got[0][i], got[1][i]
			if lo != gotlo || hi != gothi {
				t.Errorf("got (%x, %x) want (%x, %x) for %#x", lo, hi, gotlo, gothi, mem)
			}
		}
	}

	// test the zero-length input
	t.Run("zero", func(t *testing.T) {
		var ends [8]uint32
		inner(t, ends)
	})
	// exactly 8-byte values in each lane
	t.Run("ref8", func(t *testing.T) {
		var ends [8]uint32
		for i := range ends {
			ends[i] = uint32((i + 1) * 8)
		}
		inner(t, ends)
	})
	// different sizes in each lane
	t.Run("multi", func(t *testing.T) {
		var ends [8]uint32
		ends[0] = 0
		ends[1] = 9
		ends[2] = 9 + 15
		ends[3] = 9 + 15 + 25
		ends[4] = 9 + 15 + 25 + 31
		ends[5] = 250
		ends[6] = 251
		ends[7] = 256
		inner(t, ends)
	})

	// create a large random test corpus
	// and test the results against the
	// portable reference implementation
	t.Run("random-cases", func(t *testing.T) {
		for rounds := 0; rounds < 1000; rounds++ {
			var ends [8]uint32
			prev := 0
			for i := range ends {
				n := mrand.Intn(len(buf)-prev) + prev
				ends[i] = uint32(n)
				prev = n
			}
			inner(t, ends)
			if t.Failed() {
				break
			}
		}
	})
}
