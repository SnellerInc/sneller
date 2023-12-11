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

package zion

import (
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func FuzzZip1(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x90, 0x84, 'a', 'b', 'c', 'd'})
	f.Add([]byte{0x91, 0x85, 'a', 'b', 'c', 'd', 'e'})
	f.Add([]byte{0x91, 0x85, 'a', 'b', 'c', 'd', 'e', 0x92, 0x0f})
	f.Add([]byte{0x91, 0x85, 'a', 'b', 'c', 'd', 'e', 0x92, 0x11})
	f.Add([]byte{0x91, 0x85, 'a', 'b', 'c', 'd', 'e', 0x92, 0x10})
	f.Add([]byte{0x91, 0x11, 0x92, 0x85, 'a', 'b', 'c', 'd', 'e'})
	f.Add([]byte{0x90, 0x80})
	f.Fuzz(func(t *testing.T, b []byte) {
		n := 0
		var sym ion.Symbol
		var syms []ion.Symbol
		var err error

		todo := b
		for len(todo) > 0 {
			sym, todo, err = ion.ReadLabel(todo)
			if err != nil {
				return // invalid ion / not supported
			}
			s := ion.SizeOf(todo)
			if s <= 0 || s > len(todo) {
				return // invalid ion / not supported
			}
			syms = append(syms, sym)
			todo = todo[s:]
			n++
		}
		syms = append(syms, 0x23147) // a random non-matching symbol
		dstsize := (class(len(b))+1)*n + len(b) + 7
		dst := make([]byte, dstsize)
		var d Decoder
		for i := range syms {
			d.fault = 0
			consumed, wrote := zipfast1(b, dst, &d, syms[i], n)
			if consumed > len(b) {
				t.Fatalf("consumed %d > %d", consumed, len(b))
			}
			if wrote > len(dst) {
				t.Fatalf("wrote %d > %d", wrote, len(dst))
			}
		}
	})
}
