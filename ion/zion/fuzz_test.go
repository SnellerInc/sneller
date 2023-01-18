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
