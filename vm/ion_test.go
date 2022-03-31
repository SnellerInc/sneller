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

package vm

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestSymbolEncoded(t *testing.T) {
	tcs := []int{
		1,
		127,
		128,
		129,
		255,
		256,
		257,
		511,
		512,
		1023,
		1024,
		1025,
	}

	var buf [4]byte
	for i := range tcs {
		buf = [4]byte{}
		sym := ion.Symbol(tcs[i])

		enc, mask, size := encoded(sym)
		if enc&^mask != 0 {
			t.Errorf("encoded(%d): mask %x?", sym, mask)
		}
		binary.LittleEndian.PutUint32(buf[:], enc)

		val, esize := uvint(buf[:])
		if val != uint(sym) {
			t.Errorf("encoded(%d): %x", sym, buf[:])
			t.Errorf("decoded as %d", val)
		}
		if mask != (1<<(esize*8))-1 {
			t.Errorf("esize %d but mask %x", esize, mask)
		}
		if esize != int(size) {
			t.Errorf("got size %d but expected %d", size, esize)
		}
	}
}

func TestCopyObject(t *testing.T) {
	buf := unhex(parkingCitations1KLines)
	if ion.IsBVM(buf) {
		buf = buf[4:]
	}

	out := make([]byte, len(buf))

	src := buf
	off := 0
	for off < len(src) {
		off += copyobj(out[off:], src[off:])
	}
	if !bytes.Equal(buf, out) {
		t.Fatal("didn't produce an identical object?")
	}
}

func BenchmarkCopy1KObjects(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	if ion.IsBVM(buf) {
		buf = buf[4:]
	}
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		out := make([]byte, len(buf))
		for pb.Next() {
			off := 0
			for off < len(buf) {
				off += copyobj(out[off:], buf[off:])
			}
		}
	})
}
