// Copyright (C) 2023 Sneller, Inc.
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

package iguana

import (
	"bytes"
	"testing"
)

func TestANS(t *testing.T) {
	in := []byte("test message 123 test message 456")

	var enc ANSEncoder
	ans, err := enc.Encode(in)
	if err != nil {
		t.Error(err)
		return
	}
	lenIn := len(in)
	lenANS := len(ans)
	ratio := 100.0 * (1.0 - float64(lenANS)/float64(lenIn))
	t.Logf("ANS input size: %d, output size %d, compression ratio %f%%\n", lenIn, lenANS, ratio)
	dec, err := ANSDecode(ans, lenIn)
	if err != nil {
		t.Fatal(err)
	}

	if len(dec) != len(in) {
		t.Fatalf("ans length mismatch, is %d, should be %d\n", len(dec), len(in))
	}
	for i := 0; i != len(in); i++ {
		vi := in[i]
		vd := dec[i]
		if vi != vd {
			t.Fatalf("mismatch at position %d, is 0x%02x, should be 0x%02x\n", i, vd, vi)
		}
	}
}

func FuzzANSRoundtrip(f *testing.F) {
	f.Fuzz(func(t *testing.T, ref []byte) {
		refLen := len(ref)
		var enc ANSEncoder
		compressed, err := enc.Encode(ref)
		if err != nil {
			return // when would this fail?
		}
		decompressed, err := ANSDecode(compressed, refLen)
		if err != nil {
			t.Fatalf("round-trip failed: %s", err)
		}
		if !bytes.Equal(ref, decompressed) {
			t.Fatal("round trip result is not equal to the input")
		}
	})
}
