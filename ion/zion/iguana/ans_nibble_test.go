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

package iguana

import (
	"bytes"
	"testing"
)

func TestANSNibble(t *testing.T) {
	in := []byte("test message 123 test message 456")

	var enc ANSNibbleEncoder
	ans, err := enc.Encode(in)
	if err != nil {
		t.Error(err)
		return
	}
	lenIn := len(in)
	lenANS := len(ans)
	ratio := 100.0 * (1.0 - float64(lenANS)/float64(lenIn))
	t.Logf("ANS input size: %d, output size %d, compression ratio %f%%\n", lenIn, lenANS, ratio)
	dec, err := ANSNibbleDecode(ans, lenIn)
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

func FuzzANSNibbleRoundtrip(f *testing.F) {
	f.Fuzz(func(t *testing.T, ref []byte) {
		refLen := len(ref)
		var enc ANSNibbleEncoder
		compressed, err := enc.Encode(ref)
		if err != nil {
			return // when would this fail?
		}
		decompressed, err := ANSNibbleDecode(compressed, refLen)
		if err != nil {
			t.Fatalf("round-trip failed: %s", err)
		}
		if !bytes.Equal(ref, decompressed) {
			t.Fatal("round trip result is not equal to the input")
		}
	})
}
