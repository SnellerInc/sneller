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

package compr

import (
	"bytes"
	"testing"
)

func TestS2(t *testing.T) {
	comp := Compression("s2")
	if _, ok := comp.(s2Compressor); !ok {
		t.Fatalf("bad compressor for s2: %T", comp)
	} else if n := comp.Name(); n != "s2" {
		t.Fatalf("bad compressor name %q", n)
	}
	dec := Decompression("s2")
	if _, ok := dec.(s2Compressor); !ok {
		t.Fatalf("bad decompressor for s2: %T", dec)
	} else if n := dec.Name(); n != "s2" {
		t.Fatalf("bad decompressor name %q", n)
	}
	// test separate buffers
	ctl := bytes.Repeat([]byte("foo"), 1000)
	src := append([]byte(nil), ctl...)
	cmp := comp.Compress(src, nil)
	dst := make([]byte, len(src))
	if err := dec.Decompress(cmp, dst); err != nil {
		t.Error(err)
	} else if string(ctl) != string(dst) {
		t.Error("mismatch")
	}
	// test overlapping buffers
	cmp = comp.Compress(src[10:], src[:8])
	if err := dec.Decompress(cmp[8:], dst[10:]); err != nil {
		t.Error(err)
	} else if string(ctl[10:]) != string(dst[10:]) {
		t.Error("mismatch")
	}
}

func TestOverlaps(t *testing.T) {
	// trivial case
	a := make([]byte, 10)
	b := make([]byte, 20)
	if overlaps(a, b) {
		t.Error("overlaps(a, b) should be false")
	}
	// a and b are adjacent (no overlap)
	a = make([]byte, 10, 30)
	b = a[10:]
	if overlaps(a, b) {
		t.Error("overlaps(a, b) should be false")
	} else if overlaps(b, a) {
		t.Error("overlaps(b, a) should be false")
	}
	// a and b overlap by 5
	b = a[5:]
	if !overlaps(a, b) {
		t.Error("overlaps(a, b) should be true")
	} else if !overlaps(b, a) {
		t.Error("overlaps(b, a) should be true")
	}
	// a and b overlap by 1
	b = a[9:]
	if !overlaps(a, b) {
		t.Error("overlaps(a, b) should be true")
	} else if !overlaps(b, a) {
		t.Error("overlaps(b, a) should be true")
	}
}
