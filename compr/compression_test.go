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
