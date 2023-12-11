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

//go:build linux

package zion

import (
	"fmt"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"
)

func pad8(buf []byte) []byte {
	l := (len(buf) + 8) & 7
	return slices.Grow(buf, l)
}

func TestNoOverwrite(t *testing.T) {
	const pagesize = 4096
	mem, err := syscall.Mmap(-1, 0, pagesize*2, syscall.PROT_NONE, syscall.MAP_ANONYMOUS|syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mem)
	err = syscall.Mprotect(mem[:pagesize], syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		t.Fatal(err)
	}

	b0 := []byte{0x8a, 0x21, 0x03, 0, 0, 0, 0, 0}
	d := Decoder{}
	d.buckets.Shape = &d.shape
	d.buckets.Decompressed = b0
	d.st.components = []component{
		{"0x8a", ion.Symbol(0xa)},
	}
	d.buckets.SelectSymbols([]ion.Symbol{0xa})
	d.components = []string{"0x8a"}
	d.buckets.Pos[0] = 0
	for i := 1; i < zll.NumBuckets; i++ {
		d.buckets.Pos[i] = -1
	}
	d.buckets.BucketBits = 1
	shape := pad8([]byte{(0x1 | (0x00 << 6)), 0x0}) // one element; bucket 0

	// first, assert that we get an error (and no SIGSEGV)
	// when there aren't enough bytes:
	const outsize = 4
	for i := 0; i < outsize; i++ {
		dst := mem[pagesize-i : pagesize]
		consumed, wrote := zipfast(shape, dst, &d)
		if consumed != 0 {
			t.Errorf("with %d bytes available, consumed %d", i, consumed)
		}
		if wrote != 0 {
			t.Errorf("with %d bytes available, wrote %d", i, wrote)
		}
		if d.fault != faultTooLarge {
			t.Errorf("with %d remaining: unexpected fault %d", i, d.fault)
		}
	}

	// we are writing fewer than 8 bytes,
	// so the decoder assembly code should
	// fall back to 1-byte copying and *not*
	// trigger a fault here
	dst := mem[pagesize-outsize : pagesize]
	consumed, wrote := zipfast(shape, dst, &d)
	if d.fault != 0 {
		t.Fatalf("fault %d", d.fault)
	}
	if d.base[0] != 3 {
		t.Errorf("bucket 0 base is %d?", d.base[0])
	}
	if wrote != outsize {
		t.Errorf("wrote %d instead of %d?", wrote, outsize)
		t.Errorf("got %x", dst[:wrote])
	}
	if consumed != len(shape) {
		t.Errorf("consumed %d of %d?", consumed, len(shape))
	}
}

func TestZipFast1(t *testing.T) {
	testcases := []struct {
		ion      []byte
		dsc      string
		fault    fault
		consumed int
		wrote    int
		output   []byte // used if procedure didn't set any error
		dstsize  int    // non-default dst size
		count    int    // non-default count
		setcount bool
	}{
		{
			dsc:   "no varuint at the beginning",
			ion:   pad8([]byte{0x0a}),
			fault: faultBadData,
		},
		{
			dsc:      "XXX: only label followed by 1-byte NOP",
			ion:      pad8([]byte{0x85, 0x00}),
			fault:    noFault, // should be faultBadData?
			consumed: 2,
			wrote:    5,
			// we're looking for symbol 0xa, not 0x5, thus we output 5 x 0xd0 {empty structs}
			output: []byte{0xd0, 0xd0, 0xd0, 0xd0, 0xd0},
		},
		{
			dsc:   "no object length after struct marker",
			ion:   pad8([]byte{0x85, 0xde}),
			fault: faultBadData,
		},
		{
			dsc:   "object size is 248, but input has at most 8 bytes",
			ion:   pad8([]byte{0x85, 0xde, 0x02, 0x82}),
			fault: faultTruncated,
		},
		{
			dsc: "copy object (symbol 0xa encoded with 1 byte)",
			ion: pad8([]byte{
				0x85, 0x82, 0x61, 0x63,
				0x8a, 0x83, 0xaa, 0xbb, 0xcc,
			}),
			consumed: 9,
			wrote:    10,
			output:   []byte{0xd5, 0x8a, 0x83, 0xaa, 0xbb, 0xcc, 0xd0, 0xd0, 0xd0, 0xd0},
		},
		{
			dsc:      "copy object (symbol 0xa encoded with 2 bytes)",
			ion:      pad8([]byte{0x00, 0x8a, 0x82, 0xaa, 0xbb}),
			consumed: 5,
			wrote:    10,
			output:   []byte{0xd5, 0x00, 0x8a, 0x82, 0xaa, 0xbb, 0xd0, 0xd0, 0xd0, 0xd0},
		},
		{
			dsc:      "copy object (symbol 0xa encoded with 3 bytes)",
			ion:      pad8([]byte{0x00, 0x00, 0x8a, 0x82, 0xaa, 0xbb}),
			consumed: 6,
			wrote:    11,
			output:   []byte{0xd6, 0x00, 0x00, 0x8a, 0x82, 0xaa, 0xbb, 0xd0, 0xd0, 0xd0, 0xd0},
		},
		{
			dsc: "copy 10-byte string that won't fit in dst; object length encoded in TLV byte",
			ion: pad8([]byte{
				0x8a, 0x8a, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
				0x01, 0x01, 0x01, 0x01}),
			fault:   faultTooLarge,
			dstsize: 8,
		},
		{
			dsc:   "copy long string that won't fit in dst; object length encoded with 1 byte",
			ion:   pad8(ionobject(ion.Symbol(0xa), (1<<7)-4)),
			fault: faultTooLarge,
		},
		{
			dsc:   "copy long string that won't fit in dst; object length encoded with 2 bytes",
			ion:   pad8(ionobject(ion.Symbol(0xa), (1<<14)-5)),
			fault: faultTooLarge,
		},
		{
			dsc:   "copy long string that won't fit in dst; object length encoded with 3 bytes",
			ion:   pad8(ionobject(ion.Symbol(0xa), (1<<21)-6)),
			fault: faultTooLarge,
		},
		{
			dsc:   "copy string longer than allowed size (1 << 21)",
			ion:   pad8(ionobject(ion.Symbol(0xa), (1 << 22))),
			fault: faultBadData,
		},
		{
			dsc:   "wrote more empty structs than dst can fit (dst has 20 bytes)",
			ion:   pad8([]byte{0x81, 0x0f}),
			count: 42 * 5,
			fault: faultTooLarge,
		},
		{
			dsc:   "call with negative count",
			ion:   pad8([]byte{0x81, 0x11, 0x8a, 0x11}),
			count: -5,
		},
		{
			dsc:      "call with zero count",
			ion:      pad8([]byte{0x81, 0x11, 0x8a, 0x11}),
			count:    0,
			setcount: true,
		},
	}

	for i := range testcases {
		tc := &testcases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var d Decoder
			sym := ion.Symbol(0xa)

			dstsize := tc.dstsize
			if dstsize == 0 {
				dstsize = 20
			}

			count := tc.count
			if !tc.setcount && count == 0 {
				count = 5
			}

			dst := make([]byte, dstsize)
			consumed, wrote := zipfast1(tc.ion, dst, &d, sym, count)

			fail := false
			if d.fault != tc.fault {
				t.Logf("want: %s", tc.fault)
				t.Logf("got:  %s", d.fault)
				t.Error("wrong decoder fault value")
				fail = true
			}

			if consumed != tc.consumed {
				t.Logf("want: %d", tc.consumed)
				t.Logf("got:  %d", consumed)
				t.Error("wrong consumed value")
				fail = true
			}

			if wrote != tc.wrote {
				t.Logf("want: %d", tc.wrote)
				t.Logf("got:  %d", wrote)
				t.Error("wrong wrote value")
				fail = true
			}

			if d.fault == noFault && tc.output != nil {
				n := len(dst)
				if wrote < n {
					n = wrote
				}
				buf := dst[:n]
				if !slices.Equal(buf, tc.output) {
					t.Logf("want: % x", tc.output)
					t.Logf("got:  % x", buf)
					t.Error("wrong output value")
					fail = true
				}
			}

			if fail {
				t.Fail()
			}
		})
	}
}

// ionobject returns encoded label + string (without struct header)
func ionobject(sym ion.Symbol, length int) []byte {
	var buf ion.Buffer
	buf.BeginStruct(-1)
	buf.BeginField(sym)
	buf.WriteString(strings.Repeat("?", length))
	buf.EndStruct()

	ion, _ := ion.Contents(buf.Bytes())
	return ion
}
