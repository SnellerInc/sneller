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

//go:build linux

package zion

import (
	"syscall"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"

	"golang.org/x/exp/slices"
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
