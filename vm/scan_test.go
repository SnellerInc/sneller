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
	"fmt"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func bulkscan(buf []byte, start int32, blocksize int, then func(uint32, uint32)) {
	dst := make([][2]uint32, blocksize)
	count := 0
	for start < int32(len(buf)) {
		count, start = scan(buf, start, dst)
		if count == 0 {
			panic("no progress")
		}
		if int(start) > len(buf) {
			panic("start > len(buf)")
		}
		for i := range dst[:count] {
			then(dst[i][0], dst[i][1])
		}
	}
}

// fielddesc describes a field of an 10n struct
type fielddesc struct {
	symbol     uint // actual symbol value
	symoff     int  // encoded symbol (uvarint) offset from start of buffer
	fieldoff   int  // field offset from start of buffer (including descriptor!)
	fieldwidth int  // field width (including descriptor!)
}

// structdesc describes an ion structure given by 'buf'
func structdesc(buf []byte) []fielddesc {
	var out []fielddesc
	off := 0
	for off < len(buf) {
		symoff := off
		uv, uvsize := uvint(buf[off:])
		off += uvsize
		fieldoff := off
		skip, size := objsize(buf[off:])
		off += int(skip + size)
		fieldwidth := int(skip + size)
		out = append(out, fielddesc{
			symbol:     uv,
			symoff:     symoff,
			fieldoff:   fieldoff,
			fieldwidth: fieldwidth,
		})
	}
	return out
}

// structures takes a buffer and turns it into
// a slice of structure descriptors; useful for
// generating test cases for the vectorized code
func structures(buf []byte) [][]fielddesc {
	var out [][]fielddesc
	bulkscan(buf, 0, 32, func(loc, width uint32) {
		desc := structdesc(buf[loc : loc+width])
		for i := range desc {
			desc[i].symoff += int(loc)
			desc[i].fieldoff += int(loc)
		}
		out = append(out, desc)
	})
	return out
}

func TestScan(t *testing.T) {
	buf := unhex(parkingCitations1KLines)

	// first handful of lengths + offsets
	expectedStarts := []uint32{0xb9, 0x12c, 0x19a, 0x1fd, 0x254, 0x2ca, 0x33f, 0x3b2, 0x425, 0x494, 0x504, 0x574, 0x5e4, 0x654, 0x6c0, 0x729,
		0x78f, 0x7f7, 0x865, 0x8d3, 0x937, 0x98c, 0xa03, 0xa76, 0xae8, 0xb53, 0xbba, 0xc25, 0xc87, 0xcf6, 0xd5f, 0xdca,
		0xe3f, 0xeb4, 0xf1b, 0xf93, 0x1008, 0x107d, 0x10ef, 0x1158, 0x11c9, 0x1237, 0x12a7, 0x131a, 0x137b, 0x13e1, 0x1449, 0x14ad,
		0x1523, 0x1595, 0x1608, 0x1672, 0x16e5, 0x1754, 0x17c3, 0x1832, 0x1893, 0x18fe, 0x1965, 0x19cd, 0x1a43, 0x1aba, 0x1b30, 0x1ba5}
	expectedLengths := []uint32{0x71, 0x6c, 0x61, 0x55, 0x74, 0x73, 0x71, 0x71, 0x6d, 0x6e, 0x6e, 0x6e, 0x6e, 0x6a, 0x67, 0x64,
		0x66, 0x6c, 0x6c, 0x62, 0x53, 0x75, 0x71, 0x70, 0x69, 0x65, 0x69, 0x60, 0x6d, 0x67, 0x69, 0x73,
		0x73, 0x65, 0x76, 0x73, 0x73, 0x70, 0x67, 0x6f, 0x6c, 0x6e, 0x71, 0x5f, 0x64, 0x66, 0x62, 0x74,
		0x70, 0x71, 0x68, 0x71, 0x6d, 0x6d, 0x6d, 0x5f, 0x69, 0x65, 0x66, 0x74, 0x75, 0x74, 0x73, 0x6b}

	if len(expectedStarts) != len(expectedLengths) {
		t.Fatal("bad test vectors")
	}

	// test a variety of scan buffer sizes
	blocksizes := []int{1, 4, 7, 15, 16, len(expectedStarts), 128}
	for _, blocksize := range blocksizes {
		n := 0
		bulkscan(buf, 0xb7, blocksize, func(off uint32, size uint32) {
			if n >= len(expectedStarts) {
				return
			}
			if off != expectedStarts[n] {
				t.Errorf("item %d want start %x, got %x", n, expectedStarts[n], off)
			}
			if size != expectedLengths[n] {
				t.Errorf("item %d want length %x, got %x", n, expectedLengths[n], size)
			}
			n++
		})
		if n < len(expectedStarts) {
			t.Errorf("only scanned %d items?", n)
		}
	}
}

// test that scanning data that points
// outside the current slice does not
// lead to invalid delimiters being returned
func TestScanPartial(t *testing.T) {
	var buf ion.Buffer
	buf.BeginStruct(-1)
	buf.BeginField(12)
	buf.WriteString("first field")
	buf.EndStruct()
	splitpos := buf.Size()
	buf.BeginStruct(-1)
	buf.BeginField(13)
	buf.WriteString("second field")
	buf.EndStruct()

	all := buf.Bytes()

	delims := make([][2]uint32, 3)
	// try to scan *past* the first struct
	// but not including the second; this
	// should only produce 1 delimiter
	n, nb := scan(all[:splitpos+3], 0, delims)
	if n != 1 {
		t.Errorf("got %d delimiters back?", n)
	}
	if int(nb) != splitpos {
		t.Errorf("consumed %d bytes; wanted %d", nb, splitpos)
	}
	// .. but the whole buffer should work:
	n, nb = scan(all, 0, delims)
	if n != 2 {
		t.Errorf("got %d delimiters back?", n)
	}
	if int(nb) != len(all) {
		t.Errorf("got %d bytes scanned?", nb)
	}
}

// test that scanning incomplete data
// does not lead to a hang or invalid results
func TestScanIncomplete(t *testing.T) {
	var buf ion.Buffer
	buf.BeginStruct(-1)
	buf.BeginField(10)
	buf.WriteString("this is a string long enough to have a multi-byte TLV prefix")
	buf.EndStruct()
	mem := buf.Bytes()
	other := make([]byte, len(mem))
	other[0] = mem[0]

	// not enough data; should return early
	delims := make([][2]uint32, 3)
	n, nb := scan(other[:1], 0, delims)
	if n != 0 || nb != 0 {
		t.Errorf("n = %d, nb = %d? expected zeros", n, nb)
	}
	// enough to parse length, but not enough length
	other[1] = mem[1]
	n, nb = scan(other[:2], 0, delims)
	if n != 0 || nb != 0 {
		t.Errorf("n = %d, nb = %d? expected zeros", n, nb)
	}
	copy(other, mem)
	n, nb = scan(other, 0, delims)
	if n != 1 || int(nb) != len(other) {
		t.Errorf("n = %d, nb = %d? expected %d, %d", n, nb, 1, len(other))
	}
}

func BenchmarkScan(b *testing.B) {
	inner := func(b *testing.B, buf []byte, blocksize int) {
		b.SetBytes(int64(len(buf)))
		dst := make([][2]uint32, blocksize)
		for i := 0; i < b.N; i++ {
			off := int32(0xb7)
			for off < int32(len(buf)) {
				_, off = scan(buf, off, dst)
			}
		}
	}
	buf := unhex(parkingCitations1KLines)
	for _, blocksize := range []int{8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("Block%d", blocksize), func(b *testing.B) {
			inner(b, buf, blocksize)
		})
	}
}
