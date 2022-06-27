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

package ion

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func TestEncodeInt(t *testing.T) {
	ints := []struct {
		value   int64
		encoded []byte
	}{
		{0, []byte{0x20}},
		{1, []byte{0x21, 0x01}},
		{-1, []byte{0x31, 0x01}},
		{127, []byte{0x21, 0x7f}},
		{-127, []byte{0x31, 0x7f}},
		{255, []byte{0x21, 0xff}},
		{-255, []byte{0x31, 0xff}},
		{256, []byte{0x22, 0x01, 0x00}},
		{-256, []byte{0x32, 0x01, 0x00}},
		{1251, []byte{0x22, 0x04, 0xe3}},
		{1103341116, []byte{0x24, 0x41, 0xc3, 0xa6, 0x3c}},
	}

	var b Buffer
	for i := range ints {
		b.Reset()
		want := ints[i].encoded
		b.WriteInt(ints[i].value)
		got := b.Bytes()
		if !bytes.Equal(got, want) {
			t.Errorf("encoding %d: got %x, want %x", ints[i].value, got, want)
		}
		v, tail, err := ReadInt(ints[i].encoded)
		if err != nil {
			t.Fatal(err)
		}
		if v != ints[i].value {
			t.Errorf("decoding %d: got %d", ints[i].value, v)
		}
		if len(tail) != 0 {
			t.Errorf("%d bytes left over?", len(tail))
		}
		if s := SizeOf(ints[i].encoded); s != len(ints[i].encoded) {
			t.Errorf("case %d: SizeOf(msg)=%d, len(msg)=%d", i, s, len(ints[i].encoded))
		}
	}
}

func TestEncodeString(t *testing.T) {
	tcs := []struct {
		value   string
		encoded []byte
	}{
		{"", []byte{0x80}},
		{"a", []byte{0x81, 'a'}},
		{"ab", []byte{0x82, 'a', 'b'}},
		{"abcdefghijkl123456789", []byte{0x8e, 0x80 | 21,
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
			'j', 'k', 'l',
			'1', '2', '3', '4', '5', '6', '7', '8', '9',
		}},
	}

	var b Buffer
	for i := range tcs {
		b.Reset()
		want := tcs[i].encoded
		b.WriteString(tcs[i].value)
		got := b.Bytes()
		if !bytes.Equal(want, got) {
			t.Errorf("encoding %q: got %x, wanted %x", tcs[i].value, got, want)
		}
		v, tail, err := ReadString(tcs[i].encoded)
		if err != nil {
			t.Fatal(err)
		}
		if v != tcs[i].value {
			t.Errorf("decoding %q: got %q", tcs[i].value, v)
		}
		if len(tail) != 0 {
			t.Errorf("%d bytes left over?", len(tail))
		}
		if s := SizeOf(tcs[i].encoded); s != len(tcs[i].encoded) {
			t.Errorf("case %d: SizeOf(msg)=%d, len(msg)=%d", i, s, len(tcs[i].encoded))
		}
	}
}

func TestShortStruct(t *testing.T) {
	tcs := []struct {
		expr func(b *Buffer)
		out  []byte
	}{
		{
			expr: func(b *Buffer) {
				b.BeginField(1)
				b.WriteString("x")
			},
			out: []byte{0xd3, 0x81, 0x81, 'x'},
		},
		{
			expr: func(b *Buffer) {
				b.BeginField(16)
				b.WriteInt(0)
			},
			out: []byte{0xd2, 0x90, 0x20},
		},
	}

	var b Buffer
	for i := range tcs {
		b.Reset()
		want := tcs[i].out
		b.BeginStruct(-1)
		tcs[i].expr(&b)
		b.EndStruct()
		got := b.Bytes()
		if !bytes.Equal(want, got) {
			t.Errorf("case %d: wanted %x, got %x", i, want, got)
		}

		inner, tail := Contents(got)
		if inner == nil {
			t.Errorf("case %d: Contents(msg) failed", i)
		}
		if len(tail) != 0 {
			t.Errorf("case %d: len(tail) == 0?", i)
		}
		if s := SizeOf(got); s != len(got) {
			t.Errorf("case %d: SizeOf(msg)=%d, len(msg)=%d", i, s, len(got))
		}
	}
}

func TestRandomTime(t *testing.T) {
	base := date.Unix(0, 0)
	var b Buffer
	// test many random displacements of 64-bit nanoseconds
	// from the zero unix time
	for i := 0; i < 10000; i++ {
		b.Reset()
		c := base.Add(time.Duration(rand.Uint64()))
		c = c.Round(time.Duration(1000))
		b.WriteTime(c)
		out, _, err := ReadTime(b.Bytes())
		if err != nil {
			t.Fatalf("reading time %q: %s", c, err)
		}
		if !out.Equal(c) {
			t.Fatalf("%q != %q", out, c)
		}
	}
}

func TestTime(t *testing.T) {
	tcs := []struct {
		text    string
		encoded []byte
	}{
		{
			// test case from https://amzn.github.io/ion-docs/docs/binary.html#6-timestamp
			text:    "2000-01-01T00:00:00Z",
			encoded: []byte{0x68, 0x80, 0x0F, 0xD0, 0x81, 0x81, 0x80, 0x80, 0x80},
		},
		{
			text:    time.RFC3339[:len(time.RFC3339)-5],
			encoded: []byte{0x68, 0x80, 0x0F, 0xD6, 0x81, 0x82, 0x8f, 0x84, 0x85},
		},
	}

	var b Buffer
	for i := range tcs {
		d, ok := date.Parse([]byte(tcs[i].text))
		if !ok {
			t.Fatalf("parsing %q failed", tcs[i].text)
		}
		b.Reset()
		b.WriteTime(d)
		got := b.Bytes()
		if !bytes.Equal(got, tcs[i].encoded) {
			t.Errorf("case %d: got  %x", i, got)
			t.Errorf("case %d: want %x", i, tcs[i].encoded)
		}
		d2, rest, err := ReadTime(got)
		if err != nil {
			t.Errorf("case %d: ReadDate: %s", i, err)
		}
		if len(rest) != 0 {
			t.Errorf("case %d: %d bytes left over?", i, len(rest))
		}
		if !d2.Equal(d) {
			t.Errorf("case %d: decoded result %s not equal to %s", i, d2, d)
		}
	}
}

func TestSizeOf(t *testing.T) {
	tcs := []struct {
		mem  []byte
		want int
	}{
		{
			mem:  []byte{0xee, 0x02, 0x95},
			want: 280,
		},
	}
	for i := range tcs {
		if got := SizeOf(tcs[i].mem); got != tcs[i].want {
			t.Errorf("case %d: got %d, want %d", i, got, tcs[i].want)
		}
	}
}

func TestNopPadding(t *testing.T) {
	buffer := make([]byte, 32)
	for padding := 1; padding < 1024*17; padding++ {
		for i := 0; i < len(buffer); i++ {
			buffer[i] = 0
		}
		header, padbytes := NopPadding(buffer, padding)
		if TypeOf(buffer) != NullType {
			t.Errorf("case %d: wrong Ion type %s", padding, TypeOf(buffer))
		}

		if SizeOf(buffer) != header+padbytes {
			t.Errorf("case %d: wrong Ion object size %d, expected %d",
				padding, SizeOf(buffer), header+padbytes)
		}

		sum := header + padbytes
		if sum == padding {
			continue
		}

		if sum > padding {
			t.Errorf("case %d: header %d bytes and pad bytes %d - sum %d greater than padding",
				padding, header, padbytes, sum)
		}

		if padding-sum > 2 {
			t.Errorf("case %d: header %d bytes and pad bytes %d - sum %d too small",
				padding, header, padbytes, sum)
		}
	}
}

func BenchmarkSizeOf(b *testing.B) {
	sizes := []int{
		0, 5, 12, 225, 18468,
	}
	var ib Buffer
	for i := range sizes {
		ib.BeginString(sizes[i])
		ib.UnsafeAppend(make([]byte, sizes[i]))
	}
	mem := ib.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := mem
		for len(buf) > 0 {
			buf = buf[SizeOf(buf):]
		}
	}
}

func BenchmarkUvarint(b *testing.B) {
	dst := make([]byte, 0, 10)
	var ib Buffer
	ib.Set(dst)
	ib.putuv((1 << 21) + 1)
	dst = ib.Bytes()
	for i := 0; i < b.N; i++ {
		_, _, _ = readuv(dst)
	}
}
