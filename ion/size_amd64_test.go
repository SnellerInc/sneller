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

//go:build amd64
// +build amd64

package ion

import (
	"bytes"
	"testing"
)

func TestIssue419(t *testing.T) {
	tcs := []struct {
		value    uint
		encoding []byte
	}{
		{value: 0x58, encoding: []byte{0xd8}},
		{value: 0x43, encoding: []byte{0xc3}},
		{value: 0x43 << 7, encoding: []byte{0x43, 0x80}},
	}
	var buf Buffer
	for i := range tcs {
		buf.Reset()
		buf.putuv(tcs[i].value)
		if !bytes.Equal(buf.Bytes(), tcs[i].encoding) {
			t.Errorf("got %x; want %x", buf.Bytes(), tcs[i].encoding)
		}
	}
}

func TestFastuv(t *testing.T) {
	in := []uint{
		0, 1, 7, 8, 15, 16, 17, 19, 100, 127, 128,
		0x58, 0x43, 0x43 << 7,
		(1 << 14) - 1, (1 << 14), (1 << 14) + 1,
		(1 << 21) - 1, (1 << 21), (1 << 21) + 1,
		12345,
	}

	var b Buffer
	dst := make([]byte, 0, 16)
	b.Set(dst)
	for i := range in {
		b.Reset()
		u := in[i]
		b.putuv(u)
		bytes := b.Bytes()
		p, _, ok := readuv(bytes)
		if !ok {
			t.Fatal("!ok")
		}
		if p != u {
			t.Fatalf("portable %d from %d", p, u)
		}
		out, size := fastuv(bytes)
		if out != u {
			t.Errorf("0x%x: decoded as 0x%x (was 0x%x)", u, out, bytes)
		}
		if size != len(bytes) {
			t.Errorf("%x: decoded as size %d (was %d)", u, size, len(bytes))
		}
	}
}
