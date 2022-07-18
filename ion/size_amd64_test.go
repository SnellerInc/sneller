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
