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
	"os"
	"testing"
)

func TestLimit(t *testing.T) {
	buf, err := os.ReadFile("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	amounts := []int{
		0, 1, 15, 16, 17, 100, 127,
		1021, 1022, 1023,
	}
	for i := range amounts {
		var dst QueryBuffer
		l := NewLimit(int64(amounts[i]), &dst)
		s, err := NewProjection(selection("Ticket as t"), l)
		if err != nil {
			t.Fatal(err)
		}
		err = CopyRows(s, buftbl(buf), 1)
		if err != nil {
			t.Errorf("LIMIT %d: %s", amounts[i], err)
			continue
		}
		b := dst.Bytes()
		skipok(b, t)
		t.Logf("LIMIT %d: %d bytes output", amounts[i], len(dst.Bytes()))
		out := len(structures(dst.Bytes()))
		if out != amounts[i] {
			t.Errorf("len(out)=%d, LIMIT %d ?", out, amounts[i])
		}
	}
}
