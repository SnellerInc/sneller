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

package partiql

import (
	"testing"
)

func TestScannerPosition(t *testing.T) {
	lines := []string{
		"1234",
		"123456789_123456789_",
		"",
		"123456",
		"1",
	}

	var s scanner

	for _, line := range lines {
		s.from = append(s.from, []byte(line)...)
		s.from = append(s.from, []byte("\n")...)
	}

	pos := 0
	for line := range lines {
		for column := 0; column <= len(lines[line]); column++ {
			l, c, ok := s.position(pos)
			if ok == false || c != column+1 || l != line+1 {
				t.Logf("got : ok=%v, line=%d, column=%d\n", ok, l, c)
				t.Logf("want: ok=%v, line=%d, column=%d\n", true, line+1, column+1)
			}
			pos++
		}
	}
}
