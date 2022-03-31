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
	"testing"
)

func TestPathLess(t *testing.T) {
	ord := [][]Symbol{
		{0},
		{0, 1},
		{0, 1, 1},
		{0, 2},
		{0, 2, 1},
		{0, 2, 2},
		{0, 2, 3},
		{0, 3},
		{1, 0},
		{2},
	}
	for i := range ord[:len(ord)-1] {
		tail := ord[i+1:]
		if pathLess(ord[i], ord[i]) {
			t.Errorf("%v less than itself?", ord)
		}
		for j := range tail {
			if !pathLess(ord[i], tail[j]) {
				t.Errorf("%v not less than %v?", ord[i], tail[j])
			}
			if pathLess(tail[j], ord[i]) {
				t.Errorf("%v < %v ?", tail[j], ord[i])
			}
		}
	}
}
