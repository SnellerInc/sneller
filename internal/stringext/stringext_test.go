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

package stringext

import (
	"fmt"
	"testing"
)

func TestIndexByteEscape(t *testing.T) {
	type unitTest struct {
		str    string
		char   rune
		esc    rune
		expIdx int
	}

	unitTests := []unitTest{

		{"a@__", '_', '@', 3},

		{"a_b", '_', '$', 1},
		{"a_", '_', '$', 1},
		{"_b", '_', '$', 0},
		{"_", '_', '$', 0},

		{"a$_b", '_', '$', -1},
		{"a$_", '_', '$', -1},
		{"$_b", '_', '$', -1},
		{"$_", '_', '$', -1},
	}

	run := func(ut unitTest) {
		obsIdx := IndexRuneEscape([]rune(ut.str), ut.char, ut.esc)
		if obsIdx != ut.expIdx {
			t.Errorf("str %q, char=%q, esc=%q: observed %v; expected %v", ut.str, ut.char, ut.esc, obsIdx, ut.expIdx)
		}
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}

func TestLastIndexByteEscape(t *testing.T) {
	type unitTest struct {
		str    string
		wc     rune
		esc    rune
		expIdx int
	}

	unitTests := []unitTest{
		{"a_b", '_', '$', 1},
		{"a_", '_', '$', 1},
		{"_b", '_', '$', 0},
		{"_", '_', '$', 0},

		{"a$_b", '_', '$', -1},
		{"a$_", '_', '$', -1},
		{"$_b", '_', '$', -1},
		{"$_", '_', '$', -1},
	}

	run := func(ut unitTest) {
		obsIdx := LastIndexRuneEscape([]rune(ut.str), ut.wc, ut.esc)
		if obsIdx != ut.expIdx {
			t.Errorf("str %q, char=%q, esc=%q: observed %v; expected %v", ut.str, ut.wc, ut.esc, obsIdx, ut.expIdx)
		}
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}
