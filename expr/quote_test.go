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

package expr

import (
	"strconv"
	"testing"
)

func TestSQLQuote(t *testing.T) {
	tcs := []struct {
		in, out string
	}{
		{"foo", "'foo'"},
		{"", "''"},
		{"a \t", `'a \t'`},
		{"'xyz'", "'\\'xyz\\''"},
	}
	for i := range tcs {
		unq, err := strconv.Unquote(string('"') + tcs[i].in + string('"'))
		if err != nil {
			t.Fatalf("%q %s", tcs[i].in, err)
		}
		got := ToString(String(unq))
		want := tcs[i].out
		if got != want {
			t.Errorf("quote(%q) = %s want %s", unq, got, want)
		}
	}
}
