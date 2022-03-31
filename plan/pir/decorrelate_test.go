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

package pir

import (
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr/partiql"
)

// test that correlated sub-queries that would require
// a loop join or are otherwise too weird to deal with
// are correctly rejected as unsupported
func TestUnsupported(t *testing.T) {
	tests := []string{
		`select x, (select z from bar where x = y limit 2) from foo`,
		`select x, (select max(x) from bar where x = y) from foo`,
		`select x, (select max(z) from bar where x = y OR x = 2) from foo`,
		`select x, y, (select b.z from bar b where b.y = x AND b.y = y limit 1) from foo`,
		`select x, (select a from bar where x = y AND x = z limit 1) from foo`,
		`select x, (select a from bar where x = y AND x > 10 limit 1) from foo`,
		`select x, (select x+y from bar where x = z limit 1) from foo`,
	}
	for i := range tests {
		s, err := partiql.Parse([]byte(tests[i]))
		if err != nil {
			t.Fatal(err)
		}
		_, err = Build(s, nil)
		if err == nil {
			t.Errorf("didn't error on query %s", tests[i])
			continue
		}
		str := err.Error()
		if !strings.Contains(str, "correlated") {
			t.Errorf("suspicious error %q in %q", str, tests[i])
		}
	}
}
