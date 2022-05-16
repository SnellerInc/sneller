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

package expr_test

import (
	"regexp"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
)

func TestCheck2(t *testing.T) {
	testcases := []struct {
		query string
		errrx string
	}{
		{
			// don't allow table functions
			// in non-table position:
			`SELECT TABLE_GLOB("db"."x*") FROM foo`,
			"non-table position",
		},
		{
			`SELECT TABLE_PATTERN("db"."x[0-3]+") FROM foo`,
			"non-table position",
		},
		{
			// don't allow *known* non-table functions
			// in table position
			`SELECT x, y FROM UPPER('foo')`,
			"in table position",
		},
	}
	for i := range testcases {
		text := testcases[i].query
		rx := regexp.MustCompile(testcases[i].errrx)
		q, err := partiql.Parse([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		err = expr.Check(q.Body)
		if err == nil {
			t.Errorf("query %s didn't yield an error", text)
			continue
		}
		if !rx.MatchString(err.Error()) {
			t.Errorf("rx %q didn't match error %q", rx, err)
		}
	}
}
