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
	"fmt"
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
		{
			// also ensure garbage operators aren't
			// allowed in the table position
			`SELECT COUNT(*) FROM CHAR_LENGTH()%0`,
			".*",
		},
		{
			`SELECT (a ++ b ++ c)`,
			"non-table",
		},
		{
			`SELECT * FROM table WHERE x < 'y'`,
			"lhs and rhs.*never comparable",
		},
		{
			`SELECT * FROM table WHERE 'x' < 'y'`,
			"lhs and rhs.*never comparable",
		},
		{
			"SELECT * FROM table WHERE 3 = `2022-01-02T03:04:05.67Z`",
			"lhs and rhs.*never comparable",
		},
		{
			// issue #1390
			"SELECT * FROM table WHERE CAST(x AS integer) < DATE_ADD(day, -1, timestamp)",
			"can compare only number with number",
		},
		{
			// issue #1390
			"SELECT * FROM table WHERE DATE_ADD(day, -1, timestamp) >= CAST(x AS float)",
			"can compare only timestamp with timestamp",
		},
	}
	for j := range testcases {
		i := j
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			text := testcases[i].query
			rx := regexp.MustCompile(testcases[i].errrx)
			q, err := partiql.Parse([]byte(text))
			if err != nil {
				t.Fatal(err)
			}
			err = expr.Check(q.Body)
			if err == nil {
				t.Errorf("query %s didn't yield an error", text)
				return
			}
			if !rx.MatchString(err.Error()) {
				t.Errorf("rx %q didn't match error %q", rx, err)
			}
		})
	}
}
