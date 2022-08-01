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

type testcaseError struct {
	query string
	errrx string
}

func TestCheck2(t *testing.T) {
	testcases := []testcaseError{
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
			"lhs and rhs.*never comparable",
		},
		{
			// issue #1390
			"SELECT * FROM table WHERE DATE_ADD(day, -1, timestamp) >= CAST(x AS float)",
			"lhs and rhs.*never comparable",
		},
	}
	for j := range testcases {
		i := j
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			checkError(t, &testcases[j])
		})
	}
}

func TestCheckIssue1390(t *testing.T) {
	types := []string{"", "MISSING", "NULL", "STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP"}
	const COLUMN = ""
	cast := func(t string) string {
		if t == COLUMN {
			return "x"
		}

		return fmt.Sprintf("CAST(x AS %s)", t)
	}

	validComparison := func(t1, t2 string) bool {
		switch t1 {
		case COLUMN:
			switch t2 {
			case "INTEGER", "FLOAT", "TIMESTAMP", COLUMN:
				return true
			}
		case "INTEGER", "FLOAT":
			switch t2 {
			case "INTEGER", "FLOAT", COLUMN:
				return true
			}

		case "TIMESTAMP":
			switch t2 {
			case "TIMESTAMP", COLUMN:
				return true
			}
		}

		return false
	}

	expectedError := func(t1, t2 string) string {
		if validComparison(t1, t2) {
			return ""
		}
		return "lhs and rhs of comparison are never comparable"
	}

	for i := range types {
		t1 := types[i]
		for j := range types {
			t2 := types[j]
			tc := testcaseError{
				query: fmt.Sprintf("SELECT * FROM table WHERE %s < %s", cast(t1), cast(t2)),
				errrx: expectedError(t1, t2),
			}

			name := fmt.Sprintf("cmp-%s-with-%s", t1, t2)
			t.Run(name, func(t *testing.T) {
				checkError(t, &tc)
			})
		}
	}
}

func checkError(t *testing.T, tc *testcaseError) {
	text := tc.query
	q, err := partiql.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}

	expectedError := (tc.errrx != "")

	err = expr.Check(q.Body)
	if expectedError {
		if err == nil {
			t.Errorf("query %s didn't yield an error", text)
			return
		}
	} else {
		if err != nil {
			t.Log(err)
			t.Errorf("query %s shouldn't yield an error", text)
			return
		}
	}

	if expectedError {
		rx := regexp.MustCompile(tc.errrx)
		if !rx.MatchString(err.Error()) {
			t.Errorf("rx %q didn't match error %q", rx, err)
		}
	}
}
