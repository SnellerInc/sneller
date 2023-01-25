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
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

var sameq = []string{
	"SELECT x",
	"SELECT 3, x, 1 + (x + 2)",
	"SELECT x, foo FROM table WHERE x = 'foo'",
	"SELECT foo AS bar FROM table WHERE x.y = 'foo' LIMIT 1",
	"SELECT x FROM table WHERE x.y.z = 'foo'",
	"SELECT x FROM table WHERE x[0] = 'foo'",
	"SELECT x FROM table WHERE x[0][1] = 'foo'",
	"SELECT x FROM 'string' WHERE x[0].y[3] = 'foo'",
	"SELECT x FROM table AS t WHERE 'foo' = 'bar'",
	`SELECT * FROM NDJSON('{"foo": 1, "bar": 2}')`,
	// test that identifiers matching keywords are double-quoted when displayed:
	"SELECT table.\"join\", table.outer FROM table WHERE x = 'foo' AND y = 'bar' LIMIT 100",
	"SELECT x AS \"join\" FROM table WHERE x = 'foo' OR y = 'bar'",
	// test parsing of escape sequences
	`SELECT SPLIT_PART(text, '\n', 1) AS line FROM x`,
	`SELECT '\u2408' AS y`,
	"SELECT x FROM table WHERE x LIKE '%xyz'",
	"SELECT x FROM table WHERE x IS NULL",
	"SELECT x FROM table WHERE x.y IS NOT MISSING",
	"SELECT x, COUNT(y) AS \"count\" FROM table AS t GROUP BY x",
	"SELECT x, x < 3 FROM table AS t",
	"SELECT x, x LIKE 'foo%' FROM table AS t",
	"SELECT COUNT(*) FROM table WHERE x + y <= z",
	"SELECT COUNT(DISTINCT x) FROM y",
	"SELECT SUM(foo) FROM table WHERE x = y AND y = z AND z IS NULL",
	"SELECT MIN(lo), MAX(hi) AS \"limit\" FROM table WHERE x <> 3 GROUP BY x LIMIT 100",
	"SELECT l.x, r.y FROM 'first' AS l JOIN second AS r ON l.id = r.id",
	"SELECT o.field, i.other FROM 'outer' AS o CROSS JOIN 'inner' AS i WHERE o.foo = i.bar",
	"SELECT DISTINCT x, y, z FROM table ORDER BY x ASC NULLS FIRST",
	"SELECT x, MIN(y) FROM table GROUP BY x ORDER BY MIN(y) DESC NULLS FIRST LIMIT 1",
	"SELECT t.x, t.y IS MISSING <> t.x IS MISSING FROM table AS t",
	"SELECT * FROM table ORDER BY foo ASC NULLS FIRST OFFSET 7",
	"SELECT * FROM table WHERE (a AND b) = c",
	"SELECT * FROM table WHERE c = a AND b",
	"SELECT * FROM table WHERE c = (a AND b = c)",
	"SELECT COUNT(y) AS c, x FROM table AS t GROUP BY x HAVING c > 10",
	"SELECT * FROM table WHERE CASE WHEN x < 3 THEN 0 ELSE 1 END = 1",
	"SELECT CASE WHEN x IS NOT NULL THEN x ELSE 'foo' END AS t FROM table",
	"SELECT CAST(x AS INTEGER), CAST(y AS DECIMAL), CAST(z AS TIMESTAMP) FROM foo",
	"SELECT x = (SELECT y FROM z LIMIT 1) FROM a",
	"SELECT x, (SELECT y FROM z WHERE x = y) FROM foo",
	"SELECT * FROM foo WHERE date < (SELECT MIN(date) FROM y)",
	"WITH foo AS (SELECT x, y FROM table) SELECT x FROM foo",
	"WITH foo AS (SELECT x, y FROM table), bar AS (SELECT z, a FROM table) SELECT x FROM foo CROSS JOIN bar",
	"SELECT * FROM (t1 ++ t2 ++ t3)",
	"SELECT x, y INTO db.xyz FROM db.foo WHERE x = 'foo' AND y = 'bar'",
	"SELECT x, SUM(x) OVER (PARTITION BY y, z ORDER BY col0 ASC NULLS FIRST, col1 DESC NULLS FIRST) FROM db.foo",
	"SELECT COUNT(*) FROM table",
	"SELECT COUNT(*) AS total, COUNT(x) FILTER (WHERE x > 0) AS greater FROM table",
	"SELECT [a, b, c] AS lst FROM foo",
	"SELECT {'first': x, 'second': y} AS structure FROM foo",
	"SELECT DISTINCT ON (x, y) y, z, w FROM table",
	"SELECT DISTINCT ON (x) * FROM table",
	"SELECT a FROM UNPIVOT t AS a AT b",
	"SELECT a FROM UNPIVOT t AS a",
	"SELECT a FROM UNPIVOT t AT a",
	"SELECT a FROM UNPIVOT {'x': 'y'} AS a",
	"SELECT * FROM UNPIVOT t AS a AT b",
	"SELECT TRIM(x) FROM table",
	"SELECT TRIM(x, y) FROM table",
	`SELECT APPROX_COUNT_DISTINCT(x) FROM table`,
	`SELECT APPROX_COUNT_DISTINCT(x, 5) FROM table`,
	`EXPLAIN SELECT * FROM table`,
	`EXPLAIN AS text SELECT * FROM table`,
	`EXPLAIN AS list SELECT * FROM table`,
	`EXPLAIN AS graphviz SELECT * FROM table`,
	`SELECT SNELLER_DATASHAPE(*) FROM table`,
	`SELECT * FROM table1 UNION SELECT * FROM table2`,
	`SELECT * FROM table1 UNION ALL SELECT * FROM table2`,
	`SELECT * FROM table1 UNION SELECT * FROM table2 UNION ALL SELECT * FROM table3 UNION SELECT * FROM table4`,
	`SELECT agg, SUM(x), ROW_NUMBER() OVER (ORDER BY SUM(x) ASC NULLS FIRST) FROM table GROUP BY agg`,
}

func TestParseSFW(t *testing.T) {
	for i := range sameq {
		query := sameq[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			e, err := Parse([]byte(query))
			if err != nil {
				t.Logf("query: %s", query)
				t.Error(err)
				// do it again, this time with debug
				yyDebug = 3
				Parse([]byte(query))
				yyDebug = 0
				return
			}
			if e == nil {
				t.Error("didn't match")
				return
			}
			if got := e.Text(); got != query {
				t.Errorf("got %q, want %q", got, query)
			}
			testEquivalence(t, e.Body)
		})
	}
}

func BenchmarkParse(b *testing.B) {
	for i := range sameq {
		q := sameq[i]
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			buf := []byte(q)
			b.ReportAllocs()
			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, err := Parse(buf)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSerialize(b *testing.B) {
	for i := range sameq {
		q := sameq[i]
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			e, err := Parse([]byte(q))
			if err != nil {
				b.Fatal(err)
			}
			var buf ion.Buffer
			var st ion.Symtab
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				buf.Reset()
				e.Body.Encode(&buf, &st)
			}
		})
	}
}

func BenchmarkDeserialize(b *testing.B) {
	for i := range sameq {
		q := sameq[i]
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			e, err := Parse([]byte(q))
			if err != nil {
				b.Fatal(err)
			}
			var buf ion.Buffer
			var st ion.Symtab
			e.Body.Encode(&buf, &st)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, _, err := expr.Decode(&st, buf.Bytes())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestParseNormalization(t *testing.T) {
	tests := []struct {
		from, to string
	}{
		// test that the ["string"] syntax works
		{
			"select x['y'] from foo",
			"SELECT x.y FROM foo",
		},
		{
			"select {'x': 2}.x",
			"SELECT 2",
		},
		{
			// test parens
			"select * from foo where ((a IS NULL) AND b IS NULL) OR c IS NULL",
			"SELECT * FROM foo WHERE a IS NULL AND b IS NULL OR c IS NULL",
		},
		{
			// test CONCAT
			`select x || y || z from foo`,
			`SELECT CONCAT(CONCAT(x, y), z) FROM foo`,
		},
		{
			// test IN
			`select * from table where x IN (1)`,
			`SELECT * FROM table WHERE x = 1`,
		},
		{
			// test COALESCE -> CASE
			`SELECT COALESCE(x, y) FROM foo`,
			`SELECT CASE WHEN x IS NOT NULL THEN x WHEN y IS NOT NULL THEN y ELSE NULL END FROM foo`,
		},
		{
			`SELECT NULLIF(x, y) FROM foo`,
			`SELECT CASE WHEN x = y THEN NULL ELSE x END FROM foo`,
		},
		{
			"SELECT EXTRACT(minute FROM x) FROM foo",
			"SELECT DATE_EXTRACT_MINUTE(x) FROM foo",
		},
		{
			"SELECT EXTRACT(year FROM UTCNOW()) FROM foo",
			"SELECT 2006 FROM foo",
		},
		{
			"SELECT DATE_TRUNC(month, UTCNOW()) FROM foo",
			"SELECT `2006-01-01T00:00:00Z` FROM foo",
		},
		{
			"SELECT DATE_TRUNC(minute, UTCNOW()) FROM foo",
			"SELECT `2006-01-02T15:04:00Z` FROM foo",
		},
		{
			"SELECT * FROM foo WHERE x IN (SELECT COUNT(x) FROM foo ORDER BY COUNT(x) DESC NULLS FIRST LIMIT 5)",
			"SELECT * FROM foo WHERE IN_SUBQUERY(x, (SELECT COUNT(x) FROM foo ORDER BY COUNT(x) DESC NULLS FIRST LIMIT 5))",
		},
		{
			"SELECT * FROM t1 ++ t2 ++ t3 WHERE foo = bar",
			"SELECT * FROM (t1 ++ t2 ++ t3) WHERE foo = bar",
		},
		{
			"SELECT EXISTS(SELECT x, y FROM foo WHERE x = 3) AS exist",
			"SELECT (SELECT x, y FROM foo WHERE x = 3 LIMIT 1) IS NOT MISSING AS exist",
		},
		{
			"SELECT a FROM UNPIVOT t AT b AS a",
			"SELECT a FROM UNPIVOT t AS a AT b",
		},
		{
			"SELECT TRIM(x FROM y) FROM table",
			"SELECT TRIM(y, x) FROM table",
		},
		{
			"SELECT TRIM(LEADING x FROM y) FROM table",
			"SELECT LTRIM(y, x) FROM table",
		},
		{
			"SELECT TRIM(TRAILING x FROM y) FROM table",
			"SELECT RTRIM(y, x) FROM table",
		},
		{
			"SELECT TRIM(BOTH x FROM y) FROM table",
			"SELECT TRIM(y, x) FROM table",
		},
		{
			`SELECT CASE WHEN y = 1 THEN 'one' WHEN y = 2 THEN 'two' ELSE 'other' END`,
			`SELECT CASE WHEN y = 1 THEN 'one' WHEN y = 2 THEN 'two' ELSE 'other' END`,
		},
		{
			`SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END`,
			`SELECT CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END`,
		},
		{
			`SELECT 0xcafe`,
			`SELECT 51966`,
		},
		{
			`SELECT -5`,
			`SELECT -5`,
		},
		{
			`SELECT -0xff`,
			`SELECT -255`,
		},
		{
			`SELECT 5e4`,
			`SELECT 50000`,
		},
		{
			`SELECT 5e+4`,
			`SELECT 50000`,
		},
		{
			`SELECT -4e-2`,
			`SELECT -0.04`,
		},
		{
			`SELECT 5-4`,
			`SELECT 1`,
		},
		{
			`SELECT 5+4`,
			`SELECT 9`,
		},
	}

	tm, ok := date.Parse([]byte("2006-01-02T15:04:05.999Z"))
	if !ok {
		t.Fatal("failed to parse time")
	}
	t.Cleanup(func() {
		faketime = nil
	})
	faketime = &expr.Timestamp{Value: tm}

	for i := range tests {
		e, err := Parse([]byte(tests[i].from))
		if err != nil {
			yyDebug = 3
			Parse([]byte(tests[i].from))
			yyDebug = 0
			t.Errorf("case %q: %s", tests[i].from, err)
			continue
		}
		if e == nil {
			t.Errorf("case %q: didn't match?", tests[i].from)
			continue
		}
		e.Body = expr.Simplify(e.Body, expr.NoHint)
		want := tests[i].to
		if got := e.Text(); got != want {
			t.Errorf("case %q: normalized to %q", tests[i].from, got)
		}
		testEquivalence(t, e.Body)
	}
}

// very simple testing on some obviously-wrong queries
//
// TODO: this should be hooked up to a fuzz-tester.
func TestParseGarbage(t *testing.T) {
	queries := []string{
		"select * from t where",
		"select * limit 3 where foo = bar from x",
		"select CAST(x AS notatype) from y",
		"select a[1E100] from y",
		"seleCt CoAlesC%(CoAlesC%(A[10000000000000000000]))",
	}
	for i := range queries {
		_, err := Parse([]byte(queries[i]))
		if err == nil {
			t.Errorf("case %q: err == nil?", queries[i])
		}
	}
}

func TestParseErrors(t *testing.T) {
	testcases := []struct {
		query string
		msg   string
	}{
		{
			query: "SELECT `xyz`",
			msg:   `couldn't parse ion literal`,
		},
		{
			query: `SELECT x.foo[9999999999999999999] FROM table`,
			msg:   `cannot use 1e+19 as an index`,
		},
		{
			query: `SELECT DATE_ADD(TEST, x, y)`,
			msg:   `bad DATE_ADD part "TEST"`,
		},
		{
			query: `SELECT DATE_DIFF(TEST, x, y)`,
			msg:   `bad DATE_DIFF part "TEST"`,
		},
		{
			query: `SELECT DATE_TRUNC(TEST, x)`,
			msg:   `bad DATE_TRUNC part "TEST"`,
		},
		{
			query: `SELECT EXTRACT(TEST FROM x)`,
			msg:   `bad EXTRACT part "TEST"`,
		},
		{
			query: `SELECT CONTAINS(x)`,
			msg:   `cannot use reserved builtin`,
		},
		{
			query: `SELECT CONTAINS(x, y, z)`,
			msg:   `cannot use reserved builtin`,
		},
		{
			query: `SELECT SUM(DISTINCT x)`,
			msg:   `cannot use DISTINCT with SUM`,
		},
		{
			query: `SELECT AVG(DISTINCT x)`,
			msg:   `cannot use DISTINCT with AVG`,
		},
		{
			query: `SELECT BOOL_OR(DISTINCT x)`,
			msg:   `cannot use DISTINCT with BOOL_OR`,
		},
		{
			query: `SELECT BOOL_AND(DISTINCT x)`,
			msg:   `cannot use DISTINCT with BOOL_AND`,
		},
		{
			query: `SELECT BIT_AND(DISTINCT x)`,
			msg:   `cannot use DISTINCT with BIT_AND`,
		},
		{
			query: `SELECT BIT_OR(DISTINCT x)`,
			msg:   `cannot use DISTINCT with BIT_OR`,
		},
		{
			query: `SELECT BIT_XOR(DISTINCT x)`,
			msg:   `cannot use DISTINCT with BIT_XOR`,
		},
		{
			query: `SELECT APPROX_COUNT_DISTINCT(x, -5)`,
			msg:   `precision has to be in range [4, 16]`,
		},
		{
			query: `SELECT APPROX_COUNT_DISTINCT(x, 42)`,
			msg:   `precision has to be in range [4, 16]`,
		},
		{
			query: `SELECT SUM(*)`,
			msg:   `cannot use * with SUM`,
		},
		{
			query: `SELECT MIN(*)`,
			msg:   `cannot use * with MIN`,
		},
		{
			query: `SELECT MAX(*)`,
			msg:   `cannot use * with MAX`,
		},
		{
			query: `SELECT BOOL_OR(*)`,
			msg:   `cannot use * with BOOL_OR`,
		},
		{
			query: `SELECT BOOL_AND(*)`,
			msg:   `cannot use * with BOOL_AND`,
		},
		{
			query: `SELECT BIT_AND(*)`,
			msg:   `cannot use * with BIT_AND`,
		},
		{
			query: `SELECT BIT_OR(*)`,
			msg:   `cannot use * with BIT_OR`,
		},
		{
			query: `SELECT BIT_XOR(*)`,
			msg:   `cannot use * with BIT_XOR`,
		},
		{
			query: `SELECT sneller_datashape(x) FROM table`,
			msg:   `accepts only *`,
		},
		{
			query: `SELECT 1.test`,
			msg:   `strconv.ParseFloat: parsing "1.test": invalid syntax`,
		},
		{
			query: `SELECT 0x1x1234`,
			msg:   `strconv.ParseFloat: parsing "0x1x1234": invalid syntax`,
		},
		{
			query: `SELECT 1234test(5+3)`,
			msg:   `strconv.ParseFloat: parsing "1234test": invalid syntax`,
		},
		{
			query: `SELECT 2e+5e1e-5`,
			msg:   `strconv.ParseFloat: parsing "2e+5e1e-5": invalid syntax`,
		},
		{
			query: `SELECT 1e++5`,
			msg:   `strconv.ParseFloat: parsing "1e+": invalid syntax`,
		},
		{
			query: `SELECT 1e---5`,
			msg:   `strconv.ParseFloat: parsing "1e-": invalid syntax`,
		},
		{
			// for now, this syntax isn't supported
			//
			// in principle we could parse this as
			//   NUMBER '.' IDENTIFIER(x)
			// but the lexer isn't clever enough to
			// handle the second dot; that's probably okay
			query: "SELECT 3.4.x",
			msg:   "",
		},
		{
			query: `SELECT col~COUNT(col2)`,
			msg:   `unexpected AGGREGATE, expecting STRING`,
		},
		{
			query: `SELECT col~*COUNT(col2)`,
			msg:   `unexpected AGGREGATE, expecting STRING`,
		},
		{
			query: `SELECT col~~COUNT(col2)`,
			msg:   `unexpected AGGREGATE, expecting STRING`,
		},
		{
			query: `SELECT col~~*COUNT(col2)`,
			msg:   `unexpected AGGREGATE, expecting STRING`,
		},
	}

	for i := range testcases {
		tc := testcases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err := Parse([]byte(tc.query))
			if err == nil {
				t.Error("expected an error")
				return
			}

			msg := fmt.Sprintf("%s", err)
			if !strings.Contains(msg, tc.msg) {
				t.Logf("query: %s", tc.query)
				t.Logf("got:   %s", msg)
				t.Logf("want:  %s", tc.msg)
				t.Error("error message does not contain the expected substring")
			}
		})
	}
}

func TestParseIdentifiers(t *testing.T) {
	operators := []string{
		"+",
		"-",
		"*",
		"/",
		"%",
		"=",
		"<",
		"<=",
		">",
		">=",
		"!=",
		"<>",
		"<<",
		">>",
		">>>",
		"|",
		"&",
		"^",
	}

	for i := range operators {
		query := fmt.Sprintf("SELECT col%sCOUNT(col2)", operators[i])
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			e, err := Parse([]byte(query))
			if err != nil {
				t.Logf("query: %q", query)
				t.Error(err)
			}

			q := e.Body.(*expr.Select)
			v := &testaggsearch{}
			expr.Walk(v, q)

			if !v.hasagg {
				t.Logf("query: %q", query)
				t.Errorf(`"COUNT(col2)" expected to be parsed as an aggregate`)
			}
		})
	}
}

type testaggsearch struct {
	hasagg bool
}

func (t *testaggsearch) Visit(e expr.Node) expr.Visitor {
	if t.hasagg {
		return nil
	}

	_, ok := e.(*expr.Aggregate)
	if ok {
		t.hasagg = true
		return nil
	}

	return t
}

func testEquivalence(t *testing.T, e expr.Node) {
	var obuf ion.Buffer
	var st ion.Symtab
	e.Encode(&obuf, &st)

	res, _, err := expr.Decode(&st, obuf.Bytes())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}

	if !expr.Equivalent(e, res) {
		t.Errorf("input : %s", e)
		t.Errorf("output: %s", res)
	}
}
