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
}

func TestParseSFW(t *testing.T) {
	for i := range sameq {
		e, err := Parse([]byte(sameq[i]))
		if err != nil {
			t.Errorf("case %q: %s", sameq[i], err)
			// do it again, this time with debug
			yyDebug = 3
			Parse([]byte(sameq[i]))
			yyDebug = 0
			continue
		}
		if e == nil {
			t.Errorf("case %q: didn't match...?", sameq[i])
			continue
		}
		want := sameq[i]
		if got := e.Text(); got != want {
			t.Errorf("case %d: got %q, want %q", i, got, want)
		}
		testEquivalence(t, e.Body)
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
			"select x[\"y\"] from foo",
			"SELECT x.y FROM foo",
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
			"SELECT * FROM foo WHERE date < `2006-01-02T15:04:05.999Z`",
			"SELECT * FROM foo WHERE BEFORE(date, `2006-01-02T15:04:05.999Z`)",
		},
		{
			"SELECT * FROM foo WHERE date > `2006-01-02T15:04:05.999Z`",
			"SELECT * FROM foo WHERE BEFORE(`2006-01-02T15:04:05.999Z`, date)",
		},
		{
			"SELECT * FROM foo WHERE date > UTCNOW()",
			"SELECT * FROM foo WHERE BEFORE(`2006-01-02T15:04:05.999Z`, date)",
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
