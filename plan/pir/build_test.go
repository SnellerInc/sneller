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
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/tests"

	"golang.org/x/exp/slices"
)

const (
	uintType   = expr.TypeSet(1 << ion.UintType)
	stringType = expr.TypeSet(1 << ion.StringType)
	intType    = uintType | expr.TypeSet(1<<ion.IntType)
	countType  = uintType
)

func mkenv(h expr.Hint, idx *blockfmt.Index, parts []string) Env {
	if h == nil && idx == nil && parts == nil {
		return nil
	}
	return &testenv{hint: h, idx: idx, parts: parts}
}

func TestBuildError(t *testing.T) {
	tests := []struct {
		input  string
		rx     string
		schema expr.Hint
	}{
		{
			// when a table has been bound explicitly,
			// require every variable reference that terminates
			// at the table to use the table reference
			input: "select t.x, y from table as t",
			rx:    "undefined",
		},
		{
			// test that the variable binding
			// is tracked correctly here to refer
			// to "foo"+1, which should trigger a type-checking error
			input: `select 'foo' as x, x+1 as y from foo`,
			rx:    `ill-typed`,
		},
		{
			// similar to above, but across a nested select
			// (cannot perform concatenation with int + string)
			input: `select x || 'foo' from (select count(x) as x from y)`,
			rx:    `ill-typed`,
		},
		{
			// similar to above, but with a CTE
			input: `with outer AS (select count(x) as x from y) select x || 'foo' from (select x from outer)`,
			rx:    `ill-typed`,
		},
		{
			input: `select sum(count(y)) from table`,
			rx:    `nested aggregate`,
		},
		{
			// similar to above, but aggregates
			// have deeper nesting
			input: `select sum(count(y)/3)+1 from table`,
			rx:    `nested aggregate`,
		},
		{
			// reject an arithmetic expression based on
			// the types of the arguments provided by
			// the schema
			schema: mkschema("x", stringType, "y", expr.NumericType),
			input:  `select t.x+t.y from table t`,
			rx:     `ill-typed`,
		},
		{
			// same as above, but obfuscated with some re-binding
			schema: mkschema("x", stringType, "y", expr.NumericType),
			input:  `select xthree+ythree from (select xtwo as xthree, ytwo as ythree from (select x as xtwo, y as ytwo from table))`,
			rx:     `ill-typed`,
		},
		{
			input: `select x, y, z from foo order by x`,
			rx:    "requires a LIMIT",
		},
		{
			input: `select * from tbl order by timestamp desc limit 100000000000`,
			rx:    "LIMIT\\+OFFSET",
		},
		{
			input: `select * from tbl order by timestamp desc limit 10 offset 99999`,
			rx:    "LIMIT\\+OFFSET",
		},
		{
			input: `select x, y from tbl group by sum(x) over (partition by y)`,
			rx:    "GROUP BY cannot contain aggregate",
		},
		{
			input: `SELECT x FROM table WHERE AVG(x) > 1.5`,
			rx:    "aggregate functions are not allowed in WHERE",
		},
		{
			input: `SELECT SUM(x) FILTER (WHERE MAX(x) < 42) FROM table`,
			rx:    "cannot handle nested aggregate",
		},
		{
			input: `SELECT DISTINCT x, y, z FROM table GROUP BY x, y`,
			rx:    "z references an unbound variable",
		},
		{
			input: "SELECT x FROM UNPIVOT table AS v AT a",
			rx:    "path x references an unbound variable",
		},
		{
			input: "SELECT a FROM UNPIVOT table AS a AT a",
			rx:    "the AS and AT UNPIVOT labels must not be the same 'a'",
		},
		{
			input: `SELECT x, ROW_NUMBER() OVER() FROM tbl`,
			rx:    "meaningless without ORDER BY",
		},
		{
			input: `SELECT x, ROW_NUMBER() OVER () FROM tbl GROUP BY x`,
			rx:    "meaningless without ORDER BY",
		},
		{
			input: `SELECT x, COUNT(*), ROW_NUMBER() OVER () FROM tbl GROUP BY x`,
			rx:    "meaningless without ORDER BY",
		},
		{
			// legal but not supported: COUNT(*) isn't part of the outer aggregation
			input: `SELECT x, SUM(y), ROW_NUMBER() OVER (ORDER BY COUNT(*)) FROM tbl GROUP BY x`,
			rx:    "bound outside the window",
		},
		{
			// legal but not supported: PARTITION BY element isn't explicitly bound
			input: `SELECT x, SUM(y), ROW_NUMBER() OVER (PARTITION BY x+100 ORDER BY SUM(y)) FROM tbl GROUP BY x`,
			rx:    "bound outside the window",
		},
		{
			// implicit recursive aggregate via window functions:
			input: `SELECT x, COUNT(*), ROW_NUMBER() OVER (ORDER BY COUNT(*)) AS rn, RANK() OVER (ORDER BY rn)`,
			rx:    "nested aggregate",
		},
		{
			// decorellation error: x refers to expression containing the sub-query
			input: `SELECT EXISTS (SELECT 1 FROM table1 WHERE x = y) AS x FROM table`,
			rx:    `is self-referenced as "x" in the outer query`,
		},
		{
			// join on with erronous syntax (issue #2471)
			input: `SELECT passenger_count FROM table JOIN X ON X=Y`,
			rx:    `unable to eliminate join`,
		},
		{
			input: `SELECT DISTINCT ON (a, b) x, y, z FROM table GROUP BY x AS a, y AS b`,
			rx:    "x references an unbound variable",
		},
		{
			input: `SELECT DISTINCT z, x, AVG(y) AS y FROM table GROUP BY x, AVG(y), z`,
			rx:    `GROUP BY cannot contain aggregate`,
		},
		{
			// regression test: rewriter returned nil
			input: `SELECT 1 + (SELECT 1 + (SELECT X) FROM table1) FROM table2`,
			rx:    `path X references an unbound variable`,
		},
	}
	for i := range tests {
		in := tests[i].input
		rx := tests[i].rx
		schema := tests[i].schema
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			s, err := partiql.Parse([]byte(in))
			if err != nil {
				t.Fatal(err)
			}
			b, err := Build(s, mkenv(schema, nil, nil))
			if err == nil {
				var str strings.Builder
				b.Describe(&str)
				t.Logf("plan: %s", str.String())
				t.Fatal("no error?")
			}
			errstr := err.Error()
			match, err := regexp.MatchString(rx, errstr)
			if err != nil {
				t.Fatal(err)
			}
			if !match {
				t.Errorf("error '%s' didn't match '%s'", errstr, rx)
			}
		})
	}
}

type testenv struct {
	hint  expr.Hint
	idx   *blockfmt.Index
	parts []string
}

type testindex struct {
	idx   *blockfmt.Index
	parts []string
}

func (t *testindex) TimeRange(path []string) (min, max date.Time, ok bool) {
	if t.idx == nil {
		ok = false
		return
	}
	return t.idx.TimeRange(path)
}

func (t *testindex) HasPartition(x string) bool {
	return slices.Contains(t.parts, x)
}

func (e *testenv) Schema(expr.Node) expr.Hint {
	return e.hint
}

func (e *testenv) Index(expr.Node) (Index, error) {
	return &testindex{idx: e.idx, parts: e.parts}, nil
}

type nameType struct {
	field string
	typ   expr.TypeSet
}

// flatSchema is a dumb schema implementation
// that just tracks types of a flat set of
// binding values, and otherwise yields MISSING
type flatSchema []nameType

func (f flatSchema) TypeOf(e expr.Node) expr.TypeSet {
	p, ok := e.(expr.Ident)
	if !ok {
		return expr.AnyType
	}
	for i := range f {
		if f[i].field == string(p) {
			return f[i].typ
		}
	}
	return expr.MissingType
}

func mkschema(args ...interface{}) expr.Hint {
	var out flatSchema
	for i := 0; i < len(args); i += 2 {
		out = append(out, nameType{field: args[i].(string), typ: args[i+1].(expr.TypeSet)})
	}
	return out
}

type buildTestcase struct {
	input   string
	expect  []string
	split   []string
	results []expr.TypeSet
	schema  expr.Hint // applied to the inner-most table expression
	index   *blockfmt.Index
	parts   []string
}

func TestBuild(t *testing.T) {
	basetime, _ := date.Parse([]byte("2022-02-22T20:22:22Z"))
	now := func(hours int) date.Time {
		return basetime.Add(time.Duration(hours) * time.Hour)
	}
	tests := []buildTestcase{
		{
			input: "SELECT cols FROM UNPIVOT (SELECT key FROM input) AT cols GROUP BY cols",
			expect: []string{
				"ITERATE input FIELDS [key]",
				"PROJECT key AS key",
				"UNPIVOT_AT_DISTINCT cols",
				"PROJECT cols AS cols",
			},
			split: []string{
				"UNION MAP input (",
				"	ITERATE PART input FIELDS [key]",
				"	PROJECT key AS key",
				"	UNPIVOT_AT_DISTINCT cols)",
				"FILTER DISTINCT [cols]",
				"PROJECT cols AS cols",
			},
		},
		{
			input: "SELECT COUNT(*), key FROM UNPIVOT input AS val AT key GROUP BY key",
			expect: []string{
				"ITERATE input FIELDS *",
				"UNPIVOT AS val AT key",
				"AGGREGATE COUNT(*) AS \"count\" BY key AS key",
			},
		},
		{
			input: "SELECT v FROM UNPIVOT input AS v AT a",
			expect: []string{
				"ITERATE input FIELDS *",
				"UNPIVOT AS v AT a",
				"PROJECT v AS v",
			},
		},
		{
			input: "select 3, 'foo' || 'bar'",
			expect: []string{
				"[{}]",
				"PROJECT 3 AS _1, 'foobar' AS _2",
			},
			// splitting shouldn't have any semantic effect:
			split: []string{
				"[{}]",
				"PROJECT 3 AS _1, 'foobar' AS _2",
			},
		},
		{
			input: `select 'x' as x, (select * from inner_table limit 2) as y`,
			expect: []string{
				"WITH (",
				"	ITERATE inner_table FIELDS *",
				"	LIMIT 2",
				") AS REPLACEMENT(0)",
				"[{}]",
				"PROJECT 'x' AS x, LIST_REPLACEMENT(0) AS y",
			},
			split: []string{
				"WITH (",
				"	UNION MAP inner_table (",
				"		ITERATE PART inner_table FIELDS *",
				"		LIMIT 2)",
				"	LIMIT 2",
				") AS REPLACEMENT(0)",
				"[{}]",
				"PROJECT 'x' AS x, LIST_REPLACEMENT(0) AS y",
			},
		},
		{
			// test left-to-right constprop
			input: "select 3 as x, x+1 as y, (select x from foo WHERE 0 > 1) is missing as z from foo as t",
			expect: []string{
				"ITERATE foo AS t FIELDS []",
				"PROJECT 3 AS x, 4 AS y, TRUE AS z",
			},
			split: []string{
				"UNION MAP foo AS t (",
				"	ITERATE PART foo AS t FIELDS []",
				"	PROJECT 3 AS x, 4 AS y, TRUE AS z)",
			},
			// note: any arithmetic gets promoted (for now),
			// so the result of 'x+1' is analyzed as possibly float
			// even though in practice it is always integral...
			results: []expr.TypeSet{intType, expr.NumericType, expr.BoolType},
		},
		{
			// constprop with structure references
			input: "select val.y as final from (select {'y': x + 3} as val from foo)",
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"PROJECT x + 3 AS final",
			},
		},
		{
			// constprop with list references
			input: "select val[2] as final from (select [x, y, z] as val from foo)",
			expect: []string{
				"ITERATE foo FIELDS [z]",
				"PROJECT z AS final",
			},
		},
		{
			// test that we do not duplicate the "count" field
			input: `select count(*), count(field) from foo`,
			expect: []string{
				"ITERATE foo FIELDS [field]",
				"AGGREGATE COUNT(*) AS \"count\", COUNT(field) AS count_2",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [field]",
				"	AGGREGATE COUNT(*) AS $_2_0, COUNT(field) AS $_2_1)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\", SUM_COUNT($_2_1) AS count_2",
			},
		},
		{
			input: `select sum(x) from foo where y in (select y from foo order by y desc limit 5)`,
			expect: []string{
				"WITH (",
				"	ITERATE foo FIELDS [y]",
				"	ORDER BY y DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT y AS y",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x, y] WHERE IN_REPLACEMENT(y, 0)",
				"AGGREGATE SUM(x) AS \"sum\"",
			},
			split: []string{
				"WITH (",
				"	UNION MAP foo (",
				"		ITERATE PART foo FIELDS [y]",
				"		ORDER BY y DESC NULLS FIRST",
				"		LIMIT 5)",
				"	ORDER BY y DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT y AS y",
				") AS REPLACEMENT(0)",
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x, y] WHERE IN_REPLACEMENT(y, 0)",
				"	AGGREGATE SUM.PARTIAL(x) AS $_2_0)",
				"AGGREGATE SUM.MERGE($_2_0) AS \"sum\"",
			},
		},
		{
			input:  "select x, count(x) from foo group by x",
			schema: mkschema("x", stringType),
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"AGGREGATE COUNT(x) AS \"count\" BY x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x]",
				"	AGGREGATE COUNT(x) AS $_2_0 BY x AS x)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\" BY x AS x",
			},
			results: []expr.TypeSet{stringType, countType},
		},
		{
			input: `select avg(x), y from foo group by y`,
			expect: []string{
				"ITERATE foo FIELDS [x, y]",
				"AGGREGATE AVG(x) AS \"avg\" BY y AS y",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x, y]",
				"	AGGREGATE SUM.PARTIAL(x) AS $_2_0, COUNT(x + 0) AS $_2_1 BY y AS y)",
				"AGGREGATE SUM.MERGE($_2_0) AS \"avg\", SUM_COUNT($_2_1) AS $_1_0 BY y AS y",
				"PROJECT CASE WHEN $_1_0 = 0 THEN NULL ELSE \"avg\" / $_1_0 END AS \"avg\", y AS y",
			},
		},
		{
			input: "select o.x, i.y from foo as o, o.field as i where o.x <> i.y",
			expect: []string{
				"ITERATE foo AS o FIELDS [field, x]",
				"ITERATE FIELD field AS i",
				"FILTER x <> i.y",
				"PROJECT x AS x, i.y AS y",
			},
		},
		{
			// similar to above, but check that
			// we only push down part of a conjunction
			input: `select o.x, i.y from foo as o, o.field as i where o.x < 3 and i.y < 3`,
			expect: []string{
				"ITERATE foo AS o FIELDS [field, x] WHERE x < 3",
				"ITERATE FIELD field AS i",
				"FILTER i.y < 3",
				"PROJECT x AS x, i.y AS y",
			},
		},
		{
			// test that nested projection is flattened
			input: `select x+2 as x, y - 3 as y from (select 3 + a as x, b - 5 as y from foo)`,
			expect: []string{
				"ITERATE foo FIELDS [a, b]",
				"PROJECT a + 5 AS x, b - 8 AS y",
			},
			results: []expr.TypeSet{expr.NumericType | expr.MissingType, expr.NumericType | expr.MissingType},
		},
		{
			// test that nested projection is flattened; include CTE
			input: `with cte as (select 3 + a as x, b - 5 as y from foo) select x+2 as x, y - 3 as y from (select x, y from cte)`,
			expect: []string{
				"ITERATE foo FIELDS [a, b]",
				"PROJECT a + 5 AS x, b - 8 AS y",
			},
			results: []expr.TypeSet{expr.NumericType | expr.MissingType, expr.NumericType | expr.MissingType},
		},
		{
			// select * from top 5 of 'grp' by count
			input: `select * from foo
where grp in (select grp from (select count(*), grp from foo group by grp order by count(*) desc limit 5))`,
			expect: []string{
				"WITH (",
				"	ITERATE foo FIELDS [grp]",
				"	AGGREGATE COUNT(*) AS $_0_0 BY grp AS $_0_1",
				"	ORDER BY $_0_0 DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT $_0_1 AS grp",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS * WHERE IN_REPLACEMENT(grp, 0)",
			},
			split: []string{
				"WITH (",
				"	UNION MAP foo (",
				"		ITERATE PART foo FIELDS [grp]",
				"		AGGREGATE COUNT(*) AS $_2_0 BY grp AS $_0_1)",
				"	AGGREGATE SUM_COUNT($_2_0) AS $_0_0 BY $_0_1 AS $_0_1",
				"	ORDER BY $_0_0 DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT $_0_1 AS grp",
				") AS REPLACEMENT(0)",
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS * WHERE IN_REPLACEMENT(grp, 0))",
			},
		},
		{
			// TODO: these two uncorrelated sub-queries
			// could be combined and their output destructured
			// so that we only scanned the input once...
			input: `select * from foo
where x > (select min(f) from y) and x < (select max(f) from y)`,
			expect: []string{
				"WITH (",
				"	ITERATE y FIELDS [f]",
				"	AGGREGATE MIN(f) AS \"min\"",
				") AS REPLACEMENT(0)",
				"WITH (",
				"	ITERATE y FIELDS [f]",
				"	AGGREGATE MAX(f) AS \"max\"",
				") AS REPLACEMENT(1)",
				"ITERATE foo FIELDS * WHERE x > SCALAR_REPLACEMENT(0) AND x < SCALAR_REPLACEMENT(1)",
			},
		},
		{
			// test elimination of un-used bindings
			input: `select count(x) from (select x, y from foo)`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"AGGREGATE COUNT(x) AS \"count\"",
			},
			results: []expr.TypeSet{countType},
		},
		{
			// test push-down across a projection
			input: `select x from (select (y / 2) as x from foo) where x < 3 limit 100`,
			expect: []string{
				"ITERATE foo FIELDS [y] WHERE y / 2 < 3",
				"LIMIT 100",
				"PROJECT y / 2 AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [y] WHERE y / 2 < 3",
				"	LIMIT 100)",
				"LIMIT 100",
				"PROJECT y / 2 AS x",
			},
			results: []expr.TypeSet{expr.NumericType | expr.MissingType},
		},
		{
			// test that 'select *' properly makes
			// the most recent binding set live
			input: `select * from (select o.x, i.y from foo as o, o.field as i) where x = y`,
			expect: []string{
				"ITERATE foo AS o FIELDS [field, x]",
				"ITERATE FIELD field AS i",
				"FILTER x = i.y",
				"PROJECT x AS x, i.y AS y",
			},
		},
		{
			// test that references in ORDER BY
			// are correctly resolved, since the
			// scoping rules are a little weird
			input:  `select COUNT(x), y, z from foo group by y, z order by COUNT(x)`,
			schema: mkschema("x", expr.NumericType, "y", stringType, "z", countType),
			expect: []string{
				"ITERATE foo FIELDS [x, y, z]",
				"AGGREGATE COUNT(x) AS \"count\" BY y AS y, z AS z",
				"ORDER BY \"count\" ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x, y, z]",
				"	AGGREGATE COUNT(x) AS $_2_0 BY y AS y, z AS z)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\" BY y AS y, z AS z",
				"ORDER BY \"count\" ASC NULLS FIRST",
			},
			// TODO: we know the grouping column for a
			// hash aggregate is never MISSING, so really
			// the second result type here could exclude
			// the missing bit
			results: []expr.TypeSet{countType, stringType, countType},
		},
		{
			input: `select Make, count(Make) as c from 'parking.10n' group by Make having count(Make) = 122`,
			expect: []string{
				"ITERATE 'parking.10n' FIELDS [Make]",
				"AGGREGATE COUNT(Make) AS $_0_0 BY Make AS $_0_1",
				"FILTER $_0_0 = 122",
				"PROJECT $_0_1 AS Make, $_0_0 AS c",
			},
			split: []string{
				"UNION MAP 'parking.10n' (",
				"	ITERATE PART 'parking.10n' FIELDS [Make]",
				"	AGGREGATE COUNT(Make) AS $_2_0 BY Make AS $_0_1)",
				"AGGREGATE SUM_COUNT($_2_0) AS $_0_0 BY $_0_1 AS $_0_1",
				"FILTER $_0_0 = 122",
				"PROJECT $_0_1 AS Make, $_0_0 AS c",
			},
		},
		{
			// matrix unnesting (gross) with weird push-down
			input: `select top.x, middle.y, bottom.z
from table as top, top.field as middle, middle.field as bottom
where top.x = middle.y and middle.y = bottom.z`,
			expect: []string{
				"ITERATE table AS top FIELDS [field, x]",
				"ITERATE FIELD field AS middle",
				"FILTER x = middle.y",
				"ITERATE FIELD middle.field AS bottom",
				"FILTER middle.y = bottom.z",
				"PROJECT x AS x, middle.y AS y, bottom.z AS z",
			},
		},
		{
			input: `select out.Make as make, entry.Ticket as ticket, entry.Color as color
from 'parking3.ion' as out, out.Entries as entry
where out.Make = 'CHRY' and entry.BodyStyle = 'PA'`,
			expect: []string{
				"ITERATE 'parking3.ion' AS out FIELDS [Entries, Make] WHERE Make = 'CHRY'",
				"ITERATE FIELD Entries AS entry",
				"FILTER entry.BodyStyle = 'PA'",
				"PROJECT Make AS make, entry.Ticket AS ticket, entry.Color AS color",
			},
		},
		{
			input: `select count(distinct x) from table`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(x) AS \"count\"",
			},
			split: []string{
				"UNION MAP table (",
				"	ITERATE PART table FIELDS [x]",
				"	FILTER DISTINCT [x])",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(x) AS \"count\"",
			},
		},
		{
			input: `select count(distinct t.x), t.y from table as t group by t.y`,
			expect: []string{
				"ITERATE table AS t FIELDS [x, y]",
				"FILTER DISTINCT [x, y]",
				"AGGREGATE COUNT(x) AS \"count\" BY y AS y",
			},
			results: []expr.TypeSet{countType, expr.AnyType},
		},
		{
			// since count(*) does not reference any columns,
			// any projections that immediate precede it can
			// be eliminated entirely
			input: `select count(*) from (select distinct x from table)`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(*) AS \"count\"",
			},
		},
		{
			input: `select x, y, z from t order by x LIMIT 9999`,
			expect: []string{
				"ITERATE t FIELDS [x, y, z]",
				"ORDER BY x ASC NULLS FIRST",
				"LIMIT 9999",
				"PROJECT x AS x, y AS y, z AS z",
			},
			split: []string{
				"UNION MAP t (",
				"	ITERATE PART t FIELDS [x, y, z]",
				"	ORDER BY x ASC NULLS FIRST",
				"	LIMIT 9999)",
				"ORDER BY x ASC NULLS FIRST",
				"LIMIT 9999",
				"PROJECT x AS x, y AS y, z AS z",
			},
		},
		{
			input: `select count(x)+1 as x from table order by x`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"AGGREGATE COUNT(x) AS $_0_0",
				"PROJECT $_0_0 + 1 AS x",
			},
			split: []string{
				"UNION MAP table (",
				"	ITERATE PART table FIELDS [x]",
				"	AGGREGATE COUNT(x) AS $_2_0)",
				"AGGREGATE SUM_COUNT($_2_0) AS $_0_0",
				"PROJECT $_0_0 + 1 AS x",
			},
			results: []expr.TypeSet{expr.IntegerType},
		},
		{
			input: `select count(x)+count(y) as "both" from table`,
			expect: []string{
				"ITERATE table FIELDS [x, y]",
				"AGGREGATE COUNT(x) AS $_0_0, COUNT(y) AS $_0_1",
				`PROJECT $_0_0 + $_0_1 AS "both"`,
			},
			results: []expr.TypeSet{expr.UnsignedType},
		},
		{
			// aggregate expression with computation on grouping column
			//
			// NOTE: is GROUP BY ... AS ... even reasonable?
			// We support it, but mostly by coincidence.
			input: `select sum(x)/count(x) as av, TRIM(z) as c from table group by y as z`,
			expect: []string{
				"ITERATE table FIELDS [x, y]",
				"AGGREGATE SUM(x) AS $_0_0, COUNT(x) AS $_0_1 BY y AS z",
				"PROJECT $_0_0 / $_0_1 AS av, TRIM(z) AS c",
			},
			results: []expr.TypeSet{expr.NumericType | expr.MissingType, expr.StringType | expr.MissingType},
		},
		{
			// test that the type information related to count(x)
			// causes us to eliminate 'where c is not missing',
			// since it is trivially true
			input: `select * from (select count(x) as c from table) where c is not missing`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"AGGREGATE COUNT(x) AS c",
			},
			results: []expr.TypeSet{countType},
		},
		{
			input: `select count(x) as c, y from table group by y having count(x) > 100`,
			expect: []string{
				"ITERATE table FIELDS [x, y]",
				"AGGREGATE COUNT(x) AS $_0_0 BY y AS $_0_1",
				"FILTER $_0_0 > 100",
				// FIXME: this projection can be eliminated;
				// we need to notice that the preceding 'filter'
				// can trivially be re-written after changing
				// the outputs of the AGGREGATE step
				"PROJECT $_0_0 AS c, $_0_1 AS y",
			},
		},
		{
			// composite aggregate expression with
			// 'having' referencing part of the composite expression
			input: `select count(x)+count(y) as p, z from table group by z having count(y) > 100`,
			expect: []string{
				"ITERATE table FIELDS [x, y, z]",
				"AGGREGATE COUNT(y) AS $_0_0, COUNT(x) AS $_0_1 BY z AS $_0_2",
				"FILTER $_0_0 > 100",
				"PROJECT $_0_1 + $_0_0 AS p, $_0_2 AS z",
			},
			results: []expr.TypeSet{expr.UnsignedType, expr.AnyType},
		},
		{
			input: `select * from foo where 1 > 2`,
			expect: []string{
				"NO OUTPUT",
			},
		},
		{
			input: `select * from foo order by x desc limit 10`,
			expect: []string{
				"ITERATE foo FIELDS *",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS *",
				"	ORDER BY x DESC NULLS FIRST",
				"	LIMIT 10)",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10",
			},
		},
		{
			input: `select * from foo order by x desc limit 10 offset 64`,
			expect: []string{
				"ITERATE foo FIELDS *",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10 OFFSET 64",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS *",
				"	ORDER BY x DESC NULLS FIRST",
				"	LIMIT 74)",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10 OFFSET 64",
			},
		},
		{
			input: `select x, count(*) from foo group by x limit 10 offset 64`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"AGGREGATE COUNT(*) AS \"count\" BY x AS x",
				"LIMIT 10 OFFSET 64",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x]",
				"	AGGREGATE COUNT(*) AS $_2_0 BY x AS x)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\" BY x AS x",
				"LIMIT 10 OFFSET 64",
			},
		},
		{
			// check that a limit after DISTINCT is pushed
			// into the mapping *and* reduction steps
			input: `select distinct x from foo limit 50`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"FILTER DISTINCT [x]",
				"LIMIT 50",
				"PROJECT x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x]",
				"	FILTER DISTINCT [x]",
				"	LIMIT 50)",
				"FILTER DISTINCT [x]",
				"LIMIT 50",
				"PROJECT x AS x",
			},
		},
		{
			input: `select distinct x from foo limit 50 offset 150`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"FILTER DISTINCT [x]",
				"LIMIT 50 OFFSET 150",
				"PROJECT x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x]",
				"	FILTER DISTINCT [x]",
				"	LIMIT 200)",
				"FILTER DISTINCT [x]",
				"LIMIT 50 OFFSET 150",
				"PROJECT x AS x",
			},
		},
		{
			// see Issue #534
			input: `select list, list[1] from foo`,
			expect: []string{
				"ITERATE foo FIELDS [list]",
				"PROJECT list AS list, list[1] AS list_1",
			},
		},
		{
			// test left-to-right flattening of variables (see Issue #534)
			input: `select list[0] AS x, x[0] AS y, y.z AS z from foo`,
			expect: []string{
				"ITERATE foo FIELDS [list]",
				"PROJECT list[0] AS x, list[0][0] AS y, list[0][0].z AS z",
			},
		},
		{
			input: `select EARLIEST(x), LATEST(x) from foo`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"AGGREGATE EARLIEST(x) AS \"min\", LATEST(x) AS \"max\"",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x]",
				"	AGGREGATE EARLIEST(x) AS $_2_0, LATEST(x) AS $_2_1)",
				"AGGREGATE EARLIEST($_2_0) AS \"min\", LATEST($_2_1) AS \"max\"",
			},
		},
		{
			input: `with cte0 as (SELECT x, y, z FROM foo),
						 cte1 as (SELECT x, y FROM cte0)
					SELECT x FROM cte1`,
			expect: []string{
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x",
			},
		},
		{
			// test that the type information related to count(x)
			// causes us to eliminate 'where c is not missing',
			// since it is trivially true
			input: `with ccount as (select count(x) as c from table)
select * from (select c from ccount) where c is not missing`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"AGGREGATE COUNT(x) AS c",
			},
			results: []expr.TypeSet{countType},
		},
		{
			// test that aggregates can be
			// fully eliminated using the index
			input: `select EARLIEST(t.ts), LATEST(t.ts) from table`,
			index: mkindex([][]blockfmt.Range{{
				timeRange("t.ts", now(0), now(1)),
			}, {
				timeRange("t.ts", now(1), now(2)),
				timeRange("x.xx", now(10), now(20)),
			}}),
			expect: []string{
				"[{}]",
				"PROJECT `2022-02-22T20:22:22Z` AS \"min\", `2022-02-22T22:22:22Z` AS \"max\"",
			},
			results: []expr.TypeSet{
				expr.TimeType,
				expr.TimeType,
			},
		},
		{
			// test that aggregates can be
			// partially eliminated using the index
			input: `select COUNT(t), LATEST(t.ts) from table`,
			index: mkindex([][]blockfmt.Range{{
				timeRange("t.ts", now(0), now(1)),
			}, {
				timeRange("t.ts", now(1), now(2)),
				timeRange("x.xx", now(10), now(20)),
			}}),
			expect: []string{
				"ITERATE table FIELDS [t]",
				"AGGREGATE COUNT(t) AS $_0_0",
				"PROJECT $_0_0 AS \"count\", `2022-02-22T22:22:22Z` AS \"max\"",
			},
		},
		{
			input: `
SELECT m, d, h, COUNT(*)
FROM (SELECT EXTRACT(MONTH FROM timestamp) m, EXTRACT(DAY FROM timestamp) d, EXTRACT(HOUR FROM timestamp) h
      FROM foo)
WHERE m = 3 AND d >= 9
GROUP BY m, d, h
ORDER BY m, d, h`,
			expect: []string{
				"ITERATE foo FIELDS [timestamp] WHERE DATE_EXTRACT_MONTH(timestamp) = 3 AND DATE_EXTRACT_DAY(timestamp) >= 9",
				"AGGREGATE COUNT(*) AS \"count\" BY DATE_EXTRACT_MONTH(timestamp) AS m, DATE_EXTRACT_DAY(timestamp) AS d, DATE_EXTRACT_HOUR(timestamp) AS h",
				"ORDER BY m ASC NULLS FIRST, d ASC NULLS FIRST, h ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [timestamp] WHERE DATE_EXTRACT_MONTH(timestamp) = 3 AND DATE_EXTRACT_DAY(timestamp) >= 9",
				"	AGGREGATE COUNT(*) AS $_2_0 BY DATE_EXTRACT_MONTH(timestamp) AS m, DATE_EXTRACT_DAY(timestamp) AS d, DATE_EXTRACT_HOUR(timestamp) AS h)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\" BY m AS m, d AS d, h AS h",
				"ORDER BY m ASC NULLS FIRST, d ASC NULLS FIRST, h ASC NULLS FIRST",
			},
		},
		{
			input: `select x, (select z from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [y, z]",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select max(y) from bar where x = y) from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [y]",
				"	AGGREGATE MAX(y) AS \"max\" BY y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS _2",
			},
		},
		{
			input: `select x, y, (select b.z from bar b where b.y = x limit 1) from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar AS b FIELDS [y, z]",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x, y]",
				"PROJECT x AS x, y AS y, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS _3",
			},
		},
		{
			input: `select x, (select z from bar where a = 1 and x = y and b = 2 limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [a, b, y, z] WHERE a = 1 AND b = 2",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select a, b, c from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [a, b, c, y]",
				"	FILTER DISTINCT [y]",
				"	PROJECT a AS a, b AS b, c AS c, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'struct', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select min(a), max(b), count(c) from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [a, b, c, y]",
				"	AGGREGATE MIN(a) AS \"min\", MAX(b) AS \"max\", COUNT(c) AS \"count\" BY y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'struct', '$_0_0', x) AS z",
			},
		},
		{
			input: "SELECT TIME_BUCKET(timestamp, 864000) AS _tmbucket1, COUNT(*), AVG(AvgTicketPrice) AS _sum1 FROM kibana_sample_data_flights WHERE timestamp BETWEEN `2022-03-01T00:00:00.000Z` AND `2022-07-01T00:00:00.000Z` GROUP BY TIME_BUCKET(timestamp, 864000) ORDER BY _tmbucket1",
			expect: []string{
				"ITERATE kibana_sample_data_flights FIELDS [AvgTicketPrice, timestamp] WHERE timestamp >= `2022-03-01T00:00:00Z` AND timestamp <= `2022-07-01T00:00:00Z`",
				"AGGREGATE COUNT(*) AS \"count\", AVG(AvgTicketPrice) AS _sum1 BY TIME_BUCKET(timestamp, 864000) AS _tmbucket1",
				"ORDER BY _tmbucket1 ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP kibana_sample_data_flights (",
				"	ITERATE PART kibana_sample_data_flights FIELDS [AvgTicketPrice, timestamp] WHERE timestamp >= `2022-03-01T00:00:00Z` AND timestamp <= `2022-07-01T00:00:00Z`",
				"	AGGREGATE COUNT(*) AS $_2_0, SUM.PARTIAL(AvgTicketPrice) AS $_2_1, COUNT(AvgTicketPrice + 0) AS $_2_2 BY TIME_BUCKET(timestamp, 864000) AS _tmbucket1)",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\", SUM.MERGE($_2_1) AS _sum1, SUM_COUNT($_2_2) AS $_1_1 BY _tmbucket1 AS _tmbucket1",
				"ORDER BY _tmbucket1 ASC NULLS FIRST",
				"PROJECT \"count\" AS \"count\", CASE WHEN $_1_1 = 0 THEN NULL ELSE _sum1 / $_1_1 END AS _sum1, _tmbucket1 AS _tmbucket1",
			},
		},
		{
			// simple INTO with no preceding aggregation, etc.
			input: `SELECT x, y, z INTO db.table FROM foo WHERE x > 3 AND x < 100`,
			expect: []string{
				"ITERATE foo FIELDS [x, y, z] WHERE x > 3 AND x < 100",
				"PROJECT x AS x, y AS y, z AS z",
				"OUTPUT PART db/db/table",
				"OUTPUT INDEX db.table AT db/db/table",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x, y, z] WHERE x > 3 AND x < 100",
				"	PROJECT x AS x, y AS y, z AS z",
				"	OUTPUT PART db/db/table)",
				"OUTPUT INDEX db.table AT db/db/table",
			},
			results: []expr.TypeSet{expr.StringType},
		},
		{
			// INTO with leading reduction steps
			input: `SELECT x, SUM(y) INTO my.stats FROM foo GROUP BY x`,
			expect: []string{
				"ITERATE foo FIELDS [x, y]",
				"AGGREGATE SUM(y) AS \"sum\" BY x AS x",
				"OUTPUT PART db/my/stats",
				"OUTPUT INDEX my.stats AT db/my/stats",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [x, y]",
				"	AGGREGATE SUM.PARTIAL(y) AS $_2_0 BY x AS x)",
				"AGGREGATE SUM.MERGE($_2_0) AS \"sum\" BY x AS x",
				"OUTPUT PART db/my/stats",
				"OUTPUT INDEX my.stats AT db/my/stats",
			},
			results: []expr.TypeSet{expr.StringType},
		},
		{
			// EXISTS -> semi-join
			input: `SELECT x, EXISTS(SELECT * FROM other WHERE key = x) AS has_other FROM input`,
			expect: []string{
				"WITH (",
				"	ITERATE other FIELDS [key]",
				"	FILTER DISTINCT [key]",
				"	PROJECT key AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE input FIELDS [x]",
				"PROJECT x AS x, IN_REPLACEMENT(x, 0) AS has_other",
			},
		},
		{
			// weird NOT EXISTS -> semi-join
			input: `SELECT x, (SELECT TRUE FROM other WHERE key = x LIMIT 1) IS MISSING AS no_other FROM input`,
			expect: []string{
				"WITH (",
				"	ITERATE other FIELDS [key]",
				"	FILTER DISTINCT [key]",
				"	PROJECT key AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE input FIELDS [x]",
				"PROJECT x AS x, !(IN_REPLACEMENT(x, 0)) AS no_other",
			},
		},
		{
			input: `SELECT y, z FROM table GROUP BY x+1 AS y, z`,
			expect: []string{
				"ITERATE table FIELDS [x, z]",
				"FILTER DISTINCT [x + 1, z]",
				"PROJECT x + 1 AS y, z AS z",
			},
		},
		{
			input: `SELECT grp FROM table GROUP BY grp ORDER BY COUNT(*) DESC`,
			expect: []string{
				"ITERATE table FIELDS [grp]",
				"AGGREGATE COUNT(*) AS $_0_1 BY grp AS $_0_0",
				"ORDER BY $_0_1 DESC NULLS FIRST",
				"PROJECT $_0_0 AS grp",
			},
		},
		{
			input: `SELECT grp0 FROM table GROUP BY grp0, grp1 ORDER BY -COUNT(*)`,
			expect: []string{
				"ITERATE table FIELDS [grp0, grp1]",
				"AGGREGATE COUNT(*) AS $_0_1 BY grp0 AS $_0_0, grp1",
				"ORDER BY -($_0_1) ASC NULLS FIRST",
				"PROJECT $_0_0 AS grp0",
			},
		},
		{
			input: `select x, COUNT(y) OVER (PARTITION BY z) AS wind FROM foo`,
			expect: []string{
				"WITH (",
				"	ITERATE foo FIELDS [y, z]",
				"	AGGREGATE COUNT(y) AS $__val BY z AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [x, z]",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$__key', z, 0) AS wind",
			},
		},
		{
			// window function + GROUP BY needs to compute
			// the DISTINCT set of grouping columns before
			// running the window function, since that is
			// the set of bindings available in the window
			input: `select x, y, SUM(var), COUNT(x) OVER (PARTITION BY y) AS x_per_y FROM foo WHERE z = 'foo' GROUP BY x, y`,
			expect: []string{
				"WITH (",
				"	ITERATE foo FIELDS [x, y, z] WHERE z = 'foo'",
				"	FILTER DISTINCT [x, y]",
				"	AGGREGATE COUNT(x) AS $__val BY y AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS [var, x, y, z] WHERE z = 'foo'",
				"AGGREGATE SUM(var) AS $_0_2 BY x AS $_0_0, y AS $_0_1",
				"PROJECT $_0_0 AS x, $_0_1 AS y, $_0_2 AS \"sum\", HASH_REPLACEMENT(0, 'scalar', '$__key', $_0_1, 0) AS x_per_y",
			},
		},
		{
			// same as above except with data already partitioned by y
			input: `select x, y, SUM(var), COUNT(x) OVER (PARTITION BY y) AS x_per_y FROM foo WHERE z = 'foo' GROUP BY x, y HAVING x_per_y > 100`,
			expect: []string{
				"WITH (",
				"	UNION MAP foo PARTITION BY y (",
				"		ITERATE PART foo FIELDS [x, z] WHERE z = 'foo'",
				"		FILTER DISTINCT [x, PARTITION_VALUE(0)]",
				"		AGGREGATE COUNT(x) AS $__val",
				"		PROJECT PARTITION_VALUE(0) AS $__key, $__val AS $__val)",
				") AS REPLACEMENT(0)",
				// TODO: recognize that we are doing a HASH_REPLACEMENT()
				// of a PARTITION_VALUE() and move the replacement step
				// into this subquery so that the hash lookup can be eliminated altogether
				"UNION MAP foo PARTITION BY y (",
				"	ITERATE PART foo FIELDS [var, x, z] WHERE z = 'foo'",
				"	AGGREGATE SUM(var) AS $_0_2 BY x AS $_0_1",
				"	FILTER HASH_REPLACEMENT(0, 'scalar', '$__key', PARTITION_VALUE(0), 0) > 100",
				"	PROJECT $_0_1 AS x, PARTITION_VALUE(0) AS y, $_0_2 AS \"sum\", HASH_REPLACEMENT(0, 'scalar', '$__key', PARTITION_VALUE(0), 0) AS x_per_y)",
			},
			split: []string{
				"WITH (",
				"	UNION MAP foo PARTITION BY y (",
				"		UNION MAP foo (",
				"			ITERATE PART foo FIELDS [x, z] WHERE z = 'foo'",
				"			FILTER DISTINCT [x, PARTITION_VALUE(0)])",
				"		FILTER DISTINCT [x, PARTITION_VALUE(0)]",
				"		AGGREGATE COUNT(x) AS $__val",
				"		PROJECT PARTITION_VALUE(0) AS $__key, $__val AS $__val)",
				") AS REPLACEMENT(0)",
				// TODO: recognize that we are doing a HASH_REPLACEMENT()
				// of a PARTITION_VALUE() and move the replacement step
				// into this subquery so that the hash lookup can be eliminated altogether
				"UNION MAP foo PARTITION BY y (",
				"	UNION MAP foo (",
				"		ITERATE PART foo FIELDS [var, x, z] WHERE z = 'foo'",
				"		AGGREGATE SUM.PARTIAL(var) AS $_2_0 BY x AS $_0_1)",
				"	AGGREGATE SUM.MERGE($_2_0) AS $_0_2 BY $_0_1 AS $_0_1",
				"	FILTER HASH_REPLACEMENT(0, 'scalar', '$__key', PARTITION_VALUE(0), 0) > 100",
				"	PROJECT $_0_1 AS x, PARTITION_VALUE(0) AS y, $_0_2 AS \"sum\", HASH_REPLACEMENT(0, 'scalar', '$__key', PARTITION_VALUE(0), 0) AS x_per_y)",
			},
			parts: []string{"y"},
		},
		{
			input: `SELECT SUBSTRING(str, 2, 2) AS x, SUM(y+0) OVER (PARTITION BY x) AS ysum FROM input`,
			expect: []string{
				"WITH (",
				"	ITERATE input FIELDS [str, y]",
				"	AGGREGATE SUM(y + 0) AS $__val BY SUBSTRING(str, 2, 2) AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE input FIELDS [str]",
				"PROJECT SUBSTRING(str, 2, 2) AS x, HASH_REPLACEMENT(0, 'scalar', '$__key', SUBSTRING(str, 2, 2), NULL) AS ysum",
			},
			parts: []string{"x"}, // should not get distracted by this binding!
		},
		{
			input: `SELECT (x + 1) AS y, (y + 1) AS z FROM table`,
			expect: []string{
				"ITERATE table FIELDS [x]",
				"PROJECT x + 1 AS y, x + 2 AS z",
			},
		},
		{
			input: `SELECT "Carrier" AS "$key:resource_id%0", COUNT(DISTINCT "OriginCountry") AS "origin_countries"
     FROM "sample_flights"
     GROUP BY "Carrier"
     ORDER BY COUNT(*) DESC
     LIMIT 10`,
			expect: []string{
				"WITH (",
				"	ITERATE sample_flights FIELDS [Carrier, OriginCountry]",
				"	FILTER DISTINCT [Carrier, OriginCountry]",
				"	AGGREGATE COUNT(*) AS $__val BY Carrier AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE sample_flights FIELDS [Carrier]",
				"AGGREGATE COUNT(*) AS $_0_1 BY Carrier AS $_0_0",
				"ORDER BY $_0_1 DESC NULLS FIRST",
				"LIMIT 10",
				"PROJECT $_0_0 AS \"$key:resource_id%0\", HASH_REPLACEMENT(0, 'scalar', '$__key', $_0_0, 0) AS origin_countries",
			},
		},
		{
			input: `SELECT group0, group1, COUNT(DISTINCT group2) AS dist, SUM(x) AS sx FROM input GROUP BY group0, group1`,
			expect: []string{
				"WITH (",
				"	ITERATE input FIELDS [group0, group1, group2]",
				"	FILTER DISTINCT [group0, group1, group2]",
				"	AGGREGATE COUNT(*) AS $_0_0 BY group0 AS $_0_1, group1 AS $_0_2",
				"	PROJECT $_0_0 AS $__val, [$_0_1, $_0_2] AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE input FIELDS [group0, group1, x]",
				"AGGREGATE SUM(x) AS $_0_2 BY group0 AS $_0_0, group1 AS $_0_1",
				"PROJECT $_0_0 AS group0, $_0_1 AS group1, HASH_REPLACEMENT(0, 'scalar', '$__key', [$_0_0, $_0_1], 0) AS dist, $_0_2 AS sx",
			},
		},
		{
			// test that duplicate inputs
			// are removed and replaced
			input: `SELECT * FROM foo WHERE
x = (SELECT a FROM bar LIMIT 1) AND
y = (SELECT a FROM bar LIMIT 1) AND
z = (SELECT a FROM bar LIMIT 1)`,
			expect: []string{
				"WITH (",
				"	ITERATE bar FIELDS [a]",
				"	LIMIT 1",
				"	PROJECT a AS a",
				") AS REPLACEMENT(0)",
				"ITERATE foo FIELDS * WHERE x = SCALAR_REPLACEMENT(0) AND y = SCALAR_REPLACEMENT(0) AND z = SCALAR_REPLACEMENT(0)",
			},
		},
		{
			input: `SELECT outer."group", MAX(item) FROM input as outer, outer.fields as item GROUP BY outer."group" ORDER BY MAX(item)`,
			expect: []string{
				"ITERATE input AS outer FIELDS [fields, group]",
				"ITERATE FIELD fields AS item",
				"AGGREGATE MAX(item) AS \"max\" BY \"group\" AS \"group\"",
				"ORDER BY \"max\" ASC NULLS FIRST",
			},
		},
		{
			// check that the unnesting is removed appropriately:
			input: "SELECT o.x, o.z FROM table AS o, o.lst as y",
			expect: []string{
				"ITERATE table AS o FIELDS [x, z]",
				"PROJECT x AS x, z AS z",
			},
		},
		{
			input: "SELECT grp, SUM(x) AS sumx, AVG(x) FILTER(WHERE foo = 1) AS avgx, 1 + SUM(x) AS sum2 FROM foo GROUP BY grp ORDER BY grp LIMIT 1",
			expect: []string{
				"ITERATE foo FIELDS [foo, grp, x]",
				"AGGREGATE SUM(x) AS $_0_1, AVG(x) FILTER (WHERE foo = 1) AS $_0_2 BY grp AS $_0_0",
				"ORDER BY $_0_0 ASC NULLS FIRST",
				"LIMIT 1",
				"PROJECT $_0_0 AS grp, $_0_1 AS sumx, $_0_2 AS avgx, $_0_1 + 1 AS sum2",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo FIELDS [foo, grp, x]",
				"	AGGREGATE SUM.PARTIAL(x) AS $_2_0, SUM.PARTIAL(x) FILTER (WHERE foo = 1) AS $_2_1, COUNT(x + 0) FILTER (WHERE foo = 1) AS $_2_2 BY grp AS $_0_0)",
				"AGGREGATE SUM.MERGE($_2_0) AS $_0_1, SUM.MERGE($_2_1) AS $_0_2, SUM_COUNT($_2_2) AS $_1_1 BY $_0_0 AS $_0_0",
				"ORDER BY $_0_0 ASC NULLS FIRST",
				"LIMIT 1",
				"PROJECT $_0_0 AS grp, $_0_1 AS sumx, CASE WHEN $_1_1 = 0 THEN NULL ELSE $_0_2 / $_1_1 END AS avgx, $_0_1 + 1 AS sum2",
			},
		},
		{
			input: "SELECT sneller_datashape(*) FROM table",
			expect: []string{
				"ITERATE table FIELDS *",
				"AGGREGATE SNELLER_DATASHAPE(*) AS datashape",
			},
			split: []string{
				"UNION MAP table (",
				"	ITERATE PART table FIELDS *",
				"	AGGREGATE SNELLER_DATASHAPE(*) AS $_2_0)",
				"AGGREGATE SNELLER_DATASHAPE_MERGE($_2_0) AS datashape",
			},
		},
		{
			// eliminate redundant LIMIT 1
			input: `SELECT COUNT(*) FROM table WHERE x LIKE '%foo%' LIMIT 1`,
			expect: []string{
				"ITERATE table FIELDS [x] WHERE x LIKE '%foo%'",
				"AGGREGATE COUNT(*) AS \"count\"",
			},
		},
		{
			// eliminate redundant LIMIT 100 (aggregates always yield one row)
			input: `SELECT SUM(y) FROM table WHERE x LIKE '%foo%' LIMIT 100`,
			expect: []string{
				"ITERATE table FIELDS [x, y] WHERE x LIKE '%foo%'",
				"AGGREGATE SUM(y) AS \"sum\"",
			},
		},
		{
			input: `SELECT col FROM (SELECT col, COUNT(*) FROM tbl GROUP BY col)`,
			expect: []string{
				"ITERATE tbl FIELDS [col]",
				"FILTER DISTINCT [col]",
				"PROJECT col AS col",
			},
		},
		{
			// make sure "SELECT *" doesn't automatically inhibit dereference push-down
			input: `WITH data AS (SELECT * FROM tbl) SELECT COUNT(*) FROM data`,
			expect: []string{
				"ITERATE tbl FIELDS []",
				"AGGREGATE COUNT(*) AS \"count\"",
			},
		},
		{
			// same as above: make sure dereference push-down works
			input: `WITH data AS (SELECT * FROM tbl) SELECT a, b, c FROM data WHERE d = 3`,
			expect: []string{
				"ITERATE tbl FIELDS [a, b, c, d] WHERE d = 3",
				"PROJECT a AS a, b AS b, c AS c",
			},
		},
		{
			// full GROUP BY elimination on a partition
			input: `SELECT SUM(x), COUNT(y), z FROM tbl GROUP BY z`,
			expect: []string{
				"UNION MAP tbl PARTITION BY z (",
				"	ITERATE PART tbl FIELDS [x, y]",
				"	AGGREGATE SUM(x) AS \"sum\", COUNT(y) AS \"count\"",
				"	PROJECT PARTITION_VALUE(0) AS z, \"sum\" AS \"sum\", \"count\" AS \"count\")",
			},
			split: []string{
				"UNION MAP tbl PARTITION BY z (",
				"	UNION MAP tbl (",
				"		ITERATE PART tbl FIELDS [x, y]",
				"		AGGREGATE SUM.PARTIAL(x) AS $_2_0, COUNT(y) AS $_2_1)",
				"	AGGREGATE SUM.MERGE($_2_0) AS \"sum\", SUM_COUNT($_2_1) AS \"count\"",
				"	PROJECT PARTITION_VALUE(0) AS z, \"sum\" AS \"sum\", \"count\" AS \"count\")",
			},
			parts: []string{"z"},
		},
		{
			// partial GROUP BY elimination on a partition
			input: `SELECT a, b, SUM(x), COUNT(y) FROM tbl GROUP BY a, b`,
			expect: []string{
				"UNION MAP tbl PARTITION BY a (",
				"	ITERATE PART tbl FIELDS [b, x, y]",
				"	AGGREGATE SUM(x) AS \"sum\", COUNT(y) AS \"count\" BY b AS b",
				"	PROJECT PARTITION_VALUE(0) AS a, b AS b, \"sum\" AS \"sum\", \"count\" AS \"count\")",
			},
			split: []string{
				"UNION MAP tbl PARTITION BY a (",
				"	UNION MAP tbl (",
				"		ITERATE PART tbl FIELDS [b, x, y]",
				"		AGGREGATE SUM.PARTIAL(x) AS $_2_0, COUNT(y) AS $_2_1 BY b AS b)",
				"	AGGREGATE SUM.MERGE($_2_0) AS \"sum\", SUM_COUNT($_2_1) AS \"count\" BY b AS b",
				"	PROJECT PARTITION_VALUE(0) AS a, b AS b, \"sum\" AS \"sum\", \"count\" AS \"count\")",
			},
			parts: []string{"a"},
		},
		{
			// partial DISTINCT elimination via partition
			input: `SELECT DISTINCT x, y FROM tbl`,
			expect: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	ITERATE tbl FIELDS [y]",
				"	FILTER DISTINCT [y]",
				"	PROJECT PARTITION_VALUE(0) AS x, y AS y)",
			},
			split: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	UNION MAP tbl (",
				"		ITERATE PART tbl FIELDS [y]",
				"		FILTER DISTINCT [y])",
				"	FILTER DISTINCT [y]",
				"	PROJECT PARTITION_VALUE(0) AS x, y AS y)",
			},
			parts: []string{"x"},
		},
		{
			// complete DISTINCT elimination via partition
			input: `SELECT DISTINCT x FROM tbl`,
			expect: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	[{}]",
				"	PROJECT PARTITION_VALUE(0) AS x)",
			},
			split: []string{
				// no splitting b/c the terminal element is the dummy row
				"UNION MAP tbl PARTITION BY x (",
				"	[{}]",
				"	PROJECT PARTITION_VALUE(0) AS x)",
			},
			parts: []string{"x"},
		},
		{
			// complete DISTINCT elimination via partition
			input: `SELECT DISTINCT x, y FROM tbl`,
			expect: []string{
				"UNION MAP tbl PARTITION BY x, y (",
				"	[{}]",
				"	PROJECT PARTITION_VALUE(0) AS x, PARTITION_VALUE(1) AS y)",
			},
			split: []string{
				// no splitting b/c the terminal element is the dummy row
				"UNION MAP tbl PARTITION BY x, y (",
				"	[{}]",
				"	PROJECT PARTITION_VALUE(0) AS x, PARTITION_VALUE(1) AS y)",
			},
			parts: []string{"x", "y"},
		},
		{
			input: `SELECT DISTINCT x FROM tbl WHERE complex_expr`,
			expect: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	ITERATE tbl FIELDS [complex_expr] WHERE complex_expr",
				"	LIMIT 1",
				"	PROJECT PARTITION_VALUE(0) AS x)",
			},
			parts: []string{"x"},
		},
		{
			input: `SELECT DISTINCT ON(x) x, foo, bar FROM tbl`,
			expect: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	ITERATE tbl FIELDS [bar, foo]",
				"	LIMIT 1",
				"	PROJECT PARTITION_VALUE(0) AS x, foo AS foo, bar AS bar)",
			},
			split: []string{
				"UNION MAP tbl PARTITION BY x (",
				"	UNION MAP tbl (",
				"		ITERATE PART tbl FIELDS [bar, foo]",
				"		LIMIT 1)",
				"	LIMIT 1",
				"	PROJECT PARTITION_VALUE(0) AS x, foo AS foo, bar AS bar)",
			},
			parts: []string{"x"},
		},
		{
			input: `
SELECT group0, group1, COUNT(DISTINCT group2) AS gdist, SUM(x) AS sumx
FROM input
GROUP BY group0, group1
ORDER BY group0, group1, gdist DESC`,
			expect: []string{
				"WITH (",
				"	ITERATE input FIELDS [group0, group1, group2]",
				"	FILTER DISTINCT [group0, group1, group2]",
				"	AGGREGATE COUNT(*) AS $_0_0 BY group0 AS $_0_1, group1 AS $_0_2",
				"	PROJECT $_0_0 AS $__val, [$_0_1, $_0_2] AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE input FIELDS [group0, group1, x]",
				"AGGREGATE SUM(x) AS $_0_2 BY group0 AS $_0_0, group1 AS $_0_1",
				"ORDER BY $_0_0 ASC NULLS FIRST, $_0_1 ASC NULLS FIRST, HASH_REPLACEMENT(0, 'scalar', '$__key', [$_0_0, $_0_1], 0) DESC NULLS FIRST",
				"PROJECT $_0_0 AS group0, $_0_1 AS group1, HASH_REPLACEMENT(0, 'scalar', '$__key', [$_0_0, $_0_1], 0) AS gdist, $_0_2 AS sumx",
			},
		},
		{
			// issue #2587
			input: `SELECT * FROM table source, source.tags AS tag`,
			expect: []string{
				"ITERATE table AS source FIELDS *",
				"ITERATE FIELD tags AS tag",
			},
		},

		{
			input: `
SELECT SUM(b.inner.val), a.grp
FROM a a JOIN b b ON a.x = b.y AND a.z = b.a
WHERE b.foo = 3 and a.foo = 700
GROUP BY a.grp
`,
			expect: []string{
				"UNION MAP a AS a PARTITION BY x (",
				"	WITH (",
				"		ITERATE PART b AS b ON [y] FIELDS [a, foo, inner, y] WHERE foo = 3",
				"		PROJECT a AS $__key, [\"inner\"] AS $__val",
				"	) AS REPLACEMENT(0)",
				"	ITERATE a AS a FIELDS [foo, grp, x, z] WHERE foo = 700",
				"	ITERATE FIELD HASH_REPLACEMENT(0, 'joinlist', '$__key', z) AS b)",
				"AGGREGATE SUM(b[0].val) AS \"sum\" BY grp AS grp",
			},
			parts: []string{"x", "y"},
		},
		{
			// make sure we compute the cardinality of the
			// synthesized sub-query correctly
			input: `SELECT (SELECT attr, COUNT(*) "a", SUM("doesnotexist") "b" FROM input GROUP BY attr ORDER BY a DESC) "x"`,
			expect: []string{
				"WITH (",
				"	UNION MAP input PARTITION BY attr (",
				"		ITERATE PART input FIELDS [doesnotexist]",
				"		AGGREGATE COUNT(*) AS a, SUM(doesnotexist) AS b",
				"		PROJECT PARTITION_VALUE(0) AS attr, a AS a, b AS b)",
				"	ORDER BY a DESC NULLS FIRST",
				") AS REPLACEMENT(0)",
				"[{}]",
				"PROJECT LIST_REPLACEMENT(0) AS x",
			},
			parts: []string{"attr"},
		},
		{
			// regression test: flattening used to use references,
			// and this ended up with endless recursion. COALESCE
			// is by default compiled into a CASE. The comparison
			// of a case expression with a value is optimized in
			// that way, that the comparison is pulled into "WHERE"
			// limbs. Because we had references, CASE expression
			// got exploded.
			input: `SELECT COALESCE(A, X) AS X, X<X<X FROM X`,
			expect: []string{
				"ITERATE X FIELDS [A, X]",
				"PROJECT CASE WHEN A IS NOT NULL THEN A WHEN X IS NOT NULL THEN X ELSE NULL END AS X, CASE WHEN A IS NOT NULL THEN A WHEN X IS NOT NULL THEN X ELSE MISSING END < CASE WHEN A IS NOT NULL THEN A WHEN X IS NOT NULL THEN X ELSE MISSING END < CASE WHEN A IS NOT NULL THEN A WHEN X IS NOT NULL THEN X ELSE MISSING END AS _2",
			},
		},
	}

	for i := range tests {
		testcase := func() (*buildTestcase, error) {
			return &tests[i], nil
		}
		testBuild(t, fmt.Sprintf("case-%d", i), testcase)
	}

	runTestcasesFromFiles(t)
}

func buildSplit(t *testing.T, tc *buildTestcase, split bool) *Trace {
	s, err := partiql.Parse([]byte(tc.input))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(s, mkenv(tc.schema, tc.index, tc.parts))
	if err != nil {
		t.Fatal(err)
	}
	if split {
		b, err = Split(b)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	return NoSplit(b)
}

func testBuild(t *testing.T, name string, testcase func() (*buildTestcase, error)) {
	t.Run(name, func(t *testing.T) {
		tc, err := testcase()
		if err != nil {
			t.Fatal(err)
		}

		t.Log("query:", tc.input)
		b := buildSplit(t, tc, false)
		var out strings.Builder
		b.Describe(&out)
		got := out.String()
		want := strings.Join(tc.expect, "\n") + "\n"
		if got != want {
			t.Errorf("got : %s", got)
			t.Errorf("want: %s", want)
			diff, ok := tests.Diff(want, got)
			if ok {
				t.Error("\n" + diff)
			} else {
				lines := strings.Split(got, "\n")
				for i := range lines {
					if i >= len(tc.expect) {
						t.Error("unexpected line:", lines[i])
						continue
					}
					if lines[i] != tc.expect[i] {
						t.Errorf("line %d: got %q", i, lines[i])
						t.Errorf("        want %q", tc.expect[i])
					}
				}
			}
		}
		if tc.results != nil {
			t.Logf("match %v", tc.results)
			outresults := b.FinalTypes()
			t.Logf("got %v", outresults)
			for i := range outresults {
				if outresults[i] != tc.results[i] {
					t.Errorf("output %d: result type %v; wanted %v", i, outresults[i], tc.results[i])
				}
			}
			if t.Failed() {
				t.Log(tc.input)
			}
		}

		if len(tc.split) == 0 {
			return
		}
		reduce := buildSplit(t, tc, true)
		out.Reset()
		reduce.Describe(&out)
		got = out.String()
		want = strings.Join(tc.split, "\n") + "\n"
		if got != want {
			t.Errorf("split: got : %s", got)
			t.Errorf("split: want: %s", want)
			diff, ok := tests.Diff(want, got)
			if ok {
				t.Error("\n" + diff)
			} else {
				lines := strings.Split(got, "\n")
				for i := range lines {
					if i >= len(tc.split) {
						t.Error("unexpected line:", lines[i])
						continue
					}
					if lines[i] != tc.split[i] {
						t.Errorf("line %d: got %q", i, lines[i])
						t.Errorf("        want %q", tc.split[i])
					}
				}
			}
		}
	})
}

func mkindex(rs [][]blockfmt.Range) *blockfmt.Index {
	t := &blockfmt.Trailer{}
	for _, r := range rs {
		t.Blocks = append(t.Blocks, blockfmt.Blockdesc{})
		t.Sparse.Push(r)
	}
	return &blockfmt.Index{
		Inline: []blockfmt.Descriptor{{
			Trailer: *t,
		}},
	}
}

func timeRange(path string, min, max date.Time) blockfmt.Range {
	p := strings.Split(path, ".")
	return blockfmt.NewRange(p, ion.Timestamp(min), ion.Timestamp(max))
}

func runTestcasesFromFiles(t *testing.T) {
	rootdir := filepath.Clean("./testdata/build/")
	prefix := rootdir + "/"
	suffix := ".test"

	err := filepath.WalkDir(rootdir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), suffix) {
			return nil
		}

		name := strings.TrimPrefix(path, prefix)
		name = strings.TrimSuffix(name, suffix)
		name = strings.ReplaceAll(name, "/", "-")

		testcase := func() (*buildTestcase, error) {
			return parseTestcase(path)
		}

		testBuild(t, name, testcase)

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func parseTestcase(fname string) (*buildTestcase, error) {
	spec, err := tests.ReadTestCaseSpecFromFile(fname)
	if err != nil {
		return nil, err
	}

	n := len(spec.Sections)
	if n < 2 || n > 3 {
		return nil, fmt.Errorf("expected 2 or 3 sections in testcase, got %d", n)
	}

	tc := buildTestcase{
		input:  strings.Join(spec.Sections[0], "\n"),
		expect: spec.Sections[1],
	}

	if n == 3 {
		tc.split = spec.Sections[2]
	}

	if len(tc.expect) == 0 {
		return nil, fmt.Errorf("expected part of testcase is required")
	}

	return &tc, nil
}
