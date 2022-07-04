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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

const (
	uintType   = expr.TypeSet(1 << ion.UintType)
	stringType = expr.TypeSet(1 << ion.StringType)
	intType    = uintType | expr.TypeSet(1<<ion.IntType)
	countType  = uintType
)

func mkenv(h expr.Hint, idx *blockfmt.Index) Env {
	if h == nil && idx == nil {
		return nil
	}
	return &testenv{
		hint: h,
		idx:  idx,
	}
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
			rx:    "unlimited cardinality",
		},
		{
			input: `select * from tbl limit -1`,
			rx:    "negative limit -1 not supported",
		},
		{
			input: `select * from tbl limit 10 offset -5`,
			rx:    "negative offset -5 not supported",
		},
		{
			input: `select * from tbl offset 15`,
			rx:    "OFFSET without LIMIT is not supported",
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
			rx:    "window",
		},
		{
			input: `select COUNT(*) FILTER (WHERE x > 0) FROM foo`,
			rx:    "aggregate filters not yet supported",
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
			b, err := Build(s, mkenv(schema, nil))
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
	hint expr.Hint
	idx  *blockfmt.Index
}

func (e *testenv) Schema(expr.Node) expr.Hint {
	return e.hint
}

func (e *testenv) Index(expr.Node) (Index, error) {
	return e.idx, nil
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
	p, ok := e.(*expr.Path)
	if !ok {
		return expr.AnyType
	}
	if p.Rest != nil {
		// schema is flat; type must be missing
		return expr.MissingType
	}
	for i := range f {
		if f[i].field == p.First {
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

func TestBuild(t *testing.T) {
	basetime, _ := date.Parse([]byte("2022-02-22T20:22:22Z"))
	now := func(hours int) date.Time {
		return basetime.Add(time.Duration(hours) * time.Hour)
	}
	tests := []struct {
		input   string
		expect  []string
		split   []string
		results []expr.TypeSet
		schema  expr.Hint // applied to the inner-most table expression
		index   *blockfmt.Index
	}{
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
				"	ITERATE inner_table",
				"	LIMIT 2",
				") AS REPLACEMENT(0)",
				"[{}]",
				"PROJECT 'x' AS x, LIST_REPLACEMENT(0) AS y",
			},
			split: []string{
				"WITH (",
				"	UNION MAP inner_table (",
				"		ITERATE PART inner_table",
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
				"ITERATE foo AS t",
				"PROJECT 3 AS x, 4 AS y, TRUE AS z",
			},
			split: []string{
				"UNION MAP foo AS t (",
				"	ITERATE PART foo AS t",
				"	PROJECT 3 AS x, 4 AS y, TRUE AS z)",
			},
			// note: any arithmetic gets promoted (for now),
			// so the result of 'x+1' is analyzed as possibly float
			// even though in practice it is always integral...
			results: []expr.TypeSet{intType, expr.NumericType, expr.BoolType},
		},
		{
			// test that we do not duplicate the "count" field
			input: `select count(*), count(field) from foo`,
			expect: []string{
				"ITERATE foo",
				"AGGREGATE COUNT(*) AS \"count\", COUNT(field) AS count_2",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE COUNT(*) AS $_0_0, COUNT(field) AS $_0_1)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\", SUM_COUNT($_0_1) AS count_2",
			},
		},
		{
			input: `select sum(x) from foo where y in (select y from foo order by y desc limit 5)`,
			expect: []string{
				"WITH (",
				"	ITERATE foo",
				"	PROJECT y AS y",
				"	ORDER BY y DESC NULLS FIRST",
				"	LIMIT 5",
				") AS REPLACEMENT(0)",
				"ITERATE foo WHERE IN_REPLACEMENT(y, 0)",
				"AGGREGATE SUM(x) AS \"sum\"",
			},
			split: []string{
				"WITH (",
				"	UNION MAP foo (",
				"		ITERATE PART foo",
				"		PROJECT y AS y",
				"		ORDER BY y DESC NULLS FIRST",
				"		LIMIT 5)",
				"	ORDER BY y DESC NULLS FIRST",
				"	LIMIT 5",
				") AS REPLACEMENT(0)",
				"UNION MAP foo (",
				"	ITERATE PART foo WHERE IN_REPLACEMENT(y, 0)",
				"	AGGREGATE SUM(x) AS $_0_0)",
				"AGGREGATE SUM($_0_0) AS \"sum\"",
			},
		},
		{
			input:  "select x, count(x) from foo group by x",
			schema: mkschema("x", stringType),
			expect: []string{
				"ITERATE foo",
				"AGGREGATE COUNT(x) AS \"count\" BY x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE COUNT(x) AS $_0_0 BY x AS x)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\" BY x AS x",
			},
			results: []expr.TypeSet{stringType, countType},
		},
		{
			input: `select avg(x), y from foo group by y`,
			expect: []string{
				"ITERATE foo",
				"AGGREGATE AVG(x) AS \"avg\" BY y AS y",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE SUM(x) AS $_0_0, COUNT(x + 0) AS $_0_1 BY y AS y)",
				"AGGREGATE SUM($_0_0) AS \"avg\", SUM_COUNT($_0_1) AS $_1_0 BY y AS y",
				"PROJECT \"avg\" / $_1_0 AS \"avg\", y AS y",
			},
		},
		{
			input: "select o.x, i.y from foo as o, o.field as i where o.x <> i.y",
			expect: []string{
				"ITERATE foo AS o",
				"ITERATE FIELD field WHERE LOAD(0) <> y (ref: [y AS $_0_1], live: [x AS $_0_0])", // comparison pushed down
				"PROJECT $_0_0 AS x, $_0_1 AS y",
			},
		},
		{
			// similar to above, but check that
			// we only push down part of a conjunction
			input: `select o.x, i.y from foo as o, o.field as i where o.x < 3 and i.y < 3`,
			expect: []string{
				"ITERATE foo AS o WHERE x < 3",
				"ITERATE FIELD field WHERE y < 3 (ref: [y AS $_0_1], live: [x AS $_0_0])",
				"PROJECT $_0_0 AS x, $_0_1 AS y",
			},
		},
		{
			// test that nested projection is flattened
			input: `select x+2 as x, y - 3 as y from (select 3 + a as x, b - 5 as y from foo)`,
			expect: []string{
				"ITERATE foo",
				"PROJECT a + 5 AS x, b - 8 AS y",
			},
			results: []expr.TypeSet{expr.NumericType | expr.MissingType, expr.NumericType | expr.MissingType},
		},
		{
			// test that nested projection is flattened; include CTE
			input: `with cte as (select 3 + a as x, b - 5 as y from foo) select x+2 as x, y - 3 as y from (select x, y from cte)`,
			expect: []string{
				"ITERATE foo",
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
				"	ITERATE foo",
				"	AGGREGATE COUNT(*) AS \"count\" BY grp AS grp",
				"	ORDER BY \"count\" DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT grp AS grp",
				") AS REPLACEMENT(0)",
				"ITERATE foo WHERE IN_REPLACEMENT(grp, 0)",
			},
			split: []string{
				"WITH (",
				"	UNION MAP foo (",
				"		ITERATE PART foo",
				"		AGGREGATE COUNT(*) AS $_0_0 BY grp AS grp)",
				"	AGGREGATE SUM_COUNT($_0_0) AS \"count\" BY grp AS grp",
				"	ORDER BY \"count\" DESC NULLS FIRST",
				"	LIMIT 5",
				"	PROJECT grp AS grp",
				") AS REPLACEMENT(0)",
				"UNION MAP foo (",
				"	ITERATE PART foo WHERE IN_REPLACEMENT(grp, 0))",
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
				"	ITERATE y",
				"	AGGREGATE MIN(f) AS \"min\"",
				") AS REPLACEMENT(0)",
				"WITH (",
				"	ITERATE y",
				"	AGGREGATE MAX(f) AS \"max\"",
				") AS REPLACEMENT(1)",
				"ITERATE foo WHERE x > SCALAR_REPLACEMENT(0) AND x < SCALAR_REPLACEMENT(1)",
			},
		},
		{
			// test elimination of un-used bindings
			input: `select count(x) from (select x, y from foo)`,
			expect: []string{
				"ITERATE foo",
				// FIXME: teach aggregates to merge with
				// projections that produce useless bindings
				"PROJECT x AS x", // note y is no longer present
				"AGGREGATE COUNT(x) AS \"count\"",
			},
			results: []expr.TypeSet{countType},
		},
		{
			// test push-down across a projection
			input: `select x from (select (y / 2) as x from foo) where x < 3 limit 100`,
			expect: []string{
				"ITERATE foo WHERE y / 2 < 3",
				"LIMIT 100",
				"PROJECT y / 2 AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo WHERE y / 2 < 3",
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
				"ITERATE foo AS o",
				"ITERATE FIELD field WHERE LOAD(0) = y (ref: [y AS $_0_1], live: [x AS $_0_0])", // x = y pushed down
				"PROJECT $_0_0 AS x, $_0_1 AS y",
			},
		},
		{
			// test that references in ORDER BY
			// are correctly resolved, since the
			// scoping rules are a little weird
			input:  `select COUNT(x), y, z from foo group by y, z order by COUNT(x)`,
			schema: mkschema("x", expr.NumericType, "y", stringType, "z", countType),
			expect: []string{
				"ITERATE foo",
				"AGGREGATE COUNT(x) AS \"count\" BY y AS y, z AS z",
				"ORDER BY \"count\" ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE COUNT(x) AS $_0_0 BY y AS y, z AS z)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\" BY y AS y, z AS z",
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
				"ITERATE 'parking.10n'",
				"AGGREGATE COUNT(Make) AS $_0_0 BY Make AS $_0_1",
				"FILTER $_0_0 = 122",
				"PROJECT $_0_1 AS Make, $_0_0 AS c",
			},
			split: []string{
				"UNION MAP 'parking.10n' (",
				"	ITERATE PART 'parking.10n'",
				"	AGGREGATE COUNT(Make) AS $_0_0 BY Make AS $_0_1)",
				"AGGREGATE SUM_COUNT($_0_0) AS $_0_0 BY $_0_1 AS $_0_1",
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
				"ITERATE table AS top",
				"ITERATE FIELD field WHERE LOAD(0) = y (ref: [field AS $_1_2, y AS $_1_1], live: [x AS $_1_0])",
				"ITERATE FIELD $_1_2 WHERE LOAD(1) = z (ref: [z AS $_0_2], live: [$_1_0 AS $_0_0, $_1_1 AS $_0_1])",
				"PROJECT $_0_0 AS x, $_0_1 AS y, $_0_2 AS z",
			},
		},
		{
			input: `select out.Make as make, entry.Ticket as ticket, entry.Color as color
from 'parking3.ion' as out, out.Entries as entry
where out.Make = 'CHRY' and entry.BodyStyle = 'PA'`,
			expect: []string{
				"ITERATE 'parking3.ion' AS out WHERE Make = 'CHRY'",
				"ITERATE FIELD Entries WHERE BodyStyle = 'PA' (ref: [Color AS $_0_2, Ticket AS $_0_1], live: [Make AS $_0_0])",
				"PROJECT $_0_0 AS make, $_0_1 AS ticket, $_0_2 AS color",
			},
		},
		{
			input: `select count(distinct x) from table`,
			expect: []string{
				"ITERATE table",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(x) AS \"count\"",
			},
			split: []string{
				"UNION MAP table (",
				"	ITERATE PART table",
				"	FILTER DISTINCT [x])",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(x) AS \"count\"",
			},
		},
		{
			input: `select count(distinct t.x), t.y from table as t group by t.y`,
			expect: []string{
				"ITERATE table AS t",
				"FILTER DISTINCT [x y]",
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
				"ITERATE table",
				"FILTER DISTINCT [x]",
				"AGGREGATE COUNT(*) AS \"count\"",
			},
		},
		{
			input: `select x, y, z from t order by x LIMIT 9999`,
			expect: []string{
				"ITERATE t",
				"PROJECT x AS x, y AS y, z AS z",
				"ORDER BY x ASC NULLS FIRST",
				"LIMIT 9999",
			},
			split: []string{
				"UNION MAP t (",
				"	ITERATE PART t",
				"	PROJECT x AS x, y AS y, z AS z",
				"	ORDER BY x ASC NULLS FIRST",
				"	LIMIT 9999)",
				"ORDER BY x ASC NULLS FIRST",
				"LIMIT 9999",
			},
		},
		{
			input: `select count(x)+1 as x from table order by x`,
			expect: []string{
				"ITERATE table",
				"AGGREGATE COUNT(x) AS $_0_0",
				"PROJECT $_0_0 + 1 AS x",
			},
			split: []string{
				"UNION MAP table (",
				"	ITERATE PART table",
				"	AGGREGATE COUNT(x) AS $_0_0)",
				"AGGREGATE SUM_COUNT($_0_0) AS $_0_0",
				"PROJECT $_0_0 + 1 AS x",
			},
			results: []expr.TypeSet{expr.IntegerType},
		},
		{
			input: `select count(x)+count(y) as both from table`,
			expect: []string{
				"ITERATE table",
				"AGGREGATE COUNT(x) AS $_0_0, COUNT(y) AS $_0_1",
				"PROJECT $_0_0 + $_0_1 AS both",
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
				"ITERATE table",
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
				"ITERATE table",
				"AGGREGATE COUNT(x) AS c",
			},
			results: []expr.TypeSet{countType},
		},
		{
			input: `select count(x) as c, y from table group by y having count(x) > 100`,
			expect: []string{
				"ITERATE table",
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
				"ITERATE table",
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
				"ITERATE foo",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	ORDER BY x DESC NULLS FIRST",
				"	LIMIT 10)",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10",
			},
		},
		{
			input: `select * from foo order by x desc limit 10 offset 64`,
			expect: []string{
				"ITERATE foo",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10 OFFSET 64",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	ORDER BY x DESC NULLS FIRST",
				"	LIMIT 74)",
				"ORDER BY x DESC NULLS FIRST",
				"LIMIT 10 OFFSET 64",
			},
		},
		{
			input: `select x, count(*) from foo group by x limit 10 offset 64`,
			expect: []string{
				"ITERATE foo",
				"AGGREGATE COUNT(*) AS \"count\" BY x AS x",
				"LIMIT 10 OFFSET 64",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE COUNT(*) AS $_0_0 BY x AS x)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\" BY x AS x",
				"LIMIT 10 OFFSET 64",
			},
		},
		{
			// check that a limit after DISTINCT is pushed
			// into the mapping *and* reduction steps
			input: `select distinct x from foo limit 50`,
			expect: []string{
				"ITERATE foo",
				"FILTER DISTINCT [x]",
				"LIMIT 50",
				"PROJECT x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
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
				"ITERATE foo",
				"FILTER DISTINCT [x]",
				"LIMIT 50 OFFSET 150",
				"PROJECT x AS x",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
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
				"ITERATE foo",
				"PROJECT list AS list, list[1] AS list_1",
			},
		},
		{
			// test left-to-right flattening of variables (see Issue #534)
			input: `select list[0] AS x, x[0] AS y, y.z AS z from foo`,
			expect: []string{
				"ITERATE foo",
				"PROJECT list[0] AS x, list[0][0] AS y, list[0][0].z AS z",
			},
		},
		{
			input: `select EARLIEST(x), LATEST(x) from foo`,
			expect: []string{
				"ITERATE foo",
				"AGGREGATE EARLIEST(x) AS \"min\", LATEST(x) AS \"max\"",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE EARLIEST(x) AS $_0_0, LATEST(x) AS $_0_1)",
				"AGGREGATE EARLIEST($_0_0) AS \"min\", LATEST($_0_1) AS \"max\"",
			},
		},
		{
			input: `with cte0 as (SELECT x, y, z FROM foo),
						 cte1 as (SELECT x, y FROM cte0)
					SELECT x FROM cte1`,
			expect: []string{
				"ITERATE foo",
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
				"ITERATE table",
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
				"ITERATE table",
				"AGGREGATE COUNT(t) AS $_0_0",
				"PROJECT $_0_0 AS \"count\", `2022-02-22T22:22:22Z` AS \"max\"",
			},
			results: []expr.TypeSet{
				countType,
				expr.TimeType,
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
				"ITERATE foo WHERE DATE_EXTRACT_MONTH(timestamp) = 3 AND DATE_EXTRACT_DAY(timestamp) >= 9",
				"PROJECT DATE_EXTRACT_MONTH(timestamp) AS m, DATE_EXTRACT_DAY(timestamp) AS d, DATE_EXTRACT_HOUR(timestamp) AS h",
				"AGGREGATE COUNT(*) AS \"count\" BY m AS m, d AS d, h AS h",
				"ORDER BY m ASC NULLS FIRST, d ASC NULLS FIRST, h ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo WHERE DATE_EXTRACT_MONTH(timestamp) = 3 AND DATE_EXTRACT_DAY(timestamp) >= 9",
				"	PROJECT DATE_EXTRACT_MONTH(timestamp) AS m, DATE_EXTRACT_DAY(timestamp) AS d, DATE_EXTRACT_HOUR(timestamp) AS h",
				"	AGGREGATE COUNT(*) AS $_0_0 BY m AS m, d AS d, h AS h)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\" BY m AS m, d AS d, h AS h",
				"ORDER BY m ASC NULLS FIRST, d ASC NULLS FIRST, h ASC NULLS FIRST",
			},
		},
		{
			input: `select x, (select z from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select max(y) from bar where x = y) from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar",
				"	AGGREGATE MAX(y) AS \"max\" BY y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS _2",
			},
		},
		{
			input: `select x, y, (select b.z from bar b where b.y = x limit 1) from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar AS b",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, y AS y, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS _3",
			},
		},
		{
			input: `select x, (select z from bar where a = 1 and x = y and b = 2 limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar WHERE a = 1 AND b = 2",
				"	FILTER DISTINCT [y]",
				"	PROJECT z AS z, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select a, b, c from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar",
				"	FILTER DISTINCT [y]",
				"	PROJECT a AS a, b AS b, c AS c, y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'struct', '$_0_0', x) AS z",
			},
		},
		{
			input: `select x, (select min(a), max(b), count(c) from bar where x = y limit 1) as z from foo`,
			expect: []string{
				"WITH (",
				"	ITERATE bar",
				"	AGGREGATE MIN(a) AS \"min\", MAX(b) AS \"max\", COUNT(c) AS \"count\" BY y AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'struct', '$_0_0', x) AS z",
			},
		},
		{
			input: "SELECT TIME_BUCKET(timestamp, 864000) AS _tmbucket1, COUNT(*), AVG(AvgTicketPrice) AS _sum1 FROM kibana_sample_data_flights WHERE timestamp BETWEEN `2022-03-01T00:00:00.000Z` AND `2022-07-01T00:00:00.000Z` GROUP BY TIME_BUCKET(timestamp, 864000) ORDER BY _tmbucket1",
			expect: []string{
				"ITERATE kibana_sample_data_flights WHERE BEFORE(`2022-02-28T23:59:59.999999Z`, timestamp) AND BEFORE(timestamp, `2022-07-01T00:00:00.000001Z`)",
				"AGGREGATE COUNT(*) AS \"count\", AVG(AvgTicketPrice) AS _sum1 BY TIME_BUCKET(timestamp, 864000) AS _tmbucket1",
				"ORDER BY _tmbucket1 ASC NULLS FIRST",
			},
			split: []string{
				"UNION MAP kibana_sample_data_flights (",
				"	ITERATE PART kibana_sample_data_flights WHERE BEFORE(`2022-02-28T23:59:59.999999Z`, timestamp) AND BEFORE(timestamp, `2022-07-01T00:00:00.000001Z`)",
				"	AGGREGATE COUNT(*) AS $_0_0, SUM(AvgTicketPrice) AS $_0_1, COUNT(AvgTicketPrice + 0) AS $_0_2 BY TIME_BUCKET(timestamp, 864000) AS _tmbucket1)",
				"AGGREGATE SUM_COUNT($_0_0) AS \"count\", SUM($_0_1) AS _sum1, SUM_COUNT($_0_2) AS $_1_1 BY _tmbucket1 AS _tmbucket1",
				"PROJECT \"count\" AS \"count\", _sum1 / $_1_1 AS _sum1, _tmbucket1 AS _tmbucket1",
				"ORDER BY _tmbucket1 ASC NULLS FIRST",
			},
		},
		{
			// simple INTO with no preceding aggregation, etc.
			input: `SELECT x, y, z INTO db.table FROM foo WHERE x > 3 AND x < 100`,
			expect: []string{
				"ITERATE foo WHERE x > 3 AND x < 100",
				"PROJECT x AS x, y AS y, z AS z",
				"OUTPUT PART db/db/table",
				"OUTPUT INDEX db.table AT db/db/table",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo WHERE x > 3 AND x < 100",
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
				"ITERATE foo",
				"AGGREGATE SUM(y) AS \"sum\" BY x AS x",
				"OUTPUT PART db/my/stats",
				"OUTPUT INDEX my.stats AT db/my/stats",
			},
			split: []string{
				"UNION MAP foo (",
				"	ITERATE PART foo",
				"	AGGREGATE SUM(y) AS $_0_0 BY x AS x)",
				"AGGREGATE SUM($_0_0) AS \"sum\" BY x AS x",
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
				"	ITERATE other",
				"	FILTER DISTINCT [key]",
				"	PROJECT key AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE input",
				"PROJECT x AS x, IN_REPLACEMENT(x, 0) AS has_other",
			},
		},
		{
			// weird NOT EXISTS -> semi-join
			input: `SELECT x, (SELECT TRUE FROM other WHERE key = x LIMIT 1) IS MISSING AS no_other FROM input`,
			expect: []string{
				"WITH (",
				"	ITERATE other",
				"	FILTER DISTINCT [key]",
				"	PROJECT key AS $_0_0",
				") AS REPLACEMENT(0)",
				"ITERATE input",
				"PROJECT x AS x, !(IN_REPLACEMENT(x, 0)) AS no_other",
			},
		},
		{
			input: `SELECT y, z FROM table GROUP BY x+1 AS y, z`,
			expect: []string{
				"ITERATE table",
				"FILTER DISTINCT [x + 1 z]",
				"PROJECT x + 1 AS y, z AS z",
			},
		},
		{
			input: `SELECT grp FROM table GROUP BY grp ORDER BY COUNT(*) DESC`,
			expect: []string{
				"ITERATE table",
				"AGGREGATE COUNT(*) AS $_0_1 BY grp AS grp",
				"ORDER BY $_0_1 DESC NULLS FIRST",
				"PROJECT grp AS grp",
			},
		},
		{
			input: `SELECT grp0 FROM table GROUP BY grp0, grp1 ORDER BY -COUNT(*)`,
			expect: []string{
				"ITERATE table",
				"AGGREGATE COUNT(*) AS $_0_1 BY grp0 AS $_0_0, grp1",
				// FIXME: this projection is superseded
				// by the final one; we could eliminate this
				// without any semantic consequences:
				"PROJECT $_0_0 AS grp0, $_0_1 AS $_0_1",
				"ORDER BY -($_0_1) ASC NULLS FIRST",
				"PROJECT grp0 AS grp0",
			},
		},
		{
			input: `select x, COUNT(y) OVER (PARTITION BY z) AS wind FROM foo`,
			expect: []string{
				"WITH (",
				"	ITERATE foo",
				"	AGGREGATE COUNT(y) AS $__val BY z AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE foo",
				"PROJECT x AS x, HASH_REPLACEMENT(0, 'scalar', '$__key', z) AS wind",
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
				"	ITERATE foo WHERE z = 'foo'",
				"	FILTER DISTINCT [x y]",
				"	PROJECT x AS x, y AS y",
				"	AGGREGATE COUNT(x) AS $__val BY y AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE foo WHERE z = 'foo'",
				"AGGREGATE SUM(var) AS $_0_2 BY x AS $_0_0, y AS $_0_1",
				"PROJECT $_0_0 AS x, $_0_1 AS y, $_0_2 AS \"sum\", HASH_REPLACEMENT(0, 'scalar', '$__key', $_0_1) AS x_per_y",
			},
		},
		{
			input: `SELECT SUBSTRING(str, 2, 2) AS x, SUM(y+0) OVER (PARTITION BY x) AS ysum FROM input`,
			expect: []string{
				"WITH (",
				"	ITERATE input",
				"	AGGREGATE SUM(y + 0) AS $__val BY SUBSTRING(str, 2, 2) AS $__key",
				") AS REPLACEMENT(0)",
				"ITERATE input",
				"PROJECT SUBSTRING(str, 2, 2) AS x, HASH_REPLACEMENT(0, 'scalar', '$__key', SUBSTRING(str, 2, 2)) AS ysum",
			},
		},
	}

	for i := range tests {
		in := tests[i].input
		expect := tests[i].expect
		split := tests[i].split
		results := tests[i].results
		schema := tests[i].schema
		index := tests[i].index
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			t.Log("query:", in)
			s, err := partiql.Parse([]byte(in))
			if err != nil {
				t.Fatal(err)
			}
			b, err := Build(s, mkenv(schema, index))
			if err != nil {
				t.Fatal(err)
			}
			var out strings.Builder
			b.Describe(&out)
			got := out.String()
			want := strings.Join(expect, "\n") + "\n"
			if got != want {
				t.Errorf("got : %s", got)
				t.Errorf("want: %s", want)
			}
			if results != nil {
				t.Logf("match %v", results)
				outresults := b.FinalTypes()
				t.Logf("got %v", outresults)
				for i := range outresults {
					if outresults[i] != results[i] {
						t.Errorf("output %d: result type %v; wanted %v", i, outresults[i], results[i])
					}
				}
				if t.Failed() {
					t.Log(in)
				}
			}

			if len(split) == 0 {
				return
			}
			reduce, err := Split(b)
			if err != nil {
				t.Fatalf("split: %s", err)
			}
			out.Reset()
			reduce.Describe(&out)
			got = out.String()
			want = strings.Join(split, "\n") + "\n"
			if got != want {
				t.Errorf("split: got : %s", got)
				t.Errorf("split: want: %s", want)
			}
		})
	}
}

func mkindex(rs [][]blockfmt.Range) *blockfmt.Index {
	t := &blockfmt.Trailer{}
	for _, r := range rs {
		t.Blocks = append(t.Blocks, blockfmt.Blockdesc{})
		t.Sparse.Push(r)
	}
	return &blockfmt.Index{
		Inline: []blockfmt.Descriptor{{
			Trailer: t,
		}},
	}
}

func timeRange(path string, min, max date.Time) blockfmt.Range {
	p := strings.Split(path, ".")
	return blockfmt.NewRange(p, ion.Timestamp(min), ion.Timestamp(max))
}
