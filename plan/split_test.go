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

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/tests"
	"github.com/SnellerInc/sneller/vm"
)

type emptyenv struct{}

func (e emptyenv) Stat(_ expr.Node, _ *Hints) (*Input, error) {
	return &Input{}, nil
}

func (e emptyenv) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("cannot open emptyenv table")
}

func (e emptyenv) Size() int64 { return 0 }

func (e emptyenv) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

type twosplit struct {
	Env
}

func (twosplit) Geometry() *Geometry {
	return &Geometry{
		Peers: []Transport{&LocalTransport{}, &LocalTransport{}},
	}
}

func TestSplit(t *testing.T) {
	env := emptyenv{}
	tcs := []struct {
		query string
		lines []string
	}{
		{
			query: `SELECT COUNT(*) FROM foo`,
			lines: []string{
				"foo",
				"COUNT(*) AS $_2_0",
				// describes table -> [tables...] mapping
				"UNION MAP",
				"AGGREGATE SUM_COUNT($_2_0) AS \"count\"",
			},
		},
		{
			query: `SELECT MAX(n) FROM table`,
			lines: []string{
				`table`,
				`AGGREGATE MAX(n) AS $_2_0`,
				`UNION MAP`,
				`AGGREGATE MAX($_2_0) AS "max"`,
			},
		},
		{
			query: `SELECT AVG(n) AS avg FROM table`,
			lines: []string{
				`table`,
				`AGGREGATE SUM.PARTIAL(n) AS $_2_0, COUNT(n + 0) AS $_2_1`,
				`UNION MAP`,
				`AGGREGATE SUM.MERGE($_2_0) AS "avg", SUM_COUNT($_2_1) AS $_1_0`,
				`PROJECT CASE WHEN $_1_0 = 0 THEN NULL ELSE "avg" / $_1_0 END AS "avg"`,
			},
		},
		{
			query: `SELECT APPROX_COUNT_DISTINCT(field) FROM table`,
			lines: []string{
				`table`,
				`AGGREGATE APPROX_COUNT_DISTINCT.PARTIAL(field) AS $_2_0`,
				`UNION MAP`,
				`AGGREGATE APPROX_COUNT_DISTINCT.MERGE($_2_0) AS "count"`,
			},
		},
		{
			query: `SELECT AVG(x), MAX(y), APPROX_COUNT_DISTINCT(z) FROM table`,
			lines: []string{
				`table`,
				`AGGREGATE SUM.PARTIAL(x) AS $_2_0, MAX(y) AS $_2_1, APPROX_COUNT_DISTINCT.PARTIAL(z) AS $_2_2, COUNT(x + 0) AS $_2_3`,
				`UNION MAP`,
				`AGGREGATE SUM.MERGE($_2_0) AS "avg", MAX($_2_1) AS "max", APPROX_COUNT_DISTINCT.MERGE($_2_2) AS "count", SUM_COUNT($_2_3) AS $_1_0`,
				`PROJECT CASE WHEN $_1_0 = 0 THEN NULL ELSE "avg" / $_1_0 END AS "avg", "max" AS "max", "count" AS "count"`,
			},
		},
		{
			query: `SELECT STDDEV(x) as stddev FROM table`,
			lines: []string{
				`table`,
				`AGGREGATE SUM_INT(CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END) AS $_2_0, SUM.PARTIAL(x * x) AS $_2_1, SUM.PARTIAL(x) AS $_2_2`,
				`UNION MAP`,
				`AGGREGATE SUM_INT($_2_0) AS $_0_0, SUM.MERGE($_2_1) AS $_0_1, SUM.MERGE($_2_2) AS $_0_2`,
				`PROJECT CASE WHEN $_0_0 = 0 THEN NULL ELSE SQRT($_0_1 / $_0_0 - ($_0_2 / $_0_0 * ($_0_2 / $_0_0))) END AS "stddev"`,
			},
		},
	}

	for i := range tcs {
		query := tcs[i].query
		lines := tcs[i].lines
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			s, err := partiql.Parse([]byte(query))
			if err != nil {
				t.Fatal(err)
			}
			split, err := NewSplit(s, &twosplit{env})
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Join(lines, "\n") + "\n"
			if got := split.String(); got != want {
				t.Errorf("got plan\n%s", got)
				t.Errorf("wanted plan\n%s", want)
				diff, ok := tests.Diff(want, got)
				if ok {
					t.Error("\n" + diff)
				}
			}
		})
	}
}
