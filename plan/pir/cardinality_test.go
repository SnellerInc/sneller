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
	"bytes"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
)

func TestCardinality(t *testing.T) {
	cases := []struct {
		query string
		want  SizeClass
	}{
		{
			"select * from foo where 2 < 1",
			SizeZero,
		},
		{
			"select x, y from foo limit 100",
			SizeExactSmall,
		},
		{
			"select x, y from foo limit 1",
			SizeOne,
		},
		{
			"select 'x', 3",
			SizeOne,
		},
		{
			"select max(x), min(x) from input",
			SizeOne,
		},
		{
			"select x from foo limit 100000",
			SizeExactLarge,
		},
		{
			"select distinct x from foo",
			SizeColumnCardinality,
		},
		{
			"select distinct x from (select * from foo limit 10)",
			SizeExactSmall,
		},
		{
			"select col, max(stat) from input group by col",
			SizeColumnCardinality,
		},
		{
			"select col, max(stat) from input group by col order by max(stat) desc limit 10",
			SizeExactSmall,
		},
	}

	noschema := mkenv(expr.NoHint, nil)
	for i := range cases {
		s, err := partiql.Parse([]byte(cases[i].query))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Build(s, noschema)
		if err != nil {
			t.Fatal(err)
		}
		var text bytes.Buffer
		b.Describe(&text)
		want := cases[i].want
		got := b.Class()
		if got != want {
			t.Logf("built: %s", &text)
			t.Errorf("query %q: got cardinality %s, want %s", cases[i].query, got, want)
		}
	}
}
