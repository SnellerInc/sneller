// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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

	noschema := mkenv(expr.NoHint, nil, nil)
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
