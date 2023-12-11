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

package plan

import (
	"fmt"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
)

func parseExpr(str string) expr.Node {
	q, err := partiql.Parse([]byte(fmt.Sprintf("SELECT * FROM x WHERE %s", str)))
	if err != nil {
		println("couldn't parse", str)
		panic(err)
	}
	return q.Body.(*expr.Select).Where
}

// Test that our heuristics for predicate merging
// produce reasonable results. In general we look
// for timestamp comparisons and see if they are
// equivalent.
func TestMergeFilterHint(t *testing.T) {
	testcases := []struct {
		left, right string
		want        string
		ok          bool
	}{
		{
			left: "x < 3",
			// no right expr
			// no output
			ok: true,
		},
		{
			left: "x > `2022-08-01T00:00:00Z`",
			// no right expr
			// can't merge due to presence of timestamp comparison
			ok: false,
		},
		{
			left:  "x > `2022-08-01T00:00:00Z`",
			right: "x > `2022-08-01T00:00:00Z` AND y = 3",
			want:  "x > `2022-08-01T00:00:00Z`",
			ok:    true,
		},
		{
			left:  "x < 0 AND y > 1",
			right: "x < 0",
			want:  "x < 0",
			ok:    true,
		},
		{
			left:  "x < 0 AND y > 1",
			right: "x > 0 AND y = 'foo'",
			// ok to merge b/c none of the conjunctions
			// contain timestamp comparisons
			ok: true,
		},
		{
			left:  "x < `2022-08-31T00:00:00Z` and x > `2022-08-01T00:00:00Z`",
			right: "x > `2022-08-01T00:00:00Z`",
			// should not merge due to !canRemoveHint(x < `2022...`)
			ok: false,
		},
		{
			// exactly identical:
			left:  "x < `2022-08-31T00:00:00Z` and x > `2022-08-01T00:00:00Z`",
			right: "x < `2022-08-31T00:00:00Z` and x > `2022-08-01T00:00:00Z`",
			want:  "x > `2022-08-01T00:00:00Z` and x < `2022-08-31T00:00:00Z`",
			ok:    true,
		},
		{
			left:  "x < `2022-08-31T00:00:00Z` and x > `2022-08-01T00:00:00Z`",
			right: "x < `2022-08-31T00:00:00Z` and x > `2022-08-01T00:00:00Z` and y < 3 and x like '%a string%'",
			want:  "x > `2022-08-01T00:00:00Z`and x < `2022-08-31T00:00:00Z`",
			ok:    true,
		},
	}

	for i := range testcases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var left, right, want expr.Node
			if testcases[i].left != "" {
				left = parseExpr(testcases[i].left)
			}
			if testcases[i].right != "" {
				right = parseExpr(testcases[i].right)
			}
			if testcases[i].want != "" {
				want = parseExpr(testcases[i].want)
			}
			wantok := testcases[i].ok

			var il, ir input
			il.hints.Filter = left
			ir.hints.Filter = right
			ok := mergeFilterHint(&il, &ir)
			if wantok != ok {
				t.Fatalf("wanted result %v but got %v", wantok, ok)
			}
			if ok && wantok && !expr.Equivalent(il.hints.Filter, want) {
				t.Fatalf("wanted %s; got %s", expr.ToString(want), expr.ToString(il.hints.Filter))
			}
			// try reversed argument order;
			// should get the same result
			il.hints.Filter = right
			ir.hints.Filter = left
			ok = mergeFilterHint(&il, &ir)
			if wantok != ok {
				t.Fatalf("wanted result %v but got %v", wantok, ok)
			}
			if ok && wantok && !expr.Equivalent(il.hints.Filter, want) {
				t.Fatalf("flipped args: wanted %s; got %s", expr.ToString(want), expr.ToString(il.hints.Filter))
			}
		})
	}
}
