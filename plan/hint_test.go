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
