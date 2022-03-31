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
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
)

func TestSelectEquals(t *testing.T) {
	strings := []string{
		`select x, avg(y) from foo where case when y < 2 then 0 else 1 end > 0 group by x`,
		`select coalesce(x, y, z)+100 from foo where x+y/z*2 < 100`,
	}
	for i := range strings {
		s, err := partiql.Parse([]byte(strings[i]))
		if err != nil {
			t.Fatal(err)
		}
		s2, err := partiql.Parse([]byte(strings[i]))
		if err != nil {
			t.Fatal("nondeterministic error?!", err)
		}

		if !s.Equals(s2) {
			t.Errorf("case %d: query %q not equal to itself", i, expr.ToString(s))
		}

		// now try serialization and confirm
		// that things are as they should be
		var buf ion.Buffer
		var st ion.Symtab
		s.Body.Encode(&buf, &st)
		node, rest, err := expr.Decode(&st, buf.Bytes())
		if err != nil {
			t.Fatalf("case %d: decode: %s", i, err)
		}
		if len(rest) != 0 {
			t.Errorf("case %d: %d bytes left over?", i, len(rest))
		}
		s3, ok := node.(*expr.Select)
		if !ok {
			t.Errorf("case %d: decode returned %T", i, node)
			continue
		}
		if !s.Body.Equals(s3) {
			t.Errorf("case %d: nodes not equal", i)
		}
	}
}
