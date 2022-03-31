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

	"github.com/SnellerInc/sneller/expr/partiql"
)

func TestLoweringErrors(t *testing.T) {
	env := &testenv{t: t}

	tcs := []struct {
		query    string
		msg      string
		disabled bool
	}{
		{
			query: `select * from 'parking.10n' order by name asc, name desc`,
			msg:   `duplicate order by expression "name"`,
		},
		{
			query: `select * from 'parking.10n' order by size * coef asc, size * coef desc`,
			msg:   `duplicate order by expression "size * coef"`,
		},
	}

	for i := range tcs {
		if tcs[i].disabled {
			continue
		}

		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			text := tcs[i].query
			q, err := partiql.Parse([]byte(text))
			if err != nil {
				t.Logf("parsing %q", text)
				t.Fatal(err)
			}
			t.Logf("query: %+v", q)
			_, err = New(q, env)
			if err == nil {
				t.Fatalf("case %d: expected error to be reported", i)
				return
			}

			if err.Error() != tcs[i].msg {
				t.Logf("got: %q", err)
				t.Logf("expected: %q", tcs[i].msg)
				t.Fatalf("case %d: error messages do not match", i)
				return
			}
		})
	}
}
