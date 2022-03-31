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
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
)

func TestRedacted(t *testing.T) {
	const (
		magicInt    = "123456"
		magicFloat  = "0.5"
		magicString = "secret"
	)

	queries := []string{
		"SELECT x FROM input WHERE password = 0.5 OR other = 'secret' OR ID = 123456",
	}

	for i := range queries {
		q, err := partiql.Parse([]byte(queries[i]))
		if err != nil {
			t.Fatal(err)
		}
		text := expr.ToRedacted(q)
		t.Logf("redacted to: %s", text)
		for _, needle := range []string{
			magicInt, magicFloat, magicString,
		} {
			if strings.Contains(text, needle) {
				t.Errorf("%q contains %q", text, needle)
			}
		}
	}
}
