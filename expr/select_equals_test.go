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
		d, _, err := ion.ReadDatum(&st, buf.Bytes())
		if err != nil {
			t.Fatalf("case %d: decode datum: %s", i, err)
		}
		node, err := expr.Decode(d)
		if err != nil {
			t.Fatalf("case %d: decode: %s", i, err)
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
