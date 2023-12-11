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
			query: `select * from 'parking.10n' order by name asc, name desc limit 100`,
			msg:   `duplicate order by expression "name"`,
		},
		{
			query: `select * from 'parking.10n' order by size * coef asc, size * coef desc limit 100`,
			msg:   `duplicate order by expression "size * coef"`,
		},
		{
			query: `select x, count(*) from 'tbl' group by x limit 10 offset 15`,
			msg:   `plan: query not supported: non-zero OFFSET of hash aggregate result`,
		},
		{
			query: `select distinct x from 'tbl' limit 5 offset 10`,
			msg:   `plan: query not supported: non-zero OFFSET of distinct result`,
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
