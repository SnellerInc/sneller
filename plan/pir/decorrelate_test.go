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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/expr/partiql"
)

// test that correlated sub-queries that would require
// a loop join or are otherwise too weird to deal with
// are correctly rejected as unsupported
func TestUnsupported(t *testing.T) {
	tests := []string{
		`select x, (select z from bar where x = y limit 2) from foo`,
		`select x, (select max(x) from bar where x = y) from foo`,
		`select x, (select max(z) from bar where x = y OR x = 2) from foo`,
		`select x, y, (select b.z from bar b where b.y = x AND b.y = y limit 1) from foo`,
		`select x, (select a from bar where x = y AND x = z limit 1) from foo`,
		`select x, (select a from bar where x = y AND x > 10 limit 1) from foo`,
		`select x, (select x+y from bar where x = z limit 1) from foo`,
	}
	for i := range tests {
		query := tests[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			s, err := partiql.Parse([]byte(query))
			if err != nil {
				t.Fatal(err)
			}
			_, err = Build(s, nil)
			if err == nil {
				t.Errorf("didn't error on query %s", query)
				return
			}
			t.Log(err)
			var cerr *CompileError
			if !errors.As(err, &cerr) {
				t.Errorf("couldn't convert %T to a CompileError", err)
				return
			}
			var txt strings.Builder
			cerr.WriteTo(&txt)
			t.Log(txt.String())
		})
	}
}
