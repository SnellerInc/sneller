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
