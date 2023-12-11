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

package jsonrl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestConstant(t *testing.T) {
	testcases := []struct {
		in, out string
	}{
		{
			in:  `{"field": "foo"}`,
			out: `{"const0": 1, "const1": "two", "field": "foo"}`,
		},
		{
			// 'name' is pre-interned, so field order should be shifted:
			in:  `{"name": "symbol id 4"}`,
			out: `{"name": "symbol id 4", "const0": 1, "const1": "two"}`,
		},
		{
			in:  `{"const0": "overwrite me"}`,
			out: `{"const0": 1, "const1": "two"}`,
		},
	}

	cons := []ion.Field{
		{Label: "const0", Datum: ion.Uint(1)},
		{Label: "const1", Datum: ion.String("two")},
	}

	var buf bytes.Buffer
	for _, tc := range testcases {
		buf.Reset()
		cn := ion.Chunker{
			Align: 4096,
			W:     &buf,
		}
		in := strings.NewReader(tc.in)
		err := Convert(in, &cn, nil, cons)
		if err != nil {
			t.Fatal(err)
		}
		err = cn.Flush()
		if err != nil {
			t.Fatal(err)
		}
		dat, _, err := ion.ReadDatum(&cn.Symbols, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		got, err := toJSONString(dat, &cn.Symbols)
		if err != nil {
			t.Fatal(err)
		}
		got = strings.TrimSpace(got)
		if got != tc.out {
			t.Fatalf("got %q want %q", got, tc.out)
		}
	}
}
