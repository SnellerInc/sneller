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
