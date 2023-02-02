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

package expr

import (
	"errors"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestDecodeFailures(t *testing.T) {
	// keep in sync with decode.go/getEmpty()
	typenames := []string{
		"aggregate",
		"rat",
		"star",
		"dot",
		"index",
		"cmp",
		"stringmatch",
		"not",
		"logical",
		"builtin",
		"unaryArith",
		"arith",
		"append",
		"is",
		"select",
		"on",
		"join",
		"missing",
		"table",
		"case",
		"cast",
		"member",
		"struct",
		"list",
		"unpivot",
		"union",
	}

	var buf ion.Buffer
	var st ion.Symtab
	for _, name := range typenames {
		t.Run(name, func(t *testing.T) {
			// given
			buf.Reset()
			st.Reset()

			buf.BeginStruct(-1)
			buf.BeginField(st.Intern("type"))
			buf.WriteSymbol(st.Intern(name))
			buf.BeginField(st.Intern("unknown-field"))
			buf.WriteInt(42)
			buf.EndStruct()

			// when
			d, _, err := ion.ReadDatum(&st, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			_, err = Decode(d)

			// then
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, errUnexpectedField) {
				t.Fatalf("unexpected error %s", err)
			}
		})
	}
}
