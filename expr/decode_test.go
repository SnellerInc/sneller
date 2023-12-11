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
