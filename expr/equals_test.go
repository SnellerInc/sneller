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
	"math/big"
	"testing"
)

func TestEquals(t *testing.T) {
	tests := []struct {
		in, out Node
	}{
		{Integer(1), Float(1.0)},
		{Float(0.5), (*Rational)(new(big.Rat).SetFloat64(0.5))},
		{Integer(1), (*Rational)(new(big.Rat).SetInt64(1))},
		{Bool(true), Bool(true)},
		{String("foo"), String("foo")},
		{And(path("x"), path("y")), And(path("x"), path("y"))},
		{Is(path("x"), IsMissing), Is(path("x"), IsMissing)},
		{Compare(Less, path("x"), path("y")), Compare(Less, path("x"), path("y"))},
		{&Not{path("x")}, &Not{path("x")}},
		{Count(path("foo")), Count(path("foo"))},
	}

	for i := range tests {
		if !tests[i].in.Equals(tests[i].out) {
			t.Errorf("case %d: %s != %s", i, tests[i].in, tests[i].out)
		}
		// test symmetry
		if !tests[i].out.Equals(tests[i].in) {
			t.Errorf("case %d: %s != %s", i, tests[i].out, tests[i].in)
		}
		// test reflexivity
		if !tests[i].in.Equals(tests[i].in) {
			t.Errorf("case %d: %s not equal to itself", i, tests[i].in)
		}
		if !tests[i].out.Equals(tests[i].out) {
			t.Errorf("case %d: %s not equal to itself", i, tests[i].out)
		}
	}
}
