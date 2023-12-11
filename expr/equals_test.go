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
