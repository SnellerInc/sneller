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

package stringext

import (
	"fmt"
	"testing"

	"golang.org/x/exp/slices"
)

func TestIndexByteEscape(t *testing.T) {
	type unitTest struct {
		str    string
		char   rune
		esc    rune
		expIdx int
	}

	unitTests := []unitTest{

		{"a@__", '_', '@', 3},

		{"a_b", '_', '$', 1},
		{"a_", '_', '$', 1},
		{"_b", '_', '$', 0},
		{"_", '_', '$', 0},

		{"a$_b", '_', '$', -1},
		{"a$_", '_', '$', -1},
		{"$_b", '_', '$', -1},
		{"$_", '_', '$', -1},
	}

	run := func(ut unitTest) {
		obsIdx := IndexRuneEscape([]rune(ut.str), ut.char, ut.esc)
		if obsIdx != ut.expIdx {
			t.Errorf("str %q, char=%q, esc=%q: observed %v; expected %v", ut.str, ut.char, ut.esc, obsIdx, ut.expIdx)
		}
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}

func TestLastIndexByteEscape(t *testing.T) {
	type unitTest struct {
		str    string
		wc     rune
		esc    rune
		expIdx int
	}

	unitTests := []unitTest{
		{"a_b", '_', '$', 1},
		{"a_", '_', '$', 1},
		{"_b", '_', '$', 0},
		{"_", '_', '$', 0},

		{"a$_b", '_', '$', -1},
		{"a$_", '_', '$', -1},
		{"$_b", '_', '$', -1},
		{"$_", '_', '$', -1},
	}

	run := func(ut unitTest) {
		obsIdx := LastIndexRuneEscape([]rune(ut.str), ut.wc, ut.esc)
		if obsIdx != ut.expIdx {
			t.Errorf("str %q, char=%q, esc=%q: observed %v; expected %v", ut.str, ut.wc, ut.esc, obsIdx, ut.expIdx)
		}
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}

func TestSplitEscape(t *testing.T) {
	type unitTest struct {
		str       string   // expression
		expResult []string // expected result
	}

	const kc = '%'
	const esc = '@'

	unitTests := []unitTest{
		{"a%", []string{"a", ""}},

		// without wc '_'
		{"a", []string{"a"}},
		{"a%b", []string{"a", "b"}},
		{"a%b%c", []string{"a", "b", "c"}},

		{"a@%b", []string{"a@%b"}},
		{"a@%b%c", []string{"a@%b", "c"}},

		{"a%", []string{"a", ""}},
		{"%a", []string{"", "a"}},
		{"%a%", []string{"", "a", ""}},

		{"a%%b", []string{"a", "", "b"}}, // merge multiple '%'
		{"%%a%%b%%", []string{"", "", "a", "", "b", "", ""}},
		{"%@%a%%b%@%", []string{"", "@%a", "", "b", "@%"}},
	}

	run := func(ut unitTest) {
		obsResult := splitEscape([]rune(ut.str), kc, esc)
		equal := true
		if len(obsResult) != len(ut.expResult) {
			equal = false
		} else {
			for i := 0; i < len(obsResult); i++ {
				if string(obsResult[i]) != ut.expResult[i] {
					equal = false
					break
				}
			}
		}
		if !equal {
			t.Errorf("str %q, observed %q; expected %q", ut.str, obsResult, ut.expResult)
		}
	}
	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}

func TestSimplifyLikeExpr(t *testing.T) {
	type unitTest struct {
		str       string        // expression
		expResult []LikeSegment // expected result
	}

	const wc = '_'
	const kc = '%'
	const esc = '@'

	eq := func(a, b LikeSegment) bool {
		return a.SkipMin == b.SkipMin && a.SkipMax == b.SkipMax && a.Pattern.Needle == b.Pattern.Needle
	}

	p := func(s string) Pattern {
		return NewPattern(s, wc, esc)
	}

	unitTests := []unitTest{
		// without wc '_'
		{"a", []LikeSegment{
			{0, 0, p("a")},
			{0, 0, p("")},
		}},
		{"a%b", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("b")},
			{0, 0, p("")},
		}},
		{"a%b%c", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("b")},
			{0, -1, p("c")},
			{0, 0, p("")},
		}},
		{"a@%b", []LikeSegment{
			{0, 0, p("a@%b")},
			{0, 0, p("")},
		}},
		{"a@%b%c", []LikeSegment{
			{0, 0, p("a@%b")},
			{0, -1, p("c")},
			{0, 0, p("")},
		}},
		{"a%", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("")},
		}},
		{"%a", []LikeSegment{
			{0, -1, p("a")},
			{0, 0, p("")},
		}},
		{"%a%", []LikeSegment{
			{0, -1, p("a")},
			{0, -1, p("")},
		}},
		{"a%%b", []LikeSegment{ // merge multiple '%'
			{0, 0, p("a")},
			{0, -1, p("b")},
			{0, 0, p("")},
		}},
		{"a%%%b", []LikeSegment{ // merge multiple '%'
			{0, 0, p("a")},
			{0, -1, p("b")},
			{0, 0, p("")},
		}},
		{"%%a%%b%%", []LikeSegment{ // merge multiple '%'
			{0, -1, p("a")},
			{0, -1, p("b")},
			{0, -1, p("")},
		}},
		{"%@%a%%b%@%", []LikeSegment{ // merge multiple '%'
			{0, -1, p("@%a")},
			{0, -1, p("b")},
			{0, -1, p("@%")},
			{0, 0, p("")},
		}},

		// with wc '_'
		{"a_b", []LikeSegment{
			{0, 0, p("a_b")},
			{0, 0, p("")},
		}},
		{"a_", []LikeSegment{
			{0, 0, p("a")},
			{1, 1, p("")},
		}},
		{"_a", []LikeSegment{
			{1, 1, p("a")},
			{0, 0, p("")},
		}},
		{"_", []LikeSegment{
			{1, 1, p("")},
		}},
		{"_a_b_", []LikeSegment{
			{1, 1, p("a_b")},
			{1, 1, p("")},
		}},

		{"a_%b", []LikeSegment{ // absorb '_' in '%'
			{0, 0, p("a")},
			{1, -1, p("b")},
			{0, 0, p("")},
		}},
		{"a%_b", []LikeSegment{ // absorb '_' in '%'
			{0, 0, p("a")},
			{1, -1, p("b")},
			{0, 0, p("")},
		}},
		{"_%a", []LikeSegment{ // absorb '_' in '%'
			{1, -1, p("a")},
			{0, 0, p("")},
		}},
		{"%_a", []LikeSegment{ // absorb '_' in '%'
			{1, -1, p("a")},
			{0, 0, p("")},
		}},
		{"a%_", []LikeSegment{ // absorb '_' in '%'
			{0, 0, p("a")},
			{1, -1, p("")},
		}},
		{"a_%", []LikeSegment{ // absorb '_' in '%'
			{0, 0, p("a")},
			{1, -1, p("")},
		}},

		{"a%_%b", []LikeSegment{ // absorb '_' in '%' and merge multiple '%'
			{0, 0, p("a")},
			{1, -1, p("b")},
			{0, 0, p("")},
		}},
		{"a%__%b", []LikeSegment{
			{0, 0, p("a")},
			{2, -1, p("b")},
			{0, 0, p("")},
		}},
		{"a%___%b", []LikeSegment{
			{0, 0, p("a")},
			{3, -1, p("b")},
			{0, 0, p("")},
		}},
		{"b_%_", []LikeSegment{
			{0, 0, p("b")},
			{2, -1, p("")},
		}},
		{"b__%", []LikeSegment{
			{0, 0, p("b")},
			{2, -1, p("")},
		}},
		{"a%b__%_", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("b")},
			{3, -1, p("")},
		}},
		{"a%b__@%_", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("b__@%")},
			{1, 1, p("")},
		}},
		{"a%b_@_%_", []LikeSegment{
			{0, 0, p("a")},
			{0, -1, p("b_@_")},
			{1, -1, p("")},
		}},
		{"_a%_b%c_d", []LikeSegment{
			{1, 1, p("a")},
			{1, -1, p("b")},
			{0, -1, p("c_d")},
			{0, 0, p("")},
		}},
	}

	run := func(ut unitTest) {
		obsResult := SimplifyLikeExpr(ut.str, wc, kc, esc)
		equal := slices.EqualFunc(obsResult, ut.expResult, eq)
		if !equal {
			t.Errorf("str %q, observed %v; expected %v", ut.str, obsResult, ut.expResult)
		}
	}
	for i, ut := range unitTests {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			run(ut)
		})
	}
}

func FuzzSimplifyLikeExpr(f *testing.F) {
	const wc = '_'
	const kc = '%'
	const esc = '@'

	f.Fuzz(func(t *testing.T, s string) {
		// SimplifyLikeExpr should not crash
		SimplifyLikeExpr(s, wc, kc, esc)
	})
}
