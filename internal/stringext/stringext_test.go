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

package stringext

import (
	"fmt"
	"slices"
	"testing"
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

	f.Add("_")
	f.Add("a_b")
	f.Add("a@_%1")

	f.Fuzz(func(t *testing.T, s string) {
		// SimplifyLikeExpr should not crash
		SimplifyLikeExpr(s, wc, kc, esc)
	})
}

// FuzzToBCD checks whether ToBCD and DeEncodeBCD are duals
func FuzzToBCD(f *testing.F) {
	f.Add(byte(0), byte(1), byte(2), byte(3), byte(4), byte(5), byte(6), byte(7))

	f.Fuzz(func(t *testing.T, b0, b1, b2, b3, b4, b5, b6, b7 byte) {
		minX := [4]byte{b0, b1, b2, b3}
		maxX := [4]byte{b4, b5, b6, b7}
		str := ToBCD(&minX, &maxX)
		minY, maxY := DeEncodeBCD(str)

		if minX != minY {
			t.Errorf("Expected min %v but got %v ([]byte=%v)", minX, minY, []byte(str))
		}
		if maxX != maxY {
			t.Errorf("Expected max %v but got %v ([]byte=%v)", maxX, maxY, []byte(str))
		}
	})
}
