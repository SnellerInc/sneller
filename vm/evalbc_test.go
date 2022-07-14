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

package vm

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
	"golang.org/x/exp/maps"
)

// TestStringCompare tests opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompare(t *testing.T) {
	type testcase struct {
		name string
		// alphabet from which to generate needles
		alphabet []rune
		// portable comparison function
		compare func(string, string) bool
		// bytecode implementation of comparison
		op bcop
		// string immediate -> dictionary value function
		dictval func(string) string
	}

	cases := []testcase{
		{
			// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
			// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
			// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
			name:     "equal",
			alphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω'},
			compare:  func(x, y string) bool { return x == y },
			op:       opCmpStrEqCs,
			dictval:  func(x string) string { return x },
		},
		{
			name:     "equal_ci_ascii",
			alphabet: []rune{'a', 'b', 'c', 'd', 'A', 'B', 'C', 'D', 'z', '!', '@'},
			compare:  strings.EqualFold, // we're only generating ASCII
			op:       opCmpStrEqCi,
			dictval:  stringext.NormalizeStringASCIIOnly,
		},
		/* NOTE: currently disabled due to a bug
		{
			name: "equal_ci_utf8",
			alphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω', 0x0},
			compare:  strings.EqualFold,
			op:       opCmpStrEqUTF8Ci,
			dictval:  func(x string) string { return stringext.GenNeedleExt(x, false) },
		},
		*/
	}

	var padding []byte // empty padding
	var ctx bctestContext
	defer ctx.Free()
	run := func(t *testing.T, str string, group []string, tc *testcase) {
		ctx.dict = append(ctx.dict[:0], pad(tc.dictval(str)))
		ctx.setScalarStrings(group, padding)
		ctx.current = (1 << len(group)) - 1
		want := uint16(0)
		for i := range group {
			if tc.compare(str, group[i]) {
				want |= 1 << i
			}
		}
		// when
		if err := ctx.ExecuteImm2(tc.op, 0); err != nil {
			t.Error(err)
		}
		// then
		if ctx.current != want {
			delta := ctx.current ^ want
			for i := range group {
				if delta&(1<<i) != 0 {
					t.Fatalf("comparing %v to data %v: got %v expected %v", escapeNL(str), escapeNL(group[i]), ctx.current&(1<<i) != 0, want&(1<<i) != 0)
				}
			}
		}
	}

	const lanes = 16
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			var group []string
			strSpace := createSpaceRandom(4, tc.alphabet, 2000)
			for _, str1 := range strSpace {
				group = group[:0]
				for _, str2 := range strSpace {
					group = append(group, str2)
					if len(group) < lanes {
						continue
					}
					run(t, str1, group, tc)
					group = group[:0]
				}
				if len(group) > 0 {
					run(t, str1, group, tc)
				}
			}
		})
	}
}

// TestMatchpatRef tests the reference implementation of matchpat (matchPatternReference) with a much slower regex impl
func TestMatchpatRef(t *testing.T) {
	dataSpace := createSpace(4, []rune{'a', 'b', 's', 'ſ'})
	patternSpace := createSpacePatternRandom(6, []rune{'s', 'S', 'k', 'K'}, 500)

	// matchPatternRegex matches the first occurrence of the provided pattern similar to matchPatternReference
	// matchPatternRegex implementation is the refImpl for the matchPatternReference implementation.
	// the regex impl is about 10x slower and does not return expected value registers (offset and length)
	matchPatternRegex := func(msg []byte, offset, length int, pattern []byte, caseSensitive bool) (laneOut bool) {
		regex := stringext.PatternToRegex(pattern, caseSensitive)
		r, err := regexp.Compile(regex)
		if err != nil {
			t.Errorf("Could not compile regex %v", regex)
		}
		loc := r.FindIndex(stringext.ExtractFromMsg(msg, offset, length))
		return loc != nil
	}

	for _, caseSensitive := range []bool{false, true} {
		for _, pattern := range patternSpace {
			for _, data := range dataSpace {
				wantMatch := matchPatternRegex([]byte(data), 0, len(data), []byte(pattern), caseSensitive)
				obsMatch, obsOffset, obsLength := matchPatternReference([]byte(data), 0, len(data), []byte(pattern), caseSensitive)
				if wantMatch != obsMatch {
					t.Fatalf("matching data %q to pattern %q = %v: observed %v (offset %v; length %v); expected %v",
						escapeNL(data), pattern, []byte(pattern), wantMatch, obsOffset, obsLength, wantMatch)
				}
			}
		}
	}
}

// TestPatMatch tests opMatchpatCs, opMatchpatCi, opMatchpatUTF8Ci
func TestPatMatch(t *testing.T) {
	type testcase struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet, patternAlphabet []rune
		// max length of the words made of alphabet
		dataMaxlen, patternMaxlen int
		// portable reference implementation: f(data, dictval) -> match, offset, length
		refImpl func(string, string) (bool, int, int)
		// bytecode implementation of comparison
		op bcop
		// string immediate -> dictionary value function
		encode func(string) string
		// evaluate equality function: wanted (match, offset, length); observed (match, offset, length) -> equality
		evalEq func(bool, int, int, bool, uint32, uint32) bool
	}

	eqfunc1 := func(wantMatch bool, wantOffset, wantLength int, obsMatch bool, obsOffset, obsLength uint32) bool {
		if wantMatch != obsMatch {
			return false
		}
		if obsMatch { // if the wanted and observed match are equal, and the match is true, then also check the offset and length
			return (wantOffset == int(obsOffset)) && (wantLength == int(obsLength))
		}
		return true
	}

	cases := []testcase{
		{
			name:            "opMatchpatCs",
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataMaxlen:      4,
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternMaxlen:   5,
			refImpl: func(data, dictval string) (match bool, offset, length int) {
				return matchPatternReference([]byte(data), 0, len(data), []byte(dictval), true)
			},
			op:     opMatchpatCs,
			encode: func(dictval string) string { return dictval },
			evalEq: eqfunc1,
		},
		// NOTE: currently disabled due to a bug
		/* {
			name:            "opMatchpatCi",
			dataAlphabet:    []rune{'s', 'S', 'ſ', 'k'},
			dataMaxlen:      4,
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternMaxlen:   5,
			refImpl: func(data, dictval string) (match bool, offset, length int) {
				return matchPatternReference([]byte(data), 0, len(data), []byte(dictval), false)
			},
			op:     opMatchpatCi,
			encode: func(dictval string) string { return dictval },
			evalEq: eqfunc1,
		},
		*/
		{
			name:            "opMatchpatUTF8Ci",
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataMaxlen:      4,
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternMaxlen:   5,
			refImpl: func(data, dictval string) (match bool, offset, length int) {
				return matchPatternReference([]byte(data), 0, len(data), []byte(dictval), false)
			},
			op: opMatchpatUTF8Ci,
			encode: func(dictval string) string { //NOTE: dictval is encoded for regular pattern
				return stringext.GenPatternExt(stringext.PatternToSegments([]byte(dictval)))
			},
			evalEq: eqfunc1,
		},
	}
	//FIXME opMatchpatUTF8Ci only seems to work when padding is not empty
	padding := []byte{0x0}

	var ctx bctestContext
	defer ctx.Free()
	run := func(t *testing.T, dictval string, data []string, tc *testcase) {
		ctx.dict = append(ctx.dict[:0], pad(tc.encode(dictval)))
		ctx.setScalarStrings(data, padding)
		ctx.current = (1 << len(data)) - 1
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(tc.op, 0); err != nil {
			t.Error(err)
		}
		scalarAfter := ctx.getScalarUint32()

		// then
		for i := range data {
			wantLane, wantOffset, wantLength := tc.refImpl(data[i], dictval)
			obsLane := ctx.current&(1<<i) != 0
			obsOffset := scalarAfter[0][i] - scalarBefore[0][i] // NOTE the reference implementation returns offset starting from zero
			obsLength := scalarAfter[1][i]

			if !tc.evalEq(wantLane, wantOffset, wantLength, obsLane, obsOffset, obsLength) {
				t.Fatalf("matching data %q to pattern %q = %v: observed %v (offset %v; length %v); expected %v (offset %v; length %v)",
					escapeNL(data[i]), dictval, []byte(dictval), obsLane, obsOffset, obsLength, wantLane, wantOffset, wantLength)
			}
		}
	}

	const lanes = 16
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			var group []string
			dataSpace := createSpace(tc.dataMaxlen, tc.dataAlphabet)
			patternSpace := createSpacePatternRandom(tc.patternMaxlen, tc.patternAlphabet, 1000)
			for _, pattern := range patternSpace {
				group = group[:0]
				for _, data := range dataSpace {
					group = append(group, data)
					if len(group) < lanes {
						continue
					}
					run(t, pattern, group, tc)
					group = group[:0]
				}
				if len(group) > 0 {
					run(t, pattern, group, tc)
				}
			}
		})
	}
}

func TestBytecodeAbsInt(t *testing.T) {
	// given
	var ctx bctestContext
	ctx.Taint()

	values := []int64{5, -52, 1002, -412, 0, 1, -3}
	ctx.setScalarInt64(values)
	ctx.current = (1 << len(values)) - 1

	current := ctx.current

	// when
	err := ctx.Execute(opabsi)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	// then
	if ctx.current != current {
		t.Logf("current  = %02x", current)
		t.Logf("modified = %02x", ctx.current)
		t.Error("opcode changed the current mask")
	}

	expected := []int64{5, 52, 1002, 412, 0, 1, 3}
	result := ctx.getScalarInt64()
	for i := range expected {
		if expected[i] != result[i] {
			t.Logf("expected = %d", expected)
			t.Logf("got      = %d", result)
			t.Errorf("mismatch at #%d", i)
			break
		}
	}
}

func TestBytecodeAbsFloat(t *testing.T) {
	// given
	var ctx bctestContext
	ctx.Taint()

	values := []float64{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4}
	ctx.setScalarFloat64(values)
	ctx.current = (1 << len(values)) - 1

	current := ctx.current

	// when
	err := ctx.Execute(opabsf)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	// then
	if ctx.current != current {
		t.Logf("current  = %02x", current)
		t.Logf("modified = %02x", ctx.current)
		t.Error("opcode changed the current mask")
	}

	expected := []float64{5, 4, 3, 2, 1, 0, 1, 2, 3, 4}
	result := ctx.getScalarFloat64()
	for i := range expected {
		if expected[i] != result[i] {
			t.Logf("expected = %f", expected)
			t.Logf("got      = %f", result)
			t.Errorf("mismatch at #%d", i)
			break
		}
	}
}

func TestBytecodeToInt(t *testing.T) {
	// given
	var ctx bctestContext
	ctx.Taint()

	var values []interface{}
	values = append(values, []byte{0x20})
	values = append(values, []byte{0x21, 0xff})
	values = append(values, []byte{0x22, 0x11, 0x33})
	values = append(values, ion.Int(-42))
	values = append(values, ion.Uint(12345678))

	ctx.setInputIonFields(values, nil)
	ctx.current = (1 << len(values)) - 1

	current := ctx.current

	// when
	err := ctx.Execute(optoint)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	// then
	if ctx.current != current {
		t.Logf("current  = %02x", current)
		t.Logf("modified = %02x", ctx.current)
		t.Error("opcode changed the current mask")
	}

	expected := []int64{0, 255, 0x1133, -42, 12345678}
	result := ctx.getScalarInt64()
	for i := range expected {
		if expected[i] != result[i] {
			t.Logf("expected = %x", expected)
			t.Logf("got      = %x", result)
			t.Errorf("mismatch at #%d", i)
			break
		}
	}
}

func TestBytecodeIsNull(t *testing.T) {
	// given
	var ctx bctestContext
	ctx.Taint()

	var values []interface{}
	values = append(values, []byte{0x10})
	values = append(values, []byte{0x2f})
	values = append(values, []byte{0x30})
	values = append(values, []byte{0x40})

	values = append(values, []byte{0x5f})
	values = append(values, []byte{0x6f})
	values = append(values, []byte{0x70})
	values = append(values, []byte{0x80})

	values = append(values, []byte{0x90})
	values = append(values, []byte{0xaf})
	values = append(values, []byte{0xb0})
	values = append(values, []byte{0xcf})

	values = append(values, []byte{0xe0})
	values = append(values, []byte{0xef})
	values = append(values, []byte{0xff})
	values = append(values, []byte{0x00})

	ctx.current = 0xffff
	ctx.setInputIonFields(values, nil)

	// when
	err := ctx.Execute(opisnull)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	// then
	expected := uint16(0x6a32)
	if ctx.current != expected {
		t.Logf("expected = %016b (0x%02x)", expected, expected)
		t.Logf("current  = %016b (0x%02x)", ctx.current, ctx.current)
		t.Error("wrong mask")
	}
}

// regexMatch determines whether data-structure for DFA operation op matches needle
func regexMatch(t *testing.T, ctx *bctestContext, ds *[]byte, op bcop, needles []string) uint16 {
	ctx.Taint()
	ctx.dict = append(ctx.dict[:0], string(*ds))
	ctx.setScalarStrings(needles, []byte{})
	ctx.current = (1 << len(needles)) - 1

	// when
	err := ctx.ExecuteImm2(op, 0)
	if err != nil {
		t.Fatal(err)
	}
	return ctx.current
}

// regexNeedleTest tests the equality for all regexes provided in the data-structure container for one provided needle
func regexNeedleTest(t *testing.T, ctx *bctestContext, dsByte *[]byte, opStr string, op bcop, needles []string, expected uint16, ds *regexp2.DataStructures) {
	if dsByte == nil {
		return
	}
	got := regexMatch(t, ctx, dsByte, op, needles)
	if got != expected {
		delta := got ^ expected
		for i := 0; i < 16; i++ {
			if delta&(1<<i) != 0 {
				t.Errorf("issue %v (with %v) for needle %v: regexGolang=%q yields %v; regexSneller=%q yields %v",
					op, opStr, escapeNL(needles[i]), escapeNL(ds.RegexGolang.String()), got&(1<<i) != 0, escapeNL(ds.RegexSneller.String()), expected)
			}
		}
	}
}

// regexNeedlesTest tests the equality for all regexes provided in the data-structure container for all provided needles
func regexNeedlesTest(t *testing.T, ds *regexp2.DataStructures, needles []string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	var ctx bctestContext
	ctx.Taint()
	defer ctx.Free()
	const lanes = 16
	for len(needles) > 0 {
		group := needles
		if len(group) > lanes {
			group = group[:lanes]
		}
		needles = needles[len(group):]
		want := uint16(0)
		for i := range group {
			if ds.RegexGolang.MatchString(group[i]) {
				want |= 1 << i
			}
		}
		regexNeedleTest(t, &ctx, ds.DsT6, "DfaT6", opDfaT6, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsT6Z, "DfaT6Z", opDfaT6Z, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsT7, "DfaT7", opDfaT7, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsT7Z, "DfaT7Z", opDfaT7Z, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsT8, "DfaT8", opDfaT8, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsT8Z, "DfaT8Z", opDfaT8Z, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsL, "DfaL", opDfaL, group, want, ds)
		regexNeedleTest(t, &ctx, ds.DsLZ, "DfaLZ", opDfaLZ, group, want, ds)
	}
}

func TestRegexType1(t *testing.T) {
	needles := createSpace(6, []rune{'a', 'b', 'c', 'd', '\n', 'Ω'}) // U+2126 'Ω' (3 bytes)

	testCases := []struct {
		expr     string
		writeDot bool
	}{
		//automaton with flags
		{`a$`, false},
		//NOT supported {CreateDs(`a|$`, false},
		{`a|b$`, false},

		//automaton without flags
		{`.*a.b`, false},
		{`.*a.a`, false},
		{`a*.b`, false},
		{`a*.b*.c`, false},
		{`a*.b*.c*.d`, false},
		{`c*.*(aa|cd)`, false},
		{`(c*b|.a)`, false},
		{`.*b*.a`, false},
		{`b*.a*.`, false},
		{`b*..*b`, false},
		{`a*..*a`, false},
		{`..|aaaa`, false},
		{`..|aa`, false},
		{`.ba|aa`, false},
		{`a*...`, false},
		{`a*..`, false},
		{`c*.*aa`, false},
		{`.a|aaa`, false},
		{`ab|.c`, false},
		{`.*ab`, false},
		{`a*..a`, false},
		{`a*..b`, false},
		{`a*.b`, false},
		{`.*ab.*cd`, false},
	}
	for i, data := range testCases {
		t.Run(fmt.Sprintf(`case %d`, i), func(t *testing.T) {
			ds := regexp2.CreateDs(data.expr, regexp2.Regexp, data.writeDot, regexp2.MaxNodesAutomaton)
			regexNeedlesTest(t, &ds, needles, nil)
		})
	}
}

func TestRegexType2(t *testing.T) {
	needles := createSpace(6, []rune{'a', 'b', 'c', 'd', '\n', 'Ω'}) // U+2126 'Ω' (3 bytes)

	testCases := []struct {
		expr     string
		writeDot bool
	}{
		{`(aa|b*)`, false}, //issue: In Tiny: pushing $ upstream makes the start-node accepting and optimizes outgoing edges away
		{`a*`, false},      //issue: In Tiny: pushing $ upstream makes the start-node accepting and optimizes outgoing edges away
		{`ab|cd`, false},
		{`%a_b`, false},
		{`%a_a`, false},
		{`a%b`, false},
		{`a%b%c`, false},
		{`a%b%c%d`, false},
		{`c*%(aa|cd)`, false},
		{`(c*b|_a)`, false},
		{`c*b|_a`, false},
		{`%b*_a`, false},
		{`b*_a*_`, false},
		{`b*_%b`, false},
		{`a*_%a`, false},
		{`__|aaaa`, false},
		{`__|aa`, false},
		{`_ba|aa`, false},
		{`a*___`, false},
		{`a*__`, false},
		{`c*%aa`, false},
		{`_a|aaa`, false},
		{`ab|_c`, false},
		{`%ab`, false},
		{`a*__a`, false},
		{`a*__b`, false},
		{`a*_b`, false},
		{`%ab%cd`, false},
	}
	for i, data := range testCases {
		t.Run(fmt.Sprintf(`case %d`, i), func(t *testing.T) {
			ds := regexp2.CreateDs(data.expr, regexp2.SimilarTo, data.writeDot, regexp2.MaxNodesAutomaton)
			regexNeedlesTest(t, &ds, needles, nil)
		})
	}
}

func TestRegexIP4(t *testing.T) {
	needles := createSpaceRandom(12, []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'x', '.'}, 100000)
	t.Logf("Number of needles %d", len(needles))
	testCases := []struct {
		expr      string
		regexType regexp2.RegexType
		writeDot  bool
	}{
		{`^(?:[0-9]{1,3}\.){3}[0-9]{1,3}`, regexp2.SimilarTo, false},
		{`^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$`, regexp2.Regexp, false},
		{`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, regexp2.SimilarTo, false},
		{`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`, regexp2.Regexp, false},
	}
	for i, data := range testCases {
		t.Run(fmt.Sprintf(`case %d`, i), func(t *testing.T) {
			ds := regexp2.CreateDs(data.expr, data.regexType, data.writeDot, regexp2.MaxNodesAutomaton)
			regexNeedlesTest(t, &ds, needles, nil)
		})
	}
}

// bruteForceIterateRegex iterates over all regexes with the provided regex length and regex alphabet,
// and determines equality over all needles with the provided needle length and needle alphabet
func bruteForceIterateRegex(t *testing.T, regexLength, needleLength int, regexAlphabet, needleAlphabet []rune, regexType regexp2.RegexType) {
	regexAlphabetSize := len(regexAlphabet)
	regexIndices := make([]byte, regexLength)
	regexRunes := make([]rune, regexLength)
	regexDone := false

	needles := createSpace(needleLength, needleAlphabet)
	nNeedles := len(needles)
	nTests := 0

	for !regexDone {
		for i := 0; i < regexLength; i++ {
			regexRunes[i] = regexAlphabet[regexIndices[i]]
		}
		regexStr := string(regexRunes)
		if _, err := regexp2.Compile(regexStr, regexType); err != nil {
			// ignore strings that are not valid regexes
		} else if err := regexp2.IsSupported(regexStr); err != nil {
			// ignore not supported regexes
		} else {
			ds := regexp2.CreateDs(regexStr, regexType, false, regexp2.MaxNodesAutomaton)
			if nNeedles < 100 { // do serial
				regexNeedlesTest(t, &ds, needles, nil)
			} else { // do parallel
				nGroups := 20
				groupSize := (nNeedles / nGroups) + 1
				var wg sync.WaitGroup
				nItemsRemaining := len(needles)
				i := 0
				for nItemsRemaining > 0 {
					wg.Add(1)
					lowerBound := i * groupSize
					upperBound := lowerBound + groupSize
					if upperBound > nNeedles {
						upperBound = nNeedles
					}
					needleFragment := needles[lowerBound:upperBound]
					go regexNeedlesTest(t, &ds, needleFragment, &wg)
					nItemsRemaining -= len(needleFragment)
					i++
				}
				wg.Wait()
			}
			nTests += nNeedles
		}
		regexDone = !next(&regexIndices, regexAlphabetSize, regexLength)
	}
	t.Logf("brute-force did %v tests (regexLength %v; needleLength %v; nNeedles %v)", nTests, regexLength, needleLength, nNeedles)
}

// TestRegexBruteForce1 tests unicode code-points in regex and needle
func TestRegexBruteForce1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	regexType := regexp2.Regexp
	regexAlphabet := []rune{'a', 'b', '.', '*', '|', 'Ω'}
	needleAlphabet := []rune{'a', 'b', 'c', 'Ω'} // U+2126 'Ω' (3 bytes)

	for regexLength := 1; regexLength < 6; regexLength++ {
		for needleLength := 1; needleLength < 4; needleLength++ {
			bruteForceIterateRegex(t, regexLength, needleLength, regexAlphabet, needleAlphabet, regexType)
		}
	}
}

// TestRegexBruteForce2 tests newline in regex and needle
func TestRegexBruteForce2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	regexType := regexp2.Regexp
	regexAlphabet := []rune{'a', 'b', '.', '*', '|', 0x0A}
	needleAlphabet := []rune{'a', 'b', 'c', 0x0A} // 0x0A = newline

	for regexLength := 1; regexLength < 6; regexLength++ {
		for needleLength := 1; needleLength < 4; needleLength++ {
			bruteForceIterateRegex(t, regexLength, needleLength, regexAlphabet, needleAlphabet, regexType)
		}
	}
}

// TestRegexBruteForce3 tests UTF8 needles with 'SIMILAR TO'
func TestRegexBruteForce3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	regexType := regexp2.SimilarTo
	regexAlphabet := []rune{'a', 'b', '_', '%', 'Ω'} //FIXME exists an issue with '|': eg "|a"
	needleAlphabet := []rune{'a', 'b', 'c', 'Ω'}     // U+2126 'Ω' (3 bytes)

	for regexLength := 1; regexLength < 6; regexLength++ {
		for needleLength := 1; needleLength < 4; needleLength++ {
			bruteForceIterateRegex(t, regexLength, needleLength, regexAlphabet, needleAlphabet, regexType)
		}
	}
}

// TestRegexBruteForce3 tests newline with 'SIMILAR TO'
func TestRegexBruteForce4(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	regexType := regexp2.SimilarTo
	regexAlphabet := []rune{'a', 'b', '_', '%', 0x0A} //FIXME (=DfaLZ): for needle a regexGolang="(^(|a))$" yields false; regexSneller="(|a)$" yields true
	needleAlphabet := []rune{'a', 'b', 'c', 0x0A}

	for regexLength := 1; regexLength < 6; regexLength++ {
		for needleLength := 1; needleLength < 4; needleLength++ {
			bruteForceIterateRegex(t, regexLength, needleLength, regexAlphabet, needleAlphabet, regexType)
		}
	}
}

/////////////////////////////////////////////////////////////
// Helper functions

// MatchPatternReference matches the first occurrence of the provided pattern.
// The MatchPatternReference implementation is used for fuzzing since it is 10x faster than matchPatternRegex
func matchPatternReference(msg []byte, offset, length int, pattern []byte, caseSensitive bool) (laneOut bool, offsetOut, lengthOut int) {

	// indexRune is similar to strings.Index; this function accepts rune arrays
	indexRune := func(s, substr []rune) int {
		idx := strings.Index(string(s), string(substr))
		if idx == -1 {
			return -1
		}
		off := 0
		for i := range string(s) {
			if i == idx {
				return off
			}
			off++
		}
		return off
	}

	// hasPrefixRune is similar to strings.HasPrefix; this function accepts rune arrays
	hasPrefixRune := func(s, prefix []rune) bool {
		return len(prefix) <= len(s) && slices.Equal(s[:len(prefix)], prefix)
	}

	if len(pattern) == 0 { // not sure how to handle an empty pattern, currently it always matches
		return true, offset, length
	}
	msgStrOrg := string(stringext.ExtractFromMsg(msg, offset, length))
	msgStr := msgStrOrg

	if !caseSensitive { // normalize msg and pattern to make case-insensitive comparison possible
		msgStr = stringext.NormalizeString(msgStrOrg)
		pattern = stringext.PatternNormalize(pattern)
	}
	segments := stringext.PatternToSegments(pattern)
	msgRunesOrg := []rune(msgStrOrg)
	msgRunes := []rune(msgStr)
	nRunesMsg := len(msgRunes)

	for runePos := 0; runePos < nRunesMsg; runePos++ {
		nRunesInWildcards := 0 // only add the number of wildcards to the position once the segment has been found
		runePos1 := runePos
		for i, segment := range segments {
			if runePos1 >= nRunesMsg {
				break // exit the for loop; we have not found the pattern
			}
			isFirstSegment := i == 0
			isLastSegment := i == (len(segments) - 1)

			if len(segment) == 0 {
				nRunesInWildcards++ // we found an empty segment, that counts as one wildcard
			} else {
				remainingStartPos := runePos1 + nRunesInWildcards
				if remainingStartPos >= nRunesMsg {
					return false, offset + length, 0
				}
				remainingMsg := string(msgRunes[remainingStartPos:])
				remainingRunes := []rune(remainingMsg)
				segmentRunes := []rune(segment)

				if isFirstSegment {
					positionOfSegment := indexRune(remainingRunes, segmentRunes)
					if positionOfSegment == -1 { // segment not found
						runePos1++
						break
					} else { // found segment
						runePos1 += nRunesInWildcards + positionOfSegment + len(segmentRunes)
						nRunesInWildcards = 1
					}
				} else {
					if !hasPrefixRune(remainingRunes, segmentRunes) {
						break // segment not found
					} else { // found segment
						runePos1 += nRunesInWildcards + len(segmentRunes)
						nRunesInWildcards = 1
					}
				}
			}
			if isLastSegment {
				if runePos1 <= nRunesMsg {
					nBytesTillLastRune := len(string(msgRunesOrg[0:runePos1]))
					offsetOut := offset + nBytesTillLastRune
					lengthOut := length - nBytesTillLastRune
					return true, offsetOut, lengthOut
				}
			}
		}
	}
	return false, offset + length, 0
}

//next updates x to the successor; return true/false whether the x is valid
func next(x *[]byte, max, length int) bool {
	for i := 0; i < length; i++ {
		(*x)[i]++                // increment the current byte i
		if (*x)[i] < byte(max) { // is the current byte larger than the maximum value?
			return true // we have a valid successor
		}
		(*x)[i] = 0 // overflow for the current byte, try to increment the next byte i+1
	}
	return false // we have an overflow, return that we have no valid successor
}

//escapeNL escapes new line
func escapeNL(str string) string {
	return strings.ReplaceAll(str, "\n", "\\n")
}

// createSpace creates strings of length 1 upto maxLength over the provided alphabet
func createSpace(maxLength int, alphabet []rune) []string {
	result := make([]string, 0)
	alphabetSize := len(alphabet)
	indices := make([]byte, maxLength)

	for strLength := 1; strLength <= maxLength; strLength++ {
		strRunes := make([]rune, strLength)
		done := false
		for !done {
			for i := 0; i < strLength; i++ {
				strRunes[i] = alphabet[indices[i]]
			}
			result = append(result, string(strRunes))
			done = !next(&indices, alphabetSize, strLength)
		}
	}
	return result
}

// createSpaceRandom creates random strings with the provided length over the provided alphabet
func createSpaceRandom(maxLength int, alphabet []rune, maxSize int) []string {
	set := make(map[string]struct{}) // new empty set

	// Note: not the most efficient implementation: space of short strings
	// is quickly exhausted while we are still trying to find something
	strRunes := make([]rune, maxLength)
	alphabetSize := len(alphabet)

	for len(set) < maxSize {
		strLength := rand.Intn(maxLength) + 1
		for i := 0; i < strLength; i++ {
			strRunes[i] = alphabet[rand.Intn(alphabetSize)]
		}
		set[string(strRunes)] = struct{}{}
	}
	return maps.Keys(set)
}

func createSpacePatternRandom(maxLength int, alphabet []rune, maxSize int) []string {
	set := make(map[string]struct{})          // new empty set
	alphabet = append(alphabet, utf8.MaxRune) // use maxRune as a segment boundary
	alphabetSize := len(alphabet)

	for len(set) < maxSize {
		strLength := rand.Intn(maxLength) + 1
		strRunes := make([]rune, strLength)
		for i := 0; i < strLength; i++ {
			strRunes[i] = alphabet[rand.Intn(alphabetSize)]
		}
		s := string(strRunes)
		segments := strings.Split(s, string(utf8.MaxRune))
		if (len(segments[0]) > 0) && (len(segments[len(segments)-1]) > 0) {
			set[s] = struct{}{}
		}
	}

	result := make([]string, len(set))
	pos := 0
	for s := range set {
		segments := strings.Split(s, string(utf8.MaxRune))
		result[pos] = string(stringext.SegmentsToPattern(segments))
		pos++
	}
	return result
}
