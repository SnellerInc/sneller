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
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
	"golang.org/x/exp/maps"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

// exhaustive search space: all combinations are explored
const exhaustive = -1

// TestStringCompareUT unit-tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompareUT(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data, needle string
		expLane      bool
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// portable reference implementation: f(data, needle) -> lane
		refImpl func(string, string) bool
		// bytecode to run
		op bcop
		// encoder for string literals -> dictionary value
		encode func(string) string
	}
	testSuites := []testSuite{
		{
			name: "compare string case-sensitive (opCmpStrEqCs)",
			unitTests: []unitTest{
				{"aaaa", "aaaa", true},
				{"aaa", "aaaa", false},
				{"aaaa", "aaa", false},
				{"aaaa", "aaab", false},
				{"aaaa", "Aaaa", false},
				{"𐍈aaa", "𐍈aaa", true},
				{"a𐍈aa", "a𐍈aa", true},
				{"aa𐍈a", "aa𐍈a", true},
				{"aaa𐍈", "aaa𐍈", true},
			},
			refImpl: func(x, y string) bool { return x == y },
			op:      opCmpStrEqCs,
			encode:  func(x string) string { return x },
		},
		{
			name: "refImpl string case-insensitive (opCmpStrEqCi)",
			unitTests: []unitTest{
				{"aaaa", "aaaa", true},
				{"aaa", "aaaa", false},
				{"aaaa", "aaa", false},
				{"aaaa", "aaab", false},
				{"aaaa", "Aaaa", true},
				{"𐍈aaa", "𐍈aaa", true},
				{"a𐍈aa", "A𐍈aa", true},
				{"aa𐍈a", "Aa𐍈a", true},
				{"aaa𐍈", "Aaa𐍈", true},
				{"aaa𐍈", "Aaa𐍈", true},
			},
			refImpl: func(x, y string) bool {
				return stringext.NormalizeStringASCIIOnly(x) == stringext.NormalizeStringASCIIOnly(y)
			},
			op:     opCmpStrEqCi,
			encode: stringext.NormalizeStringASCIIOnly, // only normalize ASCII values, leave other values (UTF8) unchanged
		},
		{
			name: "compare string case-insensitive UTF8 (opCmpStrEqUTF8Ci)",
			unitTests: []unitTest{
				//NOTE all UTF8 byte code assumes valid UTF8 input
				{"aΩa\nb", "aΩa\nB", true},
				{"aΩaa", "aΩaa", true},
				{"aksb", "AKſB", true},
				{"kSK", "KSK", true},
				{"KſK", "KSK", true},
				{"KſKſ", "KSK", false},
				{"KſK", "KS", false},
				{"Kſ", "K", false},
				{"Kſ", "K", false},
				{"KK", "K", false},

				{"", "", false},
				{"", "X", false},
				{"X", "", false},

				{"S", "S", true},
				{"a", "A", true},
				{"ab", "AB", true},

				{"$¢", "$¢", true},
				{"𐍈", "𐍈", true},
				{"¢𐍈", "€𐍈", false},

				{"¢¢", "¢¢", true},
				{"$¢€𐍈", "$¢€𐍈", true},
				{string([]byte{0x41, 0x41, 0xC2, 0xA2, 0xC2, 0xA2, 0x41, 0x41, 0xC2, 0xA2})[6:7], "A", true},

				{"AA¢¢¢¢"[0:4], "AA¢", true},
				{"$¢€𐍈ĳĲ", "$¢€𐍈ĲĲ", true},

				// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
				// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
				// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)

				{"ſ", "S", true},
				{"Ω", "Ω", true},
				{"K", "K", true},
			},
			refImpl: strings.EqualFold,
			op:      opCmpStrEqUTF8Ci,
			encode:  func(x string) string { return stringext.GenNeedleExt(x, false) },
		},
	}

	var padding []byte // no padding

	run := func(ts *testSuite, ut *unitTest) {
		if !utf8.ValidString(ut.needle) {
			t.Logf("needle is not valid UTF8; skipping this test")
			return
		}
		enc := ts.encode(ut.needle)

		var ctx bctestContext
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], padNBytes(enc, 4))
		ctx.setScalarStrings(fill16(ut.data), padding)
		ctx.current = 0xFFFF

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			if obsLane != ut.expLane {
				t.Errorf("%v\nlane %v: comparing needle %q to data %q: observed %v expected %v (data %v; needle %v (enc %v))",
					ts.name, i, ut.needle, ut.data, obsLane, ut.expLane, []byte(ut.data), []byte(ut.needle), []byte(enc))
				break
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestStringCompareBF brute-force tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompareBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate words
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// portable reference implementation: f(data, needle) -> lane
		refImpl func(string, string) bool
		// bytecode implementation of comparison
		op bcop
		// encoder for string literals -> dictionary value
		encode func(string) string
	}
	testSuites := []testSuite{
		{
			// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
			// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
			// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
			name:         "compare string case-sensitive (opCmpStrEqCs)",
			dataAlphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω'},
			dataLenSpace: []int{1, 2, 3, 4},
			dataMaxSize:  exhaustive,
			refImpl:      func(x, y string) bool { return x == y },
			op:           opCmpStrEqCs,
			encode:       func(x string) string { return x },
		},
		{
			name:         "compare string case-insensitive (opCmpStrEqCi)",
			dataAlphabet: []rune{'a', 'b', 'c', 'd', 'A', 'B', 'C', 'D', 'z', '!', '@'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			dataMaxSize:  1000,
			refImpl: func(x, y string) bool {
				return stringext.NormalizeStringASCIIOnly(x) == stringext.NormalizeStringASCIIOnly(y)
			},
			op:     opCmpStrEqCi,
			encode: stringext.NormalizeStringASCIIOnly, // only normalize ASCII values, leave other values (UTF8) unchanged
		},
		{
			name:         "compare string case-insensitive UTF8 (opCmpStrEqUTF8Ci)",
			dataAlphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K'},
			dataLenSpace: []int{1, 2, 3, 4},
			dataMaxSize:  exhaustive,
			refImpl:      strings.EqualFold,
			op:           opCmpStrEqUTF8Ci,
			encode:       func(x string) string { return stringext.GenNeedleExt(x, false) },
		},
		{ // test to explicitly check that byte length changing normalizations work
			name:         "compare string case-insensitive UTF8 (opCmpStrEqUTF8Ci) 2",
			dataAlphabet: []rune{'a', 'Ω', 'Ω'}, // U+2126 'Ω' (E2 84 A6 = 226 132 166) -> U+03A9 'Ω' (CE A9 = 207 137)
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			refImpl:      strings.EqualFold,
			op:           opCmpStrEqUTF8Ci,
			encode:       func(x string) string { return stringext.GenNeedleExt(x, false) },
		},
	}

	var padding []byte // empty padding

	run := func(ts *testSuite, dataSpace []string) {
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		check := func(needle string, group []string) {
			enc := ts.encode(needle)

			ctx.dict = append(ctx.dict[:0], padNBytes(enc, 4))
			ctx.setScalarStrings(group, padding)
			ctx.current = (1 << len(group)) - 1
			expected16 := uint16(0)
			for i := range group {
				if ts.refImpl(needle, group[i]) {
					expected16 |= 1 << i
				}
			}
			// when
			if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
				t.Error(err)
			}
			// then
			if ctx.current != expected16 {
				for i := range group {
					observed := getBit(ctx.current, i)
					expected := getBit(expected16, i)
					if observed != expected {
						t.Errorf("%v\nlane %v: comparing needle %q to data %q: observed %v expected %v (data %v; needle %v (enc %v))",
							ts.name, i, needle, group[i], observed, expected, []byte(group[i]), []byte(needle), []byte(enc))
						break
					}
				}
			}
		}

		//TODO make a generic space partitioner that can be reused by other BF tests

		var group []string

		for _, data1 := range dataSpace {
			group = group[:0]
			for _, data2 := range dataSpace {
				group = append(group, data2)
				if len(group) < 16 {
					continue
				}
				check(data1, group)
				group = group[:0]
			}
			if len(group) > 0 {
				check(data1, group)
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzStringCompareFT fuzz tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func FuzzStringCompareFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "s", "ss", "S", "SS", "ſ", "ſſ", "a", "aa", "as", "bss", "cS", "dSS", "eſ", "fſſ", "ga", "haa", "s")

	type testSuite struct {
		// name to describe this test-suite
		name string
		// portable comparison function
		refImpl func(string, string) bool
		// bytecode to run
		op bcop
		// encoder for string literals -> dictionary value
		encode func(string) string
	}

	testSuites := []testSuite{
		{
			name:    "compare string case-sensitive (opCmpStrEqCs)",
			refImpl: func(x, y string) bool { return x == y },
			op:      opCmpStrEqCs,
			encode:  func(x string) string { return x },
		},
		{
			name: "compare string case-insensitive (opCmpStrEqCi)",
			refImpl: func(x, y string) bool {
				return stringext.NormalizeStringASCIIOnly(x) == stringext.NormalizeStringASCIIOnly(y)
			},
			op:     opCmpStrEqCi,
			encode: stringext.NormalizeStringASCIIOnly, // only normalize ASCII values, leave other values (UTF8) unchanged
		},
		{
			name:    "compare string case-insensitive UTF8 (opCmpStrEqUTF8Ci)",
			refImpl: strings.EqualFold,
			op:      opCmpStrEqUTF8Ci,
			encode:  func(x string) string { return stringext.GenNeedleExt(x, false) },
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, needle string) {
		if !utf8.ValidString(needle) || (needle == "") {
			return // invalid needles are ignored
		}
		expLanes := uint16(0)
		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				expLanes = setBit(expLanes, i, ts.refImpl(data[i], needle))
			}
		}
		enc := ts.encode(needle)

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], padNBytes(enc, 4))
		ctx.setScalarStrings(data[:], []byte{})
		ctx.current = lanes

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		if ctx.current != expLanes {
			for i := 0; i < 16; i++ {
				obsLane := getBit(ctx.current, i)
				expLane := getBit(expLanes, i)

				if obsLane != expLane {
					t.Fatalf("%v\nlane %v: comparing needle %q to data %q: observed %v expected %v (data %v; needle %v (enc %v))",
						ts.name, i, needle, data, obsLane, expLane, []byte(data[i]), []byte(needle), []byte(enc))
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data, needle)
		}
	})
}

// TestMatchpatRefBF brute-force tests for: the reference implementation of matchpat (matchPatternReference) with a much slower regex impl
func TestMatchpatRefBF(t *testing.T) {
	t.Parallel()
	dataLenSpace := []int{1, 2, 3, 4}
	dataSpace := createSpaceExhaustive(dataLenSpace, []rune{'a', 'b', 's', 'ſ'})
	patternLenSpace := []int{1, 2, 3, 4, 5, 6}
	patternSpace := createSpacePatternRandom(patternLenSpace, []rune{'s', 'S', 'k', 'K'}, 500)

	// matchPatternRegex matches the first occurrence of the provided pattern similar to matchPatternReference
	// matchPatternRegex implementation is the refImpl for the matchPatternReference implementation.
	// the regex impl is about 10x slower and does not return expected value registers (offset and length)
	matchPatternRegex := func(msg []byte, offset, length int, segments []string, caseSensitive bool) (laneOut bool) {
		regex := stringext.PatternToRegex(segments, caseSensitive)
		r, err := regexp.Compile(regex)
		if err != nil {
			t.Errorf("Could not compile regex %v", regex)
		}
		loc := r.FindIndex(stringext.ExtractFromMsg(msg, offset, length))
		return loc != nil
	}

	for _, caseSensitive := range []bool{false, true} {
		for _, pattern := range patternSpace {
			segments := stringext.PatternToSegments([]byte(pattern))
			for _, data := range dataSpace {
				wantMatch := matchPatternRegex([]byte(data), 0, len(data), segments, caseSensitive)
				obsMatch, obsOffset, obsLength := matchPatternReference([]byte(data), 0, len(data), segments, caseSensitive)
				if wantMatch != obsMatch {
					t.Fatalf("matching data %q to pattern %q = %v (case-sensitive %v): observed %v (offset %v; length %v); expected %v",
						data, pattern, []byte(pattern), caseSensitive, obsMatch, obsOffset, obsLength, wantMatch)
				}
			}
		}
	}
}

// TestMatchpatBF1 brute-force tests 1 for: opMatchpatCs, opMatchpatCi, opMatchpatUTF8Ci
func TestMatchpatBF1(t *testing.T) {
	t.Parallel()

	//FIXME opMatchpatUTF8Ci only seems to work when padding is not empty
	padding := []byte{0x0}

	type testSuite struct {
		// name to describe this test-suite
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet, patternAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, patternLenSpace []int
		// portable reference implementation: f(data, dictval) -> match, offset, length
		refImpl func([]byte, []string) (bool, int, int)
		// bytecode implementation of comparison
		op bcop
		// encoder for segments -> dictionary value
		encode func(segments []string) []byte
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

	testSuites := []testSuite{
		{
			name:            "opMatchpatCs",
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataLenSpace:    []int{1, 2, 3, 4},
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternLenSpace: []int{1, 2, 3, 4, 5},
			refImpl: func(data []byte, segments []string) (match bool, offset, length int) {
				return matchPatternReference(data, 0, len(data), segments, true)
			},
			op:     opMatchpatCs,
			encode: stringext.SegmentsToPattern,
			evalEq: eqfunc1,
		},
		// FIXME currently disabled due to a bug
		/*
			{
				name:            "opMatchpatCi",
				dataAlphabet:    []rune{'s', 'S', 'ſ', 'k'},
				dataLenSpace:    []int{1, 2, 3, 4},
				patternAlphabet: []rune{'s', 'S', 'k', 'K'},
				patternLenSpace: []int{1, 2, 3, 4, 5},
				refImpl: func(data []byte, segments []string) (match bool, offset, length int) {
					return matchPatternReference(data, 0, len(data), segments, false)
				},
				op:     opMatchpatCi,
				encode: stringext.SegmentsToPattern,
				evalEq: eqfunc1,
			},
		*/
		{
			name:            "opMatchpatUTF8Ci",
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataLenSpace:    []int{1, 2, 3, 4},
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternLenSpace: []int{1, 2, 3, 4, 5},
			refImpl: func(data []byte, segments []string) (match bool, offset, length int) {
				return matchPatternReference(data, 0, len(data), segments, false)
			},
			op:     opMatchpatUTF8Ci,
			encode: stringext.GenPatternExt,
			evalEq: eqfunc1,
		},
	}

	var ctx bctestContext
	defer ctx.Free()
	run := func(segments, data []string, tc *testSuite) {
		dictval := string(tc.encode(segments))
		ctx.dict = append(ctx.dict[:0], padNBytes(dictval, 4))
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
			wantLane, wantOffset, wantLength := tc.refImpl([]byte(data[i]), segments)
			obsLane := ctx.current&(1<<i) != 0
			obsOffset := scalarAfter[0][i] - scalarBefore[0][i] // NOTE the reference implementation returns offset starting from zero
			obsLength := scalarAfter[1][i]

			if !tc.evalEq(wantLane, wantOffset, wantLength, obsLane, obsOffset, obsLength) {
				t.Fatalf("matching data %q to pattern %q = %v: observed %v (offset %v; length %v); expected %v (offset %v; length %v)",
					data[i], dictval, []byte(dictval), obsLane, obsOffset, obsLength, wantLane, wantOffset, wantLength)
			}
		}
	}

	const lanes = 16
	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			var group []string
			dataSpace := createSpaceExhaustive(ts.dataLenSpace, ts.dataAlphabet)
			patternSpace := createSpacePatternRandom(ts.patternLenSpace, ts.patternAlphabet, 1000)
			for _, pattern := range patternSpace {
				group = group[:0]
				segments := stringext.PatternToSegments([]byte(pattern))
				for _, data := range dataSpace {
					group = append(group, data)
					if len(group) < lanes {
						continue
					}
					run(segments, group, &ts)
					group = group[:0]
				}
				if len(group) > 0 {
					run(segments, group, &ts)
				}
			}
		})
	}
}

// TestMatchpatBF2 brute-force tests 2 for: opMatchpatCs
func TestMatchpatBF2(t *testing.T) {
	t.Parallel()
	info := "match-pattern case-sensitive (opMatchpatCs) BF2"
	dataLenSpace := []int{1, 2, 3, 4, 5, 6}
	dataSpace := createSpaceExhaustive(dataLenSpace, []rune{'a', 'b', 'c', 's', 'ſ'})

	testCases := []struct {
		segments []string
	}{
		{[]string{"a"}},
		{[]string{"ab"}},
		{[]string{"a", "b"}},

		{[]string{"aa", "b"}},
		{[]string{"aaa", "b"}},
		{[]string{"aaaa", "b"}},
		{[]string{"aaaaa", "b"}},

		{[]string{"aa", "b"}},
		{[]string{"aaa", "b"}},
		{[]string{"aaaa", "b"}},
		{[]string{"aaaaa", "b"}},

		{[]string{"a", "bc"}},
		{[]string{"aa", "bc"}},
		{[]string{"aaa", "bc"}},
		{[]string{"aaaa", "bc"}},

		{[]string{"aa", "bc"}},
		{[]string{"aaa", "bc"}},
		{[]string{"aaaa", "bc"}},

		{[]string{"a", "", "b"}},
		{[]string{"a", "", "", "b"}},

		{[]string{"3", "", "1"}},
		{[]string{"h"}},
		{[]string{"11", "3"}},
		{[]string{"ſ"}},
		{[]string{"ſΩ"}},
	}
	type validateFun2 = func(data []byte, segments []string, ctx *bctestContext) (correct bool)

	//uT unitTest structure
	type unitTest struct {
		msg          []byte // data pointed to by SI
		dictValue    []byte // dictValue of the pattern: need to be encoded and passed as string constant via the immediate dictionary
		resultLane   bool   // resulting lanes K1
		resultOffset uint32 // resulting offset Z2
		resultLength uint32 // resulting length Z3
	}

	toString := func(ut *unitTest) string {
		return fmt.Sprintf("lane=%v; offset=%v; length=%v", ut.resultLane, ut.resultOffset, ut.resultLength)
	}

	equal := func(ut1, ut2 *unitTest) bool {
		if ut1.resultLane != ut2.resultLane {
			return false
		}
		if ut1.resultLane {
			return (ut1.resultOffset == ut2.resultOffset) && (ut1.resultLength == ut2.resultLength)
		}
		return true
	}

	// createUData creates unit-test data from the provided data
	createUData := func(lane bool, offset, length int) unitTest {
		return unitTest{
			resultLane:   lane,
			resultOffset: uint32(offset),
			resultLength: uint32(length),
		}
	}

	// createUDataCtx creates unit-test data from the provided bctestContext
	createUDataCtx := func(ctx *bctestContext) unitTest {
		return unitTest{
			resultLane:   (ctx.current & 1) == 1,
			resultOffset: uint32(ctx.scalar[0][0] & 0xFFFFF),
			resultLength: uint32(ctx.scalar[1][0] & 0xFFFFF),
		}
	}

	logError := func(equal bool, info string, expected, observed *unitTest) {
		if !equal {
			dictionaryStr := fmt.Sprintf("'%v' = %v", string(expected.dictValue), expected.dictValue)
			if expected.resultLane == observed.resultLane {
				t.Logf("for %v: comparing data %q with dictionary value %v:\nexpected: %v\nobserved: %v",
					info, string(expected.msg), dictionaryStr, toString(expected), toString(observed))
			} else {
				t.Logf("for %v: comparing data %q with dictionary value %v:\nexpected: lane=%v\nobserved: lane=%v",
					info, string(expected.msg), dictionaryStr, expected.resultLane, observed.resultLane)
			}
		}
	}

	// checkSpace2 tests all combinations of elements in dataSpace and all elements of dictionarySpace
	checkSpace2 := func(dataSpace []string, segmentsSpace [][]string, op bcop, valFun2 validateFun2) {
		for _, dataStr := range dataSpace {
			data := []byte(dataStr)
			values := fill16(dataStr)
			for _, segments := range segmentsSpace {
				dictElementEnc := string(stringext.SegmentsToPattern(segments))

				var ctx bctestContext
				ctx.Taint()
				ctx.dict = append(ctx.dict, padNBytes(dictElementEnc, 4))
				ctx.setScalarStrings(values, []byte{})
				ctx.current = 0xFFFF

				if err := ctx.ExecuteImm2(op, 0); err != nil {
					t.Error(err)
				}
				if !valFun2(data, segments, &ctx) {
					t.FailNow()
				}
				ctx.Free()
			}
		}
	}

	valFunc2 := func(data []byte, segments []string, ctx *bctestContext) (correct bool) {
		expected := createUData(matchPatternReference(data, 0, len(data), segments, true))
		observed := createUDataCtx(ctx)
		correct = equal(&observed, &expected)
		logError(correct, info, &expected, &observed)
		return
	}
	for i, expected := range testCases {
		t.Run(fmt.Sprintf(`case %d`, i), func(t *testing.T) {
			segmentsSpace := [][]string{expected.segments}
			checkSpace2(dataSpace, segmentsSpace, opMatchpatCs, valFunc2)
		})
	}
}

// TestMatchpatUT unit-tests for: opMatchpatCs, opMatchpatCi, opMatchpatUTF8Ci
func TestMatchpatUT(t *testing.T) {
	t.Parallel()

	//FIXME opMatchpatUTF8Ci only seems to work when padding is not empty
	padding := []byte{0x0}

	type unitTest struct {
		msg          []byte   // data pointed to by SI
		segments     []string // segments of the pattern: needs to be encoded and passed as string constant via the immediate dictionary
		resultLane   bool     // resulting lanes K1
		resultOffset uint32   // resulting offset Z2
		resultLength uint32   // resulting length Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// portable reference implementation: f(data, dictval) -> match, offset, length
		refImpl func(data []byte, segments []string) unitTest
		// bytecode implementation of comparison
		op bcop
		// encoder for segments -> dictionary value
		encode func(segments []string) []byte
	}

	equal := func(ut1, ut2 *unitTest) bool {
		if ut1.resultLane != ut2.resultLane {
			return false
		}
		if ut1.resultLane {
			return (ut1.resultOffset == ut2.resultOffset) && (ut1.resultLength == ut2.resultLength)
		}
		return true
	}

	// createUT creates unit-test data from the provided data
	createUT := func(lane bool, offset, length int) (uData unitTest) {
		uData.resultLane = lane
		uData.resultOffset = uint32(offset)
		uData.resultLength = uint32(length)
		return
	}

	logError := func(equal bool, info string, expected, observed *unitTest) {
		toString := func(unit *unitTest) string {
			return fmt.Sprintf("lane=%v; offset=%v; length=%v", unit.resultLane, unit.resultOffset, unit.resultLength)
		}
		if !equal {
			patternEnc := stringext.SegmentsToPattern(expected.segments)
			segmentsStr := stringext.PatternToPrettyString(patternEnc, 3)
			dictionaryStr := fmt.Sprintf("'%v' = %v", string(patternEnc), segmentsStr)
			if expected.resultLane == observed.resultLane {
				t.Logf("for %v: comparing data %q with dictionary value %v:\nexpected: %v\nobserved: %v",
					info, string(expected.msg), dictionaryStr, toString(expected), toString(observed))
			} else {
				t.Logf("for %v: comparing data %q with dictionary value %v:\nexpected: lane=%v\nobserved: lane=%v",
					info, string(expected.msg), dictionaryStr, expected.resultLane, observed.resultLane)
			}
		}
	}

	run := func(ts *testSuite, expected *unitTest) {
		data := string(expected.msg)
		values := fill16(data)

		var ctx bctestContext
		ctx.Taint()
		ctx.dict = append(ctx.dict, padNBytes(string(ts.encode(expected.segments)), 4))
		ctx.setScalarStrings(values, padding)
		ctx.current = 0xFFFF

		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		observed1 := ts.refImpl(expected.msg, expected.segments)
		equal1 := equal(expected, &observed1)
		logError(equal1, "refImpl: "+ts.name, expected, &observed1)

		observed2 := unitTest{
			msg:          nil,
			segments:     nil,
			resultLane:   (ctx.current & 1) == 1,
			resultOffset: uint32(ctx.scalar[0][0] & 0xFFFFF),
			resultLength: uint32(ctx.scalar[1][0] & 0xFFFFF),
		}

		equal2 := equal(expected, &observed2)
		logError(equal2, "skylakeX: "+ts.name, expected, &observed2)

		correct := equal1 && equal2
		if !correct {
			t.Fail()
		}
		ctx.Free()
	}

	testSuites := []testSuite{
		{
			name: "match-pattern case-sensitive (opMatchpatCs)",
			op:   opMatchpatCs,
			refImpl: func(data []byte, segments []string) unitTest {
				return createUT(matchPatternReference(data, 0, len(data), segments, true))
			},
			encode: stringext.SegmentsToPattern,
			unitTests: []unitTest{
				{[]byte("aa"), []string{"s", ""}, false, 0, 2},
				{[]byte("a"), []string{"ab"}, false, 0, 0},
				{[]byte("a"), []string{"a"}, true, 1, 0},

				{stringext.AddTail("ab", "-----"), []string{"a"}, true, 1, 1},
				{stringext.AddTail("ab", "-----"), []string{"ab"}, true, 2, 0},
				{stringext.AddTail("a-b", "-----"), []string{"a"}, true, 1, 2},
				{stringext.AddTail("a-b", "-----"), []string{"a", "b"}, true, 3, 0},
				{stringext.AddTail("a-b-", "-----"), []string{"a", "b"}, true, 3, 1},
				{stringext.AddTail("a-b", "-----"), []string{"ab"}, false, 3, 0},
				{stringext.AddTail("-ab", "-----"), []string{"a"}, true, 2, 1},

				// reading beyond the buffer issues:
				{stringext.AddTail("a", "b---"), []string{"a", "b"}, false, 1, 0},
				{stringext.AddTail("aa", "b---"), []string{"aa", "b"}, false, 2, 0},
				{stringext.AddTail("aaa", "b---"), []string{"aaa", "b"}, false, 3, 0},
				{stringext.AddTail("aaaa", "b---"), []string{"aaaa", "b"}, false, 4, 0},
				{stringext.AddTail("aaaaa", "b---"), []string{"aaaaa", "b"}, false, 5, 0},

				{stringext.AddTail("a", "b---"), []string{"ab"}, false, 1, 0},
				{stringext.AddTail("aa", "b---"), []string{"ab"}, false, 2, 0},
				{stringext.AddTail("aaa", "b---"), []string{"ab"}, false, 3, 0},
				{stringext.AddTail("aaaa", "b---"), []string{"ab"}, false, 4, 0},
				{stringext.AddTail("aaaaa", "b---"), []string{"ab"}, false, 5, 0},

				{stringext.AddTail("a", "-b---"), []string{"a", "b"}, false, 1, 0},
				{stringext.AddTail("aa", "-b---"), []string{"aa", "b"}, false, 2, 0},
				{stringext.AddTail("aaa", "-b---"), []string{"aaa", "b"}, false, 3, 0},
				{stringext.AddTail("aaaa", "-b---"), []string{"aaaa", "b"}, false, 4, 0},
				{stringext.AddTail("aaaaa", "-b---"), []string{"aaaaa", "b"}, false, 5, 0},

				{stringext.AddTail("a-b", "c----"), []string{"a", "bc"}, false, 3, 0},
				{stringext.AddTail("aa-b", "c----"), []string{"aa", "bc"}, false, 4, 0},
				{stringext.AddTail("aaa-b", "c----"), []string{"aaa", "bc"}, false, 5, 0},
				{stringext.AddTail("aaaa-b", "c----"), []string{"aaaa", "bc"}, false, 6, 0},

				{stringext.AddTail("a-", "bc----"), []string{"a", "bc"}, false, 2, 0},
				{stringext.AddTail("aa-", "bc----"), []string{"aa", "bc"}, false, 3, 0},
				{stringext.AddTail("aaa-", "bc----"), []string{"aaa", "bc"}, false, 4, 0},
				{stringext.AddTail("aaaa-", "bc----"), []string{"aaaa", "bc"}, false, 5, 0},

				{stringext.AddTail("a--b", "----"), []string{"a", "", "b"}, true, 4, 0},

				{stringext.AddTail("a--", "b---"), []string{"a", "", "", "b"}, false, 3, 0},
				{stringext.AddTail("-a--", "b---"), []string{"a", "", "", "b"}, false, 4, 0},
				{stringext.AddTail("--a--", "b---"), []string{"a", "", "", "b"}, false, 5, 0},
				{stringext.AddTail("---a--", "b---"), []string{"a", "", "", "b"}, false, 6, 0},

				{stringext.AddTail("a-", "-b---"), []string{"a", "", "", "b"}, false, 2, 0},
				{stringext.AddTail("-a-", "-b---"), []string{"a", "", "", "b"}, false, 3, 0},
				{stringext.AddTail("--a-", "-b---"), []string{"a", "", "", "b"}, false, 4, 0},
				{stringext.AddTail("---a-", "-b---"), []string{"a", "", "", "b"}, false, 5, 0},

				{stringext.AddTail("a", "--b---"), []string{"a", "", "", "b"}, false, 1, 0},
				{stringext.AddTail("-a", "--b---"), []string{"a", "", "", "b"}, false, 2, 0},
				{stringext.AddTail("--a", "--b---"), []string{"a", "", "", "b"}, false, 3, 0},
				{stringext.AddTail("---a", "--b---"), []string{"a", "", "", "b"}, false, 4, 0},

				{[]byte("__ab_c_d__"), []string{"ab", "c", "d"}, true, 8, 2},
				{[]byte("__ab_z_d__"), []string{"ab", "c", "d"}, false, 10, 0},
				{[]byte("__ab__c_d__"), []string{"ab", "c", "d"}, false, 11, 0},
				{[]byte("a---b"), []string{"a", "b"}, false, 5, 0},
				{[]byte("__ab_ab_c_d__"), []string{"ab", "c", "d"}, true, 11, 2},
				{[]byte("__ab_ab_z_d__"), []string{"ab", "c", "d"}, false, 13, 0}, // incorrect char z
				{[]byte("a_c"), []string{"a", "", "c"}, false, 3, 0},              // one char too few between b and c
				{[]byte("__ab_c_d__"), []string{"ab", "z", "d"}, false, 10, 0},

				//NOTE skipchar is not allowed at the beginning or ending, thus these tests fail. Keeping them for reminder.
				//{[]byte("aabb"), []string{"", "a"}, true},// initial skipchar is not allowed
				//{[]byte("aaaa"), []string{"a", ""}, true},// final skipchar is not allowed

				{ // bugfix: 21A93561 JGE -> JG
					msg:          stringext.AddTail("aaaa", "-b"),
					segments:     []string{"aaaa", "b"},
					resultLane:   false,
					resultOffset: 4,
					resultLength: 0,
				},

				{[]byte("33--1"), []string{"3", "", "1"}, true, 5, 0},
				{[]byte("€𐍈€¢"), []string{"h"}, false, 12, 0},
				{[]byte("111-3"), []string{"11", "3"}, true, 5, 0},
				{[]byte("$𐍈"), []string{"𐍈€¢"}, false, 5, 0},
				{[]byte("ac¢A"), []string{"a"}, true, 1, 4},

				{[]byte("a-b"), []string{"a", "b"}, true, 3, 0},
				{[]byte("a¢b"), []string{"a", "b"}, true, 4, 0},

				{[]byte("1a-b"), []string{"a", "b"}, true, 4, 0},
				{[]byte("¢a-b"), []string{"a", "b"}, true, 5, 0},

				{[]byte("ĳsh"), []string{"ĳ"}, true, 2, 2},
			},
		},
		{
			name: "match-pattern case-insensitive (opMatchpatCi)",
			op:   opMatchpatCi,
			refImpl: func(data []byte, segments []string) unitTest {
				return createUT(matchPatternReference(data, 0, len(data), segments, false))
			},
			encode: stringext.SegmentsToPattern,
			unitTests: []unitTest{
				//FIXME next ut seems to trigger a bug, but this issue does not exists say 1 year ago
				//{[]byte("a"), []string{"a"}, true, 1, 0},
				{[]byte("a"), []string{"A"}, true, 1, 0},
				{[]byte("11-2"), []string{"1", "2"}, true, 4, 0},
				{[]byte("€€-𐍈"), []string{"€", "𐍈"}, true, 11, 0},
				//FIXME {[]byte("ksss"), []string{"kssS"}[, true, 4, 0},
			},
		},
		{
			name: "match-pattern case-insensitive UTF8 (opMatchpatUTF8Ci)",
			op:   opMatchpatUTF8Ci,
			refImpl: func(data []byte, segments []string) unitTest {
				return createUT(matchPatternReference(data, 0, len(data), segments, false))
			},
			encode: stringext.GenPatternExt,
			unitTests: []unitTest{
				//TODO the next test only succeeds when there is padding between the messages, the test code adds some but that should not be
				{[]byte("s"), []string{"Ss"}, false, 1, 0},
				//TODO the next test triggers a bug
				//{[]byte("s"), []string{"sxs"), false, 1, 0},

				{[]byte("c-d"), []string{"c"}, true, 1, 2},
				{[]byte("cc-d"), []string{"cc"}, true, 2, 2},

				{[]byte("bb-cc-d"), []string{"bb", "cc"}, true, 5, 2},
				{[]byte("aa-bb-cc-d"), []string{"aa", "bb", "cc"}, true, 8, 2},
				{[]byte("aa-bb-ce-d"), []string{"aa", "bb", "cc"}, false, 0, 0},

				{[]byte("aaaaa"), []string{"ab"}, false, 0, 0},
				{[]byte("aa"), []string{"ab"}, false, 0, 0},
				{[]byte("a"), []string{"ab"}, false, 0, 0},

				{[]byte("a"), []string{"a"}, true, 1, 0},
				{[]byte("€"), []string{"€"}, true, 3, 0},
				{[]byte("ba"), []string{"a"}, true, 2, 0},
				{[]byte("b€"), []string{"€"}, true, 4, 0},

				{[]byte("a"), []string{"A"}, true, 1, 0},
				{[]byte("1-2"), []string{"1", "2"}, true, 3, 0},
				{[]byte("11-2"), []string{"1", "2"}, true, 4, 0},

				{[]byte("€-𐍈"), []string{"€", "𐍈"}, true, 8, 0},
				{[]byte("€€-𐍈"), []string{"€", "𐍈"}, true, 11, 0},
				{[]byte("€1€2"), []string{"1", "2"}, true, 8, 0},
				{[]byte("€1€2€"), []string{"1", "2"}, true, 8, 3},

				// regular non-trivial normalization ĳ -> Ĳ
				// hard code-points with non-trivial normalization with different byte length encodings
				{[]byte("ĳsh"), []string{"ĳ"}, true, 2, 2},
				{[]byte("ĳsh"), []string{"Ĳ"}, true, 2, 2},
				{[]byte("1ĳ1ĳ1"), []string{"Ĳ", "Ĳ"}, true, 6, 1},

				// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
				// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
				// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)

				{[]byte("ſ"), []string{"S"}, true, 2, 0},
				{[]byte("Ω"), []string{"Ω"}, true, 3, 0},
				{[]byte("K"), []string{"K"}, true, 3, 0},

				{[]byte("S"), []string{"ſ"}, true, 1, 0},
				{[]byte("Ω"), []string{"Ω"}, true, 2, 0},
				{[]byte("K"), []string{"K"}, true, 1, 0},

				{[]byte("1ſ1ĳ1"), []string{"S", "Ĳ"}, true, 6, 1},
				{[]byte("1Ω1ĳ1"), []string{"Ω", "Ĳ"}, true, 7, 1},
				{[]byte("1K1ĳ1"), []string{"K", "Ĳ"}, true, 7, 1},
			},
		},
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestRegexMatchBF1 brute-force tests 1 for: opDfaT6, opDfaT6Z, opDfaT7, opDfaT7Z, opDfaT8, opDfaT8Z, pDfaLZ
func TestRegexMatchBF1(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet, regexAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// max length of the regex made of alphabet
		regexMaxlen int
		// maximum number of elements in dataSpace; -1 means infinite
		dataMaxSize int
		// type of regex to test: can be regexp2.Regexp or regexp2.SimilarTo
		regexType regexp2.RegexType
	}
	testSuites := []testSuite{
		{
			name:          "SimilarTo with RLZ",
			dataAlphabet:  []rune{'a', 'b', 'Ω'}, // U+2126 'Ω' (3 bytes)
			dataLenSpace:  []int{1, 2, 3, 4},
			dataMaxSize:   exhaustive,
			regexAlphabet: []rune{'a', '_', '%', 'Ω'},
			regexMaxlen:   5,
			regexType:     regexp2.SimilarTo,
		},
		{
			name:          "Regexp with UTF8",
			dataAlphabet:  []rune{'a', 'b', 'c', 'Ω'}, // U+2126 'Ω' (3 bytes)
			dataLenSpace:  []int{1, 2, 3, 4},
			dataMaxSize:   exhaustive,
			regexAlphabet: []rune{'a', '.', '*', '|', 'Ω'},
			regexMaxlen:   4,
			regexType:     regexp2.Regexp,
		},
		{
			name:          "Regexp with NewLine",
			dataAlphabet:  []rune{'a', 'b', 'c', 0x0A}, // 0x0A = newline
			dataLenSpace:  []int{1, 2, 3, 4},
			dataMaxSize:   exhaustive,
			regexAlphabet: []rune{'a', '.', '*', '|', 0x0A},
			regexMaxlen:   4,
			regexType:     regexp2.Regexp,
		},
		{
			name:          "SimilarTo with UTF8",
			dataAlphabet:  []rune{'a', 'b', 'Ω'}, // U+2126 'Ω' (3 bytes)
			dataLenSpace:  []int{1, 2, 3, 4},
			dataMaxSize:   exhaustive,
			regexAlphabet: []rune{'a', '%', 'Ω', '|'},
			regexMaxlen:   4,
			regexType:     regexp2.SimilarTo,
		},
		{
			name:          "SimilarTo with NewLine",
			dataAlphabet:  []rune{'a', 'b', 0x0A}, // 0x0A = newline
			dataLenSpace:  []int{1, 2, 3, 4},
			dataMaxSize:   exhaustive,
			regexAlphabet: []rune{'a', '%', '|', 0x0A},
			regexMaxlen:   4,
			regexType:     regexp2.SimilarTo,
		},
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			regexSpace := createSpaceRegex(ts.regexMaxlen, ts.regexAlphabet, ts.regexType)
			runRegexTests(t, ts.name, dataSpace, regexSpace, ts.regexType, false)
		})
	}
}

// TestRegexMatchBF2 brute-force tests 2 for: regexp2.Regexp and regexp2.SimilarTo
func TestRegexMatchBF2(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		// regex expression to test
		expr string
		// boolean to dump the data-structures to file
		writeDot bool
	}
	type testSuite struct {
		name string
		// the actual unit-test to run
		unitTests []unitTest
		// alphabet from which to generate needles
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace; -1 means infinite
		dataMaxSize int
		// type of regex to test: can be regexp2.Regexp or regexp2.SimilarTo
		regexType regexp2.RegexType
	}
	testSuites := []testSuite{
		{
			name:         "Regexp RLZ",
			regexType:    regexp2.Regexp,
			dataAlphabet: []rune{'a', 'b', 'c', 'Ω'},
			dataLenSpace: []int{1, 2, 3, 4},
			dataMaxSize:  exhaustive,
			unitTests: []unitTest{
				{expr: "^a$"},
				{expr: "^(a*(.|\n)aa)$"}, // DfaT6Z
				{expr: "^(a*.aa)$"},      // DfaT7Z
				{expr: "a$"},
				{expr: "a.a$"},
				{expr: "b.a$"},
				{expr: ".*a$"},
				{expr: "^.*aa$"},
				{expr: "a.*a$"},
				{expr: "^a.*b"},
				{expr: "^a.a"},
				{expr: "^(^.*a$|b)"},
				{expr: "^(a$|b)"},
				{expr: ".*ab"},
				{expr: ".*a.*b"},
				{expr: "^a"},
				{expr: "b.a"},
				{expr: "b..a"},
				{expr: `a|$`},
				{expr: `a|b$`},
				//FIXME {expr: "^.*a$|b"},    // investigate how to handle this
				//FIXME {expr: "(^a$)|(^b)"}, // make special state for start of line anchor
			},
		},
		{
			name:         "Regexp NoRLZ",
			regexType:    regexp2.Regexp,
			dataAlphabet: []rune{'a', 'b', 'c', 'd', '\n', 'Ω'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			unitTests: []unitTest{
				{expr: "||"},
				{expr: "^a"},
				// regex that is too long is not valid because it has too many chars
				{expr: `(.|\n)*(71009.$qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq\\x00\\x7fqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq0000A20)`},
				//automaton without flags
				{expr: `.*a.b`},
				{expr: `.*a.a`},
				{expr: `a*.b`},
				{expr: `a*.b*.c`},
				{expr: `a*.b*.c*.d`},
				{expr: `c*.*(aa|cd)`},
				{expr: `(c*b|.a)`},
				{expr: `.*b*.a`},
				{expr: `b*.a*.`},
				{expr: `b*..*b`},
				{expr: `a*..*a`},
				{expr: `..|aaaa`},
				{expr: `..|aa`},
				{expr: `.ba|aa`},
				{expr: `a*...`},
				{expr: `a*..`},
				{expr: `c*.*aa`},
				{expr: `.a|aaa`},
				{expr: `ab|.c`},
				{expr: `.*ab`},
				{expr: `a*..a`},
				{expr: `a*..b`},
				{expr: `a*.b`},
				{expr: `.*ab.*cd`},
			},
		},
		{
			name:         "SimilarTo NoRLZ",
			regexType:    regexp2.SimilarTo,
			dataAlphabet: []rune{'a', 'b', 'Ω'},
			dataLenSpace: []int{4},
			dataMaxSize:  exhaustive,
			unitTests: []unitTest{
				{expr: "%a"},
				{expr: `a*`},
				{expr: "aaa"},
				{expr: `(aa|b*)`},
				{expr: `ab|cd`},
				{expr: `%a_a`},
				{expr: `%a_b`},
				{expr: `a%b`},
				{expr: `a%b%c`},
				{expr: `a%b%c%d`},
				{expr: `c*%(aa|cd)`},
				{expr: `(c*b|_a)`},
				{expr: `c*b|_a`},
				{expr: `%b*_a`},
				{expr: `b*_a*_`},
				{expr: `b*_%b`},
				{expr: `a*_%a`},
				{expr: `__|aaaa`},
				{expr: `__|aa`},
				{expr: `_ba|aa`},
				{expr: `a*___`},
				{expr: `a*__`},
				{expr: `c*%aa`},
				{expr: `_a|aaa`},
				{expr: `ab|_c`},
				{expr: `%ab`},
				{expr: `a*__a`},
				{expr: `a*__b`},
				{expr: `a*_b`},
				{expr: `%ab%cd`},
			},
		},
		{
			name:         "Regexp with IP4",
			regexType:    regexp2.Regexp,
			dataAlphabet: []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'x', '.'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			dataMaxSize:  100000,
			unitTests: []unitTest{
				{expr: `^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$`},
				{expr: `^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`},
			},
		},
		{
			name:         "SimilarTo with IP4",
			regexType:    regexp2.SimilarTo,
			dataAlphabet: []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'x', '.'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			dataMaxSize:  100000,
			unitTests: []unitTest{
				{expr: `^(?:[0-9]{1,3}\.){3}[0-9]{1,3}`},
				{expr: `^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`},
			},
		},
	}
	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			for _, ut := range ts.unitTests {
				regexSpace := []string{ut.expr} // space with only one element
				runRegexTests(t, ts.name, dataSpace, regexSpace, ts.regexType, ut.writeDot)
			}
		})
	}
}

// TestRegexMatchUT1 unit-tests for: regexp2.Regexp and regexp2.SimilarTo
func TestRegexMatchUT1(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      string // data pointed to by SI
		expr      string // dictValue of the pattern: need to be encoded and passed as string constant via the immediate dictionary
		expLane   bool   // resulting lanes K1
		regexType regexp2.RegexType
	}

	const regexType = regexp2.Regexp
	unitTests := []unitTest{
		{"abaa", "%a_a", false, regexp2.SimilarTo},
		{"abbb", "%a_b", false, regexp2.SimilarTo},
		{`ab`, `a($|b)`, true, regexp2.Regexp},
		{"aΩa", "^a.a", true, regexp2.Regexp},  // DFA Tiny6 with wildcard
		{"baaa", "b..a", true, regexp2.Regexp}, // DFA Tiny7 with unicode wildcard
		{"baa", "b.a", true, regexp2.Regexp},   // DFA Tiny6 with unicode wildcard
		{"Ω", "_", true, regexp2.SimilarTo},

		{"aΩb", "^a.*b", true, regexp2.Regexp},   // DFA Tiny6 with wildcard
		{"Ωab", ".*ab", true, regexp2.Regexp},    // DFA Tiny6 with wildcard
		{"ΩaΩb", ".*a.*b", true, regexp2.Regexp}, // DFA Tiny6 with wildcard
		{"Ωb\nΩbc", "^a", false, regexp2.Regexp},

		{`a`, `$`, true, regexp2.Regexp},
		{`a`, `(a|)`, true, regexp2.SimilarTo},
		{`ab`, `(a|)($|c)`, true, regexp2.Regexp},
		{`ab`, `(a|$)($|c)`, true, regexp2.Regexp},
		{`a`, `a|$`, true, regexp2.Regexp},
		//FIXME{`b`, `a|$`, false, regexp2.Regexp},
		{`ab`, `a|$`, true, regexp2.Regexp},
		{"a", "|", true, regexp2.Regexp},
		{`a`, ``, false, regexp2.SimilarTo},
		{`a`, ``, true, regexp2.Regexp},
		{`a`, `^$`, false, regexp2.Regexp},
		{`a`, `^`, true, regexp2.Regexp},
		{`bb`, `(a|)`, true, regexp2.Regexp},

		//regex used for blog post
		{`0.0.000.0`, `^(?:[0-9]{1,3}\.){3}[0-9]{1,3}`, true, regexp2.Regexp},
		{`1.1.1.1`, `^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, true, regexp2.Regexp},

		{"a", "^a", true, regexp2.Regexp},
		{"ab", "ab", true, regexp2.SimilarTo},
		{"abc", "ab", false, regexp2.SimilarTo},
		{"a", "(a|b)", true, regexp2.SimilarTo},
		{"ab", "(a|b)", false, regexp2.SimilarTo},
		{"ba", "(a|b)", false, regexp2.SimilarTo},
		{"aaa", `__|aa`, false, regexp2.SimilarTo},
		{"aba", `ab|_c`, false, regexp2.SimilarTo},
		{"aaba", `%a_b`, false, regexp2.SimilarTo},
		{"xaxb", `.*a.b`, true, regexp2.Regexp},
		{`ab`, `a(b|$)($|c)`, true, regexp2.Regexp}, //two outgoing edges b and b$

		// NOTE: end-of-line anchor $, and begin-of-line anchor '^' are not meta-chars with SIMILAR TO
		{`a`, `.$`, true, regexp2.Regexp},
		{`a`, `a`, true, regexp2.Regexp},
		{`a`, `a$`, true, regexp2.Regexp},
		{`ab`, `a`, true, regexp2.Regexp},
		{`ab`, `a$`, false, regexp2.Regexp},
		{`b`, `a|b$`, true, regexp2.Regexp},

		{`ab`, `a($|b)`, true, regexp2.Regexp},
		{`ab`, `a($|b)($|c)`, true, regexp2.Regexp},
		{`abc`, `a($|b)($|c)`, true, regexp2.Regexp},
		{`abcx`, `a($|b)($|c)`, true, regexp2.Regexp},
		{`0a0`, `0.0$`, true, regexp2.Regexp},
		{`a\nb`, `a$`, false, regexp2.Regexp},
		{`ba`, `a$`, true, regexp2.Regexp},
		{`a\nx`, `a$`, false, regexp2.Regexp},

		// NOTE: in POSIX (?s) is the default
		{`a`, `(?s)a$`, true, regexp2.Regexp},
		{`ax`, `(?s)a$`, false, regexp2.Regexp},
		{"a\n", `(?s)a$`, false, regexp2.Regexp},
		{"a\n", `(?m)a$`, true, regexp2.Regexp},

		//FIXME{`a`, `$*`, false, regexp2.Regexp}, // not equal to postgres; fault: golang
		{`e`, `^(.*e$)`, true, regexp2.Regexp},

		// NOTE: \b will issue InstEmptyWidth with EmptyWordBoundary in golang NFA
		{`0`, `\b`, true, regexp2.Regexp}, // `\b` assert position at a word boundary
		{`0`, `\\B`, false, regexp2.Regexp},

		{"\nb", "(\x0A|\x0B)b|.a", true, regexp2.Regexp}, // range with \n
		{"\nb", ".a|((.|\n)b)", true, regexp2.Regexp},
		{"\na", ".a|((.|\n)b)", false, regexp2.Regexp},
		{`xa`, "\n|.|.a", true, regexp2.Regexp}, // merge newline with .
		{`xa`, "\n|.a", true, regexp2.Regexp},

		// not sure how to use ^\B not at ASCII word boundary
		{`abc`, `x\Babc`, false, regexp2.Regexp},
		{`0`, `.*0.......1`, false, regexp2.Regexp}, // combinatoric explosion of states

		{`200000`, `^(.*1|0)`, false, regexp2.Regexp},
		{`a`, `[^0-9]`, true, regexp2.Regexp},

		//IPv6
		{`2001:0db8:85a3:0000:0000:8a2e:0370:7334`, `(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))`, true, regexp2.Regexp},

		//url
		{`google.com`, `(?:[a-zA-Z0-9]{1,62}(?:[-\.][a-zA-Z0-9]{1,62})+)(:\d+)?`, true, regexp2.Regexp},

		//email address
		{`blah@gmail.com`, `[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}`, true, regexp2.Regexp},

		{`x`, `\D`, true, regexp2.Regexp},
		{`x`, `[^0-9]`, true, regexType},
		{`xx`, `..|0001`, true, regexType},

		// counted repetition: repetition count larger than 1000 is not allowed
		{`aab`, `a{1,1000}b`, true, regexType},

		// case insensitive flag (?i) default: false
		{`ſ`, `ſ`, true, regexp2.Regexp},
		{`aS`, `(?i)aſ`, true, regexp2.Regexp},
		{`as`, `(?i)aſ`, true, regexp2.Regexp},
		{`aſ`, `(?i)aſ`, true, regexp2.Regexp},
		{`aSv`, `(?i)aſ`, true, regexp2.Regexp},
		{`ASv`, `(?i)aſ`, true, regexp2.Regexp},
		{`asv`, `(?i)aſ`, true, regexp2.Regexp},
		{`aſv`, `(?i)aſ`, true, regexp2.Regexp},
		{`v`, `(?i)aſ`, false, regexp2.Regexp},

		// multi-line mode (?m) default: false. Multi-line mode only affects the behavior of ^ and $.
		// In the multiline mode they match not only at the beginning and the end of the string, but
		// also at start/end of line.
		{`xxab`, `ab$`, true, regexp2.Regexp},
		//FIXME{`a\nxb`, `(?m)a$.b`, true, regexp2.Regexp}, // TODO (?m) does not alter multiline behaviour

		// single-line mode (?s) let . match \n, default: false
		{"a\nb", `(?s)a.b`, true, regexp2.Regexp}, //not regexType: (?s) = dot all flag (thus including nl )
		{"a\nb", `a.b`, false, regexType},
		{"a\r\nb", `(?s)a.b`, false, regexp2.Regexp}, // Note: windows eol is not recognized

		// ungreedy (?U) swap meaning of x* and x*?, x+ and x+?, etc, default: false
		// SIMILAR TO: performs only matches and once the first accepting substring is found it returns true

		{`0`, `%001207890`, false, regexType},
		{`aaaaxbbbbxc`, `a*.b*.c`, true, regexType},
		{`cca`, `^(c*b|.a)`, false, regexType},
		{`\n`, `.`, true, regexp2.Regexp}, //. matches any character (except for line terminators)

		{`200000`, `.*1|0`, true, regexType},
		{`!\\`, `a`, false, regexType},

		{`0`, `00000000'7'`, false, regexType},
		//FIXME{`a`, `^a^`, false, regexType}, // TODO ^a^ should not match anything, but sneller incorrectly does

		{`Ա`, `\x00`, false, regexType},
		{`Ա`, `\x01`, false, regexType},

		{"\x00", "\x00", true, regexType},
		{``, "\x00", false, regexType},
		{`0`, "0\x01", false, regexType},
		{`0`, "0\x00", false, regexType},
		{`0`, `^$0`, false, regexp2.Regexp},
		{`b`, `.*aa`, false, regexp2.Regexp},
		{`ba`, `.*aa`, false, regexp2.Regexp},
		{`baa`, `.*aa`, true, regexp2.Regexp},
		{`0`, ".*\x00", false, regexp2.Regexp},
		{`ac`, `(.*ac)|(.*bc)`, true, regexp2.Regexp},
		{`xayb`, `%a_b`, true, regexp2.SimilarTo},
		{`acdx`, `(.*a.*c)|(.*cd)`, true, regexp2.Regexp},

		{`acd`, `(.*a.*cd)|(.*cd)`, true, regexp2.Regexp},
		{`cd`, `(.*a.*cd)|(.*cd)`, true, regexp2.Regexp},
		{`axcd`, `(.*a.*cd)|(.*cd)`, true, regexp2.Regexp},
		{`axacd`, `(.*a.*cd)|(.*cd)`, true, regexp2.Regexp},

		{`abcd`, `.*ab.*cd`, true, regexp2.Regexp},
		{`cd`, `.*ab.*cd`, false, regexp2.Regexp},
		{`aabccd`, `.*ab.*cd`, true, regexp2.Regexp},
		{`aabacd`, `.*ab.*cd`, true, regexp2.Regexp},
		{`xabxcd`, `.*ab.*cd`, true, regexp2.Regexp},

		{`abcd`, `(.*ab)*cd`, true, regexp2.Regexp},
		{`cd`, `(.*ab)*cd`, true, regexp2.Regexp},
		{`aabcd`, `(.*ab)*cd`, true, regexp2.Regexp},
		{`xabcd`, `(.*ab)*cd`, true, regexp2.Regexp},
		{`abxcd`, `(.*ab)*cd`, true, regexp2.Regexp},

		{`abcd`, `.*abcd`, true, regexp2.Regexp},
		{`xabcd`, `.*abcd`, true, regexp2.Regexp},
		{`xbcd`, `.*abcd`, false, regexp2.Regexp},
		{`aabcd`, `.*abcd`, true, regexp2.Regexp},  // backtrack from pos1
		{`abbcd`, `.*abcd`, false, regexp2.Regexp}, // backtrack from pos2
		{`ababcd`, `.*abcd`, true, regexp2.Regexp},
		{`abcbcd`, `.*abcd`, false, regexp2.Regexp},
		{`abccd`, `.*abcd`, false, regexp2.Regexp}, // backtrack from pos3

		{`ab`, `.*ab`, true, regexp2.Regexp},
		{`xab`, `.*ab`, true, regexp2.Regexp},
		{`xb`, `.*ab`, false, regexp2.Regexp},
		{`xab`, `.*ab`, true, regexp2.Regexp},
		{`Արամab`, `.*ab`, true, regexp2.Regexp}, //NOTE UTF8 only supported in Large

		{`aab`, `.*ab`, true, regexp2.Regexp}, // backtrack from pos1
		{`xaab`, `.ab`, true, regexp2.Regexp},
		{`xxab`, `.ab`, true, regexp2.Regexp},
		{`Աab`, `.*ab`, true, regexp2.Regexp}, //NOTE UTF8 only supported in Large
		{`aab`, `.ab`, true, regexp2.Regexp},
		{`ab`, `.ab`, false, regexp2.Regexp},
		{`xab`, `.ab`, true, regexp2.Regexp},

		{`xa`, `%_a`, true, regexp2.SimilarTo},
		{`aa`, `%_a`, true, regexp2.SimilarTo},
		{`a`, `%_a`, false, regexp2.SimilarTo},
		{`x`, `%_a`, false, regexp2.SimilarTo},
		{`Աa`, `%_a`, true, regexp2.SimilarTo}, //NOTE UTF8 only supported in Large

		{`ac`, `(a|b)+c`, true, regexType},
		{`bc`, `(a|b)+c`, true, regexType},
		{`abc`, `(a|b)+c`, true, regexType},

		{`Xab`, `X([a-c]+)b`, true, regexType},
		{`Xaby`, `X([a-c]+)b`, true, regexType},
		{`Xbb`, `X([a-c]+)b`, true, regexType},
		{`Xcb`, `X([a-c]+)b`, true, regexType},
		{`Xbb`, `X([a-c]+)b`, true, regexType},
		{`Xdb`, `X([a-c]+)b`, false, regexType},

		{`0.0.000.0`, `^(?:[0-9]{1,3}\.){3}[0-9]{1,3}`, true, regexp2.Regexp},
		{`1.1.1.1`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, true, regexp2.SimilarTo},
		{`255.255.255.255`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, true, regexp2.SimilarTo},
		{`999.999.999.999`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, true, regexp2.SimilarTo},
		{`1.1.1`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, false, regexp2.SimilarTo},
		{`1.1.1a`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, false, regexp2.SimilarTo},
		{`1.1.1.1a`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, false, regexp2.SimilarTo},
		{`10.1000.10.10`, `(?:[0-9]{1,3}.){3}[0-9]{1,3}`, false, regexp2.SimilarTo},

		{`1.1.1.1`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, true, regexp2.SimilarTo},
		{`255.255.255.255`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, true, regexp2.SimilarTo},
		{`1.1.1`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, false, regexp2.SimilarTo},
		{`1.1.1a`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, false, regexp2.SimilarTo},
		{`1.1.1.1a`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, false, regexp2.SimilarTo},
		{`10.1000.10.10`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, false, regexp2.SimilarTo},
		{`0.0.0.0`, `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`, true, regexp2.SimilarTo},
	}

	run := func(ut unitTest) {
		data := fill16(ut.data)

		ds, err := regexp2.CreateDs(ut.expr, ut.regexType, false, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Errorf("%v with %v", err, ut.expr)
		}
		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, info string, op bcop, expLane bool) {
			if dsByte == nil {
				return
			}
			ctx.dict = append(ctx.dict[:0], string(dsByte))
			ctx.setScalarStrings(data, []byte{})
			ctx.current = 0xFFFF

			// when
			if err := ctx.ExecuteImm2(op, 0); err != nil {
				t.Fatal(err)
			}

			expLanes := uint16(0)
			if expLane {
				expLanes = 0xFFFF
			}
			obsLanes := ctx.current
			if obsLanes != expLanes {
				for i := 0; i < 16; i++ {
					obsLane := getBit(obsLanes, i)
					if obsLane != expLane {
						t.Errorf("%v: lane %v: issue with data %q\nregexGolang=%q yields expected %v; regexSneller=%q yields observed %v",
							info, i, data, ds.RegexGolang.String(), expLane, ds.RegexSneller.String(), obsLane)
						break
					}
				}
			}
		}

		// first: check reference implementation
		{
			obsLane := ds.RegexGolang.MatchString(ut.data)
			if ut.expLane != obsLane {
				t.Errorf("refImpl: issue with data %q\nexpected %v while RegexGolang=%q yields observed %v",
					data, ut.expLane, ds.RegexGolang.String(), obsLane)
			}
		}
		// second: check the bytecode implementation
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		regexDataTest(&ctx, ds.DsT6, "DfaT6", opDfaT6, ut.expLane)
		regexDataTest(&ctx, ds.DsT6Z, "DfaT6Z", opDfaT6Z, ut.expLane)
		regexDataTest(&ctx, ds.DsT7, "DfaT7", opDfaT7, ut.expLane)
		regexDataTest(&ctx, ds.DsT7Z, "DfaT7Z", opDfaT7Z, ut.expLane)
		regexDataTest(&ctx, ds.DsT8, "DfaT8", opDfaT8, ut.expLane)
		regexDataTest(&ctx, ds.DsT8Z, "DfaT8Z", opDfaT8Z, ut.expLane)
		regexDataTest(&ctx, ds.DsLZ, "DfaLZ", opDfaLZ, ut.expLane)
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf(`case %d:`, i), func(t *testing.T) {
			run(ut)
		})
	}
}

// TestRegexMatchUT2 unit-tests for: regexp2.Regexp and regexp2.SimilarTo
func TestRegexMatchUT2(t *testing.T) {
	t.Parallel()
	name := "regex match UnitTest2"

	type unitTest struct {
		data      [16]string // data pointed to by SI
		expr      string     // dictValue of the pattern: need to be encoded and passed as string constant via the immediate dictionary
		expLanes  uint16     // resulting lanes K1
		regexType regexp2.RegexType
	}

	unitTests := []unitTest{
		{
			data:      [16]string{"baΩ\naa", "caΩ\naa", "\naΩ\naa", "ΩaΩ\naa", "abΩ\naa", "bbΩ\naa", "cbΩ\naa", "\nbΩ\naa", "ΩbΩ\naa", "acΩ\naa", "bcΩ\naa", "ccΩ\naa", "\ncΩ\naa", "ΩcΩ\naa", "a\nΩ\naa", "b\nΩ\naa"},
			expr:      "^a",
			expLanes:  0b0100001000010000,
			regexType: regexp2.Regexp,
		},
		{
			data:      [16]string{"a", "Ω", "aa", "Ωa", "aΩ", "ΩΩ", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω"},
			expr:      "^a",
			expLanes:  0b0000000000010101,
			regexType: regexp2.Regexp,
		},
		{
			data:      [16]string{"aaaa", "baaa", "Ωaaa", "abaa", "bbaa", "Ωbaa", "aΩaa", "bΩaa", "ΩΩaa", "aaba", "baba", "Ωaba", "abba", "bbba", "Ωbba", "aΩba"},
			expr:      `a*__a`,
			expLanes:  0b1001001001001001,
			regexType: regexp2.SimilarTo,
		},
		{
			data:      [16]string{"Ωaa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa"},
			expr:      `^(a*.aa)$`,
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
		{
			data:      [16]string{"abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ"},
			expr:      "a.*b.*Ω",
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
		{
			data:      [16]string{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a"},
			expr:      "^a$",
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
	}

	run := func(ut unitTest) {
		ds, err := regexp2.CreateDs(ut.expr, ut.regexType, false, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Fatal(err)
		}
		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, info string, op bcop, needle string, expLanes uint16) {

			if dsByte == nil {
				return
			}

			ctx.dict = append(ctx.dict[:0], string(dsByte))
			ctx.setScalarStrings(ut.data[:], []byte{})
			ctx.current = 0xFFFF

			// when
			if err := ctx.ExecuteImm2(op, 0); err != nil {
				t.Fatal(err)
			}

			obsLanes := ctx.current
			if obsLanes != expLanes {
				for i := 0; i < 16; i++ {
					obsLane := getBit(obsLanes, i)
					expLane := getBit(expLanes, i)
					if obsLane != expLane {
						t.Errorf("%v-%v: issue with lane %v, \ndata=%q\nexpected=%016b (regexGolang=%q)\nobserved=%016b (regexSneller=%q)",
							name, info, i, ut.data, expLanes, ds.RegexGolang.String(), obsLanes, ds.RegexSneller.String())
						break
					}
				}
			}
		}

		// first: check reference implementation
		{
			for i := 0; i < 16; i++ {
				obsLane := ds.RegexGolang.MatchString(ut.data[i])
				expLane := getBit(ut.expLanes, i)
				if expLane != obsLane {
					t.Errorf("refImpl: lane %v: issue with data %q\nexpected %v while RegexGolang=%q yields observed %v",
						i, ut.data[i], expLane, ds.RegexGolang.String(), obsLane)
				}
			}
		}
		// second: check the bytecode implementation

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		regexDataTest(&ctx, ds.DsT6, "DfaT6", opDfaT6, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsT6Z, "DfaT6Z", opDfaT6Z, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsT7, "DfaT7", opDfaT7, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsT7Z, "DfaT7Z", opDfaT7Z, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsT8, "DfaT8", opDfaT8, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsT8Z, "DfaT8Z", opDfaT8Z, ut.expr, ut.expLanes)
		regexDataTest(&ctx, ds.DsLZ, "DfaLZ", opDfaLZ, ut.expr, ut.expLanes)
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf(`case %d:`, i), func(t *testing.T) {
			run(ut)
		})
	}
}

// FuzzRegexMatchRun runs fuzzer to search both regexes and data and compares the with a reference implementation
func FuzzRegexMatchRun(f *testing.F) {

	var padding []byte //empty padding

	run := func(t *testing.T, ds []byte, matchExpected bool, data, regexString, info string, op bcop) {
		regexMatch := func(ds []byte, needle string, op bcop) (match bool) {
			values := fill16(needle)

			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, pad(string(ds)))
			ctx.setScalarStrings(values, padding)
			ctx.current = 0xFFFF

			if err := ctx.ExecuteImm2(op, 0); err != nil {
				t.Error(err)
			}

			if ctx.current == 0 {
				return false
			}
			if ctx.current == 0xFFFF {
				return true
			}
			t.Errorf("inconstent results %x", ctx.current)
			return false
		}

		if ds != nil {
			matchObserved := regexMatch(ds, data, op)
			if matchExpected != matchObserved {
				t.Errorf(`Fuzzer found: %v yields '%v' while expected '%v'. (regexString %q; data %q)`, info, matchObserved, matchExpected, regexString, data)
			}
		}
	}

	f.Add(`.*a.b`, `xayb`)
	f.Add(`ac`, `(a|b)+c`)
	f.Add(`0`, `\B`)
	f.Add(`01|.`, `0`)
	f.Add(`\nb`, `(\n|.)|.|.a`)
	f.Add(`z`, `ab.cd`)
	f.Add(`A`, `^.*ſ$`)
	f.Add(`B`, `..x[:lower:]`)
	f.Add(`C`, `[:ascii:]+$`)
	f.Add(`D`, `[a-z0-9]+`)
	f.Add(`E`, `[0-9a-fA-F]+\r\n`)

	f.Fuzz(func(t *testing.T, data, expr string) {
		if utf8.ValidString(data) && utf8.ValidString(expr) {
			if err := regexp2.IsSupported(expr); err == nil {
				t.Log(err)
			} else {
				regexSneller, err1 := regexp2.Compile(expr, regexp2.Regexp)
				regexGolang, err2 := regexp2.Compile(expr, regexp2.GolangRegexp)

				if (err1 == nil) && (err2 == nil) && (regexSneller != nil) && (regexGolang != nil) {
					regexString2 := regexSneller.String()
					ds, err := regexp2.CreateDs(regexString2, regexp2.Regexp, false, regexp2.MaxNodesAutomaton)
					if err != nil {
						t.Log(err)
					}
					matchExpected := regexGolang.MatchString(data)
					run(t, ds.DsT6, matchExpected, data, regexString2, "DfaT6", opDfaT6)
					run(t, ds.DsT7, matchExpected, data, regexString2, "DfaT7", opDfaT7)
					run(t, ds.DsT8, matchExpected, data, regexString2, "DfaT8", opDfaT8)
					run(t, ds.DsT6Z, matchExpected, data, regexString2, "DfaT6Z", opDfaT6Z)
					run(t, ds.DsT7Z, matchExpected, data, regexString2, "DfaT7Z", opDfaT7Z)
					run(t, ds.DsT8Z, matchExpected, data, regexString2, "DfaT8Z", opDfaT8Z)
					run(t, ds.DsLZ, matchExpected, data, regexString2, "DfaLZ", opDfaLZ)
				}
			}
		}
	})
}

// FuzzRegexMatchCompile runs fuzzer to search regexes and determines that their compilation does not fail
func FuzzRegexMatchCompile(f *testing.F) {
	f.Add(`ab.cd`)
	f.Add(`..x[:lower:]`)
	f.Add(`[a-z0-9]+`)
	f.Add(`[0-9a-fA-F]+\r\n`)
	f.Add(`^.$+^+`)      // invalid noise regex
	f.Add(`.*a.......b`) // combinatorial explosion in NFA -> DFA

	f.Fuzz(func(t *testing.T, re string) {
		rec, err := regexp.Compile(re)
		if err != nil {
			return
		}
		if regexp2.IsSupported(re) != nil {
			return
		}
		store, err := regexp2.CompileDFA(rec, regexp2.MaxNodesAutomaton)
		if err != nil {
			return
		}
		if store == nil {
			t.Fatalf(`unhandled regexp: %s`, re)
		}

		hasUnicodeWildcard, wildcardRange := store.HasUnicodeWildcard()

		// none of this should panic:
		dsTiny, err := regexp2.NewDsTiny(store)
		if err != nil {
			t.Fatalf(fmt.Sprintf("DFATiny: error %v for regex \"%v\"", err, re))
		}
		dsTiny.Data(6, hasUnicodeWildcard, wildcardRange)
		dsTiny.Data(7, hasUnicodeWildcard, wildcardRange)
		dsTiny.Data(8, hasUnicodeWildcard, wildcardRange)

		if _, err = regexp2.NewDsLarge(store); err != nil {
			t.Fatalf(fmt.Sprintf("DFALarge: error %v for regex \"%v\"", err, re))
		}
	})
}

func referenceSubstr(input string, start, length int) string {
	start-- // this method is called as 1-based indexed, the implementation is 0-based indexed
	if start < 0 {
		start = 0
	}
	if length < 0 {
		length = 0
	}
	if !utf8.ValidString(input) {
		return ""
	}
	asRunes := []rune(input)
	if start >= len(asRunes) {
		return ""
	}
	if start+length > len(asRunes) {
		length = len(asRunes) - start
	}
	return string(asRunes[start : start+length])
}

// TestSubstrUT unit-tests for: opSubstr
func TestSubstrUT(t *testing.T) {
	t.Parallel()
	name := "substring (opSubstr)"

	type unitTest struct {
		data      string
		begin     int
		length    int
		expResult string // expected result
	}
	unitTests := [][16]unitTest{
		{
			{"ba", 1, -1, ""},  // length smaller than 0 should be handled as 0
			{"ba", 0, 2, "ba"}, // begin smaller than 1 should be handled as 1
			{"abbc", 2, 2, "bb"},
			{"abc", 2, 1, "b"},
			{"ab", 2, 1, "b"},
			{"ba", 1, 0, ""},
			{"ba", 1, 1, "b"},
			{"ba", 1, 2, "ba"},
			{"ba", 1, 3, "ba"},
			{"xxx𐍈yyy", 4, 1, "𐍈"},
			{"aaaa", 4, 1, "a"}, // get last character of data
			{"aaaa", 4, 2, "a"}, // TODO is this what we want? get last character of data
			{"aaaa", 5, 1, ""},  // read after the length of data
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
		},
		{
			{"", 0, 0, ""},
			{"a", 2, 1, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
		},
		{
			{"ab", 2, 1, "b"},
			{"cd", 1, 1, "c"},
			{"e", 2, 1, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
			{"", 0, 0, ""},
		},
	}

	run := func(ut [16]unitTest) {
		expResults := [16]string{}
		stackContent1 := make([]uint64, 16)
		stackContent2 := make([]uint64, 16)
		data := make([]string, 16)

		// first: check reference implementation
		for i := 0; i < 16; i++ {
			data[i] = ut[i].data
			expResults[i] = ut[i].expResult
			obsResult := referenceSubstr(data[i], ut[i].begin, ut[i].length)
			if obsResult != expResults[i] {
				t.Errorf("refImpl: substring %q; begin=%v; length=%v; observed %q; expected %q",
					data[i], ut[i].begin, ut[i].length, obsResult, expResults[i])
			}
			stackContent1[i] = uint64(ut[i].begin)
			stackContent2[i] = uint64(ut[i].length)
		}
		// second: check bytecode implementation
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.addScalarStrings(data, []byte{})
		ctx.setStackUint64(stackContent1)
		ctx.addStackUint64(stackContent2)
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		offsetStackSlot1 := uint16(0)
		offsetStackSlot2 := uint16(len(stackContent1) * 8)
		if err := ctx.Execute2Imm2(opSubstr, offsetStackSlot1, offsetStackSlot2); err != nil {
			t.Fatal(err)
		}
		//then
		scalarAfter := ctx.getScalarUint32()
		obsResults := [16]string{}
		for i := 0; i < 16; i++ {
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])

			if (obsOffset + obsLength) > len(data) {
				t.Errorf("%v\ninvalid offset or length at lane %v: data=%q; begin=%v; length=%v; Z2=%v; Z3=%v",
					name, i, data, ut[i].begin, ut[i].length, scalarAfter[0][i], scalarAfter[1][i])
			}
			obsResults[i] = data[i][obsOffset : obsOffset+obsLength]
		}
		if (ctx.current != 0xFFFF) || (obsResults != expResults) {
			t.Errorf("%v\ndata=%q\nbegin=%v\nlength=%v\nobserved=%q\nexpected=%q",
				name, data, stackContent1, stackContent2, obsResults, expResults)
		}
	}

	t.Run(name, func(t *testing.T) {
		for _, ut := range unitTests {
			run(ut)
		}
	})
}

// TestSubstrBF brute-force tests for: opSubstr
func TestSubstrBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// space of possible begin positions
		beginSpace []int
		// space of possible lengths
		lengthSpace []int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, begin, length) -> result
		refImpl func(string, int, int) string
	}
	testSuites := []testSuite{
		{
			name:         "substring (opSubstr)",
			dataAlphabet: []rune{'a', 'b', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			beginSpace:   []int{0, 1, 2, 4, 5},
			lengthSpace:  []int{-1, 0, 1, 3, 4},
			op:           opSubstr,
			refImpl:      referenceSubstr,
		},
		{
			name:         "substring (opSubstr) UTF8",
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
			beginSpace:   []int{1, 3, 4, 5},
			lengthSpace:  []int{0, 1, 3, 4},
			op:           opSubstr,
			refImpl:      referenceSubstr,
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		//TODO make a generic space partitioner that can be reused by other BF tests
		stackContent2 := make([]uint64, 16)
		stackContent1 := make([]uint64, 16)
		offsetStackSlot1 := uint16(0)
		offsetStackSlot2 := uint16(len(stackContent1) * 8)
		expLane := true

		for _, data := range dataSpace {
			data16 := fill16(data)
			for _, length := range ts.lengthSpace {
				for i := 0; i < 16; i++ {
					stackContent2[i] = uint64(length)
				}

				for _, begin := range ts.beginSpace {
					expResult := ts.refImpl(data, begin, length)

					for i := 0; i < 16; i++ {
						stackContent1[i] = uint64(begin)
					}

					var ctx bctestContext
					ctx.Taint()
					ctx.addScalarStrings(data16, []byte{})
					ctx.setStackUint64(stackContent1)
					ctx.addStackUint64(stackContent2)
					ctx.current = 0xFFFF
					scalarBefore := ctx.getScalarUint32()

					// when
					if err := ctx.Execute2Imm2(ts.op, offsetStackSlot1, offsetStackSlot2); err != nil {
						t.Fatal(err)
					}
					// then
					scalarAfter := ctx.getScalarUint32()
					for i := 0; i < 16; i++ {
						obsLane := getBit(ctx.current, i)
						obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
						obsLength := int(scalarAfter[1][i])

						if (obsOffset + obsLength) > len(data) {
							t.Errorf("%v\ninvalid offset or length at lane %v: data=%q; begin=%v; length=%v; Z2=%v; Z3=%v",
								ts.name, i, data, begin, length, scalarAfter[0][i], scalarAfter[1][i])
							return
						}
						obsResult := data[obsOffset : obsOffset+obsLength]

						if (obsLane != expLane) || (obsResult != expResult) {
							t.Errorf("%v\nlane %v: data %q; begin=%v; length=%v; observed %q; expected %q",
								ts.name, i, data, begin, length, obsResult, expResult)
							break
						}
					}
					ctx.Free()
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSubstrFT fuzz-tests for: opSubstr
func FuzzSubstrFT(f *testing.F) {
	f.Add(uint16(0xFFFF),
		"xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy", "xy",
		1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	f.Add(uint16(0xFFFF),
		"xxx𐍈yyy", "a", "", "aabbc", "xxx𐍈yy", "xx𐍈yy", "x𐍈yy", "𐍈yy", "xxx𐍈y", "xxx𐍈", "xxx𐍈y", "𐍈", "xx", "", "", "",
		4, 1, 2, 4, 4, 2, 1, 2, 3, 5, 6, 7, 3, 5, 6, 7,
		1, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 0, 1, 1, 2)

	type testSuite struct {
		// name to describe this test-suite
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, begin, length) -> result
		refImpl func(string, int, int) string
	}

	testSuites := []testSuite{
		{
			name:    "sub-string (opSubstr)",
			op:      opSubstr,
			refImpl: referenceSubstr,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, begin, length [16]int) {
		expResults := [16]string{}
		stackContent1 := make([]uint64, 16)
		stackContent2 := make([]uint64, 16)

		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				expResults[i] = ts.refImpl(data[i], begin[i], length[i])
			}
			stackContent1[i] = uint64(begin[i])
			stackContent2[i] = uint64(length[i])
		}

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.addScalarStrings(data[:], []byte{})
		ctx.setStackUint64(stackContent1)
		ctx.addStackUint64(stackContent2)
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		offsetStackSlot1 := uint16(0)
		offsetStackSlot2 := uint16(16 * 8)
		if err := ctx.Execute2Imm2(ts.op, offsetStackSlot1, offsetStackSlot2); err != nil {
			t.Fatal(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			if getBit(ctx.current, i) {
				obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
				obsLength := int(scalarAfter[1][i])
				obsResult := data[i][obsOffset : obsOffset+obsLength]

				if obsResult != expResults[i] {
					obsResults := [16]string{}
					for j := 0; j < 16; j++ {
						obsOffset2 := int(scalarAfter[0][j] - scalarBefore[0][j]) // NOTE the reference implementation returns offset starting from zero
						obsLength2 := int(scalarAfter[1][j])
						obsResults[j] = data[j][obsOffset2 : obsOffset2+obsLength2]
					}
					t.Errorf("%v\nissue with lane %v\ndata=%q\nbegin=%v\nlength=%v\nobserved=%q\nexpected=%q",
						ts.name, i, data, begin, length, obsResult, expResults)
					break
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string,
		b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15 int,
		s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		begin := [16]int{b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15}
		length := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data, begin, length)
		}
	})
}

// referenceIsSubnetOfIP4 reference implementation for opIsSubnetOfIP4
func referenceIsSubnetOfIP4(msg string, min, max uint32) bool {
	// str2ByteArray parses an IP; will also parse leasing zeros: eg. "000.001.010.100" is returned as [0,1,10,100]
	str2ByteArray := func(str string) (result []byte, ok bool) {
		result = make([]byte, 4)
		components := strings.Split(str, ".")
		if len(components) != 4 {
			return result, false
		}
		for i, segStr := range components {
			if len(segStr) > 3 {
				return result, false
			}
			for _, digit := range segStr {
				if !unicode.IsDigit(digit) {
					return result, false
				}
			}
			seg, err := strconv.Atoi(segStr)
			if err != nil {
				return result, false
			}
			if seg < 0 || seg > 255 {
				return result, false
			}
			result[i] = byte(seg)
		}
		return result, true
	}

	inRangeByteWise := func(value []byte, min, max [4]byte) bool {
		for i := 0; i < 4; i++ {
			if (min[i] > value[i]) || (value[i] > max[i]) {
				return false
			}
		}
		return true
	}

	if byteSlice, ok := str2ByteArray(msg); ok {
		r2 := inRangeByteWise(byteSlice, toArrayIP4(min), toArrayIP4(max))
		return r2
	}
	return false
}

// TestIsSubnetOfIP4UT runs unit-tests for: opIsSubnetOfIP4
func TestIsSubnetOfIP4UT(t *testing.T) {
	t.Parallel()
	name := "is-subnet-of IP4 (opIsSubnetOfIP4)"

	type unitTest struct {
		ip, min, max string
		expLane      bool
	}
	unitTests := []unitTest{
		{"100.20.", "100.20.100.20", "100.20.100.20", false}, // read beyond buffer check
		{"0.0.0.A", "0.0.0.1", "0.0.0.23", false},

		{"10.1.0.0", "10.1.0.0", "20.0.0.0", false},

		{"111.111.111.11", "100.100.100.100", "200.200.200.200", false},
		{"0.0.0.\x002", "0.0.0.1", "0.0.0.3", false},
		{"\x000.0.0.0", "0.0.0.0", "0.0.0.68", false},
		{"110.1.01.0000", "24.216.71.104", "138.13.200.124", false},
		{"1.00.0.0", "1.0.0.0", "2.0.0.0", true},

		{"052.723.308.0119", "6.255.253.81", "90.161.40.157", false},
		{"2.2.300.0", "1.1.3.0", "3.3.3.0", false},
		{"1.256.0.0", "0.0.0.0", "2.0.0.0", false}, // segment 256 is too large
		{"10...00", "0.0.0.0", "10.0.0.0", false},
		{"0.0.0.0", "1.0.0.0", "3.0.0.0", false}, // min_0 > ip_0 < max_0

		{"A.010.0", "0.0.0.0", "2.0.0.0", false},
		{"10.1.0.0", "10.1.0.0", "20.0.0.0", false},

		{"8.8.8.2", "8.8.8.1", "8.8.8.3", true},
		{"1.2", "1.1.0.0", "2.0.0.0", false},
		{string([]byte("100.100.0.0")[0:8]), "0.0.0.0", "100.100.0.0", false},  // test whether length of msg is respected
		{"1.00000", "0.0.0.0", "1.0.0.0", false},                               // check if there is a dot
		{string([]byte("100.100.0.0")[0:10]), "0.0.0.0", "100.100.0.0", false}, // test whether length of msg is respected
		{string([]byte("100.100.1.0")[0:8]), "0.0.0.0", "100.100.0.0", false},  // test whether length of msg is respected

		{"10.2.0.0", "9.0.0.0", "10.1.0.0", false},
		{"2.0.0.0", "1.0.0.0", "3.0.0.0", true},  // min_0 < ip_0 < max_0
		{"2.0.0.0", "2.0.0.0", "3.0.0.0", true},  // min_0 = ip_0 < max_0
		{"2.0.0.0", "1.0.0.0", "2.0.0.0", true},  // min_0 < ip_0 = max_0
		{"2.0.0.0", "2.0.0.0", "2.0.0.0", true},  // min_0 = ip_0 = max_0
		{"0.0.0.0", "1.0.0.0", "3.0.0.0", false}, // min_0 > ip_0 < max_0
		{"2.0.0.0", "1.0.0.0", "1.0.0.0", false}, // min_0 < ip_0 > max_0

		{"1.2.0.0", "1.1.0.0", "2.0.0.0", false},
		{"8.2.0.0", "8.1.0.0", "8.3.0.0", true},  // min_1 < ip_1 < max_1
		{"8.2.0.0", "8.2.0.0", "8.3.0.0", true},  // min_1 = ip_1 < max_1
		{"8.2.0.0", "8.1.0.0", "8.2.0.0", true},  // min_1 < ip_1 = max_1
		{"8.2.0.0", "8.2.0.0", "8.2.0.0", true},  // min_1 = ip_1 = max_1
		{"8.0.0.0", "8.1.0.0", "8.3.0.0", false}, // min_1 > ip_1 < max_1
		{"8.2.0.0", "8.1.0.0", "8.1.0.0", false}, // min_1 < ip_1 > max_1

		{"1.2.1.0", "1.2.0.0", "1.3.0.0", false},
		{"8.8.2.0", "8.8.1.0", "8.8.3.0", true},  // min_2 < ip_2 < max_2
		{"8.8.2.0", "8.8.2.0", "8.8.3.0", true},  // min_2 = ip_2 < max_2
		{"8.8.2.0", "8.8.1.0", "8.8.2.0", true},  // min_2 < ip_2 = max_2
		{"8.8.2.0", "8.8.2.0", "8.8.2.0", true},  // min_2 = ip_2 = max_2
		{"8.8.0.0", "8.8.1.0", "8.8.3.0", false}, // min_2 > ip_2 < max_2
		{"8.8.2.0", "8.8.1.0", "8.8.1.0", false}, // min_2 < ip_2 > max_2

		{"1.2.3.1", "1.2.3.0", "1.2.4.0", false},
		{"8.8.8.2", "8.8.8.1", "8.8.8.3", true},  // min_3 < ip_3 < max_3
		{"8.8.8.2", "8.8.8.2", "8.8.8.3", true},  // min_3 = ip_3 < max_3
		{"8.8.8.2", "8.8.8.1", "8.8.8.2", true},  // min_3 < ip_3 = max_3
		{"8.8.8.2", "8.8.8.2", "8.8.8.2", true},  // min_3 = ip_3 = max_3
		{"8.8.8.0", "8.8.8.1", "8.8.8.3", false}, // min_3 > ip_3 < max_3
		{"8.8.8.2", "8.8.8.1", "8.8.8.1", false}, // min_3 < ip_3 > max_3
	}

	run := func(ut unitTest) {
		min := binary.BigEndian.Uint32(net.ParseIP(ut.min).To4())
		max := binary.BigEndian.Uint32(net.ParseIP(ut.max).To4())
		// first: check reference implementation
		{
			obsLane := referenceIsSubnetOfIP4(ut.ip, min, max)
			if obsLane != ut.expLane {
				t.Errorf("refImpl: msg=%q; min=%q; max=%q; observed=%v; expected=%v",
					ut.ip, ut.min, ut.max, obsLane, ut.expLane)
			}
		}
		// second: check the bytecode implementation
		minA := toArrayIP4(min)
		maxA := toArrayIP4(max)

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], toBCD(&minA, &maxA))
		ctx.setScalarStrings(fill16(ut.ip), []byte{})
		ctx.current = 0xFFFF

		// when
		if err := ctx.ExecuteImm2(opIsSubnetOfIP4, 0); err != nil {
			t.Fatal(err)
		}
		// then
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			if obsLane != ut.expLane {
				t.Errorf("%v\nlane %v: ip %q; min %q; max %q; observed %v; expected %v",
					name, i, ut.ip, ut.min, ut.max, obsLane, ut.expLane)
				return
			}
		}
	}

	t.Run(name, func(t *testing.T) {
		for _, ut := range unitTests {
			run(ut)
		}
	})
}

// TestIsSubnetOfIP4BF runs brute-force tests for: opIsSubnetOfIP4
func TestIsSubnetOfIP4BF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate data
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, min, max) -> lane
		refImpl func(string, uint32, uint32) bool
	}
	testSuites := []testSuite{
		{
			name:         "is-subnet-of IP4 IP (opIsSubnetOfIP4)",
			dataAlphabet: []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', 'A', 0},
			dataLenSpace: []int{9, 10, 11, 12, 13, 14, 15, 16},
			dataMaxSize:  100000,
			op:           opIsSubnetOfIP4,
			refImpl:      referenceIsSubnetOfIP4,
		},
		{
			name:         "is-subnet-of IP4 IP (opIsSubnetOfIP4) 2",
			dataAlphabet: []rune{'0', '1', '.'},
			dataLenSpace: []int{9, 10, 11, 12, 13},
			dataMaxSize:  exhaustive,
			op:           opIsSubnetOfIP4,
			refImpl:      referenceIsSubnetOfIP4,
		},
	}

	// randomIP4Addr will generate all sorts of questionable addresses; things like 0.0.0.0 and 255.255.255.255,
	// as well as private IP address ranges and multicast addresses.
	randomIP4Addr := func() net.IP {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, rand.Uint32())
		return net.IPv4(bs[0], bs[1], bs[2], bs[3]).To4()
	}

	randomMinMaxValues := func() (min, max uint32) {
		max = rand.Uint32()
		min = uint32(rand.Intn(int(max)))
		return
	}

	run := func(ts testSuite, dataSpace []string) {
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		for _, ipStr := range dataSpace {
			min, max := randomMinMaxValues()
			expLane := referenceIsSubnetOfIP4(ipStr, min, max) // calculate the expected lane value

			minA := toArrayIP4(min)
			maxA := toArrayIP4(max)

			ctx.dict = append(ctx.dict[:0], toBCD(&minA, &maxA))
			ctx.setScalarStrings(fill16(ipStr), []byte{})
			ctx.current = 0xFFFF

			// when
			if err := ctx.ExecuteImm2(opIsSubnetOfIP4, 0); err != nil {
				t.Fatal(err)
			}
			// then
			for i := 0; i < 16; i++ {
				obsLane := getBit(ctx.current, i)
				if obsLane != expLane {
					t.Errorf("%v\nlane %v: ip %q; min %q; max %q; observed %v; expected %v",
						ts.name, i, ipStr, toStrIP4(min), toStrIP4(max), obsLane, expLane)
					return
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			var dataSpace []string
			if ts.dataMaxSize == exhaustive {
				dataSpace = make([]string, 0)
				for _, data := range createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize) {
					if net.ParseIP(data).To4() != nil {
						dataSpace = append(dataSpace, data)
					}
				}
			} else {
				dataSpace = make([]string, ts.dataMaxSize)
				for i := 0; i < ts.dataMaxSize; i++ {
					dataSpace[i] = randomIP4Addr().String()
				}
			}
			run(ts, dataSpace)
		})
	}
}

// FuzzIsSubnetOfIP4FT runs fuzz tests for: opIsSubnetOfIP4
func FuzzIsSubnetOfIP4FT(f *testing.F) {
	name := "is-subnet-of IP4 (opIsSubnetOfIP4)"

	str2Uint32 := func(str string) uint32 {
		result := uint32(0)
		for i, segStr := range strings.Split(str, ".") {
			seg, _ := strconv.Atoi(segStr)
			if seg < 0 || seg > 255 {
				panic("invalid ip4")
			}
			result |= uint32(seg) << (i * 8)
		}
		return result
	}

	type unitTest struct {
		ip       string
		min, max uint32
	}
	unitTests := []unitTest{
		{"10.1.0.0", str2Uint32("10.1.0.0"), str2Uint32("20.0.0.0")},
		{"10.2.0.0", str2Uint32("9.0.0.0"), str2Uint32("10.1.0.0")},
		{"2.0.0.0", str2Uint32("1.0.0.0"), str2Uint32("3.0.0.0")}, // min_0 < ip_0 < max_0
		{"2.0.0.0", str2Uint32("2.0.0.0"), str2Uint32("3.0.0.0")}, // min_0 = ip_0 < max_0
		{"2.0.0.0", str2Uint32("1.0.0.0"), str2Uint32("2.0.0.0")}, // min_0 < ip_0 = max_0
		{"2.0.0.0", str2Uint32("2.0.0.0"), str2Uint32("2.0.0.0")}, // min_0 = ip_0 = max_0
		{"0.0.0.0", str2Uint32("1.0.0.0"), str2Uint32("3.0.0.0")}, // min_0 > ip_0 < max_0
		{"2.0.0.0", str2Uint32("1.0.0.0"), str2Uint32("1.0.0.0")}, // min_0 < ip_0 > max_0

		{"1.2.0.0", str2Uint32("1.1.0.0"), str2Uint32("2.0.0.0")},
		{"8.2.0.0", str2Uint32("8.1.0.0"), str2Uint32("8.3.0.0")}, // min_1 < ip_1 < max_1
		{"8.2.0.0", str2Uint32("8.2.0.0"), str2Uint32("8.3.0.0")}, // min_1 = ip_1 < max_1
		{"8.2.0.0", str2Uint32("8.1.0.0"), str2Uint32("8.2.0.0")}, // min_1 < ip_1 = max_1
		{"8.2.0.0", str2Uint32("8.2.0.0"), str2Uint32("8.2.0.0")}, // min_1 = ip_1 = max_1
		{"8.0.0.0", str2Uint32("8.1.0.0"), str2Uint32("8.3.0.0")}, // min_1 > ip_1 < max_1
		{"8.2.0.0", str2Uint32("8.1.0.0"), str2Uint32("8.1.0.0")}, // min_1 < ip_1 > max_1

		{"1.2.1.0", str2Uint32("1.2.0.0"), str2Uint32("1.3.0.0")},
		{"8.8.2.0", str2Uint32("8.8.1.0"), str2Uint32("8.8.3.0")}, // min_2 < ip_2 < max_2
		{"8.8.2.0", str2Uint32("8.8.2.0"), str2Uint32("8.8.3.0")}, // min_2 = ip_2 < max_2
		{"8.8.2.0", str2Uint32("8.8.1.0"), str2Uint32("8.8.2.0")}, // min_2 < ip_2 = max_2
		{"8.8.2.0", str2Uint32("8.8.2.0"), str2Uint32("8.8.2.0")}, // min_2 = ip_2 = max_2
		{"8.8.0.0", str2Uint32("8.8.1.0"), str2Uint32("8.8.3.0")}, // min_2 > ip_2 < max_2
		{"8.8.2.0", str2Uint32("8.8.1.0"), str2Uint32("8.8.1.0")}, // min_2 < ip_2 > max_2

		{"1.2.3.1", str2Uint32("1.2.3.0"), str2Uint32("1.2.4.0")},
		{"8.8.8.2", str2Uint32("8.8.8.1"), str2Uint32("8.8.8.3")}, // min_3 < ip_3 < max_3
		{"8.8.8.2", str2Uint32("8.8.8.2"), str2Uint32("8.8.8.3")}, // min_3 = ip_3 < max_3
		{"8.8.8.2", str2Uint32("8.8.8.1"), str2Uint32("8.8.8.2")}, // min_3 < ip_3 = max_3
		{"8.8.8.2", str2Uint32("8.8.8.2"), str2Uint32("8.8.8.2")}, // min_3 = ip_3 = max_3
		{"8.8.8.0", str2Uint32("8.8.8.1"), str2Uint32("8.8.8.3")}, // min_3 > ip_3 < max_3
		{"8.8.8.2", str2Uint32("8.8.8.1"), str2Uint32("8.8.8.1")}, // min_3 < ip_3 > max_3
	}
	for _, ut := range unitTests {
		a := ut.ip
		f.Add(uint16(0xFFFF), a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, ut.min, ut.max)
	}

	run := func(t *testing.T, lanes uint16, data [16]string, min, max uint32) {

		expLanes := uint16(0)
		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				expLanes = setBit(expLanes, i, referenceIsSubnetOfIP4(data[i], min, max))
			}
		}
		minA := toArrayIP4(min)
		maxA := toArrayIP4(max)

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], toBCD(&minA, &maxA))
		ctx.setScalarStrings(data[:], []byte{})
		ctx.current = lanes

		// when
		if err := ctx.ExecuteImm2(opIsSubnetOfIP4, 0); err != nil {
			t.Fatal(err)
		}
		// then
		if ctx.current != expLanes {
			for i := 0; i < 16; i++ {
				obsLane := getBit(ctx.current, 1)
				expLane := getBit(expLanes, i)
				if obsLane != expLane {
					t.Errorf("%v\nlane %v: ip %q; min %q; max %q; observed %v; expected %v",
						name, i, data[i], toStrIP4(min), toStrIP4(max), obsLane, expLane)
					break
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, min, max uint32) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		run(t, lanes, data, min, max)
	})
}

// TestSkip1CharUT unit-tests for opSkip1charLeft, opSkip1charRight
func TestSkip1CharUT(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      string // data at SI
		expLane   bool   // expected lane K1
		expOffset int    // expected offset Z2
		expLength int    // expected length Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name: "skip 1 char from left (opSkip1charLeft)",
			unitTests: []unitTest{
				{"", false, 0, 0},
				{"a", true, 1, 0},
				{"aa", true, 1, 1},
				{"aaa", true, 1, 2},
				{"aaaa", true, 1, 3},
				{"aaaaa", true, 1, 4},

				{"𐍈", true, 4, 0},
				{"𐍈a", true, 4, 1},
				{"𐍈aa", true, 4, 2},
				{"𐍈aaa", true, 4, 3},
				{"𐍈aaaa", true, 4, 4},
				{"𐍈aaaaa", true, 4, 5},

				{"a𐍈", true, 1, 4},
				{"a𐍈a", true, 1, 5},
				{"a𐍈aa", true, 1, 6},
				{"a𐍈aaa", true, 1, 7},
				{"a𐍈aaaa", true, 1, 8},
			},
			op:      opSkip1charLeft,
			refImpl: referenceSkipCharLeft,
		},
		{
			name: "skip 1 char from right (opSkip1charRight)",
			unitTests: []unitTest{
				{"", false, 0, 0},
				{"a", true, 0, 0},
				{"aa", true, 0, 1},
				{"aaa", true, 0, 2},
				{"aaaa", true, 0, 3},
				{"aaaaa", true, 0, 4},

				{"𐍈", true, 0, 0},
				{"a𐍈", true, 0, 1},
				{"aa𐍈", true, 0, 2},
				{"aaa𐍈", true, 0, 3},
				{"aaaa𐍈", true, 0, 4},
				{"aaaaa𐍈", true, 0, 5},

				{"𐍈a", true, 0, 4},
				{"a𐍈a", true, 0, 5},
				{"aa𐍈a", true, 0, 6},
				{"aaa𐍈a", true, 0, 7},
				{"aaaa𐍈a", true, 0, 8},
			},
			op:      opSkip1charRight,
			refImpl: referenceSkipCharRight,
		},
	}

	run := func(ts *testSuite, ut *unitTest) {
		// first: check reference implementation
		{
			obsLane, obsOffset, obsLength := ts.refImpl(ut.data, 1)
			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("refImpl: data %q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v",
					ut.data, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
			}
		}
		// second: check bytecode implementation
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		dataPrefix := string([]byte{0, 0, 0})
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(fill16(ut.data), []byte{})
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.Execute(ts.op); err != nil {
			t.Error(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])

			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("%v\nlane %v: skipping 1 char from data %q: observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
					ts.name, i, ut.data, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
				break
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestSkip1CharBF brute-force tests for: opSkip1charLeft, opSkip1charRight
func TestSkip1CharBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name:         "skip 1 char from left (opSkip1charLeft)",
			dataAlphabet: []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			op:           opSkip1charLeft,
			refImpl:      referenceSkipCharLeft,
		},
		{
			name:         "skip 1 char from right (opSkip1charRight)",
			dataAlphabet: []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			op:           opSkip1charRight,
			refImpl:      referenceSkipCharRight,
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		//TODO make a generic space partitioner that can be reused by other BF tests

		dataPrefix := string([]byte{0, 0, 0})

		for _, data := range dataSpace {
			expLane, expOffset, expLength := ts.refImpl(data, 1)

			ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
			ctx.addScalarStrings(fill16(data), []byte{})
			ctx.current = 0xFFFF
			scalarBefore := ctx.getScalarUint32()

			// when
			if err := ctx.Execute(ts.op); err != nil {
				t.Error(err)
			}
			// then
			scalarAfter := ctx.getScalarUint32()
			for i := 0; i < 16; i++ {
				obsLane := getBit(ctx.current, i)
				obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
				obsLength := int(scalarAfter[1][i])

				if fault1x1(obsLane, expLane, obsOffset, expOffset, obsLength, expLength) {
					t.Errorf("lane %v: skipping 1 char from data %q: observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
						i, data, obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
					return
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSkip1CharFT fuzz-tests for: opSkip1charLeft, opSkip1charRight
func FuzzSkip1CharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS")

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name:    "skip 1 char from left (opSkip1charLeft)",
			op:      opSkip1charLeft,
			refImpl: referenceSkipCharLeft,
		},
		{
			name:    "skip 1 char from right (opSkip1charRight)",
			op:      opSkip1charRight,
			refImpl: referenceSkipCharRight,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string) {
		expLanes := uint16(0)
		expOffsets := [16]int{}
		expLengths := [16]int{}

		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				var expLane bool
				expLane, expOffsets[i], expLengths[i] = ts.refImpl(data[i], 1)
				expLanes = setBit(expLanes, i, expLane)
			}
		}

		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		dataPrefix := string([]byte{0, 0, 0})
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(data[:], []byte{})
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.Execute(ts.op); err != nil {
			t.Fatal(err)
		}
		// then
		obsLanes, obsOffsets, obsLengths := getObsValues(ctx, scalarBefore)
		if fault, msg := fault16x16(obsLanes, expLanes, obsOffsets, expOffsets, obsLengths, expLengths); fault {
			t.Errorf("%v\ndata=%q;\n%v", ts.name, data, msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data)
		}
	})
}

// TestSkipNCharUT unit-tests for opSkipNcharLeft, opSkipNcharRight
func TestSkipNCharUT(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      string // data at SI
		skipCount int    // number of code-points to skip
		expLane   bool   // expected lane K1
		expOffset int    // expected offset Z2
		expLength int    // expected length Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name: "skip N char from left (opSkipNcharLeft)",
			unitTests: []unitTest{
				{"", 1, false, -1, -1}, //NOTE offset and length are irrelevant
				{"a", 1, true, 1, 0},
				{"aa", 1, true, 1, 1},
				{"aaa", 1, true, 1, 2},
				{"aaaa", 1, true, 1, 3},
				{"aaaaa", 1, true, 1, 4},

				{"𐍈", 1, true, 4, 0},
				{"𐍈a", 1, true, 4, 1},
				{"𐍈aa", 1, true, 4, 2},
				{"𐍈aaa", 1, true, 4, 3},
				{"𐍈aaaa", 1, true, 4, 4},
				{"𐍈aaaaa", 1, true, 4, 5},

				{"a𐍈", 1, true, 1, 4},
				{"a𐍈a", 1, true, 1, 5},
				{"a𐍈aa", 1, true, 1, 6},
				{"a𐍈aaa", 1, true, 1, 7},
				{"a𐍈aaaa", 1, true, 1, 8},

				{"a", 2, false, -1, -1}, //NOTE offset and length are irrelevant
				{"a𐍈", 2, true, 5, 0},
				{"a𐍈a", 2, true, 5, 1},
				{"a𐍈aa", 2, true, 5, 2},
				{"a𐍈aaa", 2, true, 5, 3},
				{"a𐍈aaaa", 2, true, 5, 4},

				{"", 0, true, 0, 0},
				{"a", 0, true, 0, 1},
				{"𐍈", 2, false, -1, -1},
			},
			op:      opSkipNcharLeft,
			refImpl: referenceSkipCharLeft,
		},
		{
			name: "skip N char from right (opSkipNcharRight)",
			unitTests: []unitTest{
				{"", 1, false, -1, -1}, //NOTE offset and length are irrelevant
				{"a", 1, true, 0, 0},

				{"aa", 1, true, 0, 1},
				{"aaa", 1, true, 0, 2},
				{"aaaa", 1, true, 0, 3},
				{"aaaaa", 1, true, 0, 4},

				{"𐍈", 1, true, 0, 0},
				{"a𐍈", 1, true, 0, 1},
				{"aa𐍈", 1, true, 0, 2},
				{"aaa𐍈", 1, true, 0, 3},
				{"aaaa𐍈", 1, true, 0, 4},
				{"aaaaa𐍈", 1, true, 0, 5},

				{"𐍈a", 1, true, 0, 4},
				{"a𐍈a", 1, true, 0, 5},
				{"aa𐍈a", 1, true, 0, 6},
				{"aaa𐍈a", 1, true, 0, 7},
				{"aaaa𐍈a", 1, true, 0, 8},

				{"a", 2, false, -1, -1}, //NOTE offset and length are irrelevant
				{"𐍈a", 2, true, 0, 0},
				{"a𐍈a", 2, true, 0, 1},
				{"aa𐍈a", 2, true, 0, 2},
				{"aaa𐍈a", 2, true, 0, 3},
				{"aaaa𐍈a", 2, true, 0, 4},

				{"", 0, true, 0, 0},
				{"a", 0, true, 0, 1},
				{"𐍈", 2, false, -1, -1},
			},
			op:      opSkipNcharRight,
			refImpl: referenceSkipCharRight,
		},
	}

	run := func(ts *testSuite, ut *unitTest) {
		// first: check reference implementation
		{
			obsLane, obsOffset, obsLength := ts.refImpl(ut.data, ut.skipCount)
			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("refImpl: %v\ndata %q; skipCount=%v\nobserved lane=%v, offset=%v, length=%v\nexpected lane=%v, offset=%v, length=%v",
					ts.name, ut.data, ut.skipCount, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
			}
		}
		// second: check bytecode implementation
		stackContent := make([]uint64, 16)
		for i := 0; i < 16; i++ {
			stackContent[i] = uint64(ut.skipCount)
		}
		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		dataPrefix := string([]byte{0, 0, 0, 0})
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(fill16(ut.data), []byte{})
		ctx.setStackUint64(stackContent)
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		obsLanes, obsOffsets, obsLengths := getObsValues(ctx, scalarBefore)
		if fault16x1(obsLanes, ut.expLane, obsOffsets, ut.expOffset, obsLengths, ut.expLength) {
			t.Errorf("%v\ndata=%q; skipCount=%v\nobserved lanes=0b%16b, offset=%v, length=%v\nexpected lane=%v, offset=%v, length=%v",
				ts.name, ut.data, ut.skipCount, obsLanes, obsOffsets, obsLengths, ut.expLane, ut.expOffset, ut.expLength)
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestSkip1CharBF brute-force tests for: opSkipNcharLeft, opSkipNcharRight
func TestSkipNCharBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// space of skip counts
		skipCountSpace []int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}

	testSuites := []testSuite{
		{
			name:           "skip N char from left (opSkipNcharLeft)",
			dataAlphabet:   []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			skipCountSpace: []int{0, 1, 2, 3, 4, 5, 6},
			op:             opSkipNcharLeft,
			refImpl:        referenceSkipCharLeft,
		},
		{
			name:           "skip N char from right (opSkipNcharRight)",
			dataAlphabet:   []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			skipCountSpace: []int{0, 1, 2, 3, 4, 5, 6},
			op:             opSkipNcharRight,
			refImpl:        referenceSkipCharRight,
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		//TODO make a generic space partitioner that can be reused by other BF tests

		stackContent := make([]uint64, 16)
		dataPrefix := string([]byte{0, 0, 0})

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()

		for _, skipCount := range ts.skipCountSpace {
			for i := 0; i < 16; i++ {
				stackContent[i] = uint64(skipCount)
			}
			for _, data := range dataSpace {
				expLane, expOffset, expLength := ts.refImpl(data, skipCount)

				ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
				ctx.addScalarStrings(fill16(data), []byte{})
				ctx.setStackUint64(stackContent)
				ctx.current = 0xFFFF
				scalarBefore := ctx.getScalarUint32()

				// when
				if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
					t.Error(err)
				}
				// then
				scalarAfter := ctx.getScalarUint32()
				for i := 0; i < 16; i++ {
					obsLane := getBit(ctx.current, i)
					obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
					obsLength := int(scalarAfter[1][i])

					if fault1x1(obsLane, expLane, obsOffset, expOffset, obsLength, expLength) {
						t.Errorf("%v\nlane %v: skipping %v char(s) from data %q: observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
							ts.name, i, skipCount, data, obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
						return
					}
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSkip1CharFT fuzz tests for: opSkipNcharLeft, opSkipNcharRight
func FuzzSkipNCharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS", 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
	f.Add(uint16(0xF0F0), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS", 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
	f.Add(uint16(0b0000000000000011), "𐍈𐍈", "111𐍈", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, skipCount) -> lane, offset, length
		refImpl func(string, int) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name:    "skip N char from left (opSkipNcharLeft)",
			op:      opSkipNcharLeft,
			refImpl: referenceSkipCharLeft,
		},
		{
			name:    "skip N char from right (opSkipNcharRight)",
			op:      opSkipNcharRight,
			refImpl: referenceSkipCharRight,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, skipCount [16]int) {
		expLanes := uint16(0)
		expOffsets := [16]int{}
		expLengths := [16]int{}
		stackContent := make([]uint64, 16)

		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				var expLane bool
				expLane, expOffsets[i], expLengths[i] = ts.refImpl(data[i], skipCount[i])
				expLanes = setBit(expLanes, i, expLane)
			}
			if skipCount[i] > 0 {
				stackContent[i] = uint64(skipCount[i])
			}
		}

		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		dataPrefix := string([]byte{0, 0, 0, 0})
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(data[:], []byte{})
		ctx.setStackUint64(stackContent)
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Fatal(err)
		}
		// then
		obsLanes, obsOffsets, obsLengths := getObsValues(ctx, scalarBefore)
		if fault, msg := fault16x16(obsLanes, expLanes, obsOffsets, expOffsets, obsLengths, expLengths); fault {
			t.Errorf("%v\ndata=%q;\n%v", ts.name, data, msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		skipCount := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data, skipCount)
		}
	})
}

// referenceSplitPart splits data on delimiter and returns the offset and length of the idx part (NOTE 1-based index)
func referenceSplitPart(data string, idx int, delimiter rune) (lane bool, offset, length int) {
	idx-- // because this method is written as 0-indexed, but called as 1-indexed.
	offset = 0
	length = len(data)
	lane = false

	bytePosBegin := -1
	bytePosEnd := len(data)

	if idx == 0 {
		bytePosBegin = 0
		if x := strings.IndexRune(data, delimiter); x != -1 {
			bytePosEnd = x
		}
	} else {
		delimiterCount := 0
		for i, r := range data {
			if r == delimiter {
				delimiterCount++
				if delimiterCount == idx {
					bytePosBegin = i + utf8.RuneLen(delimiter)
				} else if delimiterCount == (idx + 1) {
					bytePosEnd = i
					break
				}
			}
		}
		if bytePosBegin == -1 {
			return
		}
	}

	offset = bytePosBegin
	length = bytePosEnd - bytePosBegin
	lane = true
	return
}

// TestSplitPartUT unit-tests for: opSplitPart
func TestSplitPartUT(t *testing.T) {
	t.Parallel()
	name := "split part (opSplitPart)"

	type unitTest struct {
		data      string
		idx       int
		delimiter rune
		expLane   bool // expected lane K1
		expOffset int  // expected offset Z2
		expLength int  // expected length Z3
	}
	unitTests := []unitTest{

		{"aa;bb", 0, ';', false, -1, -1}, // 0th part not present: offset and length are irrelevant
		{"aa;bb", 1, ';', true, 0, 2},    // select "aa"
		{"aa;bb", 2, ';', true, 3, 2},    // select "bb"
		{"aa;bb", 3, ';', false, -1, -1}, // 3rd part not present: offset and length are irrelevant

		{";bb", 0, ';', false, -1, -1}, // 0th part not present
		{";bb", 1, ';', true, 0, 0},    // select ""
		{";bb", 2, ';', true, 1, 2},    // select "bb"
		{";bb", 3, ';', false, -1, -1}, // 3rd part not present

		{";bbbbb", 0, ';', false, -1, -1}, // 0th part not present
		{";bbbbb", 1, ';', true, 0, 0},    // select ""
		{";bbbbb", 2, ';', true, 1, 5},    // select "bbbbb"
		{";bbbbb", 3, ';', false, -1, -1}, // 3rd part not present

		{"aa", 0, ';', false, -1, -1}, // 0th part not present
		{"aa", 1, ';', true, 0, 2},    // select "aa"
		{"aa", 2, ';', false, -1, -1}, // 2nd not present

		{"aa;", 0, ';', false, -1, -1}, // 0th part not present
		{"aa;", 1, ';', true, 0, 2},    // select "aa"
		{"aa;", 2, ';', true, 3, 0},    // select ""
		{"aa;", 3, ';', false, -1, -1}, // 3rd part not present

		{"aa;;", 0, ';', false, -1, -1}, // 0th part not present
		{"aa;;", 1, ';', true, 0, 2},    // select "aa"
		{"aa;;", 2, ';', true, 3, 0},    // select ""
		{"aa;;", 3, ';', true, 4, 0},    // select ""
		{"aa;;", 4, ';', false, -1, -1}, // 4th part not present

		{";", 0, ';', false, -1, -1}, // 0th part not present
		{";", 1, ';', true, 0, 0},    // select ""
		{";", 2, ';', true, 1, 0},    // select ""
		{";", 3, ';', false, -1, -1}, // 3rd part not present

		{"𐍈;bb", 1, ';', true, 0, 4}, // select "𐍈"
		{"𐍈;bb", 2, ';', true, 5, 2}, // select "bb"
		{"aa;𐍈", 1, ';', true, 0, 2}, // select "aa"
		{"aa;𐍈", 2, ';', true, 3, 4}, // select "𐍈"
	}

	run := func(ut unitTest) {
		// first: check reference implementation
		{
			obsLane, obsOffset, obsLength := referenceSplitPart(ut.data, ut.idx, ut.delimiter)
			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("refImpl: splitting %q; idx=%v; delim=%q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v",
					ut.data, ut.idx, ut.delimiter, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
			}
		}

		// second: check bytecode implementation
		stackContent := make([]uint64, 16)
		for i := 0; i < 16; i++ {
			stackContent[i] = uint64(ut.idx)
		}

		var ctx bctestContext
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], string(ut.delimiter))
		ctx.addScalarStrings(fill16(ut.data), []byte{})
		ctx.setStackUint64(stackContent)
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		offsetDict := uint16(0)
		offsetStackSlot := uint16(0)
		if err := ctx.Execute2Imm2(opSplitPart, offsetDict, offsetStackSlot); err != nil {
			t.Fatal(err)
		}

		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])

			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("%v\nlane %v: splitting %q; idx=%v; delim=%q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v",
					name, i, ut.data, ut.idx, ut.delimiter, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
				break
			}
		}
		ctx.Free()
	}

	t.Run(name, func(t *testing.T) {
		for _, ut := range unitTests {
			run(ut)
		}
	})
}

// TestSplitPartBF brute-force tests for: opSplitPart
func TestSplitPartBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// space of field indexes
		idxSpace []int
		// delimiter that separates fields (can only be ASCII)
		delimiter rune
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, idx, delimiter) -> lane, offset, length
		refImpl func(string, int, rune) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name:         "split part (opSplitPart)",
			dataAlphabet: []rune{'a', 'b', 0, ';'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			idxSpace:     []int{0, 1, 2, 3, 4, 5},
			delimiter:    ';',
			op:           opSplitPart,
			refImpl:      referenceSplitPart,
		},
		{
			name:         "split part (opSplitPart) UTF8",
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', ';'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			idxSpace:     []int{0, 1, 2, 3, 4, 5},
			delimiter:    ';',
			op:           opSplitPart,
			refImpl:      referenceSplitPart,
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		//TODO make a generic space partitioner that can be reused by other BF tests
		stackContent := make([]uint64, 16)

		for _, idx := range ts.idxSpace {
			for i := 0; i < 16; i++ {
				stackContent[i] = uint64(idx)
			}
			for _, data := range dataSpace {
				expLane, expOffset, expLength := ts.refImpl(data, idx, ts.delimiter)

				var ctx bctestContext
				ctx.Taint()
				ctx.dict = append(ctx.dict[:0], string(ts.delimiter))
				ctx.addScalarStrings(fill16(data), []byte{})
				ctx.setStackUint64(stackContent)
				ctx.current = 0xFFFF
				scalarBefore := ctx.getScalarUint32()

				// when
				offsetDict := uint16(0)
				offsetStackSlot := uint16(0)
				if err := ctx.Execute2Imm2(opSplitPart, offsetDict, offsetStackSlot); err != nil {
					t.Fatal(err)
				}

				// then
				scalarAfter := ctx.getScalarUint32()
				for i := 0; i < 16; i++ {
					obsLane := getBit(ctx.current, i)
					obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
					obsLength := int(scalarAfter[1][i])

					if fault1x1(obsLane, expLane, obsOffset, expOffset, obsLength, expLength) {
						t.Errorf("%v\nlane %v: splitting %q; idx=%v; delim=%q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
							ts.name, i, data, idx, ts.delimiter, obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
						break
					}
				}
				ctx.Free()
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSplitPartFT fuzz-tests for: opSplitPart
func FuzzSplitPartFT(f *testing.F) {
	f.Add(uint16(0xFFF0), "a", "a;b", "a;b;c", "", ";", "𐍈", "𐍈;𐍈", "𐍈;𐍈;", "a", "a;b", "a;b;c", "", ";", "𐍈", "𐍈;𐍈", "𐍈;𐍈;", 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, byte(';'))
	f.Add(uint16(0xFFFF), ";;;;;", "a", ";", "", "", "", "", "", "", "", "", "", "", "", "", "", 6, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(';'))

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, idx, delimiter) -> lane, offset, length
		refImpl func(string, int, rune) (bool, int, int)
	}
	testSuites := []testSuite{
		{
			name:    "split part (opSplitPart)",
			op:      opSplitPart,
			refImpl: referenceSplitPart,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, idx [16]int, delimiterByte byte) {
		if (delimiterByte == 0) || (delimiterByte >= 0x80) {
			return // delimiter can only be ASCII and not 0
		}
		delimiter := rune(delimiterByte)

		expLanes := uint16(0)
		expOffsets := [16]int{}
		expLengths := [16]int{}
		stackContent := make([]uint64, 16)

		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				var expLane bool
				expLane, expOffsets[i], expLengths[i] = ts.refImpl(data[i], idx[i], delimiter)
				expLanes = setBit(expLanes, i, expLane)
			}
			if idx[i] > 0 {
				stackContent[i] = uint64(idx[i])
			}
		}

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.dict = append(ctx.dict[:0], string(delimiter))
		ctx.setScalarStrings(data[:], []byte{})
		ctx.setStackUint64(stackContent)
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		offsetDict := uint16(0)
		offsetStackSlot := uint16(0)
		if err := ctx.Execute2Imm2(ts.op, offsetDict, offsetStackSlot); err != nil {
			t.Fatal(err)
		}
		// then
		obsLanes, obsOffsets, obsLengths := getObsValues(ctx, scalarBefore)
		if fault, msg := fault16x16(obsLanes, expLanes, obsOffsets, expOffsets, obsLengths, expLengths); fault {
			t.Errorf("%v\nlanes=%016b\ndata=%q\nidx=%v\ndelim=%q (0x%x)\n%v",
				ts.name, lanes, data, idx, delimiter, byte(delimiter), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16,
		d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string,
		s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int, delimiter byte) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		idx := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data, idx, delimiter)
		}
	})
}

// TestSplitPartUT unit-tests for: opLengthStr
func TestLengthStrUT(t *testing.T) {
	t.Parallel()
	name := "length string (bcLengthStr)"

	type unitTest struct {
		data     string
		expChars int // expected number of code-points in data
	}
	unitTests := []unitTest{
		{"", 0},
		{"a", 1},
		{"¢", 1},
		{"€", 1},
		{"𐍈", 1},
		{"ab", 2},
		{"a¢", 2},
		{"a€", 2},
		{"a𐍈", 2},
		{"abb", 3},
		{"ab¢", 3},
		{"ab€", 3},
		{"ab𐍈", 3},
		{"$¢€𐍈", 4},
		{string([]byte{0xC2, 0xA2, 0xC2, 0xA2, 0x24}), 3},
	}

	run := func(ut unitTest) {
		// first: check reference implementation
		{
			obsChars := utf8.RuneCountInString(ut.data)
			if ut.expChars != obsChars {
				t.Errorf("refImpl: length of %q; observed %v; expected: %v", ut.data, obsChars, ut.expChars)
			}
		}
		// second: check the bytecode implementation
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		ctx.setScalarStrings(fill16(ut.data), []byte{})
		ctx.current = 0xFFFF

		// when
		if err := ctx.Execute(opLengthStr); err != nil {
			t.Fatal(err)
		}
		// then
		scalarAfter := ctx.getScalarInt64()
		for i := 0; i < 16; i++ {
			nCharsObs := int(scalarAfter[i])
			if ut.expChars != nCharsObs {
				t.Errorf("lane %v: length of %q; observed %v; expected: %v", i, ut.data, nCharsObs, ut.expChars)
				break
			}
		}
	}

	t.Run(name, func(t *testing.T) {
		for _, ut := range unitTests {
			run(ut)
		}
	})
}

// TestSplitPartBF brute-force tests for: opLengthStr
func TestLengthStrBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate data
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data) -> expChars
		refImpl func(string) int
	}
	testSuites := []testSuite{
		{
			name:         "length string (bcLengthStr)",
			dataAlphabet: []rune{'a', 'b', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			op:           opLengthStr,
			refImpl:      utf8.RuneCountInString,
		},
		{
			name:         "length string (bcLengthStr) UTF8",
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			op:           opLengthStr,
			refImpl:      utf8.RuneCountInString,
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		//TODO make a generic space partitioner that can be reused by other BF tests

		var ctx bctestContext
		ctx.Taint()

		for _, data := range dataSpace {
			expChars := ts.refImpl(data)

			ctx.setScalarStrings(fill16(data), []byte{})
			ctx.current = 0xFFFF

			// when
			if err := ctx.Execute(ts.op); err != nil {
				t.Fatal(err)
			}
			// then
			scalarAfter := ctx.getScalarInt64()
			for i := 0; i < 16; i++ {
				obsChars := int(scalarAfter[i])
				if expChars != obsChars {
					t.Errorf("%v\nlane %v: length of %q; observed %v; expected: %v",
						ts.name, i, data, obsChars, expChars)
					break
				}
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzLengthStrFT fuzz-tests for: opLengthStr
func FuzzLengthStrFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "¢", "€", "𐍈", "ab", "a¢", "a€", "a𐍈", "abb", "ab¢", "ab€", "ab𐍈", "$¢€𐍈", "ab¢", "ab¢", "ab¢")

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data) -> nChars
		refImpl func(string) int
	}
	testSuites := []testSuite{
		{
			name:    "length string (opLengthStr)",
			op:      opLengthStr,
			refImpl: utf8.RuneCountInString,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string) {
		expLanes := lanes // counting code-points does not affect lanes death
		expLengths := [16]int{}
		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			expLengths[i] = ts.refImpl(data[i])
		}

		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		ctx.addScalarStrings(data[:], []byte{})
		ctx.current = lanes

		// when
		if err := ctx.Execute(ts.op); err != nil {
			t.Fatal(err)
		}
		// then
		scalarAfter := ctx.getScalarInt64()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			expLane := getBit(expLanes, i)
			obsLength := int(scalarAfter[i])

			if fault1x1(obsLane, expLane, 0, 0, obsLength, expLengths[i]) {
				t.Errorf("%v\nlane %v: length of %q; observed %v; expected: %v",
					ts.name, i, data[i], obsLength, expLengths[i])
				break
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data)
		}
	})
}

// TestTrimCharUT unit-tests for: bcTrim4charLeft, bcTrim4charRight
func TestTrimCharUT(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      [16]string // data at SI
		cutset    string     // characters to trim
		expResult [16]string // expected result in Z2:Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, cutset) -> result
		refImpl func(string, string) string
	}
	testSuites := []testSuite{
		{
			name: "trim char from left (opTrim4charLeft)",
			unitTests: []unitTest{
				{
					data:      [16]string{"ae", "eeeeef", "e", "b", "e¢€𐍈", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d"},
					expResult: [16]string{"ae", "f", "", "b", "¢€𐍈", "b", "c", "", "a", "b", "c", "", "a", "b", "c", ""},
					cutset:    "ed", //FIXME cutset with non-ascii not supported
				},
				{
					data:      [16]string{"0", "0", "0", "0", "0", "a", "0", "0", "0", "0", "0", "0", "", "0", "0", "0"},
					expResult: [16]string{"0", "0", "0", "0", "0", "", "0", "0", "0", "0", "0", "0", "", "0", "0", "0"},
					cutset:    "abc;",
				},
			},
			op:      opTrim4charLeft,
			refImpl: strings.TrimLeft,
		},
		{
			name: "trim char from right (opTrim4charRight)",
			unitTests: []unitTest{
				{
					data:      [16]string{"ae", "feeeee", "e", "b", "¢€𐍈e", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d"},
					expResult: [16]string{"a", "f", "", "b", "¢€𐍈", "b", "c", "", "a", "b", "c", "", "a", "b", "c", ""},
					cutset:    "ed", //FIXME cutset with non-ascii not supported
				},
			},
			op:      opTrim4charRight,
			refImpl: strings.TrimRight,
		},
	}

	run := func(ts *testSuite, ut unitTest) {
		// first: check reference implementation
		for i := 0; i < 16; i++ {
			obsResult := ts.refImpl(ut.data[i], ut.cutset)
			if ut.expResult[i] != obsResult {
				t.Errorf("refImpl: lane %v: trim %q; cutset %q: observed %q; expected: %q",
					i, ut.data[i], ut.cutset, obsResult, ut.expResult[i])
			}
		}
		// second: check the bytecode implementation
		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrim4charRight
		ctx.dict = append(ctx.dict[:0], fill4(ut.cutset))
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(ut.data[:], []byte{})
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])
			obsResult := ut.data[i][obsOffset : obsOffset+obsLength]

			if ut.expResult[i] != obsResult {
				t.Errorf("%v\nlane %v: trim %q; cutset %q: observed %q; expected: %q",
					ts.name, i, ut.data, ut.cutset, obsResult, ut.expResult)
				break
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, ut)
			}
		})
	}
}

// TestTrimCharBF brute-force for: bcTrim4charLeft, bcTrim4charRight
func TestTrimCharBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet, cutsetAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, cutsetLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize, cutsetMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, cutset) -> string
		refImpl func(string, string) string
	}
	testSuites := []testSuite{
		{
			name:           "trim char from left (opTrim4charLeft)",
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'},
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
			op:             opTrim4charLeft,
			refImpl:        strings.TrimLeft,
		},
		{
			name:           "trim char from left (opTrim4charLeft) UTF8",
			dataAlphabet:   []rune{'a', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'}, //FIXME cutset can only be ASCII
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
			op:             opTrim4charLeft,
			refImpl:        strings.TrimLeft,
		},
		{
			name:           "trim char from right (opTrim4charRight)",
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'},
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
			op:             opTrim4charRight,
			refImpl:        strings.TrimRight,
		},
		{
			name:           "trim char from right (opTrim4charRight) UTF8",
			dataAlphabet:   []rune{'a', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'}, //FIXME cutset can only be ASCII
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
			op:             opTrim4charRight,
			refImpl:        strings.TrimRight,
		},
	}

	run := func(ts *testSuite, dataSpace, cutsetSpace []string) {
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrim4charRight

		var ctx bctestContext
		ctx.Taint()

		for _, data := range dataSpace {
			data16 := fill16(data)

			for _, cutset := range cutsetSpace {
				expResult := ts.refImpl(data, cutset) // expected result

				ctx.dict = append(ctx.dict[:0], fill4(cutset))
				ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
				ctx.addScalarStrings(data16, []byte{})
				ctx.current = 0xFFFF
				scalarBefore := ctx.getScalarUint32()

				// when
				if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
					t.Fatal(err)
				}
				// then
				scalarAfter := ctx.getScalarUint32()
				for i := 0; i < 16; i++ {
					obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
					obsLength := int(scalarAfter[1][i])
					obsResult := data[obsOffset : obsOffset+obsLength] // observed result

					if expResult != obsResult {
						t.Errorf("%v\nlane %v: trim %q; cutset %q: observed %q; expected: %q",
							ts.name, i, data, cutset, obsResult, expResult)
						break
					}
				}
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			cutsetSpace := createSpace(ts.cutsetLenSpace, ts.cutsetAlphabet, ts.cutsetMaxSize)
			run(&ts, dataSpace, cutsetSpace)
		})
	}
}

// FuzzTrimCharFT fuzz-tests for: bcTrim4charLeft, bcTrim4charRight
func FuzzTrimCharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "ab", "ac", "da", "ea", "fa", "ag", "ha", "ia", "ja", "ka", "a", "a¢€𐍈", "a", "a", "a", byte('a'), byte('b'), byte('c'), byte(';'))
	f.Add(uint16(0xFFFF), "0", "0", "0", "0", "0", "a", "0", "0", "0", "0", "0", "0", "", "0", "0", "0", byte('a'), byte('b'), byte('c'), byte(';'))

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, cutset) -> string
		refImpl func(string, string) string
	}
	testSuites := []testSuite{
		{
			name:    "trim char from left (opTrim4charLeft)",
			op:      opTrim4charLeft,
			refImpl: strings.TrimLeft,
		},
		{
			name:    "trim char from right (opTrim4charRight)",
			op:      opTrim4charRight,
			refImpl: strings.TrimRight,
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, cutset string) {
		for _, c := range cutset {
			if c >= utf8.RuneSelf {
				return //FIXME cutset does not yet support non-ASCII
			}
		}
		expResults := [16]string{}
		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				expResults[i] = ts.refImpl(data[i], cutset) // expected result
			}
		}

		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrim4charRight
		ctx.dict = append(ctx.dict[:0], cutset)
		ctx.setData(dataPrefix) // prepend 4 bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(data[:], []byte{})
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Fatal(err)
		}

		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			expLane := getBit(lanes, i)

			if obsLane || expLane {
				obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
				obsLength := int(scalarAfter[1][i])
				obsResult := data[i][obsOffset : obsOffset+obsLength] // observed result

				if expResults[i] != obsResult {
					t.Errorf("%v\nlane %v: trim %q; cutset %q: observed %q; expected: %q",
						ts.name, i, data, cutset, obsResult, expResults[i])
					break
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, char1, char2, char3, char4 byte) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		cutset := string([]byte{char1, char2, char3, char4})
		for _, ts := range testSuites {
			run(t, &ts, lanes, data, cutset)
		}
	})
}

// TestTrimWhiteSpaceUT unit-tests for: opTrimWsLeft, opTrimWsRight
func TestTrimWhiteSpaceUT(t *testing.T) {
	t.Parallel()

	//FIXME: currently only ASCII whitespace chars are supported, not U+0085 (NEL), U+00A0 (NBSP)
	whiteSpace := string([]byte{'\t', '\n', '\v', '\f', '\r', ' '})

	type unitTest struct {
		data      string // data at SI
		expResult string // expected result Z2:Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// bytecode to run
		op bcop
		// portable reference implementation: f(data) -> result
		refImpl func(string) string
	}
	testSuites := []testSuite{
		{
			name: "trim white-space from left (opTrimWsLeft)",
			unitTests: []unitTest{
				{"a", "a"},
				{" a", "a"},
				{" a ", "a "},
				{" a a", "a a"},
				{"  a", "a"},
				{"     a", "a"},
				{" €", "€"},
			},
			op: opTrimWsLeft,
			refImpl: func(data string) string {
				return strings.TrimLeft(data, whiteSpace)
			},
		},
		{
			name: "trim white-space from right (opTrimWsRight)",
			unitTests: []unitTest{
				{"a", "a"},
				{"a ", "a"},
				{" a ", " a"},
				{"a a ", "a a"},
				{"a  ", "a"},
				{"a     ", "a"},
				{"€ ", "€"},
			},
			op: opTrimWsRight,
			refImpl: func(data string) string {
				return strings.TrimRight(data, whiteSpace)
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest) {
		// first: check reference implementation
		{
			resultObs := ts.refImpl(ut.data)
			if ut.expResult != resultObs {
				t.Errorf("refImpl: trim %q; observed %q; expected: %q", ut.data, resultObs, ut.expResult)
			}
		}
		// second: check the bytecode implementation
		var ctx bctestContext
		ctx.Taint()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrimWsRight
		ctx.setData(dataPrefix)                  // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(fill16(ut.data), []byte{})
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.Execute(ts.op); err != nil {
			t.Error(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])
			resultObs := ut.data[obsOffset : obsOffset+obsLength]

			if ut.expResult != resultObs {
				t.Errorf("lane %v: trim %q; observed %q; expected: %q", i, ut.data, resultObs, ut.expResult)
				break
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestTrimWhiteSpaceBF brute-force for: opTrimWsLeft, opTrimWsRight
func TestTrimWhiteSpaceBF(t *testing.T) {
	t.Parallel()
	//FIXME: currently only ASCII whitespace chars are supported, not U+0085 (NEL), U+00A0 (NBSP)
	whiteSpace := string([]byte{'\t', '\n', '\v', '\f', '\r', ' '})

	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data) -> string
		refImpl func(string) string
	}
	testSuites := []testSuite{
		{
			name:         "trim whitespace from left (opTrimWsLeft)",
			dataAlphabet: []rune{'a', '¢', '\t', '\n', '\v', '\f', '\r', ' '},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
			op:           opTrimWsLeft,
			refImpl: func(data string) string {
				return strings.TrimLeft(data, whiteSpace)
			},
		},
		{
			name:         "trim whitespace from right (opTrimWsRight)",
			dataAlphabet: []rune{'a', '¢', '\t', '\n', '\v', '\f', '\r', ' '},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
			op:           opTrimWsRight,
			refImpl: func(data string) string {
				return strings.TrimRight(data, whiteSpace)
			},
		},
	}

	run := func(ts *testSuite, dataSpace []string) {
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrimWsRight

		var ctx bctestContext
		ctx.Taint()

		for _, data := range dataSpace {
			expResult := ts.refImpl(data) // expected result

			ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
			ctx.addScalarStrings(fill16(data), []byte{})
			ctx.current = 0xFFFF
			scalarBefore := ctx.getScalarUint32()

			// when
			if err := ctx.Execute(ts.op); err != nil {
				t.Fatal(err)
			}
			// then
			scalarAfter := ctx.getScalarUint32()
			for i := 0; i < 16; i++ {
				obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
				obsLength := int(scalarAfter[1][i])
				resultObs := data[obsOffset : obsOffset+obsLength] // observed result

				if expResult != resultObs {
					t.Errorf("%v\nlane %v: trim %q: observed %q; expected: %q",
						ts.name, i, data, resultObs, expResult)
					break
				}
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			run(&ts, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzTrimWhiteSpaceFT fuzz tests for: opTrimWsLeft, opTrimWsRight
func FuzzTrimWhiteSpaceFT(f *testing.F) {
	//FIXME: currently only ASCII whitespace chars are supported, not U+0085 (NEL), U+00A0 (NBSP)
	whiteSpace := string([]byte{'\t', '\n', '\v', '\f', '\r', ' '})

	f.Add(uint16(0xFFFF), "a", "¢", "€", " 𐍈", "ab", "a¢ ", "a€", "a𐍈", "abb", " ab¢", "ab€", "ab𐍈\t", "\v$¢€𐍈", "\nab¢", "\fab¢", "\rab¢ ")

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data) -> string
		refImpl func(string) string
	}
	testSuites := []testSuite{
		{
			name: "trim char from left (opTrim4charLeft)",
			op:   opTrimWsLeft,
			refImpl: func(data string) string {
				return strings.TrimLeft(data, whiteSpace)
			},
		},
		{
			name: "trim char from left (opTrim4charLeft)",
			op:   opTrimWsRight,
			refImpl: func(data string) string {
				return strings.TrimRight(data, whiteSpace)
			},
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string) {
		expLanes := lanes
		expResult := [16]string{}
		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				expResult[i] = ts.refImpl(data[i])
			}
		}

		var ctx bctestContext
		ctx.Taint()
		defer ctx.Free()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opTrim4charRight
		ctx.setData(dataPrefix)                  // prepend 4 bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(data[:], []byte{})
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.Execute(ts.op); err != nil {
			t.Fatal(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			expLane := getBit(expLanes, i)

			if (obsLane == expLane) && (obsLane || expLane) {
				obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
				obsLength := int(scalarAfter[1][i])
				obsResult := data[i][obsOffset : obsOffset+obsLength] // observed result

				if expResult[i] != obsResult {
					t.Errorf("%v\nlane %v: trim %q; observed %q; expected: %q",
						ts.name, i, data, obsResult, expResult)
					break
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, ts := range testSuites {
			run(t, &ts, lanes, data)
		}
	})
}

func refContainsPrefix(s, prefix string, caseSensitive, ASCIIOnly bool) (lane bool, offset, length int) {
	if prefix == "" { //NOTE: empty needles are dead lanes
		return false, 0, len(s)
	}
	hasPrefix := false
	if caseSensitive {
		hasPrefix = strings.HasPrefix(s, prefix)
	} else if ASCIIOnly {
		hasPrefix = strings.HasPrefix(stringext.NormalizeStringASCIIOnly(s), stringext.NormalizeStringASCIIOnly(prefix))
	} else {
		hasPrefix = strings.HasPrefix(stringext.NormalizeString(s), stringext.NormalizeString(prefix))
	}
	if hasPrefix {
		nRunesPrefix := utf8.RuneCountInString(prefix)
		nBytesPrefix2 := len(string([]rune(s)[:nRunesPrefix]))
		return true, nBytesPrefix2, len(s) - nBytesPrefix2
	}
	return false, 0, len(s)
}

func refContainsSuffix(s, suffix string, caseSensitive, ASCIIOnly bool) (lane bool, offset, length int) {
	if suffix == "" { //NOTE: empty needles are dead lanes
		return false, 0, len(s)
	}
	hasSuffix := false
	if caseSensitive {
		hasSuffix = strings.HasSuffix(s, suffix)
	} else if ASCIIOnly {
		hasSuffix = strings.HasSuffix(stringext.NormalizeStringASCIIOnly(s), stringext.NormalizeStringASCIIOnly(suffix))
	} else {
		hasSuffix = strings.HasSuffix(stringext.NormalizeString(s), stringext.NormalizeString(suffix))
	}
	if hasSuffix {
		nRunesSuffix := utf8.RuneCountInString(suffix)
		sRunes := []rune(s)
		nBytesSuffix2 := len(string(sRunes[(len(sRunes) - nRunesSuffix):]))
		return true, 0, len(s) - nBytesSuffix2
	}
	return false, 0, len(s)
}

// TestContainsPrefixSuffixUT unit-tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func TestContainsPrefixSuffixUT(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      string // data at SI
		needle    string // prefix/suffix to test
		expLane   bool   // expected K1
		expOffset int    // expected Z2
		expLength int    // expected Z3
	}
	type testSuite struct {
		// name to describe this test-suite
		name string
		// the actual tests to run
		unitTests []unitTest
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, needle, caseSensitive) -> (lane, offset, length)
		refImpl func(string, string) (bool, int, int)
		// encoder for needle -> dictionary value
		encode func(needle string) string
	}

	testSuites := []testSuite{
		{
			name: "contains prefix case-sensitive (opContainsPrefixCs)",
			unitTests: []unitTest{
				{"s", "s", true, 1, 0},
				{"sb", "s", true, 1, 1},
				{"s", "", false, 0, 1},
				{"", "", false, 0, 0},
				{"ssss", "ssss", true, 4, 0},
				{"sssss", "sssss", true, 5, 0},
				{"ss", "b", false, 0, 2},
			},
			op:      opContainsPrefixCs,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, true, true) },
			encode:  func(needle string) string { return needle },
		},
		{
			name: "contains prefix case-insensitive (opContainsPrefixCi)",
			unitTests: []unitTest{
				{"Sb", "s", true, 1, 1},
				{"sb", "S", true, 1, 1},
				{"S", "s", true, 1, 0},
				{"s", "S", true, 1, 0},
				{"s", "", false, 0, 1},
				{"", "", false, 0, 0},
				{"sSsS", "ssss", true, 4, 0},
				{"sSsSs", "sssss", true, 5, 0},
				{"ss", "b", false, 0, 2},
			},
			op:      opContainsPrefixCi,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, true) },
			encode:  stringext.NormalizeString,
		},
		{
			name: "contains prefix case-insensitive (opContainsPrefixUTF8Ci)",
			unitTests: []unitTest{
				{"sſsSa", "ssss", true, 5, 1},
				{"ssss", "ssss", true, 4, 0},
				{"abc", "abc", true, 3, 0},
				{"abcd", "abcd", true, 4, 0},
				{"a", "aa", false, 1, 1},
				{"aa", "a", true, 1, 1},
				{"ſb", "s", true, 2, 1},
				{"sb", "ſ", true, 1, 1},
				{"ſ", "s", true, 2, 0},
				{"s", "ſ", true, 1, 0},
				{"s", "", false, 0, 1}, //NOTE: empty needles are dead lanes
				{"", "", false, 0, 0},  //NOTE: empty needles are dead lanes
				{"sſsſ", "ssss", true, 6, 0},
				{"sſsſs", "sssss", true, 7, 0},
				{"ss", "b", false, 0, 2},
				{"a", "a\x00\x00\x00", false, 0, 1},
			},
			op:      opContainsPrefixUTF8Ci,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, false) },
			encode:  func(needle string) string { return stringext.GenNeedleExt(needle, false) },
		},
		{
			name: "contains suffix case-sensitive (opContainsSuffixCs)",
			unitTests: []unitTest{
				{"ab", "b", true, 0, 1},
				{"a", "a", true, 0, 0},
				//FIXME{"a", "", false, 0, 1},// Empty needle should yield failing match
				//FIXME{"", "", false, 0, 0},// Empty needle should yield failing match
				{"aaaa", "aaaa", true, 0, 0}, // offset and length are not updated when needle has length 4
				{"aaaaa", "aaaaa", true, 0, 0},
				{"aa", "b", false, 0, 2},
			},
			op:      opContainsSuffixCs,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, true, true) },
			encode:  func(needle string) string { return needle },
		},
		{
			name: "contains suffix case-insensitive (opContainsSuffixCi)",
			unitTests: []unitTest{
				{"aB", "b", true, 0, 1},
				{"ab", "B", true, 0, 1},
				{"A", "a", true, 0, 0},
				{"a", "A", true, 0, 0},
				//FIXME{"a", "", false, 0, 1},// Empty needle should yield failing match
				//FIXME{"", "", false, 0, 0},// Empty needle should yield failing match
				{"aAaA", "aaaa", true, 0, 0}, // offset and length are not updated when needle has length 4
				{"aAaAa", "aaaaa", true, 0, 0},
				{"aa", "b", false, 0, 2},
			},
			op:      opContainsSuffixCi,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, true) },
			encode:  stringext.NormalizeString,
		},
		{
			name: "contains suffix case-insensitive (opContainsSuffixUTF8Ci)",
			unitTests: []unitTest{
				{"0", "0\x00\x00\x00", false, 0, 1},
				{"sss", "ſss", true, 0, 0},
				{"abcd", "abcd", true, 0, 0},
				{"ſ", "ss", false, 0, 0},
				{"a", "a", true, 0, 0},
				{"bſ", "s", true, 0, 1},
				{"bs", "ſ", true, 0, 1},
				{"ſ", "s", true, 0, 0},
				{"ſ", "as", false, 0, 0},
				{"s", "ſ", true, 0, 0},
				{"sſs", "ss", true, 0, 1},
				{"sſss", "sss", true, 0, 1},
				{"ssss", "ssss", true, 0, 0},
				{"sssss", "ssss", true, 0, 1},
				{"ſssss", "ssss", true, 0, 2}, //NOTE 'ſ' is 2 bytes
				{"sſsss", "ssss", true, 0, 1},
				{"ssſss", "ssss", true, 0, 1},
				{"s", "", false, 0, 1}, //NOTE: empty needles are dead lanes
				{"", "", false, 0, 0},  //NOTE: empty needles are dead lanes
				{"ss", "b", false, 0, 2},
				{"a", "a\x00\x00\x00", false, 0, 1},
				{"0", "\xff\xff\x00\x00\x00\x00aaaa", false, 0, 0}, // read beyond data length test
			},
			op:      opContainsSuffixUTF8Ci,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, false) },
			encode:  func(needle string) string { return stringext.GenNeedleExt(needle, true) },
		},
	}

	run := func(ts *testSuite, ut *unitTest) {
		// first: check reference implementation
		{
			obsLane, obsOffset, obsLength := ts.refImpl(ut.data, ut.needle)
			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("%v\nrefImpl: data %q contains needle %q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
					ts.name, ut.data, ut.needle, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
			}
		}
		// second: check the bytecode implementation
		var ctx bctestContext
		ctx.Taint()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opContainsSuffixUTF8Ci
		ctx.dict = append(ctx.dict[:0], pad(ts.encode(ut.needle)))
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(fill16(ut.data), []byte{})
		ctx.current = 0xFFFF
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		scalarAfter := ctx.getScalarUint32()
		for i := 0; i < 16; i++ {
			obsLane := getBit(ctx.current, i)
			obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
			obsLength := int(scalarAfter[1][i])

			if fault1x1(obsLane, ut.expLane, obsOffset, ut.expOffset, obsLength, ut.expLength) {
				t.Errorf("%v\nlane %v: data %q contains needle %q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
					ts.name, i, ut.data, ut.needle, obsLane, obsOffset, obsLength, ut.expLane, ut.expOffset, ut.expLength)
				break
			}
		}
		ctx.Free()
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut)
			}
		})
	}
}

// TestContainsPrefixSuffixBF brute-force tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func TestContainsPrefixSuffixBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		name string
		// alphabet from which to generate needles and patterns
		dataAlphabet, needleAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, needleLenSapce []int
		// maximum number of elements in dataSpace
		dataMaxSize, needleMaxSize int
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, needle) -> lane, offset, length
		refImpl func(string, string) (bool, int, int)
		// encoder for needle -> dictionary value
		encode func(needle string) string
	}
	testSuites := []testSuite{
		{
			name:           "contains prefix case-sensitive (opContainsPrefixCs)",
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 'b'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsPrefixCs,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, true, true) },
			encode:         func(needle string) string { return needle },
		},
		{
			name:           "contains prefix case-insensitive (opContainsPrefixCi)",
			dataAlphabet:   []rune{'a', 's', 'S'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 's', 'S'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsPrefixCi,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, true) },
			encode:         stringext.NormalizeString,
		},
		{
			name:           "contains prefix case-insensitive UTF8 (opContainsPrefixUTF8Ci)",
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsPrefixUTF8Ci,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, false) },
			encode:         func(needle string) string { return stringext.GenNeedleExt(needle, false) },
		},
		{
			name:           "contains prefix case-insensitive UTF8 (opContainsPrefixUTF8Ci)",
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			dataMaxSize:    1000,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSapce: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			needleMaxSize:  500,
			op:             opContainsPrefixUTF8Ci,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, false) },
			encode:         func(needle string) string { return stringext.GenNeedleExt(needle, false) },
		},
		{
			name:           "contains suffix case-sensitive (opContainsSuffixCs)",
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 'b'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsSuffixCs,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, true, true) },
			encode:         func(needle string) string { return needle },
		},
		{
			name:           "contains suffix case-insensitive (opContainsSuffixCi)",
			dataAlphabet:   []rune{'a', 's', 'S'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 's', 'S'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsSuffixCi,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, true) },
			encode:         stringext.NormalizeString,
		},
		{
			name:           "contains suffix case-insensitive UTF8 (opContainsSuffixUTF8Ci)",
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSapce: []int{1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
			op:             opContainsSuffixUTF8Ci,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, false) },
			encode:         func(needle string) string { return stringext.GenNeedleExt(needle, true) },
		},
		{
			name:           "contains suffix case-insensitive UTF8 (opContainsSuffixUTF8Ci)",
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			dataMaxSize:    500,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSapce: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			needleMaxSize:  1000,
			op:             opContainsSuffixUTF8Ci,
			refImpl:        func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, false) },
			encode:         func(needle string) string { return stringext.GenNeedleExt(needle, true) },
		},
	}

	run := func(ts *testSuite, dataSpace, needleSpace []string) {
		encNeedleSpace := make([]string, len(needleSpace))
		for i, needle := range needleSpace { // precompute encoded needles for speed
			encNeedleSpace[i] = pad(ts.encode(needle))
		}

		for _, data := range dataSpace {
			data16 := fill16(data)
			for i, needle := range needleSpace {
				expLane, expOffset, expLength := ts.refImpl(data, needle) // expected result

				var ctx bctestContext
				ctx.Taint()
				dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opContainsSuffixUTF8Ci
				ctx.dict = append(ctx.dict[:0], encNeedleSpace[i])
				ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
				ctx.addScalarStrings(data16, []byte{})
				ctx.current = 0xFFFF
				scalarBefore := ctx.getScalarUint32()

				// when
				if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
					t.Fatal(err)
				}
				// then
				scalarAfter := ctx.getScalarUint32()
				for i := 0; i < 16; i++ {
					obsLane := getBit(ctx.current, i)
					obsOffset := int(scalarAfter[0][i] - scalarBefore[0][i]) // NOTE the reference implementation returns offset starting from zero
					obsLength := int(scalarAfter[1][i])

					if fault1x1(obsLane, expLane, obsOffset, expOffset, obsLength, expLength) {
						t.Errorf("%v\nlane %v: data %q contains needle %q; observed (lane; offset; length) %v, %v, %v; expected: %v, %v, %v)",
							ts.name, i, data, needle, obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
						break
					}
				}
				ctx.Free()
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(ts.name, func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			needleSpace := createSpace(ts.needleLenSapce, ts.needleAlphabet, ts.needleMaxSize)
			run(&ts, dataSpace, needleSpace)
		})
	}
}

// FuzzContainsPrefixSuffixFT fuzz-tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func FuzzContainsPrefixSuffixFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "a")
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "𐍈")
	f.Add(uint16(0xFFFF), "M", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "м")

	type testSuite struct {
		name string
		// bytecode to run
		op bcop
		// portable reference implementation: f(data, needle) -> lane, offset, length
		refImpl func(string, string) (bool, int, int)
		// encoder for needle -> dictionary value
		encode func(needle string) string
	}
	testSuites := []testSuite{
		{
			name:    "contains prefix case-sensitive (opContainsPrefixCs)",
			op:      opContainsPrefixCs,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, true, true) },
			encode:  func(needle string) string { return needle },
		},
		{
			name:    "contains prefix case-insensitive (opContainsPrefixCi)",
			op:      opContainsPrefixCi,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, true) },
			encode:  stringext.NormalizeString,
		},
		{
			name:    "contains prefix case-insensitive UTF8 (opContainsPrefixUTF8Ci)",
			op:      opContainsPrefixUTF8Ci,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsPrefix(data, needle, false, false) },
			encode:  func(needle string) string { return stringext.GenNeedleExt(needle, false) },
		},
		{
			name:    "contains suffix case-sensitive (opContainsSuffixCs)",
			op:      opContainsSuffixCs,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, true, true) },
			encode:  func(needle string) string { return needle },
		},
		{
			name:    "contains suffix case-insensitive (opContainsSuffixCi)",
			op:      opContainsSuffixCi,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, true) },
			encode:  stringext.NormalizeString,
		},
		{
			name:    "contains suffix case-insensitive UTF8 (opContainsSuffixUTF8Ci)",
			op:      opContainsSuffixUTF8Ci,
			refImpl: func(data, needle string) (bool, int, int) { return refContainsSuffix(data, needle, false, false) },
			encode:  func(needle string) string { return stringext.GenNeedleExt(needle, true) },
		},
	}

	run := func(t *testing.T, ts *testSuite, lanes uint16, data [16]string, cutset string) {
		if cutset == "" {
			return // empty cutset is invalid
		}
		// only UTF8 code is supposed to handle UTF8 cutset data
		if (ts.op != opContainsPrefixUTF8Ci) && (ts.op != opContainsSuffixUTF8Ci) {
			for _, c := range cutset {
				if c >= utf8.RuneSelf {
					return
				}
			}
		}

		expLanes := uint16(0)
		expOffsets := [16]int{}
		expLengths := [16]int{}

		for i := 0; i < 16; i++ {
			if !utf8.ValidString(data[i]) {
				return // assume all input data will be valid codepoints
			}
			if getBit(lanes, i) {
				var expLane bool
				expLane, expOffsets[i], expLengths[i] = ts.refImpl(data[i], cutset)
				expLanes = setBit(expLanes, i, expLane)
			}
		}

		var ctx bctestContext
		defer ctx.Free()
		ctx.Taint()
		dataPrefix := string([]byte{0, 0, 0, 0}) // Necessary for opContainsSuffixUTF8Ci
		ctx.dict = append(ctx.dict[:0], pad(ts.encode(cutset)))
		ctx.setData(dataPrefix) // prepend three bytes to data such that we can read backwards 4bytes at a time
		ctx.addScalarStrings(data[:], []byte{})
		ctx.current = lanes
		scalarBefore := ctx.getScalarUint32()

		// when
		if err := ctx.ExecuteImm2(ts.op, 0); err != nil {
			t.Error(err)
		}
		// then
		obsLanes, obsOffsets, obsLengths := getObsValues(ctx, scalarBefore)
		if fault, msg := fault16x16(obsLanes, expLanes, obsOffsets, expOffsets, obsLengths, expLengths); fault {
			t.Errorf("%v\nlanes=%016b\ndata=%q\ncutset=%q\n%v",
				ts.name, lanes, data, cutset, msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, cutset string) {
		for _, ts := range testSuites {
			data := [16]string{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
			run(t, &ts, lanes, data, cutset)
		}
	})
}

func TestBytecodeAbsInt(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

/////////////////////////////////////////////////////////////
// Helper functions

func toStrIP4(v uint32) string {
	return fmt.Sprintf("%v.%v.%v.%v", (v>>(3*8))&0xFF, (v>>(2*8))&0xFF, (v>>(1*8))&0xFF, (v>>(0*8))&0xFF)
}

func toArrayIP4(v uint32) [4]byte {
	return [4]byte{byte(v >> (3 * 8)), byte(v >> (2 * 8)), byte(v >> (1 * 8)), byte(v >> (0 * 8))}
}

func getBit(data uint16, idx int) bool {
	return (data>>idx)&1 == 1
}

func setBit(data uint16, idx int, value bool) uint16 {
	if value {
		return data | (uint16(1) << idx)
	}
	return data
}

func padNBytes(s string, nBytes int) string {
	buf := []byte(s + strings.Repeat(string([]byte{0}), nBytes))
	return string(buf)[:len(s)]
}

func fill16(msg string) (result []string) {
	result = make([]string, 16)
	for i := 0; i < 16; i++ {
		result[i] = msg
	}
	return
}

func fill4(cutset string) string {

	cutsetRunes := []rune(cutset)
	switch len(cutsetRunes) {
	case 0:
		panic("cutset cannot be empty")
	case 1:
		r0 := cutsetRunes[0]
		return string([]rune{r0, r0, r0, r0})
	case 2:
		r0 := cutsetRunes[0]
		r1 := cutsetRunes[1]
		return string([]rune{r0, r1, r1, r1})
	case 3:
		r0 := cutsetRunes[0]
		r1 := cutsetRunes[1]
		r2 := cutsetRunes[2]
		return string([]rune{r0, r1, r2, r2})
	case 4:
		return cutset
	default:
		panic("cutset larger than 4 not supported")
	}
}

func fault16x16(obsLanes, expLanes uint16, obsOffsets, expOffsets, obsLengths, expLengths [16]int) (bool, string) {
	for i := 0; i < 16; i++ {
		if fault1x1(getBit(obsLanes, i), getBit(expLanes, i), obsOffsets[i], expOffsets[i], obsLengths[i], expLengths[i]) {
			return true, fmt.Sprintf("issue with lane %v:\nobserved lanes=0b%016b, offset=%v, length=%v\nexpected lanes=0b%016b, offset=%v, length=%v",
				i, obsLanes, obsOffsets, obsLengths, expLanes, expOffsets, expLengths)
		}
	}
	return false, ""
}

func fault16x1(obsLanes uint16, expLane bool, obsOffsets [16]int, expOffset int, obsLengths [16]int, expLength int) bool {
	for i := 0; i < 16; i++ {
		if fault1x1(getBit(obsLanes, i), expLane, obsOffsets[i], expOffset, obsLengths[i], expLength) {
			return true
		}
	}
	return false
}

func fault1x1(obsLane, expLane bool, obsOffset, expOffset, obsLength, expLength int) bool {
	if obsLane { // if the observed lane is active, all fields need to be equal
		return (obsLane != expLane) || (obsOffset != expOffset) || (obsLength != expLength)
	}
	// when the observed lane is dead, the expected lane should also be dead
	return expLane
}

func getObsValues(ctx bctestContext, initialScalar [2][16]uint32) (lanes uint16, offset, length [16]int) {
	scalarAfter := ctx.getScalarUint32()
	lanes = ctx.current
	for i := 0; i < 16; i++ {
		offset[i] = int(scalarAfter[0][i]) - int(initialScalar[0][i]) // NOTE the reference implementation returns offset starting from zero
		length[i] = int(scalarAfter[1][i])
	}
	return
}

// referenceSkipCharLeft skips n code-point from msg; valid is true if successful, false if provided string is not UTF-8
func referenceSkipCharLeft(msg string, skipCount int) (laneOut bool, offsetOut, lengthOut int) {
	if skipCount < 0 {
		skipCount = 0
	}
	length := len(msg)
	if !utf8.ValidString(msg) {
		panic("invalid msg provided")
	}
	laneOut = true
	nRunes := utf8.RuneCountInString(msg)
	nRunesToRemove := skipCount
	if nRunesToRemove > nRunes {
		nRunesToRemove = nRunes
		laneOut = false
	}
	strToRemove := string([]rune(msg)[:nRunesToRemove])
	nBytesToSkip := len(strToRemove)
	if nBytesToSkip > length {
		nBytesToSkip = length
	}
	offsetOut = nBytesToSkip
	lengthOut = length - nBytesToSkip
	return
}

// referenceSkipCharRight skips n code-point from msg; valid is true if successful, false if provided string is not UTF-8
func referenceSkipCharRight(msg string, skipCount int) (laneOut bool, offsetOut, lengthOut int) {
	if skipCount < 0 {
		skipCount = 0
	}
	length := len(msg)
	if !utf8.ValidString(msg) {
		panic("invalid msg provided")
	}
	laneOut = true
	nRunes := utf8.RuneCountInString(msg)

	nRunesToRemove := skipCount
	if nRunesToRemove > nRunes {
		nRunesToRemove = nRunes
		laneOut = false
	}
	nRunesToKeep := nRunes - nRunesToRemove

	strToRemove := string([]rune(msg)[nRunesToKeep:])
	nBytesToSkip := len(strToRemove)
	if nBytesToSkip > length {
		nBytesToSkip = length
	}
	offsetOut = 0
	lengthOut = length - nBytesToSkip
	return
}

// matchPatternReference matches the first occurrence of the provided pattern.
// The matchPatternReference implementation is used for fuzzing since it is 10x faster than matchPatternRegex
func matchPatternReference(msg []byte, offset, length int, segments []string, caseSensitive bool) (laneOut bool, offsetOut, lengthOut int) {

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

	if len(segments) == 0 { // not sure how to handle an empty pattern, currently it always matches
		return true, offset, length
	}
	msgStrOrg := string(stringext.ExtractFromMsg(msg, offset, length))
	msgStr := msgStrOrg

	if !caseSensitive { // normalize msg and pattern to make case-insensitive comparison possible
		msgStr = stringext.NormalizeString(msgStrOrg)
		for i, segment := range segments {
			segments[i] = stringext.NormalizeString(segment)
		}
	}
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

// runRegexTests iterates over all regexes with the provided regex space,and determines equality over all
// needles from the provided data space
func runRegexTests(t *testing.T, name string, dataSpace, regexSpace []string, regexType regexp2.RegexType, writeDot bool) {

	var ctx bctestContext
	ctx.Taint()
	defer ctx.Free()

	for _, regexStr := range regexSpace {
		ds, err := regexp2.CreateDs(regexStr, regexType, writeDot, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Error(err)
		}
		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, name string, op bcop, data []string, expLanes uint16) (fault bool) {
			if dsByte == nil {
				return
			}

			ctx.dict = append(ctx.dict[:0], string(dsByte))
			ctx.setScalarStrings(data, []byte{})
			ctx.current = (1 << len(data)) - 1

			// when
			if err := ctx.ExecuteImm2(op, 0); err != nil {
				t.Fatal(err)
			}

			obsLanes := ctx.current
			if obsLanes != expLanes {
				for i := 0; i < 16; i++ {
					obsLane := getBit(obsLanes, i)
					expLane := getBit(expLanes, i)
					if obsLane != expLane {
						t.Errorf("%v: issue with lane %v, \ndata=%q\nexpected=%016b (regexGolang=%q)\nobserved=%016b (regexSneller=%q)",
							name, i, data, expLanes, ds.RegexGolang.String(), obsLanes, ds.RegexSneller.String())
						return true
					}
				}
			}
			return false
		}

		const lanes = 16
		for len(dataSpace) > 0 {
			group := dataSpace
			if len(group) > lanes {
				group = group[:lanes]
			}
			dataSpace = dataSpace[len(group):]
			expLanes := uint16(0)
			for i, data := range group {
				expLanes = setBit(expLanes, i, ds.RegexGolang.MatchString(data))
			}

			hasFault1 := regexDataTest(&ctx, ds.DsT6, name+":DfaT6", opDfaT6, group, expLanes)
			hasFault2 := regexDataTest(&ctx, ds.DsT6Z, name+":DfaT6Z", opDfaT6Z, group, expLanes)
			hasFault3 := regexDataTest(&ctx, ds.DsT7, name+":DfaT7", opDfaT7, group, expLanes)
			hasFault4 := regexDataTest(&ctx, ds.DsT7Z, name+":DfaT7Z", opDfaT7Z, group, expLanes)
			hasFault5 := regexDataTest(&ctx, ds.DsT8, name+":DfaT8", opDfaT8, group, expLanes)
			hasFault6 := regexDataTest(&ctx, ds.DsT8Z, name+":DfaT8Z", opDfaT8Z, group, expLanes)
			hasFault7 := regexDataTest(&ctx, ds.DsLZ, name+":DfaLZ", opDfaLZ, group, expLanes)
			if hasFault1 || hasFault2 || hasFault3 || hasFault4 || hasFault5 || hasFault6 || hasFault7 {
				return
			}
		}
	}
}

// next updates x to the successor; return true/false whether the x is valid
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

// max returns the maximal value of slice, or 0 if slice is empty
func max(slice []int) int {
	if len(slice) == 0 {
		return 0
	}
	result := slice[0]
	for i := 1; i < len(slice); i++ {
		v := slice[i]
		if result < v {
			result = v
		}
	}
	return result
}

func createSpaceExhaustive(dataLenSpace []int, alphabet []rune) []string {
	result := make([]string, 0)
	alphabetSize := len(alphabet)
	indices := make([]byte, max(dataLenSpace))

	for _, strLength := range dataLenSpace {
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

func createSpace(dataLenSpace []int, alphabet []rune, maxSize int) []string {
	// createSpaceRandom creates random strings with the provided length over the provided alphabet
	createSpaceRandom := func(maxLength int, alphabet []rune, maxSize int) []string {

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

	if maxSize == exhaustive {
		return createSpaceExhaustive(dataLenSpace, alphabet)
	}
	return createSpaceRandom(max(dataLenSpace), alphabet, maxSize)
}

func createSpacePatternRandom(lengthSpace []int, alphabet []rune, maxSize int) []string {
	set := make(map[string]struct{})          // new empty set
	alphabet = append(alphabet, utf8.MaxRune) // use maxRune as a segment boundary
	alphabetSize := len(alphabet)
	maxLength := max(lengthSpace)

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

// createSpaceExhaustive creates strings of length 1 upto maxLength over the provided alphabet
func createSpaceRegex(maxLength int, alphabet []rune, regexType regexp2.RegexType) []string {
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
			regexStr := string(strRunes)
			if _, err := regexp2.Compile(regexStr, regexType); err != nil {
				// ignore strings that are not valid regexes
			} else if err := regexp2.IsSupported(regexStr); err != nil {
				// ignore not supported regexes
			} else {
				result = append(result, regexStr)
			}
			done = !next(&indices, alphabetSize, strLength)
		}
	}
	return result
}
