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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/fuzzy"

	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
	"golang.org/x/exp/maps"
)

// exhaustive search space: all combinations are explored
const exhaustive = -1

type Data = stringext.Data
type Needle = stringext.Needle
type OffsetZ2 int
type LengthZ3 int

var fullMask = kRegData{mask: 0xFFFF}

func prettyName(op bcop) string {
	switch op {
	case opCmpStrEqCs:
		return "compare string case-sensitive (opCmpStrEqCs)"
	case opCmpStrEqCi:
		return "compare string case-insensitive (opCmpStrEqCi)"
	case opCmpStrEqUTF8Ci:
		return "compare string case-insensitive unicode (opCmpStrEqUTF8Ci)"
	case opCmpStrFuzzyA3:
		return "compare string fuzzy (opCmpStrFuzzyA3)"
	case opCmpStrFuzzyUnicodeA3:
		return "compare string fuzzy unicode (opCmpStrFuzzyUnicodeA3)"
	case opHasSubstrFuzzyA3:
		return "has string fuzzy (opHasSubstrFuzzyA3)"
	case opHasSubstrFuzzyUnicodeA3:
		return "has string fuzzy unicode (opHasSubstrFuzzyUnicodeA3)"
	case opSkip1charLeft:
		return "skip 1 char from left (opSkip1charLeft)"
	case opSkip1charRight:
		return "skip 1 char from right (opSkip1charRight)"
	case opSkipNcharLeft:
		return "skip N char from left (opSkipNcharLeft)"
	case opSkipNcharRight:
		return "skip N char from right (opSkipNcharRight)"

	case opSubstr:
		return "substring (opSubstr)"
	case opSplitPart:
		return "split part (opSplitPart)"
	case opLengthStr:
		return "length string (bcLengthStr)"
	case opIsSubnetOfIP4:
		return "is-subnet-of IP4 IP (opIsSubnetOfIP4)"

	case opTrim4charLeft:
		return "trim char from left (opTrim4charLeft)"
	case opTrim4charRight:
		return "trim char from right (opTrim4charRight)"
	case opTrimWsLeft:
		return "trim white-space from left (opTrimWsLeft)"
	case opTrimWsRight:
		return "trim white-space from right (opTrimWsRight)"

	case opContainsPrefixCs:
		return "contains prefix case-sensitive (opContainsPrefixCs)"
	case opContainsPrefixCi:
		return "contains prefix case-insensitive (opContainsPrefixCi)"
	case opContainsPrefixUTF8Ci:
		return "contains prefix case-insensitive unicode (opContainsPrefixUTF8Ci)"
	case opContainsSuffixCs:
		return "contains suffix case-sensitive (opContainsSuffixCs)"
	case opContainsSuffixCi:
		return "contains suffix case-insensitive (opContainsSuffixCi)"
	case opContainsSuffixUTF8Ci:
		return "contains suffix case-insensitive unicode (opContainsSuffixUTF8Ci)"
	case opContainsSubstrCs:
		return "contains substr case-sensitive (opContainsSubstrCs)"
	case opContainsSubstrCi:
		return "contains substr case-insensitive (opContainsSubstrCi)"
	case opContainsSubstrUTF8Ci:
		return "contains substr case-insensitive unicode (opContainsSubstrUTF8Ci)"
	case opContainsPatternCs:
		return "contains pattern case-sensitive (opContainsPatternCs)"
	case opContainsPatternCi:
		return "contains pattern case-insensitive (opContainsPatternCi)"
	case opContainsPatternUTF8Ci:
		return "contains pattern case-insensitive unicode (opContainsPatternUTF8Ci)"
	case opEqPatternCs:
		return "equal pattern case-sensitive (opEqPatternCs)"
	case opEqPatternCi:
		return "equal pattern case-insensitive (opEqPatternCi)"
	case opEqPatternUTF8Ci:
		return "equal pattern case-insensitive unicode (opEqPatternUTF8Ci)"
	default:
		return "unknown op"
	}
}

// refFunc return the reference function for the provided bc operation
func refFunc(op bcop) any {
	switch op {

	case opCmpStrEqCs:
		return func(data Data, needle Needle) bool {
			if len(needle) == 0 { //NOTE: empty needles match with nothing (by design)
				return false
			}
			return string(data) == string(needle)
		}
	case opCmpStrEqCi:
		return func(data Data, needle Needle) bool {
			if len(needle) == 0 { //NOTE: empty needles match with nothing (by design)
				return false
			}
			return stringext.NormalizeStringASCIIOnlyString(string(data)) == stringext.NormalizeStringASCIIOnlyString(string(needle))
		}
	case opCmpStrEqUTF8Ci:
		return func(data Data, needle Needle) bool {
			if len(needle) == 0 { //NOTE: empty needles match with nothing (by design)
				return false
			}
			return strings.EqualFold(string(data), string(needle))
		}

	case opCmpStrFuzzyA3:
		return fuzzy.RefCmpStrFuzzyASCIIApprox3
	case opCmpStrFuzzyUnicodeA3:
		return fuzzy.RefCmpStrFuzzyUnicodeApprox3
	case opHasSubstrFuzzyA3:
		return fuzzy.RefHasSubstrFuzzyASCIIApprox3
	case opHasSubstrFuzzyUnicodeA3:
		return fuzzy.RefHasSubstrFuzzyUnicodeApprox3

	case opSkip1charLeft, opSkipNcharLeft:
		return referenceSkipCharLeft
	case opSkip1charRight, opSkipNcharRight:
		return referenceSkipCharRight

	case opSubstr:
		return referenceSubstr
	case opSplitPart:
		return referenceSplitPart
	case opLengthStr:
		return func(data Data) LengthZ3 {
			return LengthZ3(utf8.RuneCountInString(string(data)))
		}
	case opIsSubnetOfIP4:
		return referenceIsSubnetOfIP4

	case opTrim4charLeft:
		return func(data Data, needle Needle) (OffsetZ2, LengthZ3) {
			result := strings.TrimLeft(string(data), string(needle))
			return OffsetZ2(len(data) - len(result)), LengthZ3(len(result))
		}
	case opTrim4charRight:
		return func(data Data, needle Needle) (OffsetZ2, LengthZ3) {
			result := strings.TrimRight(string(data), string(needle))
			return OffsetZ2(0), LengthZ3(len(result))
		}
	case opTrimWsLeft:
		return func(data Data) (OffsetZ2, LengthZ3) {
			// TODO: currently only ASCII whitespace chars are supported, not U+0085 (NEL), U+00A0 (NBSP)
			whiteSpace := string([]byte{'\t', '\n', '\v', '\f', '\r', ' '})
			result := strings.TrimLeft(data, whiteSpace)
			return OffsetZ2(len(data) - len(result)), LengthZ3(len(result))
		}
	case opTrimWsRight:
		return func(data Data) (OffsetZ2, LengthZ3) {
			// TODO: currently only ASCII whitespace chars are supported, not U+0085 (NEL), U+00A0 (NBSP)
			whiteSpace := string([]byte{'\t', '\n', '\v', '\f', '\r', ' '})
			result := strings.TrimRight(data, whiteSpace)
			return OffsetZ2(0), LengthZ3(len(result))
		}

	case opContainsPrefixCs:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsPrefix(data, needle, true, true)
		}
	case opContainsPrefixCi:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsPrefix(data, needle, false, true)
		}
	case opContainsPrefixUTF8Ci:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsPrefix(data, needle, false, false)
		}

	case opContainsSuffixCs:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsSuffix(data, needle, true, true)
		}
	case opContainsSuffixCi:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsSuffix(data, needle, false, true)
		}
	case opContainsSuffixUTF8Ci:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			return refContainsSuffix(data, needle, false, false)
		}

	case opContainsSubstrCs:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			wildcard := make([]bool, utf8.RuneCountInString(string(needle)))
			pattern := stringext.Pattern{WC: utf8.MaxRune, Escape: stringext.NoEscape, Needle: needle, Wildcard: wildcard, HasWildcard: false}
			return matchPatternRef(data, &pattern, stringext.CS, true)
		}
	case opContainsSubstrCi:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			wildcard := make([]bool, utf8.RuneCountInString(string(needle)))
			pattern := stringext.Pattern{WC: utf8.MaxRune, Escape: stringext.NoEscape, Needle: needle, Wildcard: wildcard, HasWildcard: false}
			return matchPatternRef(data, &pattern, stringext.CiASCII, true)
		}
	case opContainsSubstrUTF8Ci:
		return func(data Data, needle Needle) (bool, OffsetZ2, LengthZ3) {
			wildcard := make([]bool, utf8.RuneCountInString(string(needle)))
			pattern := stringext.Pattern{WC: utf8.MaxRune, Escape: stringext.NoEscape, Needle: needle, Wildcard: wildcard, HasWildcard: false}
			return matchPatternRef(data, &pattern, stringext.CiUTF8, true)
		}

	case opContainsPatternCs:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CS, true)
		}
	case opContainsPatternCi:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CiASCII, true)
		}
	case opContainsPatternUTF8Ci:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CiUTF8, true)
		}

	case opEqPatternCs:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CS, false)
		}
	case opEqPatternCi:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CiASCII, false)
		}
	case opEqPatternUTF8Ci:
		return func(data Data, pattern *stringext.Pattern) (bool, OffsetZ2, LengthZ3) {
			return matchPatternRef(data, pattern, stringext.CiUTF8, false)
		}
	default:
		panic("X")
		return nil
	}
}

// TestStringCompareUT1 unit-tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompareUT1(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data    Data
		needle  Needle
		expLane bool
	}

	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opCmpStrEqCs,
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
		},
		{
			op: opCmpStrEqCi,
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
		},
		{
			op: opCmpStrEqUTF8Ci,
			unitTests: []unitTest{
				{"0000", "0000", true},
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

				{"", "", false}, //NOTE: empty needles match with nothing (by design)
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
				{Data([]byte{0x41, 0x41, 0xC2, 0xA2, 0xC2, 0xA2, 0x41, 0x41, 0xC2, 0xA2})[6:7], "A", true},

				{"AA¢¢¢¢"[0:4], "AA¢", true},
				{"$¢€𐍈ĳĲ", "$¢€𐍈ĲĲ", true},

				// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
				// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
				// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)

				{"ſ", "S", true},
				{"Ω", "Ω", true},
				{"K", "K", true},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		if !utf8.ValidString(string(ut.needle)) {
			t.Logf("needle is not valid UTF8; skipping this test")
			return
		}

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		inputS := ctx.sRegFromStrings(fill16(string(ut.data)))
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, 0, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) bool)
		expLane := ref(ut.data, ut.needle)

		if nok, msg := reportIssueKRef(expLane, ut.expLane); nok {
			t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				if expLane {
					expK.setBit(i)
				}
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestStringCompareUT2 unit-tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompareUT2(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data16   [16]Data // data pointed to by SI
		needle   Needle   // segments of the pattern: needs to be encoded and passed as string constant via the immediate dictionary
		expLanes uint16   // expected lanes K1
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opCmpStrEqCs,
			unitTests: []unitTest{
				{
					needle:   "0000",
					data16:   [16]Data{"0000", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
					expLanes: uint16(0b0000000000000001),
				},
			},
		},
		{
			op: opCmpStrEqCi,
			unitTests: []unitTest{
				{
					needle:   "0000",
					data16:   [16]Data{"0000", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
					expLanes: uint16(0b0000000000000001),
				},
			},
		},
		{
			op: opCmpStrEqUTF8Ci,
			unitTests: []unitTest{
				{
					needle:   "0000",
					data16:   [16]Data{"0000", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
					expLanes: uint16(0b0000000000000001),
				},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		if !utf8.ValidString(string(ut.needle)) {
			t.Logf("needle is not valid UTF8; skipping this test")
			return
		}

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		inputS := ctx.sRegFromStrings(ut.data16[:])
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, 0, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) bool)

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane := ref(ut.data16[i], ut.needle)
				if expLane {
					expK.setBit(i)
				}
				if nok, msg := reportIssueKRef(expLane, getBit(ut.expLanes, i)); nok {
					t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data16, msg)
					return
				}
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, prettyData(ut.data16), msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestStringCompareBF brute-force tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func TestStringCompareBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		// alphabet from which to generate words
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode implementation of comparison
		op bcop
	}

	testSuites := []testSuite{
		{
			op: opCmpStrEqCs,
			// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
			// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
			// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
			dataAlphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω'},
			dataLenSpace: []int{1, 2, 3, 4},
			dataMaxSize:  exhaustive,
		},
		{
			op:           opCmpStrEqCi,
			dataAlphabet: []rune{'a', 'b', 'c', 'd', 'A', 'B', 'C', 'D', 'z', '!', '@'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			dataMaxSize:  1000,
		},
		{
			op:           opCmpStrEqUTF8Ci,
			dataAlphabet: []rune{'s', 'S', 'ſ', 'k', 'K', 'K'},
			dataLenSpace: []int{1, 2, 3, 4},
			dataMaxSize:  exhaustive,
		},
		{ // test to explicitly check that byte length changing normalizations work
			op:           opCmpStrEqUTF8Ci,
			dataAlphabet: []rune{'a', 'Ω', 'Ω'}, // U+2126 'Ω' (E2 84 A6 = 226 132 166) -> U+03A9 'Ω' (CE A9 = 207 137)
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
		},
		{
			op:           opCmpStrEqUTF8Ci,
			dataAlphabet: []rune{'0', '1'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			dataMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, needleSpace []Needle) {
		// pre-compute encoded needles for speed
		encNeedles := make([]string, len(needleSpace))
		for i, needle := range needleSpace {
			encNeedles[i] = padNBytes(encodeNeedleOp(needle, ts.op), 4)
		}

		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, Needle) bool)

		for _, data16 := range dataSpace {
			for needleIdx, needle := range needleSpace {
				ctx.clear()
				ctx.setDict(encNeedles[needleIdx])

				inputS := ctx.sRegFromStrings(data16)
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, 0, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					if inputK.getBit(i) {
						expLane := ref(data16[i], needle)
						if expLane {
							expK.setBit(i)
						}
					}
				}

				if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
					t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), needle, prettyDataSlice(data16), msg)
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			needleSpace := flatten(dataSpace)
			run(&ts, fullMask, dataSpace, needleSpace)
		})
	}
}

// FuzzStringCompareFT fuzz tests for: opCmpStrEqCs, opCmpStrEqCi, opCmpStrEqUTF8Ci
func FuzzStringCompareFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "s", "ss", "S", "SS", "ſ", "ſſ", "a", "aa", "as", "bss", "cS", "dSS", "eſ", "fſſ", "ga", "haa", "s")

	testSuites := []bcop{
		opCmpStrEqCs,
		opCmpStrEqCi,
		opCmpStrEqUTF8Ci,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, needle Needle) {
		var ctx bctestContext
		defer ctx.Free()

		if !utf8.ValidString(string(needle)) || needle == "" {
			return // invalid needles are ignored
		}

		ctx.setDict(encodeNeedleOp(needle, op))

		inputS := ctx.sRegFromStrings(data16[:])
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsK, &inputS, 0, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, Needle) bool)

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
			if inputK.getBit(i) {
				expLane := ref(data16[i], needle)
				if expLane {
					expK.setBit(i)
				}
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(op), needle, prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, Needle(needle))
		}
	})
}

// TestStrFuzzyUT1 unit-tests for: opCmpStrFuzzy, opCmpStrFuzzyUnicode, opHasSubstrFuzzy, opHasSubstrFuzzyUnicode
func TestStrFuzzyUT1(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		needle    Needle
		data      Data
		threshold int
		expLane   bool
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}
	testSuites := []testSuite{
		{
			op: opCmpStrFuzzyA3,
			unitTests: []unitTest{

				{"abc", "aXc", 1, true}, // substitution at pos 1: b -> X
				{"abcde", "ade", 4, true},

				{"Nicole Kidman", "nicol kidman", 1, true},
				{"Nicole Kidman", "nico kidman", 2, true},

				{"AAB", "\uffdeB", 1, false}, // test with invalid UTF8 data

				{"\x00", "", 0, false},
				{"A", "\x00", 0, false},

				{"aaaa", "abcdefgh", 1, false},
				{"abcdefgh", "aXcdefgh", 1, true}, // substitution at pos 1: b -> X
				{"abcdefgh", "abXdefgh", 1, true}, // substitution at pos 2: c -> X

				{"abcdefgh", "bcdefgh", 1, true}, // deletion at pos 0
				{"abcdefgh", "acdefgh", 1, true}, // deletion at pos 1
				{"abcdefgh", "abdefgh", 1, true}, // deletion at pos 2

				{"abcdefgh", "Xabcdefgh", 1, true}, // insertion X at pos 0
				{"abcdefgh", "aXbcdefgh", 1, true}, // insertion X at pos 1
				{"abcdefgh", "abXcdefgh", 1, true}, // insertion X at pos 2

				{"abcdefgh", "bacdefgh", 1, true}, // transposition pos0: ab->ba
				{"abcdefgh", "acbdefgh", 1, true}, // transposition pos1: bc->cb
				{"abcdefgh", "abdcefgh", 1, true}, // transposition pos1: cd->dc

				{"aaaa", "abcdefgh", 2, false},
				{"abcdefgh", "aXcdXfgh", 2, true}, // substitution at pos 1: b -> X
				{"abcdefgh", "abXdeXgh", 2, true}, // substitution at pos 2: c -> X

				{"abcdefgh", "bcdfgh", 2, true}, // deletion at pos 0
				{"abcdefgh", "acdegh", 2, true}, // deletion at pos 1
				{"abcdefgh", "abdefh", 2, true}, // deletion at pos 2

				{"abcdefgh", "XabcdXefgh", 2, true}, // insertion X at pos 0
				{"abcdefgh", "aXbcdeXfgh", 2, true}, // insertion X at pos 1
				{"abcdefgh", "abXcdefXgh", 2, true}, // insertion X at pos 2

				{"abcdefgh", "bacedfgh", 2, true}, // transposition pos0: ab->ba
				{"abcdefgh", "acbdfegh", 2, true}, // transposition pos1: bc->cb
				{"abcdefgh", "abdcegfh", 2, true}, // transposition pos1: cd->dc
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		if !utf8.ValidString(string(ut.needle)) {
			t.Logf("needle is not valid UTF8; skipping this test")
			return
		}

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		inputThreshold := i64RegDataFromScalar(int64(ut.threshold))
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, &inputThreshold, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle, int) bool)
		expLane := ref(ut.data, ut.needle, ut.threshold)

		if nok, msg := reportIssueKRef(expLane, ut.expLane); nok {
			t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
			return
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestStrFuzzyUT2 unit-tests for: opCmpStrFuzzy, opCmpStrFuzzyUnicode, opHasSubstrFuzzy, opHasSubstrFuzzyUnicode
func TestStrFuzzyUT2(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data16    [16]Data // data pointed to by SI
		needle    Needle   // segments of the pattern: needs to be encoded and passed as string constant via the immediate dictionary
		expLanes  uint16   // expected lanes K1
		threshold [16]int
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opCmpStrFuzzyA3,
			unitTests: []unitTest{
				{
					needle:    "0",
					threshold: [16]int{25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25},
					data16:    [16]Data{"0", "0", "0", "0", "", "", "0", "0", "0", "0", "0", "0", "0", "00000000\x00000000000000000忧", "0", "0"},
					expLanes:  uint16(0b1101111111111111),
				},
				{
					needle:    "BAC",
					threshold: [16]int{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
					data16:    [16]Data{"A", "B", "C", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω"},
					expLanes:  uint16(0b0000000000000110),
				},
				{
					needle:    "BBA",
					threshold: [16]int{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000001),
				},
				{
					needle:    "A",
					threshold: [16]int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"BA", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000001),
				},
				{
					needle:    "C",
					threshold: [16]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					data16:    [16]Data{"A", "B", "C", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω"},
					expLanes:  uint16(0b0000000000000111),
				},
			},
		},
		{
			op: opCmpStrFuzzyUnicodeA3,
			unitTests: []unitTest{
				{
					needle:    "020",
					threshold: [16]int{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
					data16:    [16]Data{"1", "0", "0", "0", "", "ſſ", "0", "0", "0", "000", "00", "0000000000000000000", "0ſ", "0", "0", "0"},
					expLanes:  uint16(0b1111011111111111),
				},
				{
					needle:    "0",
					threshold: [16]int{46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46, 46},
					data16:    [16]Data{"0", "0", "0", "0", "", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "0", "0", "0", "\x1d", "0", "0", "0", "0", "0", "0"},
					expLanes:  uint16(0b1111111111011111),
				},
				{
					needle:    "A",
					threshold: [16]int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"BA", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000001),
				},
				{
					needle:    "AA",
					threshold: [16]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000000),
				},
				{
					needle:    "A",
					threshold: [16]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000001),
				},
				{
					needle:    "BBA",
					threshold: [16]int{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000001),
				},
			},
		},
		{
			op: opHasSubstrFuzzyA3,
			unitTests: []unitTest{
				{
					needle:    "A",
					threshold: [16]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"B", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000000),
				},
			},
		},
		{
			op: opHasSubstrFuzzyUnicodeA3,
			unitTests: []unitTest{
				{
					needle:    "AA",
					threshold: [16]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					data16:    [16]Data{"A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:  uint16(0b0000000000000000),
				},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(ut.data16[:])
		inputThreshold := i64RegData{}
		var obsK, expK kRegData

		ref := refFunc(ts.op).(func(Data, Needle, int) bool)

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane := ref(ut.data16[i], ut.needle, ut.threshold[i])
				if expLane {
					inputThreshold.values[i] = int64(ut.threshold[i])
					expK.setBit(i)
				}
				if nok, msg := reportIssueKRef(expLane, getBit(ut.expLanes, i)); nok {
					t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data16, msg)
					return
				}
			}
		}

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, &inputThreshold, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, prettyData(ut.data16), msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestStrFuzzyBF brute-force tests for: opCmpStrFuzzy, opCmpStrFuzzyUnicode, opHasSubstrFuzzy, opHasSubstrFuzzyUnicode
func TestStrFuzzyBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		// alphabet from which to generate words
		dataAlphabet, needleAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, needleLenSpace, thresholdSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize, needleMaxSize int
		// bytecode implementation of comparison
		op bcop
	}

	testSuites := []testSuite{
		{
			op:             opCmpStrFuzzyA3,
			dataAlphabet:   []rune{'A', 'B', 'C', 'Ω'},
			dataLenSpace:   []int{0, 1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'A', 'B', 'C'},
			needleLenSpace: []int{1, 2, 3, 4},
			needleMaxSize:  exhaustive,
			thresholdSpace: []int{1, 2, 3},
		},
		{
			op:             opCmpStrFuzzyUnicodeA3,
			dataAlphabet:   []rune{'A', 'B', 'C', 'Ω'},
			dataLenSpace:   []int{0, 1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'A', 'B', 'C', 'Ω'},
			needleLenSpace: []int{1, 2, 3, 4},
			needleMaxSize:  exhaustive,
			thresholdSpace: []int{0, 1, 2, 3},
		},
		{
			op:             opHasSubstrFuzzyA3,
			dataAlphabet:   []rune{'A', 'B', 'C', 'Ω'},
			dataLenSpace:   []int{0, 1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'A', 'B', 'C'},
			needleLenSpace: []int{1, 2, 3, 4},
			needleMaxSize:  exhaustive,
			thresholdSpace: []int{0, 1, 2, 3},
		},
		{
			op:             opHasSubstrFuzzyUnicodeA3,
			dataAlphabet:   []rune{'A', '$', '¢', '€', '𐍈'},
			dataLenSpace:   []int{0, 1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'$', '¢', '€', '𐍈'},
			needleLenSpace: []int{1, 2, 3},
			needleMaxSize:  exhaustive,
			thresholdSpace: []int{1, 2, 3},
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, needleSpace []Needle) {
		var ctx bctestContext
		defer ctx.Free()

		// pre-compute encoded needles for speed
		encNeedles := make([]string, len(needleSpace))
		for i, needle := range needleSpace {
			encNeedles[i] = encodeNeedleOp(needle, ts.op)
		}

		ref := refFunc(ts.op).(func(Data, Needle, int) bool)

		for _, threshold := range ts.thresholdSpace {
			inputThreshold := i64RegDataFromScalar(int64(threshold))

			for _, data16 := range dataSpace {
				for needleIdx, needle := range needleSpace {
					ctx.clear()
					ctx.setDict(encNeedles[needleIdx])
					dictOffset := uint16(0)

					inputS := ctx.sRegFromStrings(data16)
					var obsK, expK kRegData

					for i := 0; i < bcLaneCount; i++ {
						if inputK.getBit(i) {
							expLane := ref(data16[i], needle, threshold)
							if expLane {
								expK.setBit(i)
							}
						}
					}

					if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, &inputThreshold, dictOffset, &inputK}, inputK); err != nil {
						t.Fatal(err)
					}

					if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
						t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), needle, prettyDataSlice(data16), msg)
					}
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			needleSpace := createSpace(ts.needleLenSpace, ts.needleAlphabet, ts.needleMaxSize)
			run(&ts, fullMask, dataSpace, flatten(needleSpace))
		})
	}
}

// FuzzStrFuzzyFT fuzz tests for: opCmpStrFuzzy, opCmpStrFuzzyUnicode, opHasSubstrFuzzy, opHasSubstrFuzzyUnicode
func FuzzStrFuzzyFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "s", "ss", "S", "SS", "ſ", "ſſ", "a", "aa", "as", "bss", "cS", "dSS", "eſ", "fſſ", "ga", "haa", "s", 1)

	testSuites := []bcop{
		opCmpStrFuzzyA3,
		opCmpStrFuzzyUnicodeA3,
		opHasSubstrFuzzyA3,
		opHasSubstrFuzzyUnicodeA3,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, needle Needle, threshold int) {
		var ctx bctestContext
		defer ctx.Free()

		if op == opCmpStrFuzzyA3 || op == opHasSubstrFuzzyA3 {
			for _, c := range needle {
				if c >= utf8.RuneSelf {
					return // ascii code do not accept unicode code-points
				}
			}
		}

		if !utf8.ValidString(string(needle)) || (needle == "") {
			return // invalid needles are ignored
		}

		ctx.setDict(encodeNeedleOp(needle, op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		inputThreshold := i64RegDataFromScalar(int64(threshold))
		var obsK, expK kRegData

		ref := refFunc(op).(func(Data, Needle, int) bool)

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
			if inputK.getBit(i) {
				expLane := ref(data16[i], needle, threshold)
				if expLane {
					expK.setBit(i)
				}
			}
		}

		if err := ctx.ExecuteOpcode(op, []any{&obsK, &inputS, &inputThreshold, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(op), needle, prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string, threshold int) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, Needle(needle), threshold)
		}
	})
}

// TestRegexMatchBF1 brute-force tests 1 for: opDfaT6, opDfaT6Z, opDfaT7, opDfaT7Z, opDfaT8, opDfaT8Z, opDfaLZ
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
				{expr: "$"},
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
		{`a\nxb`, `(?m)a$.b`, false, regexp2.Regexp}, // NOTE (?m) does not alter multiline behaviour in go

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

	run := func(ut unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		data16 := fill16(ut.data)

		ds, err := regexp2.CreateDs(ut.expr, ut.regexType, false, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Errorf("%v with %v", err, ut.expr)
		}

		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, info string, op bcop, expLane bool) {
			if dsByte == nil {
				return
			}

			ctx.clear()
			ctx.setDict(string(dsByte))
			dictOffset := uint16(0)

			inputS := ctx.sRegFromStrings(data16)
			outputK := kRegData{}

			if err := ctx.ExecuteOpcode(op, []any{&outputK, &inputS, dictOffset, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < bcLaneCount; i++ {
				if outputK.getBit(i) != expLane {
					t.Errorf("%v: lane %v: issue with data %q\nregexGolang=%q yields expected %v; regexSneller=%q yields observed %v",
						info, i, data16[i], ds.RegexGolang.String(), expLane, ds.RegexSneller.String(), outputK.getBit(i))
					break
				}
			}
		}

		// first: check reference implementation
		{
			obsLane := ds.RegexGolang.MatchString(ut.data)
			if ut.expLane != obsLane {
				t.Errorf("refImpl: issue with data %q\nexpected %v while RegexGolang=%q yields observed %v",
					ut.data, ut.expLane, ds.RegexGolang.String(), obsLane)
			}
		}

		// second: check the bytecode implementation
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
			run(ut, fullMask)
		})
	}
}

// TestRegexMatchUT2 unit-tests for: regexp2.Regexp and regexp2.SimilarTo
func TestRegexMatchUT2(t *testing.T) {
	t.Parallel()
	name := "regex match UnitTest2"

	type unitTest struct {
		data16    [16]Data // data pointed to by SI
		expr      Needle   // dictValue of the pattern: need to be encoded and passed as string constant via the immediate dictionary
		expLanes  uint16   // resulting lanes K1
		regexType regexp2.RegexType
	}

	unitTests := []unitTest{
		{
			data16:    [16]string{"baΩ\naa", "caΩ\naa", "\naΩ\naa", "ΩaΩ\naa", "abΩ\naa", "bbΩ\naa", "cbΩ\naa", "\nbΩ\naa", "ΩbΩ\naa", "acΩ\naa", "bcΩ\naa", "ccΩ\naa", "\ncΩ\naa", "ΩcΩ\naa", "a\nΩ\naa", "b\nΩ\naa"},
			expr:      "^a",
			expLanes:  0b0100001000010000,
			regexType: regexp2.Regexp,
		},
		{
			data16:    [16]string{"a", "Ω", "aa", "Ωa", "aΩ", "ΩΩ", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω", "Ω"},
			expr:      "^a",
			expLanes:  0b0000000000010101,
			regexType: regexp2.Regexp,
		},
		{
			data16:    [16]string{"aaaa", "baaa", "Ωaaa", "abaa", "bbaa", "Ωbaa", "aΩaa", "bΩaa", "ΩΩaa", "aaba", "baba", "Ωaba", "abba", "bbba", "Ωbba", "aΩba"},
			expr:      `a*__a`,
			expLanes:  0b1001001001001001,
			regexType: regexp2.SimilarTo,
		},
		{
			data16:    [16]string{"Ωaa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa", "baa"},
			expr:      `^(a*.aa)$`,
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
		{
			data16:    [16]string{"abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ", "abΩ"},
			expr:      "a.*b.*Ω",
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
		{
			data16:    [16]string{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a"},
			expr:      "^a$",
			expLanes:  0b1111111111111111,
			regexType: regexp2.Regexp,
		},
	}

	run := func(ut unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ds, err := regexp2.CreateDs(string(ut.expr), ut.regexType, false, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Error(err)
		}

		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, info string, op bcop, needle Needle, expLanes kRegData) {
			if dsByte == nil {
				return
			}

			ctx.clear()
			ctx.setDict(string(dsByte))
			dictOffset := uint16(0)

			inputS := ctx.sRegFromStrings(ut.data16[:])
			outputK := kRegData{}

			if err := ctx.ExecuteOpcode(op, []any{&outputK, &inputS, dictOffset, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			if outputK != expLanes {
				for i := 0; i < bcLaneCount; i++ {
					obsLane := outputK.getBit(i)
					expLane := expLanes.getBit(i)
					if obsLane != expLane {
						t.Errorf("%v-%v: issue with lane %v, \ndata=%q\nexpected=%016b (regexGolang=%q)\nobserved=%016b (regexSneller=%q)",
							name, info, i, prettyData(ut.data16), expLanes, ds.RegexGolang.String(), outputK.mask, ds.RegexSneller.String())
						break
					}
				}
			}
		}

		// first: check reference implementation
		{
			for i := 0; i < bcLaneCount; i++ {
				obsLane := ds.RegexGolang.MatchString(ut.data16[i])
				expLane := getBit(ut.expLanes, i)
				if expLane != obsLane {
					t.Errorf("refImpl: lane %v: issue with data %q\nexpected %v while RegexGolang=%q yields observed %v",
						i, ut.data16[i], expLane, ds.RegexGolang.String(), obsLane)
				}
			}
		}

		// second: check the bytecode implementation
		expLanes := kRegData{mask: ut.expLanes}
		regexDataTest(&ctx, ds.DsT6, "DfaT6", opDfaT6, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsT6Z, "DfaT6Z", opDfaT6Z, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsT7, "DfaT7", opDfaT7, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsT7Z, "DfaT7Z", opDfaT7Z, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsT8, "DfaT8", opDfaT8, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsT8Z, "DfaT8Z", opDfaT8Z, ut.expr, expLanes)
		regexDataTest(&ctx, ds.DsLZ, "DfaLZ", opDfaLZ, ut.expr, expLanes)
	}

	for i, ut := range unitTests {
		t.Run(fmt.Sprintf(`case %d:`, i), func(t *testing.T) {
			run(ut, fullMask)
		})
	}
}

// FuzzRegexMatchRun runs fuzzer to search both regexes and data and compares the with a reference implementation
func FuzzRegexMatchRun(f *testing.F) {
	run := func(t *testing.T, ds []byte, expMatch bool, data, regexString, info string, op bcop) {
		regexMatch := func(ds []byte, needle string, op bcop) (match bool) {
			var ctx bctestContext
			defer ctx.Free()

			ctx.setDict(string(ds))
			dictOffset := uint16(0)

			inputS := ctx.sRegFromStrings(fill16(needle))
			inputK := fullMask
			outputK := kRegData{}

			if err := ctx.ExecuteOpcode(op, []any{&outputK, &inputS, dictOffset, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			if outputK.mask == 0 {
				return false
			}

			if outputK.mask == 0xFFFF {
				return true
			}

			t.Errorf("inconstent results %x", outputK.mask)
			return false
		}

		if ds != nil {
			obsMatch := regexMatch(ds, data, op)
			if obsMatch != expMatch {
				t.Errorf(`Fuzzer found: %v yields '%v' while expected '%v'. (regexString %q; data %q)`, info, obsMatch, expMatch, regexString, data)
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
	f.Add("$")

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
			t.Fatalf(fmt.Sprintf("DFATiny: error (%v) for regex %q", err, re))
		}
		dsTiny.Data(6, hasUnicodeWildcard, wildcardRange)
		dsTiny.Data(7, hasUnicodeWildcard, wildcardRange)
		dsTiny.Data(8, hasUnicodeWildcard, wildcardRange)

		if _, err = regexp2.NewDsLarge(store); err != nil {
			t.Fatalf(fmt.Sprintf("DFALarge: error (%v) for regex %q", err, re))
		}
	})
}

// referenceSubstr is the reference implementation for: opSubstr
func referenceSubstr(input Data, start, length int) (OffsetZ2, LengthZ3) {
	if !utf8.ValidString(input) {
		return 0, 0
	}

	// this method is called as 1-based indexed, the implementation is 0-based indexed
	start--

	if start < 0 {
		start = 0
	}

	if length < 0 {
		length = 0
	}

	asRunes := []rune(input)
	if start >= len(asRunes) {
		return 0, 0
	}

	if (start + length) > len(asRunes) {
		length = len(asRunes) - start
	}

	// we need the content of the removed string so we can output the offset+length result
	removedStr := string(asRunes[0:start])
	outputStr := string(asRunes[start : start+length])

	return OffsetZ2(len(removedStr)), LengthZ3(len(outputStr))
}

// TestSubstrUT unit-tests for: opSubstr
func TestSubstrUT(t *testing.T) {
	t.Parallel()

	const op = opSubstr

	type unitTest struct {
		data      Data
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
			{"aaaa", 4, 2, "a"}, // is this what we want? Yes, this is as expected
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

	run := func(ut [16]unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		data16 := make([]Data, 16)
		var inputFrom, inputLength i64RegData
		for i := 0; i < bcLaneCount; i++ {
			data16[i] = ut[i].data
			inputFrom.values[i] = int64(ut[i].begin)
			inputLength.values[i] = int64(ut[i].length)
		}

		inputS := ctx.sRegFromStrings(data16)
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, &inputFrom, &inputLength, &inputK}, fullMask); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int, int) (OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expOffset, expLength := ref(data16[i], ut[i].begin, ut[i].length)
				expString := ut[i].data[expOffset : int(expOffset)+int(expLength)]

				if expString != ut[i].expResult {
					t.Errorf("refImpl: input %q; begin=%v; length=%v; reference %q; expected %q",
						data16[i], ut[i].begin, ut[i].length, expString, ut[i].expResult)
				}
				if expLength > 0 {
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyDataSlice(data16), msg)
		}
	}

	t.Run(prettyName(op), func(t *testing.T) {
		for _, ut := range unitTests {
			run(ut, fullMask)
		}
	})
}

// TestSubstrBF brute-force tests for: opSubstr
func TestSubstrBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
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
	}
	testSuites := []testSuite{
		{
			op:           opSubstr,
			dataAlphabet: []rune{'a', 'b', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
			beginSpace:   []int{0, 1, 2, 4, 5},
			lengthSpace:  []int{-1, 0, 1, 3, 4},
		},
		{
			op:           opSubstr,
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
			beginSpace:   []int{1, 3, 4, 5},
			lengthSpace:  []int{0, 1, 3, 4},
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, int, int) (OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			for _, length := range ts.lengthSpace {
				inputLength := i64RegDataFromScalar(int64(length))
				for _, begin := range ts.beginSpace {
					ctx.clear()

					inputS := ctx.sRegFromStrings(data16)
					inputFrom := i64RegDataFromScalar(int64(begin))
					var obsS, expS sRegData

					if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, &inputFrom, &inputLength, &inputK}, inputK); err != nil {
						t.Fatal(err)
					}

					for i := 0; i < bcLaneCount; i++ {
						if inputK.getBit(i) {
							expOffset, expLength := ref(data16[i], begin, length)
							if expLength > 0 {
								expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
								expS.sizes[i] = uint32(expLength)
							}
						}
					}

					if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
						t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), prettyDataSlice(data16), msg)
						return
					}
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
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

	testSuites := []bcop{
		opSubstr,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, begin, length [16]int) {
		var ctx bctestContext
		defer ctx.Free()

		inputFrom := i64RegData{}
		inputLength := i64RegData{}

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
			inputFrom.values[i] = int64(begin[i])
			inputLength.values[i] = int64(length[i])
		}

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, &inputFrom, &inputLength, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int, int) (OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expOffset, expLength := ref(data16[i], begin[i], length[i])
				if expLength > 0 {
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string,
		b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15 int,
		s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		begin := [16]int{b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15}
		length := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, begin, length)
		}
	})
}

// referenceIsSubnetOfIP4 reference implementation for opIsSubnetOfIP4
func referenceIsSubnetOfIP4(data Data, min, max uint32) bool {
	// str2ByteArray parses an IP; will also parse leasing zeros: eg. "000.001.010.100" is returned as [0,1,10,100]
	str2ByteArray := func(data Data) (result []byte, ok bool) {
		result = make([]byte, 4)
		components := strings.Split(string(data), ".")
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

	if byteSlice, ok := str2ByteArray(data); ok {
		r2 := inRangeByteWise(byteSlice, toArrayIP4(min), toArrayIP4(max))
		return r2
	}
	return false
}

// TestIsSubnetOfIP4UT1 runs unit-tests for: opIsSubnetOfIP4
func TestIsSubnetOfIP4UT1(t *testing.T) {
	t.Parallel()

	const op = opIsSubnetOfIP4

	type unitTest struct {
		data, min, max string
		expLane        bool
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
		{string([]byte("100.100.0.0")[0:8]), "0.0.0.0", "100.100.0.0", false},  // test whether length of data is respected
		{"1.00000", "0.0.0.0", "1.0.0.0", false},                               // check if there is a dot
		{string([]byte("100.100.0.0")[0:10]), "0.0.0.0", "100.100.0.0", false}, // test whether length of data is respected
		{string([]byte("100.100.1.0")[0:8]), "0.0.0.0", "100.100.0.0", false},  // test whether length of data is respected

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

	run := func(ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		min := binary.BigEndian.Uint32(net.ParseIP(ut.min).To4())
		max := binary.BigEndian.Uint32(net.ParseIP(ut.max).To4())

		minA := toArrayIP4(min)
		maxA := toArrayIP4(max)

		ctx.setDict(stringext.ToBCD(&minA, &maxA))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, uint32, uint32) bool)
		expLane := ref(ut.data, min, max)

		if nok, msg := reportIssueKRef(expLane, ut.expLane); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), ut.data, msg)
		}
	}

	t.Run(prettyName(op), func(t *testing.T) {
		for _, ut := range unitTests {
			run(&ut, fullMask)
		}
	})
}

// TestIsSubnetOfIP4BF runs brute-force tests for: opIsSubnetOfIP4
func TestIsSubnetOfIP4BF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		dataAlphabet []rune // alphabet from which to generate data
		dataLenSpace []int  // space of lengths of the words made of alphabet
		dataMaxSize  int    // maximum number of elements in dataSpace
		op           bcop   // bytecode to run
	}

	testSuites := []testSuite{
		{
			op:           opIsSubnetOfIP4,
			dataAlphabet: []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', 'A', 0},
			dataLenSpace: []int{9, 10, 11, 12, 13, 14, 15, 16},
			dataMaxSize:  100000,
		},
		{
			op:           opIsSubnetOfIP4,
			dataAlphabet: []rune{'0', '1', '.'},
			dataLenSpace: []int{9, 10, 11, 12, 13},
			dataMaxSize:  exhaustive,
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

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, uint32, uint32) bool)

		for _, data16 := range dataSpace {
			min, max := randomMinMaxValues()
			minA := toArrayIP4(min)
			maxA := toArrayIP4(max)

			ctx.clear()
			ctx.setDict(stringext.ToBCD(&minA, &maxA))
			dictOffset := uint16(0)

			inputS := ctx.sRegFromStrings(data16)
			var obsK, expK kRegData

			if err := ctx.ExecuteOpcode(ts.op, []any{&obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < bcLaneCount; i++ {
				expLane := ref(data16[i], min, max)
				if inputK.getBit(i) && expLane {
					expK.setBit(i)
				}
			}

			if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
				t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), prettyDataSlice(data16), msg)
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			var dataSpace []Data
			if ts.dataMaxSize == exhaustive {
				dataSpace = make([]Data, 0)
				for _, data := range flatten(createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)) {
					if net.ParseIP(string(data)).To4() != nil {
						dataSpace = append(dataSpace, Data(data))
					}
				}
			} else {
				dataSpace = make([]Data, ts.dataMaxSize)
				for i := 0; i < ts.dataMaxSize; i++ {
					dataSpace[i] = randomIP4Addr().String()
				}
			}
			run(&ts, fullMask, split16(dataSpace))
		})
	}
}

// FuzzIsSubnetOfIP4FT runs fuzz tests for: opIsSubnetOfIP4
func FuzzIsSubnetOfIP4FT(f *testing.F) {

	const op = opIsSubnetOfIP4

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

	run := func(t *testing.T, inputK kRegData, data16 [16]Data, min, max uint32) {
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		minA := toArrayIP4(min)
		maxA := toArrayIP4(max)

		ctx.clear()
		ctx.setDict(stringext.ToBCD(&minA, &maxA))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, uint32, uint32) bool)

		for i := 0; i < bcLaneCount; i++ {
			expLane := ref(data16[i], min, max)
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
			}
		}

		if nok, msg := reportIssueK(&inputK, &obsK, &expK); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, min, max uint32) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		run(t, kRegData{mask: lanes}, data, min, max)
	})
}

// TestSkip1CharUT1 unit-tests for opSkip1charLeft, opSkip1charRight
func TestSkip1CharUT1(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data      Data     // data at SI
		expLane   bool     // expected lane K1
		expOffset OffsetZ2 // expected offset Z2
		expLength LengthZ3 // expected length Z3
	}

	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opSkip1charLeft,
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
		},
		{
			op: opSkip1charRight,
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
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, int) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, 1)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\ndata=%v\n%v", prettyName(ts.op), ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if expLane && inputK.getBit(i) {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestSkip1CharBF brute-force tests for: opSkip1charLeft, opSkip1charRight
func TestSkip1CharBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
	}

	testSuites := []testSuite{
		{
			op:           opSkip1charLeft,
			dataAlphabet: []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
		},
		{
			op:           opSkip1charRight,
			dataAlphabet: []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6},
			dataMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		for _, data16 := range dataSpace {
			ctx.clear()

			inputS := ctx.sRegFromStrings(data16)
			var obsS, expS sRegData
			var obsK, expK kRegData

			if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			ref := refFunc(ts.op).(func(Data, int) (bool, OffsetZ2, LengthZ3))

			for i := 0; i < bcLaneCount; i++ {
				if inputK.getBit(i) {
					expLane, expOffset, expLength := ref(data16[i], 1)
					if expLane {
						expK.setBit(i)
						expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
						expS.sizes[i] = uint32(expLength)
					}
				}
			}

			if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
				t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), prettyDataSlice(data16), msg)
				return
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSkip1CharFT fuzz-tests for: opSkip1charLeft, opSkip1charRight
func FuzzSkip1CharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS")

	testSuites := []bcop{
		opSkip1charLeft,
		opSkip1charRight,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(data16[i], 1)
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data)
		}
	})
}

// TestSkipNCharUT1 unit-tests for opSkipNcharLeft, opSkipNcharRight
func TestSkipNCharUT1(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data      Data     // data at SI
		skipCount int      // number of code-points to skip
		expLane   bool     // expected lane K1
		expOffset OffsetZ2 // expected offset Z2
		expLength LengthZ3 // expected length Z3
	}

	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opSkipNcharLeft,
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
		},
		{
			op: opSkipNcharRight,
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
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		inputCount := i64RegDataFromScalar(int64(ut.skipCount))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, &inputCount, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, int) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, ut.skipCount)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\ndata=%v\n%v", prettyName(ts.op), ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nskipCount=%q\ndata=%v\n%v", prettyName(ts.op), ut.skipCount, ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestSkip1CharBF brute-force tests for: opSkipNcharLeft, opSkipNcharRight
func TestSkipNCharBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
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
	}

	testSuites := []testSuite{
		{
			op:             opSkipNcharLeft,
			dataAlphabet:   []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			skipCountSpace: []int{0, 1, 2, 3, 4, 5, 6},
		},
		{
			op:             opSkipNcharRight,
			dataAlphabet:   []rune{'s', 'S', 'ſ', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			skipCountSpace: []int{0, 1, 2, 3, 4, 5, 6},
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		for _, skipCount := range ts.skipCountSpace {
			for _, data16 := range dataSpace {
				ctx.clear()

				inputS := ctx.sRegFromStrings(data16)
				inputCountS := i64RegDataFromScalar(int64(skipCount))
				var obsS, expS sRegData
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, &inputCountS, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				ref := refFunc(ts.op).(func(Data, int) (bool, OffsetZ2, LengthZ3))

				for i := 0; i < bcLaneCount; i++ {
					expLane, expOffset, expLength := ref(data16[i], skipCount)
					if inputK.getBit(i) && expLane {
						expK.setBit(i)
						expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
						expS.sizes[i] = uint32(expLength)
					}
				}

				if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
					t.Errorf("%v\nskipCount=%q\ndata=%v\n%v", prettyName(ts.op), skipCount, prettyDataSlice(data16), msg)
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSkip1CharFT fuzz tests for: opSkipNcharLeft, opSkipNcharRight
func FuzzSkipNCharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS", 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
	f.Add(uint16(0xF0F0), "a", "b", "c", "d", "e", "f", "g", "h", "ſ", "ſſ", "s", "SS", "SSS", "SSSS", "ſS", "ſSS", 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
	f.Add(uint16(0b0000000000000011), "𐍈𐍈", "111𐍈", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	testSuites := []bcop{
		opSkipNcharLeft,
		opSkipNcharRight,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, skipCount [16]int) {
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(data16[:])
		inputCount := i64RegData{}
		var obsS, expS sRegData
		var obsK, expK kRegData

		for i := 0; i < bcLaneCount; i++ {
			if skipCount[i] >= 0 {
				inputCount.values[i] = int64(skipCount[i])
			}
		}

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, &inputCount, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			expLane, expOffset, expLength := ref(data16[i], skipCount[i])
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nskipCount=%q\ndata=%v\n%v", prettyName(op), skipCount, prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		skipCount := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, skipCount)
		}
	})
}

// referenceSplitPart splits data on delimiter and returns the offset and length of the idx part (NOTE 1-based index)
func referenceSplitPart(data Data, idx int, delimiter rune) (lane bool, offset OffsetZ2, length LengthZ3) {
	idx-- // because this method is written as 0-indexed, but called as 1-indexed.
	offset = 0
	length = LengthZ3(len(data))
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

	offset = OffsetZ2(bytePosBegin)
	length = LengthZ3(bytePosEnd - bytePosBegin)
	lane = true
	return
}

// TestSplitPartUT1 unit-tests for: opSplitPart
func TestSplitPartUT1(t *testing.T) {
	t.Parallel()
	const op = opSplitPart

	type unitTest struct {
		data      Data
		idx       int
		delimiter rune
		expLane   bool     // expected lane K1
		expOffset OffsetZ2 // expected offset Z2
		expLength LengthZ3 // expected length Z3
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

	run := func(ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(string(ut.delimiter))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		inputIndex := i64RegDataFromScalar(int64(ut.idx))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, dictOffset, &inputIndex, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int, rune) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, ut.idx, ut.delimiter)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\nidx=%v\ndata=%v\n%v", prettyName(op), ut.idx, ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nidx=%q\ndata=%v\n%v", prettyName(op), ut.idx, ut.data, msg)
		}
	}

	t.Run(prettyName(op), func(t *testing.T) {
		for _, ut := range unitTests {
			run(&ut, fullMask)
		}
	})
}

// TestSplitPartBF brute-force tests for: opSplitPart
func TestSplitPartBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
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
	}

	testSuites := []testSuite{
		{
			op:           opSplitPart,
			dataAlphabet: []rune{'a', 'b', 0, ';'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			idxSpace:     []int{0, 1, 2, 3, 4, 5},
			delimiter:    ';',
		},
		{
			op:           opSplitPart,
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', ';'},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
			idxSpace:     []int{0, 1, 2, 3, 4, 5},
			delimiter:    ';',
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, int, rune) (bool, OffsetZ2, LengthZ3))

		for _, idx := range ts.idxSpace {
			for _, data16 := range dataSpace {
				ctx.clear()
				ctx.setDict(string(ts.delimiter))
				dictOffset := uint16(0)

				inputS := ctx.sRegFromStrings(data16)
				inputIndex := i64RegDataFromScalar(int64(idx))
				var obsS, expS sRegData
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputIndex, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					if inputK.getBit(i) {
						expLane, expOffset, expLength := ref(data16[i], idx, ts.delimiter)
						if expLane {
							expK.setBit(i)
							expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
							expS.sizes[i] = uint32(expLength)
						}
					}
				}

				if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
					t.Errorf("%v\nidx=%q\ndata=%v\n%v", prettyName(ts.op), idx, prettyDataSlice(data16), msg)
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzSplitPartFT fuzz-tests for: opSplitPart
func FuzzSplitPartFT(f *testing.F) {
	f.Add(uint16(0xFFF0), "a", "a;b", "a;b;c", "", ";", "𐍈", "𐍈;𐍈", "𐍈;𐍈;", "a", "a;b", "a;b;c", "", ";", "𐍈", "𐍈;𐍈", "𐍈;𐍈;", 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, byte(';'))
	f.Add(uint16(0xFFFF), ";;;;;", "a", ";", "", "", "", "", "", "", "", "", "", "", "", "", "", 6, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(';'))

	testSuites := []bcop{
		opSplitPart,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, idx [16]int, delimiterByte byte) {
		if (delimiterByte == 0) || (delimiterByte >= 0x80) {
			return // delimiter can only be ASCII and not 0
		}

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		delimiter := rune(delimiterByte)
		ctx.setDict(string(delimiter))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		inputIndex := i64RegData{}
		for i := 0; i < bcLaneCount; i++ {
			if idx[i] >= 0 {
				inputIndex.values[i] = int64(idx[i])
			}
		}

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, dictOffset, &inputIndex, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, int, rune) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(data16[i], idx[i], delimiter)
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nidx=%q\ndata=%v\n%v", prettyName(op), idx, prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16,
		d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string,
		s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 int, delimiter byte) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		idx := [16]int{s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, idx, delimiter)
		}
	})
}

// TestSplitPartUT unit-tests for: opLengthStr
func TestLengthStrUT(t *testing.T) {
	t.Parallel()
	const op = opLengthStr

	type unitTest struct {
		data     Data
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

	run := func(ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS i64RegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		expChars := utf8.RuneCountInString(ut.data)
		if expChars != ut.expChars {
			t.Errorf("refImpl: length of %q; observed %v; expected: %v", ut.data, expChars, ut.expChars)
		}

		for i := 0; i < bcLaneCount; i++ {
			expS.values[i] = int64(expChars)
		}
		verifyI64RegOutput(t, &obsS, &expS)
	}

	t.Run(prettyName(op), func(t *testing.T) {
		for _, ut := range unitTests {
			run(&ut, fullMask)
		}
	})
}

// TestSplitPartBF brute-force tests for: opLengthStr
func TestLengthStrBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		// alphabet from which to generate data
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
	}
	testSuites := []testSuite{
		{
			op:           opLengthStr,
			dataAlphabet: []rune{'a', 'b', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
		},
		{
			op:           opLengthStr,
			dataAlphabet: []rune{'$', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace: []int{1, 2, 3, 4, 5, 6, 7},
			dataMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data) LengthZ3)

		for _, data16 := range dataSpace {
			ctx.clear()

			inputS := ctx.sRegFromStrings(data16)
			var obsS, expS i64RegData

			if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < bcLaneCount; i++ {
				if inputK.getBit(i) {
					expS.values[i] = int64(ref(data16[i]))
				}
			}

			verifyI64RegOutput(t, &obsS, &expS)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzLengthStrFT fuzz-tests for: opLengthStr
func FuzzLengthStrFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "¢", "€", "𐍈", "ab", "a¢", "a€", "a𐍈", "abb", "ab¢", "ab€", "ab𐍈", "$¢€𐍈", "ab¢", "ab¢", "ab¢")

	testSuites := []bcop{
		opLengthStr,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data) {
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS i64RegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data) LengthZ3)

		for i := 0; i < bcLaneCount; i++ {
			expS.values[i] = int64(ref(data16[i]))
		}

		verifyI64RegOutput(t, &obsS, &expS)
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data)
		}
	})
}

// TestTrimCharUT2 unit-tests for: opTrim4charLeft, opTrim4charRight
func TestTrimCharUT2(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      [16]Data // data at SI
		cutset    Needle   // characters to trim
		expResult [16]Data // expected result in Z2:Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}
	testSuites := []testSuite{
		{
			op: opTrim4charLeft,
			unitTests: []unitTest{
				{
					data:      [16]Data{"ae", "eeeeef", "e", "b", "e¢€𐍈", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d"},
					expResult: [16]Data{"ae", "f", "", "b", "¢€𐍈", "b", "c", "", "a", "b", "c", "", "a", "b", "c", ""},
					cutset:    "ed", //TODO cutset with non-ascii not supported
				},
				{
					data:      [16]Data{"0", "0", "0", "0", "0", "a", "0", "0", "0", "0", "0", "0", "", "0", "0", "0"},
					expResult: [16]Data{"0", "0", "0", "0", "0", "", "0", "0", "0", "0", "0", "0", "", "0", "0", "0"},
					cutset:    "abc;",
				},
			},
		},
		{
			op: opTrim4charRight,
			unitTests: []unitTest{
				{
					data:      [16]Data{"ae", "feeeee", "e", "b", "¢€𐍈e", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d"},
					expResult: [16]Data{"a", "f", "", "b", "¢€𐍈", "b", "c", "", "a", "b", "c", "", "a", "b", "c", ""},
					cutset:    "ed", //TODO cutset with non-ascii not supported
				},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(fill4(string(ut.cutset)))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(ut.data[:])
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) (OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expOffset, expLength := ref(ut.data[i], ut.cutset)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestTrimCharBF brute-force for: opTrim4charLeft, opTrim4charRight
func TestTrimCharBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		// alphabet from which to generate needles and patterns
		dataAlphabet, cutsetAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, cutsetLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize, cutsetMaxSize int
		// bytecode to run
		op bcop
	}
	testSuites := []testSuite{
		{
			op:             opTrim4charLeft,
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'},
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
		},
		{
			op:             opTrim4charLeft,
			dataAlphabet:   []rune{'a', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'}, //TODO cutset can only be ASCII
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
		},
		{
			op:             opTrim4charRight,
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'},
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
		},
		{
			op:             opTrim4charRight,
			dataAlphabet:   []rune{'a', '¢', '€', '𐍈', '\n', 0},
			dataLenSpace:   []int{1, 2, 3, 4},
			dataMaxSize:    exhaustive,
			cutsetAlphabet: []rune{'a', 'b'}, //TODO cutset can only be ASCII
			cutsetLenSpace: []int{1, 2, 3, 4},
			cutsetMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, cutsetSpace []Needle) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, Needle) (OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			for _, cutset := range cutsetSpace {
				ctx.clear()

				ctx.setDict(fill4(string(cutset)))
				dictOffset := uint16(0)

				inputS := ctx.sRegFromStrings(data16)
				var obsS, expS sRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, dictOffset, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					if inputK.getBit(i) {
						expOffset, expLength := ref(data16[i], cutset)
						expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
						expS.sizes[i] = uint32(expLength)
					}
				}

				if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
					t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), prettyDataSlice(data16), msg)
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			cutsetSpace := flatten(createSpace(ts.cutsetLenSpace, ts.cutsetAlphabet, ts.cutsetMaxSize))
			run(&ts, fullMask, dataSpace, cutsetSpace)
		})
	}
}

// FuzzTrimCharFT fuzz-tests for: opTrim4charLeft, opTrim4charRight
func FuzzTrimCharFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "ab", "ac", "da", "ea", "fa", "ag", "ha", "ia", "ja", "ka", "a", "a¢€𐍈", "a", "a", "a", byte('a'), byte('b'), byte('c'), byte(';'))
	f.Add(uint16(0xFFFF), "0", "0", "0", "0", "0", "a", "0", "0", "0", "0", "0", "0", "", "0", "0", "0", byte('a'), byte('b'), byte('c'), byte(';'))

	testSuites := []bcop{
		opTrim4charLeft,
		opTrim4charRight,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, cutset Needle) {
		for _, c := range cutset {
			if c >= utf8.RuneSelf {
				return //TODO cutset does not yet support non-ASCII
			}
		}

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(string(cutset))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, Needle) (OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expOffset, expLength := ref(data16[i], cutset)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, char1, char2, char3, char4 byte) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		cutset := Needle([]byte{char1, char2, char3, char4})
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data, cutset)
		}
	})
}

// TestTrimWhiteSpaceUT unit-tests for: opTrimWsLeft, opTrimWsRight
func TestTrimWhiteSpaceUT(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data      Data   // data at SI
		expResult string // expected result Z2:Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}
	testSuites := []testSuite{
		{
			op: opTrimWsLeft,
			unitTests: []unitTest{
				{"a", "a"},
				{" a", "a"},
				{" a ", "a "},
				{" a a", "a a"},
				{"  a", "a"},
				{"     a", "a"},
				{" €", "€"},
			},
		},
		{
			op: opTrimWsRight,
			unitTests: []unitTest{
				{"a", "a"},
				{"a ", "a"},
				{" a ", " a"},
				{"a a ", "a a"},
				{"a  ", "a"},
				{"a     ", "a"},
				{"€ ", "€"},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data) (OffsetZ2, LengthZ3))
		expOffset, expLength := ref(ut.data)

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}
		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestTrimWhiteSpaceBF brute-force for: opTrimWsLeft, opTrimWsRight
func TestTrimWhiteSpaceBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		// alphabet from which to generate needles and patterns
		dataAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize int
		// bytecode to run
		op bcop
	}
	testSuites := []testSuite{
		{
			op:           opTrimWsLeft,
			dataAlphabet: []rune{'a', '¢', '\t', '\n', '\v', '\f', '\r', ' '},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
		},
		{
			op:           opTrimWsRight,
			dataAlphabet: []rune{'a', '¢', '\t', '\n', '\v', '\f', '\r', ' '},
			dataLenSpace: []int{1, 2, 3, 4, 5},
			dataMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data) {
		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data) (OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			ctx.clear()

			inputS := ctx.sRegFromStrings(data16)
			var obsS, expS sRegData

			if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < bcLaneCount; i++ {
				if inputK.getBit(i) {
					expOffset, expLength := ref(data16[i])
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
			if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
				t.Errorf("%v\ndata=%v\n%v", prettyName(ts.op), prettyDataSlice(data16), msg)
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			run(&ts, fullMask, createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize))
		})
	}
}

// FuzzTrimWhiteSpaceFT fuzz tests for: opTrimWsLeft, opTrimWsRight
func FuzzTrimWhiteSpaceFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "¢", "€", " 𐍈", "ab", "a¢ ", "a€", "a𐍈", "abb", " ab¢", "ab€", "ab𐍈\t", "\v$¢€𐍈", "\nab¢", "\fab¢", "\rab¢ ")

	testSuites := []bcop{
		opTrimWsLeft,
		opTrimWsRight,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data) {
		var ctx bctestContext
		defer ctx.Free()

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &inputS, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data) (OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return // assume all input data will be valid codepoints
			}
			if inputK.getBit(i) {
				expOffset, expLength := ref(data16[i])
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}
		if nok, msg := reportIssueS(&inputK, &obsS, &expS); nok {
			t.Errorf("%v\ndata=%v\n%v", prettyName(op), prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string) {
		data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data)
		}
	})
}

func refContainsPrefix(s Data, prefix Needle, caseSensitive, ASCIIOnly bool) (lane bool, offset OffsetZ2, length LengthZ3) {
	dataLen := LengthZ3(len(s))
	if prefix == "" { //NOTE: empty needles are dead lanes
		return false, 0, dataLen
	}
	hasPrefix := false
	if caseSensitive {
		hasPrefix = strings.HasPrefix(string(s), string(prefix))
	} else if ASCIIOnly {
		hasPrefix = strings.HasPrefix(stringext.NormalizeStringASCIIOnlyString(string(s)), stringext.NormalizeStringASCIIOnlyString(string(prefix)))
	} else {
		hasPrefix = strings.HasPrefix(stringext.NormalizeString(string(s)), stringext.NormalizeString(string(prefix)))
	}
	if hasPrefix {
		nRunesPrefix := utf8.RuneCountInString(string(prefix))
		nBytesPrefix2 := len(string([]rune(s)[:nRunesPrefix]))
		return true, OffsetZ2(nBytesPrefix2), dataLen - LengthZ3(nBytesPrefix2)
	}
	return false, 0, dataLen
}

func refContainsSuffix(s Data, suffix Needle, caseSensitive, ASCIIOnly bool) (lane bool, offset OffsetZ2, length LengthZ3) {
	dataLen := LengthZ3(len(s))
	if suffix == "" { //NOTE: empty needles are dead lanes
		return false, 0, dataLen
	}
	hasSuffix := false
	if caseSensitive {
		hasSuffix = strings.HasSuffix(s, string(suffix))
	} else if ASCIIOnly {
		hasSuffix = strings.HasSuffix(stringext.NormalizeStringASCIIOnlyString(s), stringext.NormalizeStringASCIIOnlyString(string(suffix)))
	} else {
		hasSuffix = strings.HasSuffix(stringext.NormalizeString(s), stringext.NormalizeString(string(suffix)))
	}
	if hasSuffix {
		nRunesSuffix := utf8.RuneCountInString(string(suffix))
		sRunes := []rune(s)
		nBytesSuffix2 := len(string(sRunes[(len(sRunes) - nRunesSuffix):]))
		return true, 0, dataLen - LengthZ3(nBytesSuffix2)
	}
	return false, 0, dataLen
}

// TestContainsPrefixSuffixUT1 unit-tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func TestContainsPrefixSuffixUT1(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      Data     // data at SI
		needle    Needle   // prefix/suffix to test
		expLane   bool     // expected K1
		expOffset OffsetZ2 // expected Z2
		expLength LengthZ3 // expected Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opContainsPrefixCs,
			unitTests: []unitTest{
				{"s", "s", true, 1, 0},
				{"sb", "s", true, 1, 1},
				{"s", "", false, 0, 1},
				{"", "", false, 0, 0},
				{"ssss", "ssss", true, 4, 0},
				{"sssss", "sssss", true, 5, 0},
				{"ss", "b", false, 0, 2},
			},
		},
		{
			op: opContainsPrefixCi,
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
		},
		{
			op: opContainsPrefixUTF8Ci,
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
		},
		{
			op: opContainsSuffixCs,
			unitTests: []unitTest{
				{"ab", "b", true, 0, 1},
				{"a", "a", true, 0, 0},
				{"", "a", false, 0, 0},
				{"a", "", false, 0, 1}, // Empty needle gives failing match
				{"", "", false, 0, 0},  // Empty needle gives failing match
				{"aaaa", "aaaa", true, 0, 0},
				{"aaaaa", "aaaaa", true, 0, 0},
				{"aa", "b", false, 0, 2},
			},
		},
		{
			op: opContainsSuffixCi,
			unitTests: []unitTest{
				{"aB", "b", true, 0, 1},
				{"ab", "B", true, 0, 1},
				{"A", "a", true, 0, 0},
				{"", "a", false, 0, 0},
				{"a", "A", true, 0, 0},
				{"a", "", false, 0, 1}, // Empty needle should yield failing match
				{"", "", false, 0, 0},  // Empty needle should yield failing match
				{"aAaA", "aaaa", true, 0, 0},
				{"aAaAa", "aaaaa", true, 0, 0},
				{"aa", "b", false, 0, 2},
			},
		},
		{
			op: opContainsSuffixUTF8Ci,
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
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		// TODO [UNSAFE]: prepend three bytes to data such that we can read backwards 4 bytes at a time
		ctx.appendData(string([]byte{0, 0, 0, 0}))

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, ut.needle)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
			return
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestContainsPrefixSuffixBF brute-force tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func TestContainsPrefixSuffixBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		// alphabet from which to generate needles and patterns
		dataAlphabet, needleAlphabet []rune
		// space of lengths of the words made of alphabet
		dataLenSpace, needleLenSpace []int
		// maximum number of elements in dataSpace
		dataMaxSize, needleMaxSize int
		// bytecode to run
		op bcop
	}
	testSuites := []testSuite{
		{
			op:             opContainsPrefixCs,
			dataAlphabet:   []rune{'a', 'b', '\n'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 'b'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsPrefixCi,
			dataAlphabet:   []rune{'a', 's', 'S'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 's', 'S'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsPrefixUTF8Ci,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsPrefixUTF8Ci,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			dataMaxSize:    1000,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			needleMaxSize:  500,
		},
		{
			op:             opContainsSuffixCs,
			dataAlphabet:   []rune{'a', 'b', '\n', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5, 6},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 'b'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5, 6},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSuffixCi,
			dataAlphabet:   []rune{'s', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5, 6},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'s', 'S'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5, 6},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSuffixUTF8Ci,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5},
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSuffixUTF8Ci,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			dataMaxSize:    500,
			needleAlphabet: []rune{'s', 'S', 'ſ'},
			needleLenSpace: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			needleMaxSize:  1000,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, needleSpace []Needle) {
		var ctx bctestContext
		defer ctx.Free()

		// pre-compute encoded needles for speed
		encNeedles := make([]string, len(needleSpace))
		for i, needle := range needleSpace { // precompute encoded needles for speed
			encNeedles[i] = encodeNeedleOp(needle, ts.op)
		}

		ref := refFunc(ts.op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			for needleIdx, needle := range needleSpace {
				ctx.clear()

				// TODO [UNSAFE]: prepend three bytes to data such that we can read backwards 4 bytes at a time
				ctx.appendData(string([]byte{0, 0, 0, 0}))

				ctx.setDict(encNeedles[needleIdx])
				dictOffset := uint16(0)

				inputS := ctx.sRegFromStrings(data16)
				var obsS, expS sRegData
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					expLane, expOffset, expLength := ref(data16[i], needle)
					if inputK.getBit(i) && expLane {
						expK.setBit(i)
						expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
						expS.sizes[i] = uint32(expLength)
					}
				}

				if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
					t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), needle, prettyDataSlice(data16), msg)
					return
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			needleSpace := flatten(createSpace(ts.needleLenSpace, ts.needleAlphabet, ts.needleMaxSize))
			run(&ts, fullMask, dataSpace, needleSpace)
		})
	}
}

// FuzzContainsPrefixSuffixFT fuzz-tests for: opContainsPrefixCs, opContainsPrefixCi, opContainsPrefixUTF8Ci,
// opContainsSuffixCs, opContainsSuffixCi, opContainsSuffixUTF8Ci
func FuzzContainsPrefixSuffixFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "a")
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "𐍈")
	f.Add(uint16(0xFFFF), "M", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "м")

	testSuites := []bcop{
		opContainsPrefixCs,
		opContainsPrefixCi,
		opContainsPrefixUTF8Ci,
		opContainsSuffixCs,
		opContainsSuffixCi,
		opContainsSuffixUTF8Ci,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, needle Needle) {
		if needle == "" {
			return // empty needle is invalid
		}
		// only UTF8 code is supposed to handle UTF8 needle data16
		if (op != opContainsPrefixUTF8Ci) && (op != opContainsSuffixUTF8Ci) {
			for _, c := range needle {
				if c >= utf8.RuneSelf {
					return
				}
			}
		}

		// assume all input data will be valid codepoints
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		// TODO [UNSAFE]: prepend three bytes to data such that we can read backwards 4 bytes at a time
		ctx.appendData(string([]byte{0, 0, 0, 0}))

		ctx.setDict(encodeNeedleOp(needle, op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			expLane, expOffset, expLength := ref(data16[i], needle)
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(op), needle, prettyData(data16), msg)
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string) {
		for _, op := range testSuites {
			data := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
			run(t, op, kRegData{mask: lanes}, data, Needle(needle))
		}
	})
}

// TestContainsSubstrUT1 unit-tests for: opContainsSubstrCs, opContainsSubstrCi, opContainsSubstrUTF8Ci
func TestContainsSubstrUT1(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      Data     // data at SI
		needle    Needle   // substr to test
		expLane   bool     // expected K1
		expOffset OffsetZ2 // expected Z2
		expLength LengthZ3 // expected Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opContainsSubstrCs,
			unitTests: []unitTest{
				{"Ω", "Ω", true, 3, 0},
				{"因", "因", true, 3, 0}, // chinese with no equal-fold alternative
				{"s", "s", true, 1, 0},
				{"sb", "s", true, 1, 1},
				{"ssss", "ssss", true, 4, 0},
				{"sssss", "sssss", true, 5, 0},
				{"ss", "b", false, 0, 2},
			},
		},
		{
			op: opContainsSubstrCi,
			unitTests: []unitTest{
				{"s", "s", true, 1, 0},
				{"sb", "s", true, 1, 1},
				{"sSsS", "ssss", true, 4, 0},
				{"ssSss", "sssss", true, 5, 0},
				{"sS", "b", false, 0, 2},
			},
		},
		{
			op: opContainsSubstrUTF8Ci,
			unitTests: []unitTest{
				{"s", "s", true, 1, 0},
				{"sb", "s", true, 1, 1},
				{"sSsS", "ssss", true, 4, 0},
				{"ssSss", "sssss", true, 5, 0},
				{"sS", "b", false, 0, 2},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, ut.needle)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestContainsSubstrUT2 unit-tests for: opContainsSubstrCs, opContainsSubstrCi, opContainsSubstrUTF8Ci
func TestContainsSubstrUT2(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		data16     [16]Data     // data pointed to by SI
		needle     Needle       // needle needs to be encoded and passed as string constant via the immediate dictionary
		expLanes   uint16       // expected lanes K1
		expOffsets [16]OffsetZ2 // expected offset Z2
		expLengths [16]LengthZ3 // expected length Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	testSuites := []testSuite{
		{
			op: opContainsSubstrCs,
			unitTests: []unitTest{
				{
					data16:     [16]Data{"0100", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "00",
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{4, 4, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"Ax", "xxxxAx", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "A",
					expLanes:   uint16(0b0000000000000011),
					expOffsets: [16]OffsetZ2{1, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"Axxxxxxx", "xxxxAxxx", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "A",
					expLanes:   uint16(0b0000000000000011),
					expOffsets: [16]OffsetZ2{1, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{7, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aaaa", "ab", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b000000000000001),
					expOffsets: [16]OffsetZ2{2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aaaab", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aaaaa",
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aa", "ab", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{2, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"0100000", "100000", "00000", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "00000",
					expLanes:   uint16(0b0000000000000111),
					expOffsets: [16]OffsetZ2{7, 6, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16: [16]Data{
						"Axxxxxxx", "xxxxAxxx", "A", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx"},
					needle:     "A",
					expLanes:   uint16(0b0000000000000111),
					expOffsets: [16]OffsetZ2{1, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{7, 3, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
				},
				{
					data16: [16]Data{
						"AAxxxxxx", "xxxAAxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx"},
					needle:     "AA",
					expLanes:   uint16(0b0000000000000011),
					expOffsets: [16]OffsetZ2{2, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{6, 3, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
				},
			},
		},
		{
			op: opContainsSubstrCi,
			unitTests: []unitTest{
				{
					data16:     [16]Data{"aAaA", "ab", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b000000000000001),
					expOffsets: [16]OffsetZ2{2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
		{
			op: opContainsSubstrUTF8Ci,
			unitTests: []unitTest{
				{
					data16:     [16]Data{"asa", "aaa", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "asa",
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 3, 3, 4, 3, 3, 3, 4, 3, 3, 3, 4, 4, 4, 4, 5},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"ſſa", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "sa",
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{5, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"saa", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aa", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b000000000000001),
					expOffsets: [16]OffsetZ2{2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aaa", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "aa",
					expLanes:   uint16(0b000000000000001),
					expOffsets: [16]OffsetZ2{2, 3, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"aſ", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "ass",
					expLanes:   uint16(0b000000000000000),
					expOffsets: [16]OffsetZ2{2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"a", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					needle:     "A",
					expLanes:   uint16(0b000000000000001),
					expOffsets: [16]OffsetZ2{1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"a", "s", "S", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ"},
					needle:     "aa",
					expLanes:   uint16(0b000000000000000),
					expOffsets: [16]OffsetZ2{3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					data16:     [16]Data{"a", "s", "S", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ", "ſ"},
					needle:     "ss",
					expLanes:   uint16(0b000000000000000),
					expOffsets: [16]OffsetZ2{3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodeNeedleOp(ut.needle, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(ut.data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(ut.data16[i], ut.needle)
				if nok, msg := reportIssueKSRef(expLane, getBit(ut.expLanes, i), expOffset, ut.expOffsets[i], expLength, ut.expLengths[i]); nok {
					t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, ut.data16, msg)
					return
				}
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.needle, prettyData(ut.data16), msg)
			return
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestContainsSubstrBF brute-force tests for: opContainsSubstrCs, opContainsSubstrCi, opContainsSubstrUTF8Ci
func TestContainsSubstrBF(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		dataAlphabet, needleAlphabet []rune // alphabet from which to generate needles and patterns
		dataLenSpace, needleLenSpace []int  // space of lengths of the words made of alphabet
		dataMaxSize, needleMaxSize   int    // maximum number of elements in dataSpace
		op                           bcop   // bytecode to run
	}

	testSuites := []testSuite{
		{
			op:             opContainsSubstrCs,
			dataAlphabet:   []rune{'a', 'b', 0x0, 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 'b'},
			needleLenSpace: []int{1, 2, 3, 4, 5}, // NOTE empty needle is handled in go
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSubstrCs,
			dataAlphabet:   []rune{'a', 'b'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a'},
			needleLenSpace: []int{1, 4, 5}, // NOTE empty needle is handled in go
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSubstrCi,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 's'},
			needleLenSpace: []int{1, 2, 3, 4, 5}, // NOTE empty needle is handled in go
			needleMaxSize:  exhaustive,
		},
		{
			op:             opContainsSubstrUTF8Ci,
			dataAlphabet:   []rune{'a', 's', 'S', 'ſ'},
			dataLenSpace:   []int{0, 1, 2, 3, 4, 5},
			dataMaxSize:    exhaustive,
			needleAlphabet: []rune{'a', 's', 'S', 'ſ'},
			needleLenSpace: []int{1, 2, 3, 4, 5}, // NOTE empty needle is handled in go
			needleMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, needleSpace []Needle) {
		// pre-compute encoded needles for speed
		encNeedles := make([]string, len(needleSpace))
		for i, needle := range needleSpace { // precompute encoded needles for speed
			encNeedles[i] = encodeNeedleOp(needle, ts.op)
		}

		var ctx bctestContext
		defer ctx.Free()

		ref := refFunc(ts.op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			for needleIdx, needle := range needleSpace {
				ctx.clear()
				ctx.setDict(encNeedles[needleIdx])
				dictOffset := uint16(0)

				inputS := ctx.sRegFromStrings(data16)
				var obsS, expS sRegData
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					if inputK.getBit(i) {
						expLane, expOffset, expLength := ref(data16[i], needle)
						if expLane {
							expK.setBit(i)
							expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
							expS.sizes[i] = uint32(expLength)
						}
					}
				}

				if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
					t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), needle, prettyDataSlice(data16), msg)
					return
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			needleSpace := flatten(createSpace(ts.needleLenSpace, ts.needleAlphabet, ts.needleMaxSize))
			run(&ts, fullMask, dataSpace, needleSpace)
		})
	}
}

// FuzzContainsSubstrFT fuzz-tests for: opContainsSubstrCs, opContainsSubstrCi, opContainsSubstrUTF8Ci
func FuzzContainsSubstrFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "a")
	f.Add(uint16(0xFFFF), "M", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "м")

	testSuites := []bcop{
		opContainsSubstrCs,
		opContainsSubstrCi,
		opContainsSubstrUTF8Ci,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, needle Needle) {
		// assume all input data will be valid codepoints
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return
			}
		}

		if needle == "" {
			return // empty needle is invalid
		}

		// only UTF8 code is supposed to handle UTF8 needle
		if op != opContainsSubstrUTF8Ci {
			for _, c := range needle {
				if c >= utf8.RuneSelf {
					return
				}
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodeNeedleOp(needle, op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, Needle) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(data16[i], needle)
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(op), needle, prettyData(data16), msg)
			return
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string) {
		data16 := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		for _, op := range testSuites {

			run(t, op, kRegData{mask: lanes}, data16, Needle(needle))
		}
	})
}

// TestContainsPatternUT1 unit-tests for: opContainsPatternCs, opContainsPatternCi, opContainsPatternUTF8Ci,
// opEqPatternCs, opEqPatternCi, opEqPatternUTF8Ci
func TestContainsPatternUT1(t *testing.T) {
	t.Parallel()
	type unitTest struct {
		data      Data // data at SI
		pattern   stringext.Pattern
		expLane   bool     // expected K1
		expOffset OffsetZ2 // expected Z2
		expLength LengthZ3 // expected Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	const wc = '_'
	const escape = '@'

	testSuites := []testSuite{
		{
			op: opContainsPatternCs,
			unitTests: []unitTest{
				{"s", stringext.NewPattern("s", wc, escape), true, 1, 0},
				{"sb", stringext.NewPattern("s", wc, escape), true, 1, 1},
				{"ssss", stringext.NewPattern("ssss", wc, escape), true, 4, 0},
				{"sssss", stringext.NewPattern("sssss", wc, escape), true, 5, 0},
				{"ss", stringext.NewPattern("b", wc, escape), false, 0, 2},
			},
		},
		{
			op: opEqPatternCs,
			unitTests: []unitTest{
				{"a", stringext.NewPattern("a", wc, escape), true, 1, 0},
				{"a", stringext.NewPattern("b", wc, escape), false, 0, 0},
				{"axa", stringext.NewPattern("a_a", wc, escape), true, 3, 0},
				{"ax", stringext.NewPattern("a_b", wc, escape), false, 0, 0},
				{"ax", stringext.NewPattern("a_", wc, escape), true, 2, 0},
			},
		},
		{
			op: opEqPatternCi,
			unitTests: []unitTest{
				{"A", stringext.NewPattern("a", wc, escape), true, 1, 0},
				{"A", stringext.NewPattern("b", wc, escape), false, 0, 0},
				{"Axa", stringext.NewPattern("a_a", wc, escape), true, 3, 0},
				{"Ax", stringext.NewPattern("a_b", wc, escape), false, 0, 0},
				{"Ax", stringext.NewPattern("a_", wc, escape), true, 2, 0},
			},
		},
		{
			op: opEqPatternUTF8Ci,
			unitTests: []unitTest{
				{"as", stringext.NewPattern("s", wc, escape), false, 1, 0},
				{"ſſ", stringext.NewPattern("s", wc, escape), false, 0, 0},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodePatternOp(&ut.pattern, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(fill16(ut.data))
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, *stringext.Pattern) (bool, OffsetZ2, LengthZ3))
		expLane, expOffset, expLength := ref(ut.data, &ut.pattern)

		if nok, msg := reportIssueKSRef(expLane, ut.expLane, expOffset, ut.expOffset, expLength, ut.expLength); nok {
			t.Errorf("RefImpl: %v\npattern=%q\ndata=%v\n%v", prettyName(ts.op), ut.pattern, ut.data, msg)
		}

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) && expLane {
				expK.setBit(i)
				expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
				expS.sizes[i] = uint32(expLength)
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\npattern=%q\ndata=%v\n%v", prettyName(ts.op), ut.pattern, ut.data, msg)
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestContainsPatternUT2 unit-tests for: opContainsPatternCs, opContainsPatternCi, opContainsPatternUTF8Ci
// opEqPatternCs, opEqPatternCi, opEqPatternUTF8Ci
func TestContainsPatternUT2(t *testing.T) {
	t.Parallel()

	type unitTest struct {
		pattern    stringext.Pattern // pattern needs to be encoded and passed as string constant via the immediate dictionary
		data16     [16]Data          // data pointed to by SI
		expLanes   uint16            // expected lanes K1
		expOffsets [16]OffsetZ2      // expected offset Z2
		expLengths [16]LengthZ3      // expected length Z3
	}
	type testSuite struct {
		unitTests []unitTest
		op        bcop
	}

	const wc = '_'
	const escape = '@'

	testSuites := []testSuite{
		{
			op: opContainsPatternCs,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("a", wc, escape),
					data16:     [16]Data{"a¢", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{1, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{2, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("b", wc, escape),
					data16:     [16]Data{"ba", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("a_b", wc, escape),
					data16:     [16]Data{"", "a€x", "b", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("a_a", wc, escape),
					data16:     [16]Data{"𐍈$a", "a¢a", "b¢a", "$¢a", "¢¢a", "€¢a", "𐍈¢a", "a€a", "b€a", "$€a", "¢€a", "€€a", "𐍈€a", "a𐍈a", "b𐍈a", "$𐍈a"},
					expLanes:   uint16(0b0010000010000010),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("a_a", wc, escape),
					data16:     [16]Data{"aba", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 4, 4, 3, 4, 4, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4},
					expLengths: [16]LengthZ3{0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{ // read beyond end of data situation
					pattern:    stringext.NewPattern("a_a", wc, escape),
					data16:     [16]Data{"a¢", "a", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("00", wc, escape),
					data16:     [16]Data{"0100", "100", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000011),
					expOffsets: [16]OffsetZ2{4, 3, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("ba", wc, escape),
					data16:     [16]Data{"cb", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("aaaaa", wc, escape),
					data16:     [16]Data{"aaaabb", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{2, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("ba", wc, escape),
					data16:     [16]Data{"bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb", "bb"},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("aa", wc, escape),
					data16:     [16]Data{"ab", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{2, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("aa", wc, escape),
					data16:     [16]Data{"baabb", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("aa", wc, escape),
					data16:     [16]Data{"baabb", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("aa", wc, escape),
					data16:     [16]Data{"aa", "ab", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{2, 2, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 3, 3, 3, 4},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("00000", wc, escape),
					data16:     [16]Data{"0100000", "100000", "00000", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000111),
					expOffsets: [16]OffsetZ2{7, 6, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern: stringext.NewPattern("A", wc, escape),
					data16: [16]Data{
						"Axxxxxxx", "xxxxAxxx", "A", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx"},
					expLanes:   uint16(0b0000000000000111),
					expOffsets: [16]OffsetZ2{1, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{7, 3, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
				},
				{
					pattern: stringext.NewPattern("AA", wc, escape),
					data16: [16]Data{
						"AAxxxxxx", "xxxAAxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx"},
					expLanes:   uint16(0b0000000000000011),
					expOffsets: [16]OffsetZ2{2, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{6, 3, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
				},
				{
					pattern: stringext.NewPattern("AA_BB", wc, escape),
					data16: [16]Data{
						"AAxBBxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx",
						"xxxxxxxx", "xxxxxxxx", "xxxxxxxx", "xxxxxxxx"},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{3, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
				},
			},
		},
		{
			op: opContainsPatternCi,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("ss_s", wc, escape),
					data16:     [16]Data{"sscs", "ſass", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("b", wc, escape),
					data16:     [16]Data{"Ba", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("a_b", wc, escape),
					data16:     [16]Data{"", "A€x", "b", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
		{
			op: opContainsPatternUTF8Ci,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("ss_s", wc, escape),
					data16:     [16]Data{"sscs", "ſass", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("a_b", wc, escape),
					data16:     [16]Data{"", "A€x", "b", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("b", wc, escape),
					data16:     [16]Data{"Ba", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					expLengths: [16]LengthZ3{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
		{
			op: opEqPatternCs,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("a", wc, escape),
					data16:     [16]Data{"a¢", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{1, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{2, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{ // read beyond end of data situation
					pattern:    stringext.NewPattern("a_a", wc, escape),
					data16:     [16]Data{"a¢", "a", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
		{
			op: opEqPatternCi,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("a", wc, escape),
					data16:     [16]Data{"A¢", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{1, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{2, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{ // read beyond end of data situation
					pattern:    stringext.NewPattern("a_a", wc, escape),
					data16:     [16]Data{"A¢", "A", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{6, 4, 4, 4, 5, 6, 7, 5, 5, 5, 6, 7, 8, 6, 6, 6},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
		},
		{
			op: opEqPatternUTF8Ci,
			unitTests: []unitTest{
				{
					pattern:    stringext.NewPattern("00000", wc, escape),
					data16:     [16]Data{"00000", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{5, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1},
					expLengths: [16]LengthZ3{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("s_s", wc, escape),
					data16:     [16]Data{"sbs", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{3, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("ss", wc, escape),
					data16:     [16]Data{"ſſ", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000001),
					expOffsets: [16]OffsetZ2{4, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("s", wc, escape),
					data16:     [16]Data{"ſſ", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{1, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{2, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
				{
					pattern:    stringext.NewPattern("s", wc, escape),
					data16:     [16]Data{"as", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
					expLanes:   uint16(0b0000000000000000),
					expOffsets: [16]OffsetZ2{1, 1, 3, 3, 4, 5, 6, 1, 4, 4, 5, 6, 7, 1, 5, 5},
					expLengths: [16]LengthZ3{2, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0},
				},
			},
		},
	}

	run := func(ts *testSuite, ut *unitTest, inputK kRegData) {
		var ctx bctestContext
		defer ctx.Free()

		ctx.setDict(encodePatternOp(&ut.pattern, ts.op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(ut.data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(ts.op).(func(Data, *stringext.Pattern) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(ut.data16[i], &ut.pattern)
				if nok, msg := reportIssueKSRef(expLane, getBit(ut.expLanes, i), expOffset, ut.expOffsets[i], expLength, ut.expLengths[i]); nok {
					t.Errorf("RefImpl: %v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.pattern, ut.data16, msg)
					return
				}
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\nneedle=%q\ndata=%v\n%v", prettyName(ts.op), ut.pattern, prettyData(ut.data16), msg)
			return
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			for _, ut := range ts.unitTests {
				run(&ts, &ut, fullMask)
			}
		})
	}
}

// TestContainsPatternBF brute-force tests for: opContainsPatternCs, opContainsPatternCi, opContainsPatternUTF8Ci
// opEqPatternCs, opEqPatternCi, opEqPatternUTF8Ci
func TestContainsPatternBF(t *testing.T) {
	t.Parallel()
	type testSuite struct {
		dataAlphabet, patternAlphabet []rune // alphabet from which to generate needles and patterns
		dataLenSpace, patternLenSpace []int  // space of lengths of the words made of alphabet
		dataMaxSize, patternMaxSize   int    // maximum number of elements in dataSpace
		op                            bcop   // bytecode to run
	}
	testSuites := []testSuite{
		{
			op:              opContainsPatternCs,
			dataAlphabet:    []rune{'a', 'b', '$', '¢', '€', '𐍈'},
			dataLenSpace:    []int{2, 3, 4, 5},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'a', 'b'},
			patternLenSpace: []int{1, 2, 3, 4, 5}, // NOTE empty pattern is handled in go
			patternMaxSize:  exhaustive,
		},
		{
			op:              opContainsPatternCs,
			dataAlphabet:    []rune{'a', 'b'},
			dataLenSpace:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'a', 'b'},
			patternLenSpace: []int{4, 5}, // NOTE empty pattern is handled in go
			patternMaxSize:  exhaustive,
		},
		{
			op:              opContainsPatternCi,
			dataAlphabet:    []rune{'a', 'b'},
			dataLenSpace:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'a', 'b'},
			patternLenSpace: []int{4, 5}, // NOTE empty pattern is handled in go
			patternMaxSize:  exhaustive,
		},
		{
			op:              opContainsPatternUTF8Ci,
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataLenSpace:    []int{1, 2, 3, 4},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternLenSpace: []int{1, 2, 3, 4, 5},
			patternMaxSize:  exhaustive,
		},
		{
			op:              opEqPatternCs,
			dataAlphabet:    []rune{'a', 'b', '$', '¢', '€', '𐍈'},
			dataLenSpace:    []int{2, 3, 4, 5},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'a', 'b'},
			patternLenSpace: []int{1, 2, 3, 4, 5}, // NOTE empty pattern is handled in go
			patternMaxSize:  exhaustive,
		},
		{
			op:              opEqPatternCs,
			dataAlphabet:    []rune{'0', '1'},
			dataLenSpace:    []int{1, 2, 3, 4, 5, 6, 7, 8},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'0', '1'},
			patternLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8},
			patternMaxSize:  exhaustive,
		},
		{
			op:              opEqPatternCi,
			dataAlphabet:    []rune{'a', 'b'},
			dataLenSpace:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'a', 'b'},
			patternLenSpace: []int{4, 5}, // NOTE empty pattern is handled in go
			patternMaxSize:  exhaustive,
		},
		{
			op:              opEqPatternUTF8Ci,
			dataAlphabet:    []rune{'a', 'b', 'c', 's', 'ſ'},
			dataLenSpace:    []int{1, 2, 3, 4},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'s', 'S', 'k', 'K'},
			patternLenSpace: []int{1, 2, 3, 4, 5},
			patternMaxSize:  exhaustive,
		},
		{
			op:              opEqPatternUTF8Ci,
			dataAlphabet:    []rune{'0', '1'},
			dataLenSpace:    []int{1, 2, 3, 4, 5, 6, 7, 8},
			dataMaxSize:     exhaustive,
			patternAlphabet: []rune{'0', '1'},
			patternLenSpace: []int{1, 2, 3, 4, 5, 6, 7, 8},
			patternMaxSize:  exhaustive,
		},
	}

	run := func(ts *testSuite, inputK kRegData, dataSpace [][]Data, patternSpace []stringext.Pattern) {
		var ctx bctestContext
		defer ctx.Free()

		// pre-compute encoded patterns for speed
		encPattern := make([]string, len(patternSpace))
		for patternIdx, pattern := range patternSpace { // precompute encoded needles for speed
			encPattern[patternIdx] = encodePatternOp(&pattern, ts.op)
		}

		ref := refFunc(ts.op).(func(Data, *stringext.Pattern) (bool, OffsetZ2, LengthZ3))

		for _, data16 := range dataSpace {
			for patternIdx, pattern := range patternSpace {
				ctx.clear()
				ctx.setDict(encPattern[patternIdx])
				dictOffset := uint16(0)

				inputS := ctx.sRegFromStrings(data16)
				var obsS, expS sRegData
				var obsK, expK kRegData

				if err := ctx.ExecuteOpcode(ts.op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < bcLaneCount; i++ {
					if inputK.getBit(i) {
						expLane, expOffset, expLength := ref(data16[i], &pattern)
						if expLane {
							expK.setBit(i)
							expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
							expS.sizes[i] = uint32(expLength)
						}
					}
				}

				if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
					t.Errorf("%v\npattern=%q\ndata=%v\n%v", prettyName(ts.op), pattern, prettyDataSlice(data16), msg)
					return
				}
			}
		}
	}

	for _, ts := range testSuites {
		t.Run(prettyName(ts.op), func(t *testing.T) {
			dataSpace := createSpace(ts.dataLenSpace, ts.dataAlphabet, ts.dataMaxSize)
			patternSpace := createSpacePattern(ts.patternLenSpace, ts.patternAlphabet, ts.patternMaxSize)
			run(&ts, fullMask, dataSpace, patternSpace)
		})
	}
}

// FuzzContainsPatternFT fuzz-tests for: opContainsPatternCs, opContainsPatternCi, opContainsPatternUTF8Ci
// opEqPatternCs, opEqPatternCi, opEqPatternUTF8Ci
func FuzzContainsPatternFT(f *testing.F) {
	f.Add(uint16(0xFFFF), "a", "a;", "a\n", "a𐍈", "𐍈a", "𐍈", "aaa", "abbb", "accc", "a𐍈", "𐍈aaa", "𐍈aa", "aaa", "bbba", "cca", "da", "a")
	f.Add(uint16(0xFFFF), "M", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "м")

	const wc = '_'
	const escape = '@'

	testSuites := []bcop{
		opContainsPatternCs,
		opContainsPatternCi,
		opContainsPatternUTF8Ci,
		opEqPatternCs,
		opEqPatternCi,
		opEqPatternUTF8Ci,
	}

	run := func(t *testing.T, op bcop, inputK kRegData, data16 [16]Data, pattern *stringext.Pattern) {
		// empty pattern is invalid
		if pattern.Needle == "" {
			return
		}

		// first and last character of pattern may not be a wildcard
		if pattern.Wildcard[0] || pattern.Wildcard[len(pattern.Wildcard)-1] {
			return
		}

		// only UTF8 code is supposed to handle UTF8 pattern
		if op != opContainsPatternUTF8Ci {
			for _, c := range pattern.Needle {
				if c >= utf8.RuneSelf {
					return
				}
			}
		}

		// assume all input data will be valid codepoints
		for i := 0; i < bcLaneCount; i++ {
			if !utf8.ValidString(data16[i]) {
				return
			}
		}

		var ctx bctestContext
		defer ctx.Free()

		ctx.clear()
		ctx.setDict(encodePatternOp(pattern, op))
		dictOffset := uint16(0)

		inputS := ctx.sRegFromStrings(data16[:])
		var obsS, expS sRegData
		var obsK, expK kRegData

		if err := ctx.ExecuteOpcode(op, []any{&obsS, &obsK, &inputS, dictOffset, &inputK}, inputK); err != nil {
			t.Fatal(err)
		}

		ref := refFunc(op).(func(Data, *stringext.Pattern) (bool, OffsetZ2, LengthZ3))

		for i := 0; i < bcLaneCount; i++ {
			if inputK.getBit(i) {
				expLane, expOffset, expLength := ref(data16[i], pattern)
				if expLane {
					expK.setBit(i)
					expS.offsets[i] = uint32(expOffset) + inputS.offsets[i]
					expS.sizes[i] = uint32(expLength)
				}
			}
		}

		if nok, msg := reportIssueKS(&inputK, &obsK, &expK, &obsS, &expS); nok {
			t.Errorf("%v\npattern=%q\ndata=%v\n%v", prettyName(op), pattern, prettyData(data16), msg)
			return
		}
	}

	f.Fuzz(func(t *testing.T, lanes uint16, d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 string, needle string) {
		data16 := [16]Data{d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15}
		pattern := stringext.NewPattern(needle, wc, escape)
		for _, op := range testSuites {
			run(t, op, kRegData{mask: lanes}, data16, &pattern)
		}
	})
}

func TestBytecodeAbsInt(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.Free()

	inputS := i64RegData{values: [16]int64{5, -52, 1002, -412, 0, 1, -3}}
	inputK := kRegData{mask: uint16((1 << 7) - 1)}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.ExecuteOpcode(opabsi64, []any{&outputS, &outputK, &inputS, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK, &inputK)
	verifyI64RegOutput(t, &outputS, &i64RegData{values: [16]int64{5, 52, 1002, 412, 0, 1, 3}})
}

func TestBytecodeAbsFloat(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.Free()

	inputS := f64RegData{values: [16]float64{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4}}
	inputK := kRegData{mask: uint16((1 << 10) - 1)}

	outputS := f64RegData{}
	outputK := kRegData{}

	if err := ctx.ExecuteOpcode(opabsf64, []any{&outputS, &outputK, &inputS, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK, &inputK)
	verifyF64RegOutput(t, &outputS, &f64RegData{values: [16]float64{5, 4, 3, 2, 1, 0, 1, 2, 3, 4}})
}

func TestBytecodeToInt(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.Free()

	inputV := ctx.vRegFromValues([]any{
		[]byte{0x20},
		[]byte{0x21, 0xff},
		[]byte{0x22, 0x11, 0x33},
		ion.Int(-42),
		ion.Uint(12345678),
	}, nil)
	inputK := kRegData{mask: uint16((1 << 5) - 1)}

	outputS := i64RegData{}
	outputK := kRegData{}

	if err := ctx.ExecuteOpcode(opunboxcoercei64, []any{&outputS, &outputK, &inputV, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK, &inputK)
	verifyI64RegOutput(t, &outputS, &i64RegData{values: [16]int64{0, 255, 0x1133, -42, 12345678}})
}

func TestBytecodeIsNull(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.Free()

	inputV := ctx.vRegFromValues([]any{
		[]byte{0x10}, []byte{0x2f}, []byte{0x30}, []byte{0x40},
		[]byte{0x5f}, []byte{0x6f}, []byte{0x70}, []byte{0x80},
		[]byte{0x90}, []byte{0xaf}, []byte{0xb0}, []byte{0xcf},
		[]byte{0xe0}, []byte{0xef}, []byte{0xff}, []byte{0x00},
	}, nil)
	inputK := kRegData{mask: 0xFFFF}
	outputK := kRegData{}

	if err := ctx.ExecuteOpcode(opisnullv, []any{&outputK, &inputV, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK, &kRegData{mask: 0x6A32})
}

/////////////////////////////////////////////////////////////
// Helper functions

// prettyDataSlice joins values with comma's such that you can copy it a go array
func prettyDataSlice(values []Data) (result string) {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i := 0; i < len(values); i++ {
		sb.WriteString(fmt.Sprintf("%q", values[i])) // NOTE strings.Join(values, ",") does not escape
		if i < len(values)-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

func prettyData(values [16]Data) string {
	return prettyDataSlice(values[:])
}

type strCmpType int

const (
	cs      strCmpType = iota
	ciASCII            // case-insensitive on ASCII only
	ciUTF8             // case-insensitive all unicode code-points
)

func (t strCmpType) String() string {
	switch t {
	case cs:
		return "CS"
	case ciASCII:
		return "CI_ASCII"
	case ciUTF8:
		return "CI_UTF8"
	}
	return "??"
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

// flatten flattens the provided slice of slices into one single slice; dual of split16
func flatten(dataSpace [][]Data) []Needle {
	result := make([]Needle, len(dataSpace)*16)
	for j, data16 := range dataSpace {
		for i := 0; i < bcLaneCount; i++ {
			result[j*16+i] = Needle(data16[i])
		}
	}
	return result
}

// split16 splits the provided slice into slices of slice with size 16
func split16(data []Data) [][]Data {
	numberOfSlices := (len(data) + 15) / 16
	results := make([][]Data, numberOfSlices)
	for i := range data {
		group := i / 16
		results[group] = append(results[group], data[i])
	}
	tailLength := len(results[numberOfSlices-1])
	if tailLength < 16 {
		lastValue := data[len(data)-1]
		for i := tailLength; i < 16; i++ {
			results[numberOfSlices-1] = append(results[numberOfSlices-1], lastValue)
		}
	}
	return results
}

func fill16(msg string) (result []string) {
	result = make([]string, 16)
	for i := 0; i < bcLaneCount; i++ {
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

func reportIssueKS(initK, obsK, expK *kRegData, obsS, expS *sRegData) (hasIssue bool, msg string) {

	btou := func(b bool) uint8 {
		if b {
			return 1
		}
		return 0
	}

	toStringWithColor := func(initK, obsK, expK *kRegData, obsS, expS *sRegData) (result [6]string) {
		colorRed := "\033[31m"
		colorReset := "\033[0m"

		result[0] = ""
		result[1] = ""
		result[2] = "["
		result[3] = "["
		result[4] = "["
		result[5] = "["
		for j := 0; j < bcLaneCount; j++ {
			color1 := colorReset
			color2 := colorReset
			color3 := colorReset

			initLane := initK.getBit(j)
			obsLane := obsK.getBit(j)
			expLane := expK.getBit(j)

			if initLane && (obsLane || expLane) {
				if obsLane != expLane {
					color1 = colorRed
				}
				if obsS.offsets[j] != expS.offsets[j] {
					color2 = colorRed
				}
				if obsS.sizes[j] != expS.sizes[j] {
					color3 = colorRed
				}
			}
			result[0] = fmt.Sprintf("%v%v%v", color1, btou(obsLane), colorReset) + result[0]
			result[1] = fmt.Sprintf("%v%v%v", color1, btou(expLane), colorReset) + result[1]
			result[2] += fmt.Sprintf("%v%v%v", color2, obsS.offsets[j], colorReset)
			result[3] += fmt.Sprintf("%v%v%v", color2, expS.offsets[j], colorReset)
			result[4] += fmt.Sprintf("%v%v%v", color3, obsS.sizes[j], colorReset)
			result[5] += fmt.Sprintf("%v%v%v", color3, expS.sizes[j], colorReset)

			if j == 15 {
				result[0] = "0b" + result[0]
				result[1] = "0b" + result[1]
				result[2] += "]"
				result[3] += "]"
				result[4] += "]"
				result[5] += "]"
			} else {
				result[2] += ","
				result[3] += ","
				result[4] += ","
				result[5] += ","
			}
		}
		return

	}

	for i := 0; i < bcLaneCount; i++ {
		obsLane := obsK.getBit(i)
		kMismatch := obsLane != expK.getBit(i)
		sMismatch := obsLane && (obsS.offsets[i] != expS.offsets[i]) && (obsS.sizes[i] != expS.sizes[i])
		if kMismatch || sMismatch {
			str := toStringWithColor(initK, obsK, expK, obsS, expS)

			sb := strings.Builder{}
			sb.WriteString(fmt.Sprintf("issue with lane %v:\n", i))
			sb.WriteString(fmt.Sprintf("initial:  lanes=0b%016b\n", initK.mask))
			sb.WriteString(fmt.Sprintf("observed: lanes=%v, offset=%v, length=%v\n", str[0], str[2], str[4]))
			sb.WriteString(fmt.Sprintf("expected: lanes=%v, offset=%v, length=%v\n", str[1], str[3], str[5]))
			return true, sb.String()
		}
	}
	return false, ""
}

func reportIssueK(initK, obsK, expK *kRegData) (hasIssue bool, msg string) {
	var obsS, expS sRegData
	return reportIssueKS(initK, obsK, expK, &obsS, &expS)
}

func reportIssueS(initK *kRegData, obsS, expS *sRegData) (hasIssue bool, msg string) {
	return reportIssueKS(initK, &fullMask, &fullMask, obsS, expS)
}

func reportIssueKSRef(obsLane, expLane bool, obsOffset, expOffset OffsetZ2, obsLength, expLength LengthZ3) (hasIssue bool, msg string) {
	if obsLane != expLane {
		return true, fmt.Sprintf("observed: lane=%v, offset=%v, length=%v\nexpected: lane=%v, offset=%v, length=%v\n-----------------------------------",
			obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
	}
	if obsLane { // if the expected and observed lane are equal, and the match is true, then also check the offset and length
		if (obsOffset != expOffset) || (obsLength != expLength) {
			return true, fmt.Sprintf("observed: lane=%v, offset=%v, length=%v\nexpected: lane=%v, offset=%v, length=%v\n-----------------------------------",
				obsLane, obsOffset, obsLength, expLane, expOffset, expLength)
		}
	}
	return false, ""
}

func reportIssueKRef(obsLane, expLane bool) (hasIssue bool, msg string) {
	return reportIssueKSRef(obsLane, expLane, 0, 0, 0, 0)
}

// referenceSkipCharLeft skips n code-point from data; valid is true if successful, false if provided string is not UTF-8
func referenceSkipCharLeft(data Data, skipCount int) (laneOut bool, offsetOut OffsetZ2, lengthOut LengthZ3) {
	if skipCount < 0 {
		skipCount = 0
	}
	length := len(data)
	if !utf8.ValidString(string(data)) {
		panic("invalid data provided")
	}
	laneOut = true
	nRunes := utf8.RuneCountInString(string(data))
	nRunesToRemove := skipCount
	if nRunesToRemove > nRunes {
		nRunesToRemove = nRunes
		laneOut = false
	}
	strToRemove := string([]rune(data)[:nRunesToRemove])
	nBytesToSkip := len(strToRemove)
	if nBytesToSkip > length {
		nBytesToSkip = length
	}
	offsetOut = OffsetZ2(nBytesToSkip)
	lengthOut = LengthZ3(length - nBytesToSkip)
	return
}

// referenceSkipCharRight skips n code-point from data; valid is true if successful, false if provided string is not UTF-8
func referenceSkipCharRight(data Data, skipCount int) (laneOut bool, offsetOut OffsetZ2, lengthOut LengthZ3) {
	if skipCount < 0 {
		skipCount = 0
	}
	length := len(data)
	if !utf8.ValidString(string(data)) {
		panic("invalid data provided")
	}
	laneOut = true
	nRunes := utf8.RuneCountInString(string(data))

	nRunesToRemove := skipCount
	if nRunesToRemove > nRunes {
		nRunesToRemove = nRunes
		laneOut = false
	}
	nRunesToKeep := nRunes - nRunesToRemove

	strToRemove := string([]rune(data)[nRunesToKeep:])
	nBytesToSkip := len(strToRemove)
	if nBytesToSkip > length {
		nBytesToSkip = length
	}
	offsetOut = 0
	lengthOut = LengthZ3(length - nBytesToSkip)
	return
}

func matchPatternRef(data Data, pattern *stringext.Pattern, cmpType stringext.StrCmpType, useContains bool) (bool, OffsetZ2, LengthZ3) {
	eq := func(r1, r2 rune, cmpType stringext.StrCmpType) bool {
		if r1 == r2 {
			return true
		}
		if cmpType == stringext.CiASCII {
			if r1 < utf8.RuneSelf {
				r1 = stringext.NormalizeRune(r1)
			}
			if r2 < utf8.RuneSelf {
				r2 = stringext.NormalizeRune(r2)
			}
			return r1 == r2
		}
		if cmpType == stringext.CiUTF8 {
			r1 = stringext.NormalizeRune(r1)
			r2 = stringext.NormalizeRune(r2)
			return r1 == r2
		}
		return false
	}

	nBytesData := LengthZ3(len(data))

	if len(pattern.Needle) == 0 { // not sure how to handle an empty pattern, currently it always matches
		return true, 0, nBytesData
	}

	dataRune := []rune(data)
	needleRune := []rune(pattern.Needle)

	nRunesData := len(dataRune)
	nRunesNeedle := len(needleRune)

	if len(pattern.Wildcard) != nRunesNeedle {
		panic("incorrect wildcard length")
	}

	bytePosData := 0
	if useContains {
		for runeDataIdx := 0; runeDataIdx < nRunesData; runeDataIdx++ {
			match := true
			nBytesNeedle := 0 // number of bytes of the needle when matched in the data
			for runeNeedleIdx := 0; runeNeedleIdx < nRunesNeedle; runeNeedleIdx++ {
				dataPos := runeDataIdx + runeNeedleIdx
				if dataPos >= nRunesData {
					match = false
					break
				}
				dr := dataRune[dataPos]
				if !pattern.Wildcard[runeNeedleIdx] && !eq(dr, needleRune[runeNeedleIdx], cmpType) {
					match = false
					break
				}
				nBytesNeedle += utf8.RuneLen(dr)
			}
			if match {
				x := bytePosData + nBytesNeedle
				return true, OffsetZ2(x), nBytesData - LengthZ3(x)
			}
			bytePosData += utf8.RuneLen(dataRune[runeDataIdx])
		}
		if bytePosData != int(nBytesData) {
			panic("Should not happen")
		}
	} else {
		if len(dataRune) != len(needleRune) {
			return false, OffsetZ2(nBytesData), 0
		}
		match := true
		nBytesNeedle := 0 // number of bytes of the needle when matched in the data
		for runeNeedleIdx := 0; runeNeedleIdx < nRunesNeedle; runeNeedleIdx++ {
			dataPos := runeNeedleIdx
			if dataPos >= nRunesData {
				match = false
				break
			}
			dr := dataRune[dataPos]
			if !pattern.Wildcard[runeNeedleIdx] && !eq(dr, needleRune[runeNeedleIdx], cmpType) {
				match = false
				break
			}
			nBytesNeedle += utf8.RuneLen(dr)
		}
		if match {
			x := bytePosData + nBytesNeedle
			return true, OffsetZ2(x), nBytesData - LengthZ3(x)
		}
	}

	return false, OffsetZ2(nBytesData), 0
}

// runRegexTests iterates over all regexes with the provided regex space,and determines equality over all
// needles from the provided data space
func runRegexTests(t *testing.T, name string, dataSpace [][]Data, regexSpace []string, regexType regexp2.RegexType, writeDot bool) {
	var ctx bctestContext
	defer ctx.Free()

	for _, regexStr := range regexSpace {
		ds, err := regexp2.CreateDs(regexStr, regexType, writeDot, regexp2.MaxNodesAutomaton)
		if err != nil {
			t.Error(err)
		}
		// regexDataTest tests the equality for all regexes provided in the data-structure container for one provided needle
		regexDataTest := func(ctx *bctestContext, dsByte []byte, name string, op bcop, data16 []Data, expLanes uint16) (fault bool) {
			if dsByte == nil {
				return
			}

			ctx.clear()
			ctx.setDict(string(dsByte))
			dictOffset := uint16(0)

			inputS := ctx.sRegFromStrings(data16)
			inputK := fullMask
			outputK := kRegData{}

			if err := ctx.ExecuteOpcode(op, []any{&outputK, &inputS, dictOffset, &inputK}, inputK); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < bcLaneCount; i++ {
				if outputK.getBit(i) != getBit(expLanes, i) {
					t.Errorf("%v: issue with lane %v, \ndata=%q\nexpected=%016b (regexGolang=%q)\nobserved=%016b (regexSneller=%q)",
						name, i, prettyDataSlice(data16), expLanes, ds.RegexGolang.String(), outputK.mask, ds.RegexSneller.String())
					return true
				}
			}
			return false
		}

		for _, data16 := range dataSpace {
			expLanes := uint16(0)
			for i := 0; i < bcLaneCount; i++ {
				expLanes = setBit(expLanes, i, ds.RegexGolang.MatchString(data16[i]))
			}

			hasFault1 := regexDataTest(&ctx, ds.DsT6, name+":DfaT6", opDfaT6, data16, expLanes)
			hasFault2 := regexDataTest(&ctx, ds.DsT6Z, name+":DfaT6Z", opDfaT6Z, data16, expLanes)
			hasFault3 := regexDataTest(&ctx, ds.DsT7, name+":DfaT7", opDfaT7, data16, expLanes)
			hasFault4 := regexDataTest(&ctx, ds.DsT7Z, name+":DfaT7Z", opDfaT7Z, data16, expLanes)
			hasFault5 := regexDataTest(&ctx, ds.DsT8, name+":DfaT8", opDfaT8, data16, expLanes)
			hasFault6 := regexDataTest(&ctx, ds.DsT8Z, name+":DfaT8Z", opDfaT8Z, data16, expLanes)
			hasFault7 := regexDataTest(&ctx, ds.DsLZ, name+":DfaLZ", opDfaLZ, data16, expLanes)
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

func createSpaceExhaustive(dataLenSpace []int, alphabet []rune) [][]Data {
	result := make([][]Data, 0)
	alphabetSize := len(alphabet)
	indices := make([]byte, max(dataLenSpace))

	for _, strLength := range dataLenSpace {
		strRunes := make([]rune, strLength)
		done := false
		j := 0

		data16 := make([]Data, 16)
		for !done {
			for i := 0; i < strLength; i++ {
				strRunes[i] = alphabet[indices[i]]
			}
			if j < 16 {
				data16[j] = Data(strRunes)
				j++
			} else {
				result = append(result, data16)
				data16 = make([]Data, 16)
				j = 0
			}
			done = !next(&indices, alphabetSize, strLength)
		}

		if j > 0 {
			k := j - 1
			for ; j < 16; j++ {
				data16[j] = data16[k]
			}
			result = append(result, data16)
		}
	}
	return result
}

func createSpace(dataLenSpace []int, alphabet []rune, maxSize int) [][]Data {
	createSpaceRandom := func(maxLength int, alphabet []rune, maxSize int) []Data {
		set := make(map[Data]struct{}) // new empty set

		// Note: not the most efficient implementation: space of short strings
		// is quickly exhausted while we are still trying to find something
		strRunes := make([]rune, maxLength)
		alphabetSize := len(alphabet)

		for len(set) < maxSize {
			strLength := rand.Intn(maxLength) + 1
			for i := 0; i < strLength; i++ {
				strRunes[i] = alphabet[rand.Intn(alphabetSize)]
			}
			set[Data(strRunes)] = struct{}{}
		}
		return maps.Keys(set)
	}

	if maxSize == exhaustive {
		return createSpaceExhaustive(dataLenSpace, alphabet)
	}
	return split16(createSpaceRandom(max(dataLenSpace), alphabet, maxSize))
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

// createSpacePattern creates a space with wildcards
func createSpacePattern(dataLenSpace []int, alphabet []rune, maxSize int) []stringext.Pattern {
	alphabetExt := append(alphabet, utf8.MaxRune) // use maxRune as a wildcard identifier
	result := make([]stringext.Pattern, 0)
	for _, data16 := range createSpace(dataLenSpace, alphabetExt, maxSize) {
		for i := 0; i < bcLaneCount; i++ {
			dataRune := []rune(data16[i])
			nRunes := len(dataRune)
			if (dataRune[0] != utf8.MaxRune) && (dataRune[nRunes-1] != utf8.MaxRune) {
				wildcard := make([]bool, nRunes)
				for j := 1; j < nRunes-1; j++ {
					if dataRune[j] == utf8.MaxRune {
						wildcard[j] = true
						dataRune[j] = '_' // any ASCII would do
					}
				}
				pattern := stringext.Pattern{WC: utf8.MaxRune, Escape: stringext.NoEscape, Needle: Needle(dataRune), Wildcard: wildcard, HasWildcard: false}
				result = append(result, pattern)
			}
		}
	}
	return result
}
