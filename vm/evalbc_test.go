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
	"strings"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/regexp2"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func TestCmpStrEqCsBruteForce1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
	// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
	// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
	strAlphabet := []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω'}
	strSpace := createSpaceRandom(4, strAlphabet, 2000)

	for _, str1 := range strSpace {
		str1Bytes := []byte(str1)
		for _, str2 := range strSpace {
			str2Bytes := []byte(str2)
			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, pad(str1))

			var values []interface{}
			for i := 0; i < 16; i++ {
				values = append(values, str2)
			}
			ctx.setScalarIonFields(values)
			ctx.current = 0xFFFF

			// when
			if err := ctx.ExecuteImm2(opCmpStrEqCs, 0); err != nil {
				t.Error(err)
			}
			// then
			expected := uint16(0x0000)
			if slices.Equal(str1Bytes, str2Bytes) {
				expected = 0xFFFF
			}
			if ctx.current != expected {
				t.Errorf("comparing %v to data %v: observed %04x (%016b); expected %04x (%016b)", escapeNL(str1), escapeNL(str2), ctx.current, ctx.current, expected, expected)
			}
			ctx.Free()
		}
	}
}

func TestCmpStrEqCiBruteForce1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
	// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
	// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
	strAlphabet := []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω', 0x0}
	strSpace := createSpaceRandom(4, strAlphabet, 2000)

	for _, str1 := range strSpace {
		str1Norm := stringext.NormalizeStringASCIIOnly(str1)

		for _, str2 := range strSpace {
			str2Norm := stringext.NormalizeStringASCIIOnly(str2)

			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, pad(str1Norm))

			var values []interface{}
			for i := 0; i < 16; i++ {
				values = append(values, str2)
			}
			ctx.setScalarIonFields(values)
			ctx.current = 0xFFFF

			// when
			if err := ctx.ExecuteImm2(opCmpStrEqCi, 0); err != nil {
				t.Error(err)
			}
			// then
			expected := uint16(0x0000)

			if str1Norm == str2Norm {
				expected = 0xFFFF
			}
			if ctx.current != expected {
				t.Errorf("comparing %v to data %v: observed %04x (%016b); expected %04x (%016b)",
					escapeNL(str1), escapeNL(str2), ctx.current, ctx.current, expected, expected)
			}
			ctx.Free()
		}
	}
}

// TestStrEqUTF8CiBruteForce1 tests special runes ſ and K for case-insensitive string compare
func TestCmpStrEqUTF8CiBruteForce1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
	// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
	// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
	strAlphabet := []rune{'s', 'S', 'ſ', 'k', 'K', 'K'}
	strSpace := createSpace(4, strAlphabet)

	for _, str1 := range strSpace {
		str1Ext := stringext.GenNeedleExt(str1, false)
		for _, str2 := range strSpace {
			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, pad(str1Ext))

			var values []interface{}
			for i := 0; i < 16; i++ {
				values = append(values, str2)
			}
			ctx.setScalarIonFields(values)
			ctx.current = 0xFFFF
			// when
			if err := ctx.ExecuteImm2(opCmpStrEqUTF8Ci, 0); err != nil {
				t.Error(err)
			}
			// then
			expected := uint16(0x0000)
			if strings.EqualFold(str1, str2) {
				expected = 0xFFFF
			}
			if ctx.current != expected {
				t.Errorf("comparing %v to data %v: observed %04x (%016b); expected %04x (%016b)",
					escapeNL(str1), escapeNL(str2), ctx.current, ctx.current, expected, expected)
			}
			ctx.Free()
		}
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
	ctx.setScalarStrings(needles)
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
	type void struct{}
	var member void
	set := make(map[string]void) // new empty set

	// Note: not the most efficient implementation: space of short strings
	// is quickly exhausted while we are still trying to find something
	strRunes := make([]rune, maxLength)
	alphabetSize := len(alphabet)

	for len(set) < maxSize {
		strLength := rand.Intn(maxLength + 1)
		for i := 0; i < strLength; i++ {
			strRunes[i] = alphabet[rand.Intn(alphabetSize)]
		}
		set[string(strRunes)] = member
	}
	return maps.Keys(set)
}
