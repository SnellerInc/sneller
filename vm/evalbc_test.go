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
	"math/rand"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/internal/stringext"
	"github.com/SnellerInc/sneller/ion"
	"golang.org/x/exp/slices"
)

func TestCmpStrEqCsBruteForce1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// U+017F 'ſ' (2 bytes) -> U+0053 'S' (1 bytes)
	// U+2126 'Ω' (3 bytes) -> U+03A9 'Ω' (2 bytes)
	// U+212A 'K' (3 bytes) -> U+004B 'K' (1 bytes)
	strAlphabet := []rune{'s', 'S', 'ſ', 'k', 'K', 'K', 'Ω', 'Ω', 0x0}
	strSearchSpace := createRandomSearchSpaceMaxLength(4, 2000, strAlphabet)

	for _, str1 := range strSearchSpace {
		str1Bytes := []byte(str1)
		for _, str2 := range strSearchSpace {
			str2Bytes := []byte(str2)
			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, str1)

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
	strSearchSpace := createRandomSearchSpaceMaxLength(4, 2000, strAlphabet)

	for _, str1 := range strSearchSpace {
		str1Norm := stringext.NormalizeStringASCIIOnly(str1)

		for _, str2 := range strSearchSpace {
			str2Norm := stringext.NormalizeStringASCIIOnly(str2)

			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, str1Norm)

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
	strSearchSpace := createSearchSpaceMaxLength(4, strAlphabet)

	for _, str1 := range strSearchSpace {
		str1Ext := stringext.GenNeedleExt(str1, false)
		for _, str2 := range strSearchSpace {
			// given
			var ctx bctestContext
			ctx.Taint()
			ctx.dict = append(ctx.dict, str1Ext)

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

//next updates x to the successor; return true/false whether the x is valid
func next(x *[]byte, max byte, length int) bool {
	for i := 0; i < length; i++ {
		(*x)[i]++          // increment the current byte i
		if (*x)[i] < max { // is the current byte larger than the maximum value?
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

// createSearchSpace creates strings of the provided length over the provided alphabet
func createSearchSpace(strLength int, alphabet []rune) []string {
	alphabetSize := byte(len(alphabet))
	indices := make([]byte, strLength)
	strRunes := make([]rune, strLength)
	result := make([]string, 0)
	done := false
	for !done {
		for i := 0; i < strLength; i++ {
			strRunes[i] = alphabet[indices[i]]
		}
		result = append(result, string(strRunes))
		done = !next(&indices, alphabetSize, strLength)
	}
	return result
}

// createSearchSpaceMaxLength creates strings of length 1 upto maxNeedleLength over the provided alphabet
func createSearchSpaceMaxLength(maxStrLength int, alphabet []rune) []string {
	result := make([]string, 0)
	for i := 1; i <= maxStrLength; i++ {
		result = append(result, createSearchSpace(i, alphabet)...)
	}
	return result
}

// createRandomSearchSpace creates random strings with the provided length over the provided alphabet
func createRandomSearchSpace(strLength, numberOfStr int, alphabet []rune) []string {
	alphabetSize := len(alphabet)
	strRunes := make([]rune, strLength)
	result := make([]string, 0, numberOfStr)
	for k := 0; k < numberOfStr; k++ {
		for i := 0; i < strLength; i++ {
			strRunes[i] = alphabet[rand.Intn(alphabetSize)]
		}
		result = append(result, string(strRunes))
	}
	return result
}

func createRandomSearchSpaceMaxLength(maxStrLength, numberOfStr int, alphabet []rune) []string {
	result := make([]string, 0)
	for i := 1; i <= maxStrLength; i++ {
		result = append(result, createRandomSearchSpace(i, numberOfStr/maxStrLength, alphabet)...)
	}
	return result
}
