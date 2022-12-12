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

package fuzzy

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

type Data = string
type Needle = string
type idxNeedleT int
type idxDataT int

type MatchMethod int64

const (
	TrueEditDistance MatchMethod = iota
	Approx1
	Approx2
	Approx3
	Approx4
)

type KernelFunc func(data, needle []rune, posData idxDataT, posNeedle idxNeedleT) (distance int, advData idxDataT, advNeedle idxNeedleT)

func editDistanceKernel(data, needle []rune, posData idxDataT, posNeedle idxNeedleT, lookahead int) (distance int, advData idxDataT, advNeedle idxNeedleT) {
	//NOTE: awkward implementation to mimic assembly code

	getElement := func(a []rune, idx int) rune {
		if idx >= len(a) {
			return utf8.MaxRune
		}
		return a[idx]
	}

	fillReg := func(n, d0, d1, d2, d3 rune) (result uint32) {
		if n == d0 {
			result |= 0x000000FF
		}
		if n == d1 {
			result |= 0x0000FF00
		}
		if n == d2 {
			result |= 0x00FF0000
		}
		if n == d3 {
			result |= 0xFF000000
		}
		return result
	}

	test := func(reg uint32, desc string) bool {
		mask := uint32(0)
		v := uint32(0)

		if desc[0] == '1' {
			v |= 0x000000FF
		}
		if desc[0] == 'X' {
			mask |= 0x000000FF
		}
		if desc[1] == '1' {
			v |= 0x0000FF00
		}
		if desc[1] == 'X' {
			mask |= 0x0000FF00
		}
		if desc[2] == '1' {
			v |= 0x00FF0000
		}
		if desc[2] == 'X' {
			mask |= 0x00FF0000
		}
		if desc[3] == '1' {
			v |= 0xFF000000
		}
		if desc[3] == 'X' {
			mask |= 0xFF000000
		}
		return (reg | mask) == (v | mask)
	}

	d0 := getElement(data, int(posData)+0)
	d1 := getElement(data, int(posData)+1)
	d2 := getElement(data, int(posData)+2)
	d3 := getElement(data, int(posData)+3)

	n0 := getElement(needle, int(posNeedle)+0)
	n1 := getElement(needle, int(posNeedle)+1)
	n2 := getElement(needle, int(posNeedle)+2)
	n3 := getElement(needle, int(posNeedle)+3)

	z10 := fillReg(n0, d0, d1, d2, d3)
	z11 := fillReg(n1, d0, d1, d2, d3)
	z12 := fillReg(n2, d0, d1, d2, d3)
	z13 := fillReg(n3, d0, d1, d2, d3)

	// lookahead equal to 1 is the manhattan distance
	if lookahead == 1 {
		if test(z10, "1XXX") {
			return 0, 1, 1
		}
		return 1, 1, 1
	}
	if lookahead == 2 {
		//equality: N0==D0
		if test(z10, "1XXX") {
			return 0, 1, 1
		}
		// transposition: (N0!=D0) && (N0==D1) && (N1==D0)
		if test(z10, "01XX") && test(z11, "10XX") {
			//NOTE 10XX is here equal to 1XXX because 11XX is not possible
			return 1, 2, 2
		}
		// deletion: (N0!=D0) && (N0!=D1) && (N1==D0)
		if test(z10, "00XX") && test(z11, "1XXX") {
			return 1, 0, 1
		}
		// insertion: (N0!=D0) && (N0==D1) && (N1!=D0)
		if test(z10, "01XX") && test(z11, "0XXX") {
			return 1, 1, 0
		}
		// substitution: (N0!=D0) && (N0!=D1) && (N1!=D0)
		return 1, 1, 1
	}
	if lookahead == 3 {
		//equality: N0==D0
		if test(z10, "1XXX") {
			return 0, 1, 1
		}

		// transposition: (N0!=D0) && (N0==D1) && (N1==D0)
		if test(z10, "01XX") && test(z11, "1XXX") {
			return 1, 2, 2
		}

		// deletion 1x // NOTE adding the 1 fixes the 'wrong' choice
		if test(z10, "00XX") && test(z11, "1XXX") && test(z12, "X1XX") {
			return 1, 0, 1
		}
		// deletion 2x
		if test(z10, "000X") && test(z11, "000X") && test(z12, "100X") {
			return 1, 0, 1
		}

		// insertion 1x
		if test(z10, "01XX") && test(z11, "0X1X") { // NOTE adding the 1 fixes the 'wrong' choice
			return 1, 1, 0
		}
		// insertion 2x
		if test(z10, "001X") && test(z11, "000X") && test(z12, "000X") {
			return 1, 1, 0
		}

		//tra+ins: special case
		if test(z10, "001X") && test(z11, "100X") && test(z12, "000X") {
			return 2, 3, 2
		}

		// substitution 1x
		return 1, 1, 1
	}
	if lookahead == 4 {
		//equality: N0==D0
		if test(z10, "1XXX") {
			return 0, 1, 1
		}

		// transposition: (N0!=D0) && (N0==D1) && (N1==D0)
		if test(z10, "01XX") && test(z11, "1XXX") {
			return 1, 2, 2
		}

		// deletion 1x
		if test(z10, "00XX") && test(z11, "1XXX") && test(z12, "X1XX") && test(z13, "XX1X") {
			return 1, 0, 1
		}
		// deletion 2x
		if test(z10, "000X") && test(z11, "0000") && test(z12, "1000") {
			return 1, 0, 1
		}
		// deletion 3x
		if test(z10, "0000") && test(z11, "0000") && test(z12, "0000") && test(z13, "1000") {
			return 1, 0, 1
		}

		// insertion 1x
		if test(z10, "01XX") && test(z11, "0X1X") {
			return 1, 1, 0
		}
		// insertion 2x
		if test(z10, "0010") && test(z11, "0001") {
			return 1, 1, 0
		}
		// insertion 3x
		if test(z10, "0001") && test(z11, "0000") && test(z12, "0000") && test(z13, "0000") {
			return 1, 1, 0
		}

		//tra+ins: special case
		if test(z10, "001X") && test(z11, "100X") && test(z12, "000X") {
			return 2, 3, 2
		}

		// substitution 1x
		if test(z10, "000X") && test(z11, "X1XX") {
			return 1, 1, 1
		}

		return 1, 1, 1
	}
	return 0, 1, 1
}

func calcEditDistanceRunes(data, needle []rune, matchTail bool, kernel KernelFunc) int {
	lenNeedle := idxNeedleT(len(needle))
	lenData := idxDataT(len(data))
	editDistanceTotal := 0
	posNeedle := idxNeedleT(0)
	posData := idxDataT(0)

	if matchTail { // the tail needs to match for equality check
		for (posNeedle < lenNeedle) || (posData < lenData) {
			editDistance, advData, advNeedle := kernel(data, needle, posData, posNeedle)
			editDistanceTotal += editDistance
			posData += advData
			posNeedle += advNeedle
		}
	} else { // the tail does not need to match for prefix check
		for (posNeedle < lenNeedle) && (posData < lenData) {
			editDistance, advData, advNeedle := kernel(data, needle, posData, posNeedle)
			editDistanceTotal += editDistance
			posData += advData
			posNeedle += advNeedle
		}
		if posNeedle < lenNeedle {
			editDistanceTotal += int(lenNeedle - posNeedle)
		}
	}
	return editDistanceTotal
}

func calcEditDistanceString(dataS Data, needleS Needle, ascii, matchTail bool, method MatchMethod) int {

	// NormalizeStringASCIIOnly normalizes the provided string into a string with runes that are smallest
	// and equal wrt case-folding, and leaves non-ASCII values unchanged.
	NormalizeStringASCIIOnly := func(bytes []byte) []byte {
		result := make([]byte, len(bytes))
		for i, r := range bytes {
			if r < utf8.RuneSelf { // r is an ASCII value
				result[i] = byte(unicode.ToUpper(rune(r)))
			} else {
				result[i] = r
			}
		}
		return result
	}

	if method == TrueEditDistance {
		return editDistanceRef(dataS, needleS)
	}

	needleBytes := NormalizeStringASCIIOnly([]byte(needleS))
	dataBytes := NormalizeStringASCIIOnly([]byte(dataS))
	var needle, data []rune

	if ascii {
		//turn every byte into a rune even if the byte sequence is a multibyte unicode code-point.
		needle = make([]rune, len(needleBytes))
		for i, b := range needleBytes {
			needle[i] = rune(b)
		}
		data = make([]rune, len(dataBytes))
		for i, b := range dataBytes {
			data[i] = rune(b)
		}
	} else {
		needle = []rune(string(needleBytes))
		data = []rune(string(dataBytes))
	}

	lookahead := -1
	switch method {
	case Approx1:
		lookahead = 1
	case Approx2:
		lookahead = 2
	case Approx3:
		lookahead = 3
	case Approx4:
		lookahead = 4
	}
	return calcEditDistanceRunes(data, needle, matchTail,
		func(data, needle []rune, posData idxDataT, posNeedle idxNeedleT) (distance int, advData idxDataT, advNeedle idxNeedleT) {
			return editDistanceKernel(data, needle, posData, posNeedle, lookahead)
		})
}

func calcFuzzyMatch(data Data, needle Needle, threshold int, ascii, matchTail bool, method MatchMethod) bool {
	return calcEditDistanceString(data, needle, ascii, matchTail, method) <= threshold
}

// refHasPrefixFuzzy is the reference implementation for the has-prefix-fuzzy functionality
func refHasPrefixFuzzy(data Data, prefix Needle, threshold int, ascii bool, method MatchMethod) bool {
	return calcFuzzyMatch(data, prefix, threshold, ascii, false, method)
}

// refHasSubstrFuzzy is the reference implementation for the has-substr-fuzzy functionality
func refHasSubstrFuzzy(data Data, needle Needle, threshold int, ascii bool, method MatchMethod) bool {
	lenData := len(data)
	if lenData == 0 {
		return utf8.RuneCountInString(needle) <= threshold
	}
	for i := 0; i < lenData; i++ {
		if refHasPrefixFuzzy(data[i:], needle, threshold, ascii, method) {
			return true
		}
	}
	return false
}

// RefHasSubstrFuzzyASCIIApprox3 is the reference implementation for the has-substr-fuzzy functionality
func RefHasSubstrFuzzyASCIIApprox3(data Data, needle Needle, threshold int) bool {
	return refHasSubstrFuzzy(data, needle, threshold, true, Approx3)
}

// RefHasSubstrFuzzyUnicodeApprox3 is the reference implementation for the has-substr-fuzzy functionality
func RefHasSubstrFuzzyUnicodeApprox3(data Data, needle Needle, threshold int) bool {
	return refHasSubstrFuzzy(data, needle, threshold, false, Approx3)
}

// refCmpStrFuzzy is the reference implementation for the str-match-fuzzy functionality
func refCmpStrFuzzy(data Data, needle Needle, threshold int, ascii bool, method MatchMethod) bool {
	return calcFuzzyMatch(data, needle, threshold, ascii, true, method)
}

// RefCmpStrFuzzyASCIIApprox3 is the reference implementation for the str-match-fuzzy functionality
func RefCmpStrFuzzyASCIIApprox3(data Data, needle Needle, threshold int) bool {
	return refCmpStrFuzzy(data, needle, threshold, true, Approx3)
}

// RefCmpStrFuzzyUnicodeApprox3 is the reference implementation for the str-match-fuzzy functionality
func RefCmpStrFuzzyUnicodeApprox3(data Data, needle Needle, threshold int) bool {
	return refCmpStrFuzzy(data, needle, threshold, false, Approx3)
}

// EditDistance calculates the edit distance with the provided method
func EditDistance(data Data, needle Needle, ascii bool, method MatchMethod) int {
	return calcEditDistanceString(data, needle, ascii, true, method)
}

// GenFuzzyApprox3Spec generates the data-structure for the fuzzy approximation (with 3 characters lookahead)
//
//lint:ignore U1000 Ignore unused needed to generate content of stringext.fuzzyApprox3Spec
func GenFuzzyApprox3Spec(genCode bool) (dataStructure []byte, goCode string) {
	type tableData struct {
		info, spec     string
		ed, advD, advN int
	}

	compileTableData := func(tda []tableData) (dataStructure []byte, goCode string) {

		pack := func(ed, advD, advN int) byte {
			return byte((ed << 4) | (advN << 2) | advD)
		}

		getBit := func(value, pos int) bool {
			return ((value >> pos) & 1) == 1
		}

		getIdxSpec := func(n, d int) int {
			if (n == 0) && (d == 0) {
				return 0
			}
			if (n == 0) && (d == 1) {
				return 1
			}
			if (n == 0) && (d == 2) {
				return 2
			}
			if (n == 0) && (d == 3) {
				return 3
			}

			if (n == 1) && (d == 0) {
				return 5
			}
			if (n == 1) && (d == 1) {
				return 6
			}
			if (n == 1) && (d == 2) {
				return 7
			}
			if (n == 1) && (d == 3) {
				return 8
			}

			if (n == 2) && (d == 0) {
				return 10
			}
			if (n == 2) && (d == 1) {
				return 11
			}
			if (n == 2) && (d == 2) {
				return 12
			}
			if (n == 2) && (d == 3) {
				return 13
			}

			if (n == 3) && (d == 0) {
				return 15
			}
			if (n == 3) && (d == 1) {
				return 16
			}
			if (n == 3) && (d == 2) {
				return 17
			}
			if (n == 3) && (d == 3) {
				return 18
			}
			return -1
		}

		getIdxKey := func(n, d int) int {
			if (n == 0) && (d == 0) {
				return 0
			}
			if (n == 1) && (d == 1) {
				return 3
			}
			if (n == 2) && (d == 2) {
				return 6
			}

			if (n == 1) && (d == 0) {
				return 2
			}
			if (n == 2) && (d == 1) {
				return 5
			}
			if (n == 0) && (d == 2) {
				return 8
			}

			if (n == 2) && (d == 0) {
				return 1
			}
			if (n == 0) && (d == 1) {
				return 4
			}
			if (n == 1) && (d == 2) {
				return 7
			}
			return -1
		}

		getIdxKeyInv := func(idx int) (n, d int) {
			for n = 0; n < 3; n++ {
				for d = 0; d < 3; d++ {
					if getIdxKey(n, d) == idx {
						return
					}
				}
			}
			panic("X")
		}

		dataIdxToString := func(idx int) string {
			result := ""
			for b := 0; b < 9; b++ {
				n, d := getIdxKeyInv(b)
				if getBit(idx, b) {
					result += fmt.Sprintf("N%v==D%v", n, d)
				} else {
					result += fmt.Sprintf("N%v!=D%v", n, d)
				}
				if b < 8 {
					result += "; "
				}
			}
			return result
		}

		applies := func(offset int, spec string) bool {
			test := func(c byte, b bool) bool {
				if c == '1' {
					return b
				} else if c == '0' {
					return !b
				}
				return true
			}
			return test(spec[getIdxSpec(0, 0)], getBit(offset, getIdxKey(0, 0))) &&
				test(spec[getIdxSpec(0, 1)], getBit(offset, getIdxKey(0, 1))) &&
				test(spec[getIdxSpec(0, 2)], getBit(offset, getIdxKey(0, 2))) &&
				test(spec[getIdxSpec(1, 0)], getBit(offset, getIdxKey(1, 0))) &&
				test(spec[getIdxSpec(1, 1)], getBit(offset, getIdxKey(1, 1))) &&
				test(spec[getIdxSpec(1, 2)], getBit(offset, getIdxKey(1, 2))) &&
				test(spec[getIdxSpec(2, 0)], getBit(offset, getIdxKey(2, 0))) &&
				test(spec[getIdxSpec(2, 1)], getBit(offset, getIdxKey(2, 1))) &&
				test(spec[getIdxSpec(2, 2)], getBit(offset, getIdxKey(2, 2)))
		}

		data := make([]byte, 0x200)
		for idx := range data {
			data[idx] = 0xFF
		}

		codeData := [0x200]string{}

		for _, td := range tda {
			for idx := range data {
				if applies(idx, td.spec) {
					if data[idx] != 0xFF {
						panic("overwrite")
					}
					data[idx] = pack(td.ed, td.advD, td.advN)
					if genCode {
						codeData[idx] = fmt.Sprintf("%v:\tkey %09b (%v) -> ed=%v; advD=%v; advN=%v", td.info, idx, dataIdxToString(idx), td.ed, td.advD, td.advN)
					}
				}
			}
		}
		for idx, v := range data {
			if v == 0xFF {
				data[idx] = pack(1, 1, 1)
				if genCode {
					codeData[idx] = fmt.Sprintf("%v:\tkey %09b (%v) -> ed=%v; advD=%v; advN=%v", "sub", idx, dataIdxToString(idx), 1, 1, 1)
				}
			}
		}

		if genCode {
			ann := "return []byte{\n"
			ann += fmt.Sprintf("0x%02x, //// %v\n", data[0], codeData[0])
			for idx := 1; idx < 0x200; idx++ {
				ann += fmt.Sprintf("0x%02x, //// %v\n", data[idx], codeData[idx])
			}
			ann += "}\n"
			return data, ann
		}
		return data, ""
	}

	tda := []tableData{
		{"eq", "1XXX:XXXX:XXXX:XXXX", 0, 1, 1},
		{"tra", "01XX:1XXX:XXXX:XXXX", 1, 2, 2},
		{"del1", "00XX:1XXX:X1XX:XXXX", 1, 0, 1},
		{"del2", "000X:000X:100X:XXXX", 1, 0, 1},
		{"ins1", "01XX:0X1X:XXXX:XXXX", 1, 1, 0},
		{"ins2", "001X:000X:000X:XXXX", 1, 1, 0},
		{"tra+ins", "001X:100X:000X:XXXX", 2, 3, 2},
		// note: all other configuration are sub: with ed=1, advD=1, advN=1
	}
	return compileTableData(tda)
}
