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

// Package stringext defines extra string functions.
package stringext

import (
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/exp/slices"
)

// Needle string type to distinguish from the Data string type
type Needle = string

// Data string type to distinguish from the Needle string type
type Data = string

func runeToUtf8Array(r rune) []byte {
	buf := make([]byte, 4)
	utf8.EncodeRune(buf, r)
	return buf
}

// alternativeRune gives all case-insensitive alternatives for the provided rune
func alternativeRune(r0 rune) []rune {
	r1 := unicode.SimpleFold(r0)
	if r1 == r0 {
		return []rune{r0}
	}
	r2 := unicode.SimpleFold(r1)
	if (r2 == r0) || (r2 == r1) {
		return []rune{r0, r1}
	}
	r3 := unicode.SimpleFold(r2)
	if (r3 == r0) || (r3 == r1) || (r3 == r2) {
		return []rune{r0, r1, r2}
	}
	return []rune{r0, r1, r2, r3}
}

func alternativeString(str string) (upper []rune, alt [][]rune) {

	min2 := func(r0, r1 rune) rune {
		if r0 < r1 {
			return r0
		}
		return r1
	}

	min3 := func(r0, r1, r2 rune) rune {
		return min2(r0, min2(r1, r2))
	}

	min4 := func(r0, r1, r2, r3 rune) rune {
		return min2(r0, min3(r1, r2, r3))
	}

	runes := []rune(str)
	nRunes := len(runes)
	alt1 := make([]rune, nRunes)
	alt2 := make([]rune, nRunes)
	alt3 := make([]rune, nRunes)
	alt4 := make([]rune, nRunes)
	upper = make([]rune, nRunes)

	inUse1, inUse2, inUse3, inUse4 := false, false, false, false

	for i, r := range runes {
		alt := alternativeRune(r)
		switch len(alt) {
		case 1:
			alt1[i] = alt[0]
			alt2[i] = alt[0]
			alt3[i] = alt[0]
			alt4[i] = alt[0]
			upper[i] = alt[0]
			inUse1 = true
		case 2:
			alt1[i] = alt[0]
			alt2[i] = alt[1]
			alt3[i] = alt[1]
			alt4[i] = alt[1]
			upper[i] = min2(alt[0], alt[1])
			inUse2 = true
		case 3:
			alt1[i] = alt[0]
			alt2[i] = alt[1]
			alt3[i] = alt[2]
			alt4[i] = alt[2]
			upper[i] = min3(alt[0], alt[1], alt[2])
			inUse3 = true
		case 4:
			alt1[i] = alt[0]
			alt2[i] = alt[1]
			alt3[i] = alt[2]
			alt4[i] = alt[3]
			upper[i] = min4(alt[0], alt[1], alt[2], alt[3])
			inUse4 = true
		}
	}

	if inUse4 {
		return upper, [][]rune{alt1, alt2, alt3, alt4}
	}
	if inUse3 {
		return upper, [][]rune{alt1, alt2, alt3}
	}
	if inUse2 {
		return upper, [][]rune{alt1, alt2}
	}
	if inUse1 {
		return upper, [][]rune{alt1}
	}
	return upper, [][]rune{} // unreachable
}

func to4ByteArray(value int) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(value))
	return buf
}

// genNeedleUTF8Ci generates an extended string representation needed for UTF8 ci comparisons
func genNeedleUTF8Ci(needle string, reversed bool) string {

	stringAlternatives := func(str string, reversed bool) (alt []byte) {
		nRunes := utf8.RuneCountInString(str)
		alt = make([]byte, 0) // alt = alternative list of runes encoded as utf8 code-points stored in 32bits int
		upperRunes, altArr := alternativeString(str)

		upper := []byte(string(upperRunes))
		upper = append(upper, 0, 0, 0)

		var alt1, alt2, alt3, alt4 []rune
		switch len(altArr) {
		case 1:
			alt1 = altArr[0]
			alt2 = altArr[0]
			alt3 = altArr[0]
			alt4 = altArr[0]
		case 2:
			alt1 = altArr[0]
			alt2 = altArr[1]
			alt3 = altArr[1]
			alt4 = altArr[1]
		case 3:
			alt1 = altArr[0]
			alt2 = altArr[1]
			alt3 = altArr[2]
			alt4 = altArr[2]
		case 4:
			alt1 = altArr[0]
			alt2 = altArr[1]
			alt3 = altArr[2]
			alt4 = altArr[3]
		}

		for i := 0; i < nRunes; i++ {
			if reversed {

				x3 := byte(0)
				if i-3 >= 0 {
					x3 = upper[i-3]
				}
				x2 := byte(0)
				if i-2 >= 0 {
					x2 = upper[i-2]
				}
				x1 := byte(0)
				if i-1 >= 0 {
					x1 = upper[i-1]
				}
				x0 := byte(0)
				if i-0 >= 0 {
					x0 = upper[i-0]
				}
				alt = append([]byte{x3, x2, x1, x0}, alt...)
				alt = append(runeToUtf8Array(alt1[i]), alt...)
				alt = append(runeToUtf8Array(alt2[i]), alt...)
				alt = append(runeToUtf8Array(alt3[i]), alt...)
				alt = append(runeToUtf8Array(alt4[i]), alt...)
			} else {
				alt = append(alt, runeToUtf8Array(alt1[i])...)
				alt = append(alt, runeToUtf8Array(alt2[i])...)
				alt = append(alt, runeToUtf8Array(alt3[i])...)
				alt = append(alt, runeToUtf8Array(alt4[i])...)
				alt = append(alt, upper[i+0], upper[i+1], upper[i+2], upper[i+3])
			}
		}
		return
	}

	result := make([]byte, 0)
	result = append(result, to4ByteArray(utf8.RuneCountInString(needle))...)
	result = append(result, stringAlternatives(needle, reversed)...)
	return string(result)
}

// fuzzyApprox3Spec returns the data-structure for the fuzzy approximation (with 3 characters lookahead)
func fuzzyApprox3Spec() []byte {
	// To generate the specification returned by this method, run genFuzzyApprox3Spec
	//if false { // please leave this dead code to document how to generate the update tables
	//	ds, goCode := fuzzy.GenFuzzyApprox3Spec(true)
	//	fmt.Printf(goCode)
	//	return ds
	//}
	return []byte{
		0x15, // sub:   key 000000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del2:  key 000000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 000000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 000100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 000100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 000100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 000100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 000101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 000101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 000101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 000101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 000111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 000111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 000111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 000111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 001100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 001100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 001100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 001100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 001101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 001101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 001101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 001101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 001111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 001111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 001111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 001111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 010100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 010100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 010100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 010100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 010101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 010101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 010101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 010101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 010101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 010101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 010111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 010111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 010111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 010111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 011100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 011100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 011100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 011100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 011101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 011101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 011101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 011101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 011101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 011101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 011111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 011111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 011111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 011111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0!=D2) -> ed=0; advD=1; advN=1
		0x11, // ins2:  key 100000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 100000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x2b, // tra+ins:       key 100000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=2; advD=3; advN=2
		0x05, // eq:    key 100000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 100100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 100100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 100100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 100100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 100101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 100101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 100101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 100101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 100111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 100111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 100111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 100111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 101100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 101100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 101100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 101100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 101101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 101101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 101101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 101101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 101111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 101111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 101111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 101111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1!=D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 110100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 110100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 110100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 110100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 110101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 110101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 110101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 110101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 110101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 110101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 110111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 110111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 110111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 110111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2!=D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111000000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111000001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111000010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111000011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111000100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111000101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111000110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111000111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111001000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111001001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111001010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111001011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111001100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111001101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111001110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111001111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111010000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111010001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111010010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111010011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111010100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111010101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111010110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111010111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111011000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111011001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111011010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111011011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111011100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111011101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111011110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111011111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2!=D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111100000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111100001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111100010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111100011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 111100100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 111100101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 111100110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 111100111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111101000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111101001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x15, // sub:   key 111101010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=1
		0x05, // eq:    key 111101011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 111101100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 111101101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x14, // del1:  key 111101110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=0; advN=1
		0x05, // eq:    key 111101111 (N0==D0; N2==D0; N1==D0; N1==D1; N0!=D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111110000 (N0!=D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111110001 (N0==D0; N2!=D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111110010 (N0!=D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111110011 (N0==D0; N2==D0; N1!=D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111110100 (N0!=D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111110101 (N0==D0; N2!=D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111110110 (N0!=D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111110111 (N0==D0; N2==D0; N1==D0; N1!=D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111111000 (N0!=D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111111001 (N0==D0; N2!=D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x11, // ins1:  key 111111010 (N0!=D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=1; advN=0
		0x05, // eq:    key 111111011 (N0==D0; N2==D0; N1!=D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111111100 (N0!=D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111111101 (N0==D0; N2!=D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
		0x1a, // tra:   key 111111110 (N0!=D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=1; advD=2; advN=2
		0x05, // eq:    key 111111111 (N0==D0; N2==D0; N1==D0; N1==D1; N0==D1; N2==D1; N2==D2; N1==D2; N0==D2) -> ed=0; advD=1; advN=1
	}
}

// EncodeFuzzyNeedleUnicode encode a needle (string) for fuzzy unicode comparisons
func EncodeFuzzyNeedleUnicode(needle Needle) string {
	runes := []rune(needle)
	nRunes := len(runes)
	result := fuzzyApprox3Spec()
	result = append(result, to4ByteArray(nRunes)...)
	for _, r := range runes {
		r2 := r
		if r < utf8.RuneSelf { // r is an ASCII value
			r2 = unicode.ToUpper(r)
		}
		result = append(result, runeToUtf8Array(r2)...)
	}
	return string(result)
}

// EncodeFuzzyNeedleASCII encode a needle (string) for fuzzy ASCII comparisons
func EncodeFuzzyNeedleASCII(needle Needle) string {
	nBytes := len(needle)
	result := fuzzyApprox3Spec()
	result = append(result, to4ByteArray(nBytes)...)
	result = append(result, NormalizeStringASCIIOnly([]byte(needle))...)
	result = append(result, byte(0xFF), byte(0xFF), byte(0xFF)) // pad with 3 invalid ascii bytes 0xFF
	return string(result)
}

// NormalizeRune normalizes the provided rune into the smallest and equal rune wrt case-folding.
// For ascii this normalization is equal to UPPER
func NormalizeRune(r rune) rune {
	// NOTE a counter example for an intuitive 'return unicode.ToUpper(unicode.ToLower(r))' is
	// U+0130 'İ' and U+0131 'ı'
	result := r
	for c := unicode.SimpleFold(r); c != r; c = unicode.SimpleFold(c) {
		if c < result {
			result = c
		}
	}
	return result
}

// NormalizeString normalizes the provided string into a string with runes that are smallest
// and equal wrt case-folding. For ascii this normalization is equal to UPPER
func NormalizeString(str string) string {
	// NOTE a counter example for an intuitive 'return strings.ToUpper(strings.ToLower(str))' is
	// U+0130 'İ' and U+0131 'ı'
	runes := []rune(str)
	for i, r := range runes {
		runes[i] = NormalizeRune(r)
	}
	return string(runes)
}

// NormalizeStringASCIIOnlyString normalizes the provided string into a string with runes that are smallest
// and equal wrt case-folding, and leaves non-ASCII values unchanged.
func NormalizeStringASCIIOnlyString(str string) string {
	return string(NormalizeStringASCIIOnly([]byte(str)))
}

// NormalizeStringASCIIOnly normalizes the provided string into a string with runes that are smallest
// and equal wrt case-folding, and leaves non-ASCII values unchanged.
func NormalizeStringASCIIOnly(bytes []byte) []byte {
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

// HasNtnRune return true when the provided rune contains a non-trivial normalization; false otherwise
func HasNtnRune(r rune) bool {
	if EqualRuneFold(r, 'S') || EqualRuneFold(r, 'K') {
		return true
	}
	return (r >= utf8.RuneSelf) && (r != unicode.SimpleFold(r))
}

// HasNtnString return true when the provided string contains a non-trivial normalization; false otherwise
func HasNtnString(str Needle) bool {
	for _, r := range str {
		if HasNtnRune(r) {
			return true
		}
	}
	return false
}

// HasCaseSensitiveChar returns true when the provided string contains a case-sensitive char
func HasCaseSensitiveChar(str Needle) bool {
	for _, r := range str {
		if r != unicode.SimpleFold(r) {
			return true
		}
	}
	return false
}

func EqualRuneFold(a, b rune) bool {
	return NormalizeRune(a) == NormalizeRune(b)
}

type StrCmpType int

const (
	CS      StrCmpType = iota
	CiASCII            // case-insensitive on ASCII only
	CiUTF8             // case-insensitive all unicode code-points
)

func (t StrCmpType) String() string {
	switch t {
	case CS:
		return "CS"
	case CiASCII:
		return "CI_ASCII"
	case CiUTF8:
		return "CI_UTF8"
	}
	return "??"
}

func stringAlternatives(str string) (alt []byte) {
	nRunes := utf8.RuneCountInString(str)
	alt = make([]byte, 0) // alt = alternative list of runes encoded as utf8 code-points stored in 32bits int
	_, altArr := alternativeString(str)

	var alt1, alt2, alt3, alt4 []rune
	switch len(altArr) {
	case 1:
		alt1 = altArr[0]
		alt2 = altArr[0]
		alt3 = altArr[0]
		alt4 = altArr[0]
	case 2:
		alt1 = altArr[0]
		alt2 = altArr[1]
		alt3 = altArr[1]
		alt4 = altArr[1]
	case 3:
		alt1 = altArr[0]
		alt2 = altArr[1]
		alt3 = altArr[2]
		alt4 = altArr[2]
	case 4:
		alt1 = altArr[0]
		alt2 = altArr[1]
		alt3 = altArr[2]
		alt4 = altArr[3]
	}
	for i := 0; i < nRunes; i++ {
		alt = append(alt, runeToUtf8Array(alt1[i])...)
		alt = append(alt, runeToUtf8Array(alt2[i])...)
		alt = append(alt, runeToUtf8Array(alt3[i])...)
		alt = append(alt, runeToUtf8Array(alt4[i])...)
	}
	return
}

// EncodeEqualStringCS encodes the provided string for usage with bcStrCmpEqCs
func EncodeEqualStringCS(needle string) string {
	return needle
}

// EncodeEqualStringCI encodes the provided string for usage with bcStrCmpEqCi
func EncodeEqualStringCI(needle string) string {
	// only normalize ASCII values, leave other values (UTF8) unchanged
	return NormalizeStringASCIIOnlyString(needle)
}

// EncodeEqualStringUTF8CI encodes the provided string for usage with bcStrCmpEqUTF8Ci
func EncodeEqualStringUTF8CI(needle string) string {
	return genNeedleUTF8Ci(needle, false)
}

// EncodeContainsPrefixCS encodes the provided string for usage with bcContainsPrefixCs
func EncodeContainsPrefixCS(needle string) string {
	return needle
}

// EncodeContainsPrefixCI encodes the provided string for usage with bcContainsPrefixCi
func EncodeContainsPrefixCI(needle string) string {
	return NormalizeString(needle)
}

// EncodeContainsPrefixUTF8CI encodes the provided string for usage with bcContainsPrefixUTF8Ci
func EncodeContainsPrefixUTF8CI(needle string) string {
	return genNeedleUTF8Ci(needle, false)
}

// EncodeContainsSuffixCS encodes the provided string for usage with bcContainsSuffixCs
func EncodeContainsSuffixCS(needle string) string {
	return needle
}

// EncodeContainsSuffixCI encodes the provided string for usage with bcContainsSuffixCi
func EncodeContainsSuffixCI(needle string) string {
	return NormalizeString(needle)
}

// EncodeContainsSuffixUTF8CI encodes the provided string for usage with bcContainsSuffixUTF8Ci
func EncodeContainsSuffixUTF8CI(needle string) string {
	return genNeedleUTF8Ci(needle, true)
}

// EncodeContainsSubstrCS encodes the provided string for usage with bcContainsSubstrCs
func EncodeContainsSubstrCS(needle string) string {
	return needle
}

// EncodeContainsSubstrCI encodes the provided string for usage with bcContainsSubstrCi
func EncodeContainsSubstrCI(needle string) string {
	return NormalizeStringASCIIOnlyString(needle)
}

// EncodeContainsSubstrUTF8CI encodes the provided string for usage with bcContainsSubstrUTF8Ci
func EncodeContainsSubstrUTF8CI(needle string) string {
	result := to4ByteArray(utf8.RuneCountInString(needle))
	result = append(result, stringAlternatives(needle)...)
	return string(result)
}

// EncodeContainsPatternCS encodes the provided string for usage with bcContainsPatternCs
func EncodeContainsPatternCS(pattern *Pattern) string {
	result := to4ByteArray(len(pattern.Needle)) // add number of bytes of the needle
	result = append(result, []byte(pattern.Needle)...)
	needleRune := []rune(pattern.Needle)
	for i := 0; i < len(needleRune); i++ {
		m := byte(0)
		if pattern.Wildcard[i] {
			m = 0xFF
		}
		for j := 0; j < utf8.RuneLen(needleRune[i]); j++ {
			result = append(result, m)
		}
	}
	return string(append(result, 0xFF, 0xFF, 0xFF))
}

// EncodeContainsPatternCI encodes the provided string for usage with bcContainsPatternCi
func EncodeContainsPatternCI(pattern *Pattern) string {
	pattern.Needle = NormalizeStringASCIIOnlyString(pattern.Needle)
	return EncodeContainsPatternCS(pattern)
}

// EncodeContainsPatternUTF8CI encodes the provided string for usage with bcContainsPatternUTF8Ci
func EncodeContainsPatternUTF8CI(pattern *Pattern) string {
	result := to4ByteArray(utf8.RuneCountInString(pattern.Needle))
	result = append(result, stringAlternatives(pattern.Needle)...)
	needleRune := []rune(pattern.Needle)

	for i := 0; i < len(needleRune); i++ {
		m := byte(0)
		if pattern.Wildcard[i] {
			m = 0xFF
		}
		for j := 0; j < utf8.RuneLen(needleRune[i]); j++ {
			// the UTF8 code does a "KMOVW (ptr), K3", thus we need two bytes
			result = append(result, m, m)
		}
	}
	return string(result)
}

// ToBCD converts two byte arrays to byte sequence of binary coded digits, needed by opIsSubnetOfIP4.
// Create an encoding of an IP4 as 16 bytes that is convenient. eg., byte sequence [192,1,2,3] becomes byte
// sequence 2,9,1,0, 1,0,0,0, 2,0,0,0, 3,0,0,0
func ToBCD(min, max *[4]byte) string {
	ipBCD := make([]byte, 32)
	minStr := []byte(fmt.Sprintf("%04d%04d%04d%04d", min[0], min[1], min[2], min[3]))
	maxStr := []byte(fmt.Sprintf("%04d%04d%04d%04d", max[0], max[1], max[2], max[3]))
	for i := 0; i < 16; i += 4 {
		ipBCD[0+i] = (minStr[3+i] & 0b1111) | ((maxStr[3+i] & 0b1111) << 4) // keep only the lower nibble from ascii '0'-'9' gives byte 0-9
		ipBCD[1+i] = (minStr[2+i] & 0b1111) | ((maxStr[2+i] & 0b1111) << 4)
		ipBCD[2+i] = (minStr[1+i] & 0b1111) | ((maxStr[1+i] & 0b1111) << 4)
		ipBCD[3+i] = (minStr[0+i] & 0b1111) | ((maxStr[0+i] & 0b1111) << 4)
	}
	return string(ipBCD)
}

// NoEscape is the default escape for LIKE parameters; it signals no escape
const NoEscape = utf8.RuneError

// IndexRuneEscape returns the index of the first instance of the Unicode code point
// r, or -1 if rune is not present in runes; an escaped r is not matched.
func IndexRuneEscape(runes []rune, r, escape rune) int {
	for idx := 0; idx < len(runes); idx++ {
		if runes[idx] == r {
			if idx == 0 {
				return 0
			}
			if runes[idx-1] != escape {
				return idx
			}
		}
	}
	return -1
}

// LastIndexRuneEscape returns the index of the last instance of r in runes,
// or -1 if c is not present in runes; an escaped r is not matched.
func LastIndexRuneEscape(runes []rune, r, escape rune) int {
	for idx := len(runes) - 1; idx >= 0; idx-- {
		if runes[idx] == r {
			if idx == 0 {
				return 0
			}
			if runes[idx-1] != escape {
				return idx
			}
		}
	}
	return -1
}

// Pattern is string literal (of type Needle) with wildcards which
// can be escaped by Escape, use NoEscape to signal the absence of an
// escape character.
type Pattern struct {
	WC          rune   // wildcard character of this pattern
	Escape      rune   // escape character of this pattern; if available
	Needle      Needle // NOTE: needle does not contain the Escape character
	Wildcard    []bool // for every rune in Needle exists a wildcard bool
	HasWildcard bool   // whether the Needle has at least one wildcard
}

// NewPattern creates a new Pattern for the provided string, wildcard and
// escape character. Eg. for "a@b_c@_d" with wildcard '_' and escape '@'
// a pattern is created with Pattern.Needle = "ab_c_d", and Pattern.Wildcard =
// [false, false, true, false, false, false]. Appreciate that the second wildcard
// is escaped and thus corresponds to the value true in the wildcard slice.
// NOTE: Pattern.WC and Pattern.Escape cannot be the same character.
func NewPattern(str string, wc, escape rune) Pattern {
	p := Pattern{}
	p.WC = wc
	p.Escape = escape

	if escape == NoEscape {
		for _, r := range str {
			if r == wc {
				p.Needle += "_"
				p.Wildcard = append(p.Wildcard, true)
				p.HasWildcard = true
			} else {
				p.Needle += string(r)
				p.Wildcard = append(p.Wildcard, false)
			}
		}
	} else {
		runes := []rune(str)
		for i, r := range runes {
			if r == escape {
				continue // do not keep escapes
			}
			if (i > 0) && (r == wc) && (runes[i-1] != escape) {
				p.Needle += "_"
				p.Wildcard = append(p.Wildcard, true)
				p.HasWildcard = true
			} else {
				p.Needle += string(r)
				p.Wildcard = append(p.Wildcard, false)
			}
		}
	}
	return p
}

// SplitWC splits the needle on wc and concatenates consecutive wildcards,
// eg. "a__b", (with WC = '_') becomes ["a", "__", "b"], [[false], [true, true], [false]]
func (p Pattern) SplitWC() ([]Needle, [][]bool) {
	needles := make([]Needle, 0)
	wildcards := make([][]bool, 0)
	runes := []rune(p.Needle)
	var runes2 []rune
	var wildcard2 []bool

	if len(p.Wildcard) > 0 {
		b := p.Wildcard[0]
		runes2 = []rune{runes[0]}
		wildcard2 = []bool{b}

		for i := 1; i < len(p.Wildcard); i++ {
			if p.Wildcard[i] == b {
				runes2 = append(runes2, runes[i])
				wildcard2 = append(wildcard2, b)
			} else {
				needles = append(needles, string(runes2))
				wildcards = append(wildcards, wildcard2)

				b = p.Wildcard[i]
				runes2 = []rune{runes[i]}
				wildcard2 = []bool{b}
			}
		}
	}
	if len(runes2) > 0 {
		needles = append(needles, string(runes2))
		wildcards = append(wildcards, wildcard2)
	}
	return needles, wildcards
}

func (p Pattern) String() string {
	colorGreen := "\033[32m"
	colorReset := "\033[0m"

	sb := strings.Builder{}
	for i, r := range []rune(p.Needle) {
		if p.Wildcard[i] {
			sb.WriteString(fmt.Sprintf("%v%v%v", colorGreen, "█", colorReset))
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// LikeSegment is a number of character skips followed by a Pattern.
// The number of skips is defined by a minimum and maximum count.
// Eg, {SkipMin:1, SkipMax:1, Pattern:"abc"} states that the segment
// matches when one (and only one) character is skipped and
// pattern "abc" matches. SkipMax can be -1 which indicates any number
// of slips. E.g {SKipMin:1, SkipMax:-1, "a"} corresponds to '_%a' in
// A LIKE expression.
type LikeSegment struct {
	SkipMin int
	SkipMax int
	Pattern Pattern
}

func (ls LikeSegment) String() string {
	return fmt.Sprintf("%v~%v:%v", ls.SkipMin, ls.SkipMax, ls.Pattern)
}

// SplitEscape splits the provided slice runes on rune r, but only if not escaped
func splitEscape(runes []rune, r, escape rune) [][]rune {
	result := make([][]rune, 0)
	for len(runes) > 0 {
		idx := IndexRuneEscape(runes, r, escape)
		if idx == -1 {
			return append(result, runes)
		}
		result = append(result, runes[:idx])
		runes = runes[idx+1:]
		if len(runes) == 0 {
			result = append(result, []rune{})
		}
	}
	return result
}

// SimplifyLikeExpr simplifies a LIKE expression into a minimal sequence of LikeSegment
func SimplifyLikeExpr(expr string, wc, ks, escape rune) []LikeSegment {

	// tmpStruct is similar to the LikeSegment except that the pattern is a string instead of
	// a Pattern struct. This is because the Pattern struct does not store the literal string
	// that is needed when calling NewPattern, and recalculating the pattern when characters are
	// skipped is thus not always possible.
	type tmpStruct struct {
		SkipMin int
		SkipMax int
		Pattern string
	}
	var tmp []tmpStruct
	{
		elements := splitEscape([]rune(expr), ks, escape)
		nElements := len(elements)
		for i := range elements {
			if i == 0 {
				tmp = append(tmp, tmpStruct{SkipMin: 0, SkipMax: 0, Pattern: string(elements[i])})
			} else {
				tmp = append(tmp, tmpStruct{SkipMin: 0, SkipMax: -1, Pattern: string(elements[i])})
			}
			if i == nElements-1 {
				tmp = append(tmp, tmpStruct{SkipMin: 0, SkipMax: 0, Pattern: ""})
			}
		}
	}

	add := func(a, b int) int {
		if a == -1 || b == -1 {
			return -1
		}
		return a + b
	}

	{ // merge wc '_' with '%'
		countLeading := func(s string) int {
			for i, c := range s {
				if c != wc {
					return i
				}
			}
			return utf8.RuneCountInString(s)
		}
		countTrailing := func(s string) int {
			runes := []rune(s)
			count := 0
			for i := len(runes) - 1; i >= 0; i-- {
				if (runes[i] != wc) || (i == 0) || (runes[i-1] == escape) {
					return count
				}
				count++
			}
			return count
		}

		nResults := len(tmp)
		for i := 0; i < nResults; i++ {
			curr := tmp[i]
			// (a,b,'__x') -> (a+2,b+2,'x')
			if skipLeft := countLeading(curr.Pattern); skipLeft > 0 {
				tmp[i].SkipMin = add(curr.SkipMin, skipLeft)
				tmp[i].SkipMax = add(curr.SkipMax, skipLeft)
				tmp[i].Pattern = curr.Pattern[skipLeft:]
			}
			// (a,b,'x__')(c,d,'y') -> (a,b,'x')(c+2,d+2,'y')
			if i < nResults-1 {
				next := tmp[i+1]
				if skipRight := countTrailing(curr.Pattern); skipRight > 0 {
					tmp[i+1].SkipMin = add(next.SkipMin, skipRight)
					tmp[i+1].SkipMax = add(next.SkipMax, skipRight)
					newLen := len(tmp[i].Pattern) - skipRight
					tmp[i].Pattern = tmp[i].Pattern[:newLen]
				}
			}
		}
	}
	// simplify
	for i := 0; i < len(tmp); i++ {
		if i > 0 {
			curr := tmp[i]
			prev := tmp[i-1]

			// simplify (a,b,"")(c,d,"x") -> (a+c,b+d,"x")
			if prev.Pattern == "" {
				tmp[i-1].SkipMin = add(prev.SkipMin, curr.SkipMin)
				tmp[i-1].SkipMax = add(prev.SkipMax, curr.SkipMax)
				tmp[i-1].Pattern = curr.Pattern
				tmp = slices.Delete(tmp, i, i+1)
				i--
			}
		}
	}
	result := make([]LikeSegment, len(tmp))
	for i, t := range tmp {
		pattern := NewPattern(t.Pattern, wc, escape)
		result[i] = LikeSegment{SkipMin: t.SkipMin, SkipMax: t.SkipMax, Pattern: pattern}
	}
	return result
}
