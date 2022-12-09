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
	"unicode"
	"unicode/utf8"
)

// Needle string type to distinguish from the Data string type
type Needle = string

// Data string type to distinguish from the Needle string type
type Data = string

// AddTail concats the tail to the capacity of the string
func AddTail(str, tail string) []byte {
	return append([]byte(str), tail...)[:len(str)]
}

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

// EncodeFuzzyNeedleUnicode encode a needle (string) for fuzzy unicode comparisons
func EncodeFuzzyNeedleUnicode(needle Needle) string {
	runes := []rune(needle)
	nRunes := len(runes)
	result := to4ByteArray(nRunes)
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
	result := to4ByteArray(nBytes)
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
func HasNtnString(str string) bool {
	for _, r := range str {
		if HasNtnRune(r) {
			return true
		}
	}
	return false
}

// HasCaseSensitiveChar returns true when the provided string contains a case-sensitive char
func HasCaseSensitiveChar(str string) bool {
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
func EncodeContainsPatternCS(needle string, wildcard []bool) string {
	result := to4ByteArray(len(needle)) // add number of bytes of the needle
	result = append(result, []byte(needle)...)
	needleRune := []rune(needle)
	for i := 0; i < len(needleRune); i++ {
		m := byte(0)
		if wildcard[i] {
			m = 0xFF
		}
		for j := 0; j < utf8.RuneLen(needleRune[i]); j++ {
			result = append(result, m)
		}
	}
	return string(append(result, 0xFF, 0xFF, 0xFF))
}

// EncodeContainsPatternCI encodes the provided string for usage with bcContainsPatternCi
func EncodeContainsPatternCI(needle string, wildcard []bool) string {
	return EncodeContainsPatternCS(NormalizeStringASCIIOnlyString(needle), wildcard)
}

// EncodeContainsPatternUTF8CI encodes the provided string for usage with bcContainsPatternUTF8Ci
func EncodeContainsPatternUTF8CI(needle string, wildcard []bool) string {
	result := to4ByteArray(utf8.RuneCountInString(needle))
	result = append(result, stringAlternatives(needle)...)
	needleRune := []rune(needle)

	for i := 0; i < len(needleRune); i++ {
		m := byte(0)
		if wildcard[i] {
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

// LastIndexRuneEscape returns the index of the last instance of r in runes, or -1 if c is
// not present in runes; an escaped r is not matched.
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
