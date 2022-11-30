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
	"regexp"
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

func stringAlternatives(str string, nAlternatives int, reversed bool) (upper, alt []byte) {
	nRunes := utf8.RuneCountInString(str)
	alt = make([]byte, 0) // alt = alternative list of runes encoded as utf8 code-points stored in 32bits int
	upperRunes, altArr := alternativeString(str)

	if reversed {
		upper = []byte(reverseString(string(upperRunes)))
	} else {
		upper = []byte(string(upperRunes))
	}

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

	if nAlternatives == 1 {
		for i := 0; i < nRunes; i++ {
			if reversed {
				alt = append(runeToUtf8Array(alt1[i]), alt...)
			} else {
				alt = append(alt, runeToUtf8Array(alt1[i])...)
			}
		}
	} else if nAlternatives == 2 {
		for i := 0; i < nRunes; i++ {
			if reversed {
				alt = append(runeToUtf8Array(alt1[i]), alt...)
				alt = append(runeToUtf8Array(alt2[i]), alt...)
			} else {
				alt = append(alt, runeToUtf8Array(alt1[i])...)
				alt = append(alt, runeToUtf8Array(alt2[i])...)
			}
		}
	} else if nAlternatives == 3 {
		for i := 0; i < nRunes; i++ {
			if reversed {
				alt = append(runeToUtf8Array(alt1[i]), alt...)
				alt = append(runeToUtf8Array(alt2[i]), alt...)
				alt = append(runeToUtf8Array(alt3[i]), alt...)
			} else {
				alt = append(alt, runeToUtf8Array(alt1[i])...)
				alt = append(alt, runeToUtf8Array(alt2[i])...)
				alt = append(alt, runeToUtf8Array(alt3[i])...)
			}
		}
	} else if nAlternatives == 4 {
		for i := 0; i < nRunes; i++ {
			if reversed {
				alt = append(runeToUtf8Array(alt1[i]), alt...)
				alt = append(runeToUtf8Array(alt2[i]), alt...)
				alt = append(runeToUtf8Array(alt3[i]), alt...)
				alt = append(runeToUtf8Array(alt4[i]), alt...)
			} else {
				alt = append(alt, runeToUtf8Array(alt1[i])...)
				alt = append(alt, runeToUtf8Array(alt2[i])...)
				alt = append(alt, runeToUtf8Array(alt3[i])...)
				alt = append(alt, runeToUtf8Array(alt4[i])...)
			}
		}
	} else {
		panic("not implemented")
	}
	return
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

// reverseString reverses the provided string
func reverseString(s string) string { // nicked from https://stackoverflow.com/questions/1752414/how-to-reverse-a-string-in-go
	size := len(s)
	buf := make([]byte, size)
	for start := 0; start < size; {
		r, n := utf8.DecodeRuneInString(s[start:])
		start += n
		utf8.EncodeRune(buf[size-start:], r)
	}
	return string(buf)
}

// GenNeedleExt generates an extended string representation needed for UTF8 ci comparisons
func GenNeedleExt(needle string, reversed bool) string {

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

// GenPatternExt generates an extended pattern representation needed for UTF8 ci comparisons
func GenPatternExt(segments []string) string {
	const nAlternatives = 4
	nBytes := 0
	for _, segment := range segments {
		nBytes += 4 // for the length of the segment
		nBytes += nAlternatives * 4 * utf8.RuneCountInString(segment)
	}
	result := make([]byte, 0)
	for _, segment := range segments {
		nRunes := utf8.RuneCountInString(segment)
		result = append(result, to4ByteArray(nRunes)...)
		_, alt := stringAlternatives(segment, nAlternatives, false)
		result = append(result, alt...)
	}
	return string(result[0:nBytes])
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

func EqualRuneFold(a, b rune) bool {
	return NormalizeRune(a) == NormalizeRune(b)
}

func PatternToSegments(pattern string) []string {
	var result []string
	for len(pattern) > 0 {
		segmentLength := pattern[0] //NOTE: 1byte segment length
		if int(segmentLength) >= len(pattern) {
			panic(fmt.Sprintf("invalid segment length %v (char %v)", segmentLength, string(rune(segmentLength))))
		}
		result = append(result, pattern[1:segmentLength+1])
		pattern = pattern[segmentLength+1:]
	}
	return result
}

func SegmentToPattern(segment string, normalize bool) (pattern string) {
	if normalize {
		segment = NormalizeString(segment)
	}
	return string(byte(len(segment))) + segment
}

func SegmentsToPattern(segments []string, normalize bool) (pattern string) {
	var p []byte
	for _, segment := range segments {
		p = append(p, SegmentToPattern(segment, normalize)...)
	}
	return string(p)
}

func PatternToPrettyString(pattern string, method int) (result string) {
	if !utf8.ValidString(pattern) {
		return "<invalid UTF8>"
	}
	switch method {
	case 0:
		segments := PatternToSegments(pattern)
		result = PatternToRegex(segments, true)
	case 1:
		pos := 0
		for pos < len(pattern) {
			segmentLength := int(pattern[pos])
			pos++
			if segmentLength == 0 {
				result += "_"
			} else {
				result += "%" + pattern[pos:pos+segmentLength]
			}
			pos += segmentLength
		}
		result += "%"
	case 2: // print golang code for the pattern
		result += "[]string{"
		first := true
		pos := 0
		for pos < len(pattern) {
			if !first {
				result += ", "
			}
			first = false
			segmentLength := int(pattern[pos])
			pos++
			result += "\"" + pattern[pos:pos+segmentLength] + "\""
			pos += segmentLength
		}
		result += "}"
	case 3:
		pos := 0
		patternRunes := []rune(pattern)
		nRunes := len(patternRunes)
		for pos < nRunes {
			segmentLength := int(patternRunes[pos])
			pos++
			if pos+segmentLength < nRunes {
				result += fmt.Sprintf("%v[%v]", segmentLength, patternRunes[pos:pos+segmentLength])
			} else {
				result += fmt.Sprintf("%v[<invalid segment length %v>]", segmentLength, segmentLength)
			}
			pos += segmentLength
		}
	default:
		panic("PatternToString: unsupported method")
	}
	return result
}

func PatternToRegex(segments []string, caseSensitive bool) string {
	regex := ".*"
	lastSegmentIndex := len(segments) - 1
	for i, segment := range segments {
		regex += regexp.QuoteMeta(segment) + "."
		if i == lastSegmentIndex { // we are at the last segment
			regex += "*"
		}
	}
	if !caseSensitive {
		regex = "(?i)" + regex
	}
	return regex
}
