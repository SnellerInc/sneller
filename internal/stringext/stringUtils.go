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

package stringext

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"
)

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

func min2(r0, r1 rune) rune {
	if r0 < r1 {
		return r0
	}
	return r1
}

func min3(r0, r1, r2 rune) rune {
	return min2(r0, min2(r1, r2))
}

func min4(r0, r1, r2, r3 rune) rune {
	return min2(r0, min3(r1, r2, r3))
}

func alternativeString(str string) (upper []rune, alt [][]rune) {
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
			upper[i] = alt[0]
			inUse1 = true
		case 2:
			alt1[i] = alt[0]
			alt2[i] = alt[1]
			upper[i] = min2(alt[0], alt[1])
			inUse2 = true
		case 3:
			alt1[i] = alt[0]
			alt2[i] = alt[1]
			alt3[i] = alt[2]
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
	const nAlternatives = 4
	const nTailPadding = 4 // number of bytes placed with content 0 directly after the needle such that we can load 32 bytes
	nRunes := utf8.RuneCountInString(needle)
	needleUpper, alt := stringAlternatives(needle, nAlternatives, reversed)
	nBytesNeedle := len(needleUpper)
	offsetAlt := (4 * 4) + nBytesNeedle + nTailPadding

	result := make([]byte, 0)
	result = append(result, to4ByteArray(nRunes)...)
	result = append(result, to4ByteArray(nAlternatives)...)
	result = append(result, to4ByteArray(offsetAlt)...)
	result = append(result, to4ByteArray(nBytesNeedle)...)
	result = append(result, needleUpper...)
	result = append(result, make([]byte, nTailPadding)...)
	result = append(result, alt...)
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

// NormalizeStringASCIIOnly normalizes the provided string into a string with runes that are smallest
// and equal wrt case-folding, and leaves non-ASCII values unchanged.
func NormalizeStringASCIIOnly(str string) string {
	runes := []rune(str)
	for i, r := range runes {
		if r < utf8.RuneSelf {
			runes[i] = NormalizeRune(r)
		}
	}
	return string(runes)
}

func NormalizeByteArray(a []byte) []byte {
	return []byte(NormalizeString(string(a)))
}

//HasNtnRune return true when the provided rune contains a non-trivial normalization; false otherwise
func HasNtnRune(r rune) bool {
	if EqualRuneFold(r, 'S') || EqualRuneFold(r, 'K') {
		return true
	}
	return (r >= utf8.RuneSelf) && (r != unicode.SimpleFold(r))
}

//HasNtnString return true when the provided string contains a non-trivial normalization; false otherwise
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

// FlattenRunes flattens a string into 32-bit runes so we can stick it in a dictionary
func FlattenRunes(p string) string {
	size := 4 * utf8.RuneCountInString(p)
	// ensure that we can always load the
	// next 16 FlattenRunes using a regular VMOVDQA*
	cap := size
	if cap < 16*4 {
		cap = 16 * 4
	}
	out := make([]byte, cap)
	i := 0
	for _, r := range p {
		binary.LittleEndian.PutUint32(out[i:], uint32(r))
		i += 4
	}
	return string(out)[:size]
}

func PatternToSegments(pattern []byte) []string {
	var result []string
	for len(pattern) > 0 {
		segmentLength := pattern[0] //NOTE: 1byte segment length
		result = append(result, string(pattern[1:segmentLength+1]))
		pattern = pattern[segmentLength+1:]
	}
	return result
}

func SegmentToPattern(segment string) (pattern []byte) {
	nBytesInSegment := len(segment)
	pattern = append(pattern, byte(nBytesInSegment)) //NOTE: 1byte segment length
	pattern = append(pattern, []byte(segment)...)
	return
}

func SegmentsToPattern(segments []string) (pattern []byte) {
	for _, segment := range segments {
		pattern = append(pattern, SegmentToPattern(segment)...)
	}
	return
}

func PatternToPrettyString(pattern []byte, method1 int) (result string) {
	switch method1 {
	case 0:
		result = PatternToRegex(pattern)
	case 1:
		pos := 0
		for pos < len(pattern) {
			segmentLength := int(pattern[pos])
			pos++
			if segmentLength == 0 {
				result += "_"
			} else {
				result += "%" + string(pattern[pos:pos+segmentLength])
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
			result += "\"" + string(pattern[pos:pos+segmentLength]) + "\""
			pos += segmentLength
		}
		result += "}"
	case 3:
		pos := 0
		for pos < len(pattern) {
			segmentLength := int(pattern[pos])
			pos++
			result += fmt.Sprint(segmentLength) + "[" + string(pattern[pos:pos+segmentLength]) + "]"
			pos += segmentLength
		}
	default:
		panic("PatternToString: unsupported method")
	}
	return result
}

func PatternNormalize(pattern []byte) []byte {
	segments := PatternToSegments(pattern)
	for i, segment := range segments {
		segments[i] = NormalizeString(segment)
	}
	return SegmentsToPattern(segments)
}

func PatternToRegex(pattern []byte) string {
	regex := ".*"
	segments := PatternToSegments(pattern)
	lastSegmentIndex := len(segments) - 1
	for i, segment := range segments {
		regex += regexp.QuoteMeta(segment) + "."
		if i == lastSegmentIndex { // we are at the last segment
			regex += "*"
		}
	}
	return regex
}

// PatternLength returns the number of bytes in the provided pattern when the pattern is written as a string,
// e.g., pattern ["ab", "", "c"] matches with "ab--c" thus length is 5
func PatternLength(pattern []byte) int {
	segments := PatternToSegments(pattern)
	length := 0
	for _, segment := range segments {
		length += len(segment) + 1 // plus 1 for the trailing skipchar
	}
	if length > 0 {
		length-- // remove the last trailing skipchar
	}
	return length
}
