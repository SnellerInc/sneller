// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package vm

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/fuzzy"
	"github.com/SnellerInc/sneller/internal/stringext"
)

type Data = stringext.Data
type Needle = stringext.Needle
type OffsetZ2 int
type LengthZ3 int

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
	case opcharlength:
		return func(data Data) int {
			return utf8.RuneCountInString(string(data))
		}
	case opoctetlength:
		return func(data Data) int {
			return len([]byte(data))
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
		panic("Provided bytecode does not have a corresponding reference implementation")
		return nil
	}
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

func toArrayIP4(v uint32) [4]byte {
	return [4]byte{byte(v >> (3 * 8)), byte(v >> (2 * 8)), byte(v >> (1 * 8)), byte(v >> (0 * 8))}
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
