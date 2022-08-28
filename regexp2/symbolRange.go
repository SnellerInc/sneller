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

package regexp2

import "fmt"

// symbolRangeT contains two runes: min and max
type symbolRangeT uint64

const edgeEpsilonRange = symbolRangeT(edgeEpsilonRune) | (symbolRangeT(edgeEpsilonRune) << 32)
const edgeAnyRange = symbolRangeT(edgeAnyRune) | (symbolRangeT(edgeAnyRune) << 32)
const edgeAnyNotLfRange = symbolRangeT(edgeAnyNotLfRune) | (symbolRangeT(edgeAnyNotLfRune) << 32)

func newSymbolRange(min, max rune, rlza bool) symbolRangeT {
	result := symbolRangeT(min) | (symbolRangeT(max) << 32)
	if rlza {
		return result.setRLZA()
	}
	return result
}

// split returns the min and maximum rune (of the range), and the remaining length
// zero assertion (RLZA) bool
func (symbolRange symbolRangeT) split() (min, max rune, rlza bool) {
	min = rune(symbolRange & 0xFFFFFFFF)
	max = rune((symbolRange >> 32) & 0x7FFFFFFF) // clear the rlza flag
	rlza = (symbolRange & 0x8000000000000000) != 0
	return
}

func (symbolRange symbolRangeT) setRLZA() symbolRangeT {
	return 0x8000000000000000 | symbolRange
}

func (symbolRange symbolRangeT) clearRLZA() symbolRangeT {
	return 0x7FFFFFFFFFFFFFFF & symbolRange
}

func (symbolRange symbolRangeT) String() string {
	min, max, rlza := symbolRange.split()
	rlzaStr := rlzaToString(rlza)

	if (min == edgeEpsilonRune) && (max == edgeEpsilonRune) {
		return "<ε>" + rlzaStr
	}
	if (min == edgeAnyNotLfRune) && (max == edgeAnyNotLfRune) {
		return "<anyNotLf>" + rlzaStr
	}
	if (min == edgeAnyRune) && (max == edgeAnyRune) {
		return "<any>" + rlzaStr
	}
	if (min == edgeLfRune) && (max == edgeLfRune) {
		return "<lf>" + rlzaStr
	}

	var minStr string
	if ((min >= '0') && (min <= '9')) ||
		((min >= 'A') && (min <= 'Z')) ||
		((min >= 'a') && (min <= 'z')) {
		minStr = string(min)
	} else {
		minStr = fmt.Sprintf("0x%X", min)
	}
	var maxStr string
	if ((max >= '0') && (max <= '9')) ||
		((max >= 'A') && (max <= 'Z')) ||
		((max >= 'a') && (max <= 'z')) {
		maxStr = string(max)
	} else {
		maxStr = fmt.Sprintf("0x%X", max)
	}
	if maxStr == "0x10FFFF" {
		maxStr = "∞"
	}
	if minStr == maxStr {
		return minStr + rlzaStr
	}
	return fmt.Sprintf("%v%v..%v%v", minStr, rlzaStr, maxStr, rlzaStr)
}

func makeValidSymbolRange(min, max rune) []symbolRangeT {
	if min > max {
		return []symbolRangeT{}
	}
	return []symbolRangeT{newSymbolRange(min, max, false)}
}

// symbolRangeSubtract2 subtract b from a
func symbolRangeSubtract2(a, b symbolRangeT) []symbolRangeT {
	min1, max1, _ := a.split()
	min2, max2, _ := b.split()

	// 5 different situation to consider

	//  min1  max1
	//a: |----|
	//           min2  max2
	//b:	      |----|
	//c: |----|
	if max1 < min2 {
		return []symbolRangeT{a}
	}

	//  min1     max1
	//a: |-------|
	//       min2  max2
	//b:	  |----|
	//c: |---|
	if (min2 <= max1) && (max2 >= max1) {
		return makeValidSymbolRange(min1, min2-1)
	}

	//  min1         max1
	//a: |---------|
	//     min2 max2
	//b:    |---|
	//c: |-|     |-|
	if (min2 <= max1) && (max2 < max1) {
		x1 := makeValidSymbolRange(min1, min2-1)
		x2 := makeValidSymbolRange(max2+1, max1)
		return append(x1, x2...)
	}

	//       min1     max1
	//a:      |-------|
	//     min2  max2
	//b:    |----|
	//c:          |---|
	if (min2 <= min1) && (max2 >= min1) {
		return makeValidSymbolRange(max2+1, max1)
	}

	//           min1  max1
	//a:          |----|
	//  min2  max2
	//b: |----|
	//c:          |----|
	if max1 < min2 {
		return []symbolRangeT{a}
	}
	panic("unreachable")
}

func symbolRangeSubtract1(a []symbolRangeT, b symbolRangeT) []symbolRangeT {
	result := make([]symbolRangeT, 0)
	for _, symbolRange := range a {
		result = append(result, symbolRangeSubtract2(symbolRange, b)...)
	}
	return result
}

// symbolRangeSubtract subtract b from a
func symbolRangeSubtract(a, b []symbolRangeT) []symbolRangeT {
	for _, symbolRange := range b {
		a = symbolRangeSubtract1(a, symbolRange)
	}
	return a
}

func symbolRangesToString(symbolRanges []symbolRangeT) string {
	result := ""
	for _, symbolRange := range symbolRanges {
		result += symbolRange.String() + ","
	}
	return result
}

func rlzaToString(rlza bool) string {
	if rlza {
		return "$"
	}
	return ""
}
