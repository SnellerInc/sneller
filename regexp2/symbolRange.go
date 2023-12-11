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

package regexp2

import "fmt"

// symbolRangeT contains two runes: min and max
type symbolRangeT uint64

const edgeEpsilonRange = symbolRangeT(edgeEpsilonRune) | (symbolRangeT(edgeEpsilonRune) << 32)
const edgeAnyRange = symbolRangeT(edgeAnyRune) | (symbolRangeT(edgeAnyRune) << 32)
const edgeAnyNotLfRange = symbolRangeT(edgeAnyNotLfRune) | (symbolRangeT(edgeAnyNotLfRune) << 32)
const edgeRLZARange = symbolRangeT(edgeRLZARune) | (symbolRangeT(edgeAnyNotLfRune) << 32)

func newSymbolRange(min, max rune) symbolRangeT {
	return symbolRangeT(min) | (symbolRangeT(max) << 32)
}

// split returns the min and maximum rune (of the range), and the remaining length
// zero assertion (RLZA) bool
func (symbolRange symbolRangeT) split() (min, max rune) {
	min = rune(symbolRange & 0xFFFFFFFF)
	max = rune((symbolRange >> 32) & 0x7FFFFFFF) // clear the rlza flag
	return
}

func (symbolRange symbolRangeT) String() string {
	min, max := symbolRange.split()
	if min == edgeEpsilonRune {
		return "<ε>"
	}
	if min == edgeAnyNotLfRune {
		return "<anyNotLf>"
	}
	if min == edgeAnyRune {
		return "<any>"
	}
	if min == edgeLfRune {
		return "<lf>"
	}
	if min == edgeRLZARune {
		return "<$>"
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
		return minStr
	}
	return fmt.Sprintf("%v..%v", minStr, maxStr)
}

func makeValidSymbolRange(min, max rune) []symbolRangeT {
	if min > max {
		return []symbolRangeT{}
	}
	return []symbolRangeT{newSymbolRange(min, max)}
}

// symbolRangeSubtract2 subtract b from a
func symbolRangeSubtract2(a, b symbolRangeT) []symbolRangeT {
	min1, max1 := a.split()
	min2, max2 := b.split()

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
