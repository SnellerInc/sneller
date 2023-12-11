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

import "unicode/utf8"

type edgeT struct {
	symbolRange symbolRangeT
	to          nodeIDT
}

//TODO consider using edgeT with 21 bits for min, 21 bits for max, and 22 bits for RuneId

func (e *edgeT) epsilon() bool {
	return (0x7FFFFFFFFFFFFFFF & e.symbolRange) == edgeEpsilonRange
}

// rlza return true when the edge has a Remaining Length Zero Assertion ('$')
func (e *edgeT) rlza() bool {
	return e.symbolRange == edgeRLZARange
}

func (e *edgeT) symbolRanges() []symbolRangeT {
	if e.epsilon() {
		return []symbolRangeT{}
	} else if e.symbolRange == newSymbolRange(edgeAnyRune, edgeAnyRune) {
		return []symbolRangeT{newSymbolRange(0, utf8.MaxRune)}
	} else if e.symbolRange == newSymbolRange(edgeAnyNotLfRune, edgeAnyNotLfRune) {
		return []symbolRangeT{newSymbolRange(0, edgeLfRune-1), newSymbolRange(edgeLfRune+1, utf8.MaxRune)}
	} else {
		return []symbolRangeT{e.symbolRange}
	}
}
