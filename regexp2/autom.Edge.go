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

import "unicode/utf8"

type edgeT struct {
	symbolRange symbolRangeT
	to          nodeIDT
}

//TODO consider using edgeT with 21 bits for min, 21 bits for max, and 22 bits for RuneId

func (e *edgeT) epsilon() bool {
	return (0x7FFFFFFFFFFFFFFF & e.symbolRange) == edgeEpsilonRange
}

func (e *edgeT) symbolRanges() []symbolRangeT {
	if e.epsilon() {
		return []symbolRangeT{}
	} else if e.symbolRange == newSymbolRange(edgeAnyRune, edgeAnyRune, false) {
		return []symbolRangeT{newSymbolRange(0, utf8.MaxRune, false)}
	} else if e.symbolRange == newSymbolRange(edgeAnyNotLfRune, edgeAnyNotLfRune, false) {
		return []symbolRangeT{newSymbolRange(0, edgeLfRune-1, false), newSymbolRange(edgeLfRune+1, utf8.MaxRune, false)}
	} else {
		return []symbolRangeT{e.symbolRange}
	}
}
