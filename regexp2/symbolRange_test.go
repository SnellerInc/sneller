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

import (
	"slices"
	"testing"
)

func equalSymbolRanges(a, b []symbolRangeT) bool {
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func TestSymbolRangeSubtract2(t *testing.T) {
	{
		a := newSymbolRange('0', '3')
		b := newSymbolRange('4', '9')
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '3')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("A: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('0', '6')
		b := newSymbolRange('4', '9')
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '3')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("B: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('0', '9')
		b := newSymbolRange('2', '3')
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '1'), newSymbolRange('4', '9')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("C: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('4', '9')
		b := newSymbolRange('0', '6')
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('7', '9')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("D: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('6', '9')
		b := newSymbolRange('0', '5')
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('6', '9')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("E: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
}

func TestSymbolRangeSubtract(t *testing.T) {
	{
		a := []symbolRangeT{newSymbolRange('0', '3'), newSymbolRange('5', '9')}
		b := []symbolRangeT{newSymbolRange('3', '6'), newSymbolRange('9', '@')}
		observed := symbolRangeSubtract(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '2'), newSymbolRange('7', '8')}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("A: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
}
