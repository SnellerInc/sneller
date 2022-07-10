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
	"testing"

	"golang.org/x/exp/slices"
)

func equalSymbolRanges(a, b []symbolRangeT) bool {
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func TestSymbolRangeSubtract2(t *testing.T) {
	{
		a := newSymbolRange('0', '3', false)
		b := newSymbolRange('4', '9', false)
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '3', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("A: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('0', '6', false)
		b := newSymbolRange('4', '9', false)
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '3', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("B: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('0', '9', false)
		b := newSymbolRange('2', '3', false)
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '1', false), newSymbolRange('4', '9', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("C: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('4', '9', false)
		b := newSymbolRange('0', '6', false)
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('7', '9', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("D: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
	{
		a := newSymbolRange('6', '9', false)
		b := newSymbolRange('0', '5', false)
		observed := symbolRangeSubtract2(a, b)
		expected := []symbolRangeT{newSymbolRange('6', '9', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("E: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
}

func TestSymbolRangeSubtract(t *testing.T) {
	{
		a := []symbolRangeT{newSymbolRange('0', '3', false), newSymbolRange('5', '9', false)}
		b := []symbolRangeT{newSymbolRange('3', '6', false), newSymbolRange('9', '@', false)}
		observed := symbolRangeSubtract(a, b)
		expected := []symbolRangeT{newSymbolRange('0', '2', false), newSymbolRange('7', '8', false)}

		if !equalSymbolRanges(observed, expected) {
			t.Errorf("A: Observed %v expected %v\n", symbolRangesToString(observed), symbolRangesToString(expected))
		}
	}
}
