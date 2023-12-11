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
