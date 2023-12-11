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
	"testing"
)

func TestFindCharGroupsRange(t *testing.T) {
	{
		observed := newCharGroupsRange()
		observed.add(newSymbolRange('0', '2'))
		observed.add(newSymbolRange('0', '9'))

		expected := newSet[symbolRangeT]()
		expected.insert(newSymbolRange('0', '2'))
		expected.insert(newSymbolRange('3', '9'))

		if !observed.data.equal(&expected) {
			t.Errorf("A: Observed %v; expected %v\n", symbolRangesToString(observed.data.toVector()), symbolRangesToString(expected.toVector()))
		}
	}
	{
		observed := newCharGroupsRange()
		observed.add(newSymbolRange('0', '2'))
		observed.add(newSymbolRange('2', '9'))

		expected := newSet[symbolRangeT]()
		expected.insert(newSymbolRange('0', '1'))
		expected.insert(newSymbolRange('2', '2'))
		expected.insert(newSymbolRange('3', '9'))

		if !observed.data.equal(&expected) {
			t.Errorf("B: Observed %v; expected %v\n", symbolRangesToString(observed.data.toVector()), symbolRangesToString(expected.toVector()))
		}
	}
	{
		observed := newCharGroupsRange()
		observed.add(newSymbolRange('0', '0'))
		observed.add(newSymbolRange('0', '9'))

		expected := newSet[symbolRangeT]()
		expected.insert(newSymbolRange('0', '0'))
		expected.insert(newSymbolRange('1', '9'))

		if !observed.data.equal(&expected) {
			t.Errorf("C: Observed %v; expected %v\n", symbolRangesToString(observed.data.toVector()), symbolRangesToString(expected.toVector()))
		}
	}
	{ // groups for regex := "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
		observed := newCharGroupsRange()
		observed.add(newSymbolRange('2', '2'))
		observed.add(newSymbolRange('.', '.'))
		observed.add(newSymbolRange('0', '9'))
		observed.add(newSymbolRange('0', '4'))
		observed.add(newSymbolRange('0', '5'))
		observed.add(newSymbolRange('0', '1'))
		observed.add(newSymbolRange('3', '9'))
		observed.add(newSymbolRange('5', '5'))
		observed.add(newSymbolRange('6', '9'))

		expected := newSet[symbolRangeT]()
		expected.insert(newSymbolRange('.', '.'))
		expected.insert(newSymbolRange('0', '1'))
		expected.insert(newSymbolRange('2', '2'))
		expected.insert(newSymbolRange('3', '4'))
		expected.insert(newSymbolRange('5', '5'))
		expected.insert(newSymbolRange('6', '9'))

		if !observed.data.equal(&expected) {
			t.Errorf("D: Observed %v; expected %v\n", symbolRangesToString(observed.data.toVector()), symbolRangesToString(expected.toVector()))
		}
	}
	{
		observed := newCharGroupsRange()
		observed.add(newSymbolRange('a', 'a'))
		observed.add(newSymbolRange('t', 't'))
		observed.add(newSymbolRange('a', 'b'))
		observed.add(newSymbolRange('s', 'u'))

		expected := newSet[symbolRangeT]()
		expected.insert(newSymbolRange('a', 'a'))
		expected.insert(newSymbolRange('t', 't'))
		expected.insert(newSymbolRange('b', 'b'))
		expected.insert(newSymbolRange('s', 's'))
		expected.insert(newSymbolRange('u', 'u'))

		if !observed.data.equal(&expected) {
			t.Errorf("E: Observed %v; expected %v\n", symbolRangesToString(observed.data.toVector()), symbolRangesToString(expected.toVector()))
		}
	}
}
