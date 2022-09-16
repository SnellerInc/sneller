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
