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

package main

import "testing"

type matchTestData struct {
	Text    string
	Pattern string
	IsMatch bool
}

func TestMatches(t *testing.T) {

	testData := []matchTestData{
		{Text: "abc", Pattern: "a.c", IsMatch: false},
		{Text: "a.c", Pattern: "a.c", IsMatch: true},
		{Text: "abbc", Pattern: "a.*c", IsMatch: false},
		{Text: "a.*c", Pattern: "a.*c", IsMatch: true},
		{Text: "_abc", Pattern: "_a.c", IsMatch: false},
		{Text: "_a.c", Pattern: "_a.c", IsMatch: true},
		{Text: "_abbc", Pattern: "_a.*c", IsMatch: false},
		{Text: "_a.*c", Pattern: "_a.*c", IsMatch: true},
		{Text: "abc", Pattern: "abc", IsMatch: true},
		{Text: "abbc", Pattern: "a%c", IsMatch: true},
		{Text: "abc", Pattern: "a_c", IsMatch: true},
		{Text: "abbc", Pattern: "a__c", IsMatch: true},
		{Text: "abd", Pattern: "a%c", IsMatch: false},
		{Text: "aabc", Pattern: "%bc", IsMatch: true},
		{Text: "abc", Pattern: "b%c", IsMatch: false},
		{Text: "abc", Pattern: "abcd", IsMatch: false},
		{Text: "abc", Pattern: "a%cd", IsMatch: false},
		{Text: "abc", Pattern: "%bcd", IsMatch: false},
		{Text: "abc", Pattern: "b%cd", IsMatch: false},
		{Text: "bc", Pattern: "_bc", IsMatch: false},
		{Text: "abc", Pattern: "_bc", IsMatch: true},
		{Text: "aabc", Pattern: "__bc", IsMatch: true},
		{Text: "bc", Pattern: "__bc", IsMatch: false},
	}
	for _, i := range testData {
		if matchPattern(i.Text, i.Pattern) != i.IsMatch {
			t.Errorf("'%v' on pattern '%v' fails.", i.Text, i.Pattern)
		}
	}
}
