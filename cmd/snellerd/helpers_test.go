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
