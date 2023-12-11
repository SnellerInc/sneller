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

package testquery

import (
	"testing"
)

func TestExecute(t *testing.T) {
	{ //  test for a query that is supposed to succeed
		queryStr := "SELECT name FROM input"
		inputsStr := []string{`{"name": "aaaa"}`, `{"name": "bbbb"}`}
		outputStr := []string{`{"name": "aaaa"}`, `{"name": "bbbb"}`}

		tci, err := parseCase([]string{queryStr}, [][]string{inputsStr}, outputStr, nil)
		if err != nil {
			t.Error(err)
		}
		flags := RunFlags(0)
		if err = tci.Execute(flags); err != nil {
			t.Errorf("test should have passed, but didn't: %v", err)
		}
	}
	{ // test for a query that is supposed to fail
		queryStr := "SELECT name FROM input"
		inputsStr := []string{`{"name": "aaaa"}`, `{"name": "bbbb"}`}
		outputStr := []string{`{"name": "aaaa"}`, `{"name": "cccc"}`}

		tci, err := parseCase([]string{queryStr}, [][]string{inputsStr}, outputStr, nil)
		if err != nil {
			t.Error(err)
		}
		flags := RunFlags(0)
		if err = tci.Execute(flags); err == nil {
			t.Error("test should have failed, but didn't")
		}
	}
	{ // test for a query that is supposed to fail
		queryStr := "SELECT name AS res1 FROM input"
		inputsStr := []string{`{"age": 7, "name": "aaaa"}`, `{"age": null, "name": "bbbb"}`}
		outputStr := []string{`{"res1": "aaaa"}`, `{"res1": "cccc"}`}

		tci, err := parseCase([]string{queryStr}, [][]string{inputsStr}, outputStr, nil)
		if err != nil {
			t.Error(err)
		}
		flags := RunFlags(0)
		if err = tci.Execute(flags); err == nil {
			t.Error("test should have failed, but didn't")
		}
	}
}
