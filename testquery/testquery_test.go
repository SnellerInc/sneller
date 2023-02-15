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

package testquery

import (
	"testing"
)

func TestExecute(t *testing.T) {
	{ //  test for a query that is supposed to succeed
		queryStr := "SELECT name FROM input"
		inputsStr := []string{`{"name": "aaaa"}`, `{"name": "bbbb"}`}
		outputStr := []string{`{"name": "aaaa"}`, `{"name": "bbbb"}`}

		tci, err := ParseTestCaseIon([]string{queryStr}, [][]string{inputsStr}, outputStr)
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

		tci, err := ParseTestCaseIon([]string{queryStr}, [][]string{inputsStr}, outputStr)
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

		tci, err := ParseTestCaseIon([]string{queryStr}, [][]string{inputsStr}, outputStr)
		if err != nil {
			t.Error(err)
		}
		flags := RunFlags(0)
		if err = tci.Execute(flags); err == nil {
			t.Error("test should have failed, but didn't")
		}
	}
}
