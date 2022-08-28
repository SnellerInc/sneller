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

// Package tests provides common functions used in tests.
package tests

import (
	"os"
	"os/exec"
	"strings"
)

// Diff produces diff of two strings.
func Diff(s1, s2 string) (string, bool) {
	f1, err := os.CreateTemp("", "diff*")
	if err != nil {
		return "", false
	}
	defer f1.Close()
	defer os.Remove(f1.Name())

	f2, err := os.CreateTemp("", "diff*")
	if err != nil {
		return "", false
	}
	defer f2.Close()
	defer os.Remove(f1.Name())

	_, err = f1.WriteString(s1)
	if err != nil {
		return "", false
	}
	_, err = f2.WriteString(s2)
	if err != nil {
		return "", false
	}

	cmd := exec.Command("diff", "-u", f1.Name(), f2.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		if !strings.HasPrefix(err.Error(), "exit status ") {
			return "", false
		}
	}

	return string(output), true
}
