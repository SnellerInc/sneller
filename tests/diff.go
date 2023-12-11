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
	defer os.Remove(f2.Name())

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
