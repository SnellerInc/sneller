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

package partiql

import (
	"testing"
)

func TestScannerPosition(t *testing.T) {
	lines := []string{
		"1234",
		"123456789_123456789_",
		"",
		"123456",
		"1",
	}

	var s scanner

	for _, line := range lines {
		s.from = append(s.from, []byte(line)...)
		s.from = append(s.from, []byte("\n")...)
	}

	pos := 0
	for line := range lines {
		for column := 0; column <= len(lines[line]); column++ {
			l, c, ok := s.position(pos)
			if ok == false || c != column+1 || l != line+1 {
				t.Logf("got : ok=%v, line=%d, column=%d\n", ok, l, c)
				t.Logf("want: ok=%v, line=%d, column=%d\n", true, line+1, column+1)
			}
			pos++
		}
	}
}
