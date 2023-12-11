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

func TestHuman(t *testing.T) {
	for _, td := range []struct {
		size int64
		text string
	}{
		{6, "6"},
		{600, "600"},
		{1023, "1023"},
		{1024, "1.000 KiB"},
		{1024 * 1024, "1.000 MiB"},
		{(1*1024 + 6) * 1024, "1.006 MiB"},
		{(1*1024 + 60) * 1024, "1.059 MiB"},
		{1024 * 1024 * 1024, "1.000 GiB"},
		{(1*1024 + 6) * 1024 * 1024, "1.006 GiB"},
		{(1*1024 + 60) * 1024 * 1024, "1.059 GiB"},
		{(1*1024 + 600) * 1024 * 1024, "1.586 GiB"},
		{1024 * 1024 * 1024 * 1024, "1.000 TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024, "1.000 PiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 1024, "1.000 EiB"},
	} {
		t.Run(td.text, func(t *testing.T) {
			text := human(td.size)
			if text != td.text {
				t.Fatalf("got %q, expected %q", text, td.text)
			}
		})
	}
}
