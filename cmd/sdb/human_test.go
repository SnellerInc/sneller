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
