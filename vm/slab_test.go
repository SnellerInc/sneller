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

package vm

import (
	"testing"
)

func TestSlab(t *testing.T) {
	t.Cleanup(func() {
		if PagesUsed() > 0 {
			t.Errorf("leak: %d pages marked used", PagesUsed())
		}
	})
	var s slab
	defer s.reset()

	for count := 0; count < 10; count++ {
		total := 0
		var allocated [][]byte
		for i, size := range []int{
			10, 100, 1000, 10000, 100000,
			1_000_000,
		} {
			mem := s.malloc(size)
			if len(mem) != size {
				t.Errorf("malloc(%d) -> %d bytes?", len(mem), size)
			}
			// fill the n'th allocation with repeated n
			for j := range mem {
				mem[j] = byte(i)
			}
			total += len(mem)
			pages := (total + PageSize - 1) / PageSize
			if len(s.pages) != pages {
				t.Errorf("expected %d pages used; got %d", pages, len(s.pages))
			}
			allocated = append(allocated, mem)
		}
		for i, mem := range allocated {
			for j := range mem {
				if mem[j] != byte(i) {
					t.Fatalf("mem %d has byte %d", i, mem[j])
				}
			}
		}
		if count&1 != 0 {
			s.reset()
		} else {
			s.resetNoFree()
		}
	}
}
