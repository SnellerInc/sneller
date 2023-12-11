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
