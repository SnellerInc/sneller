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

package tenant

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func checkheap(t *testing.T, lst []fprio) {
	for i := range lst {
		left := (i * 2) + 1
		right := left + 1
		if len(lst) > left && lst[left].atime > lst[i].atime {
			t.Errorf("heap invariant violated: element %d > %d", left, i)
		}
		if len(lst) > right && lst[right].atime > lst[i].atime {
			t.Errorf("heap invariant violated: element %d > %d", right, i)
		}
	}
}

func TestHeapOrder(t *testing.T) {
	var e evictHeap

	const entries = 1000
	for i := 0; i < entries; i++ {
		atime := rand.Int63() >> 1
		name := fmt.Sprintf("atime=%d", atime)
		size := 1000
		e.push(name, atime, int64(size))
	}
	checkheap(t, e.lst)
	if t.Failed() {
		t.FailNow()
	}
	if len(e.lst) != entries {
		t.Fatalf("len(e.lst)=%d, wanted %d", len(e.lst), entries)
	}
	prevatime := int64(math.MaxInt64)
	elem := 0
	for len(e.lst) > 0 {
		// pop() should yield atimes in descending order
		name, atime, size := e.pop()
		if atime > prevatime {
			t.Fatalf("unsorted at elem %d: prev=%d, atime=%d", elem, prevatime, atime)
		}
		if name != fmt.Sprintf("atime=%d", atime) {
			t.Fatalf("name corrupted: %q", name)
		}
		if size != 1000 {
			t.Fatalf("size corrupted: %d", size)
		}
		prevatime = atime
		elem++
	}
}
