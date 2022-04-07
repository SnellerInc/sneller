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

func TestMalloc(t *testing.T) {
	var bufs [][]byte
	for i := 0; i < 10; i++ {
		n := Malloc()
		n[10] = 'x'
		n[(1024*1024)-1] = 'y'
		bufs = append(bufs, n)

		if vmPageBits() != PagesUsed() {
			t.Fatalf("%d bits, %d pages used", vmPageBits(), PagesUsed())
		}
	}
	for i := range bufs {
		if !Allocated(bufs[i]) {
			t.Fatalf("didn't allocate %p?", &bufs[i][0])
		}
		Free(bufs[i])
		if vmPageBits() != PagesUsed() {
			t.Fatalf("%d bits, %d pages used", vmPageBits(), PagesUsed())
		}
	}
}
