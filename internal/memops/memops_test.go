// Copyright (C) 2023 Sneller, Inc.
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

package memops

import (
	"testing"
)

func BenchmarkZeroMemoryReference(b *testing.B) {
	buf := make([]uint64, 1024*1024)

	for n := 0; n < b.N; n++ {
		ZeroMemoryReference(buf)
	}
}

func BenchmarkZeroMemory(b *testing.B) {
	buf := make([]uint64, 1024*1024)

	for n := 0; n < b.N; n++ {
		ZeroMemory(buf)
	}
}

func ZeroMemoryReference(buf []uint64) {
	for i := range buf {
		buf[i] = 0
	}
}

func TestZeroMemory(t *testing.T) {
	{
		buf := make([]int32, 16)
		ZeroMemory(buf)
	}

	{
		buf := make([]float32, 16)
		ZeroMemory(buf)
	}

	{
		type composite = [2]int64
		buf := make([]composite, 16)
		ZeroMemory(buf)
	}
}
