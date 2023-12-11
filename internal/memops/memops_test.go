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
