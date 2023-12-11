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
