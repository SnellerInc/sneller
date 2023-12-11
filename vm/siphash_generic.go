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

//go:build !amd64
// +build !amd64

package vm

import (
	"unsafe"

	"github.com/dchest/siphash"
)

func siphashx8(k0, k1 uint64, base *byte, ends *[8]uint32) [2][8]uint64 {
	var r [2][8]uint64
	offs := uint32(0)

	for i := 0; i < 8; i++ {
		end := ends[i]
		buf := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(base), offs)), end-offs)
		r[0][i], r[1][i] = siphash.Hash128(k0, k1, buf)
		offs = end
	}
	return r
}
