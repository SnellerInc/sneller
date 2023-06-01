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

package ints

import (
	"crypto/rand"
	"unsafe"

	"golang.org/x/exp/constraints"
)

// RandomFillSlice fills a slice of []T with content produced by a cryptographically strong random number generator
func RandomFillSlice[T constraints.Integer](out []T) error {
	if n := len(out); n > 0 {
		_, err := rand.Read(unsafe.Slice((*byte)(unsafe.Pointer(&out[0])), n*int(unsafe.Sizeof(out[0]))))
		return err
	}
	return nil
}
