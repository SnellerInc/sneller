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

// Package ints provides int-related common functions.
package ints

import (
	"golang.org/x/exp/constraints"
)

// Min returns the smaller value of x and y
func Min[T constraints.Integer](x, y T) T {
	if x <= y {
		return x
	}
	return y
}

// Max returns the greater value of x and y
func Max[T constraints.Integer](x, y T) T {
	if x >= y {
		return x
	}
	return y
}

// Clamp returns x if it is in [lo, hi]. Otherwise, the nearest bounding value is returned
func Clamp[T constraints.Integer](x, lo, hi T) T {
	return Max(lo, Min(x, hi))
}
