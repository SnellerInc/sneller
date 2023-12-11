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
	"encoding/binary"
	"math"
)

func bufferMinFloat64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := math.Float64frombits(binary.LittleEndian.Uint64(dst))
	b := math.Float64frombits(binary.LittleEndian.Uint64(src))
	result := a
	if b < a {
		result = b
	}
	binary.LittleEndian.PutUint64(dst, math.Float64bits(result))
}

func bufferMaxFloat64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := math.Float64frombits(binary.LittleEndian.Uint64(dst))
	b := math.Float64frombits(binary.LittleEndian.Uint64(src))
	result := a
	if b > a {
		result = b
	}
	binary.LittleEndian.PutUint64(dst, math.Float64bits(result))
}

func bufferAddInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := binary.LittleEndian.Uint64(dst)
	b := binary.LittleEndian.Uint64(src)
	result := a + b
	binary.LittleEndian.PutUint64(dst, result)
}

func bufferMinInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := int64(binary.LittleEndian.Uint64(dst))
	b := int64(binary.LittleEndian.Uint64(src))
	result := a
	if b < a {
		result = b
	}
	binary.LittleEndian.PutUint64(dst, uint64(result))
}

func bufferMaxInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := int64(binary.LittleEndian.Uint64(dst))
	b := int64(binary.LittleEndian.Uint64(src))
	result := a
	if b > a {
		result = b
	}
	binary.LittleEndian.PutUint64(dst, uint64(result))
}

func bufferAndInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := binary.LittleEndian.Uint64(dst)
	b := binary.LittleEndian.Uint64(src)
	result := a & b
	binary.LittleEndian.PutUint64(dst, result)
}

func bufferOrInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := binary.LittleEndian.Uint64(dst)
	b := binary.LittleEndian.Uint64(src)
	result := a | b
	binary.LittleEndian.PutUint64(dst, result)
}

func bufferXorInt64(dst, src []byte) {
	_ = dst[:8]
	_ = src[:8]

	a := binary.LittleEndian.Uint64(dst)
	b := binary.LittleEndian.Uint64(src)
	result := a ^ b
	binary.LittleEndian.PutUint64(dst, result)
}
