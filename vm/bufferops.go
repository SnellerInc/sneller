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
