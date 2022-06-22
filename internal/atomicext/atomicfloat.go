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

// Package atomicext provides extensions complementing the built-in atomic package
package atomicext

import (
	"math"
	"sync/atomic"
	"unsafe"
)

func AddFloat64(ptr *float64, value float64) {
	for {
		before := math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(ptr))))
		after := before + value

		if atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(ptr)), math.Float64bits(before), math.Float64bits(after)) {
			return
		}
	}
}

func MinFloat64(ptr *float64, value float64) {
	for {
		before := math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(ptr))))

		if before <= value {
			return
		}

		if atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(ptr)), math.Float64bits(before), math.Float64bits(value)) {
			return
		}
	}
}

func MaxFloat64(ptr *float64, value float64) {
	for {
		before := math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(ptr))))

		if before >= value {
			return
		}

		if atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(ptr)), math.Float64bits(before), math.Float64bits(value)) {
			return
		}
	}
}

func MinInt64(ptr *int64, value int64) {
	for {
		before := atomic.LoadInt64(ptr)

		if before <= value {
			return
		}

		if atomic.CompareAndSwapInt64(ptr, before, value) {
			return
		}
	}
}

func MaxInt64(ptr *int64, value int64) {
	for {
		before := atomic.LoadInt64(ptr)

		if before >= value {
			return
		}

		if atomic.CompareAndSwapInt64(ptr, before, value) {
			return
		}
	}
}

func AndInt64(ptr *int64, value int64) {
	for {
		before := atomic.LoadInt64(ptr)
		after := before & value

		if before == after {
			return
		}

		if atomic.CompareAndSwapInt64(ptr, before, after) {
			return
		}
	}
}

func OrInt64(ptr *int64, value int64) {
	for {
		before := atomic.LoadInt64(ptr)
		after := before | value

		if before == after {
			return
		}

		if atomic.CompareAndSwapInt64(ptr, before, after) {
			return
		}
	}
}

func XorInt64(ptr *int64, value int64) {
	if value == 0 {
		return
	}

	for {
		before := atomic.LoadInt64(ptr)
		after := before ^ value

		if atomic.CompareAndSwapInt64(ptr, before, after) {
			return
		}
	}
}
