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
