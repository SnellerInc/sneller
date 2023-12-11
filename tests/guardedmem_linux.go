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

//go:build linux && amd64

package tests

import (
	"syscall"

	"github.com/SnellerInc/sneller/ints"
)

const (
	cpuPageSize = 4 << 10
)

// GuardMemory puts user data at the end of CPU page(s) and unmaps the next
// page to catch off-buffer accesses.
func GuardMemory(userdata []byte) (*GuardedMemory, error) {
	size := uint64(cap(userdata))
	rounded := ints.AlignUp64(size, cpuPageSize) // size in pages

	var gm GuardedMemory
	var err error

	// map n + 1 pages
	gm.mapped, err = syscall.Mmap(0, 0, int(rounded+cpuPageSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	// unmap the last page
	if err := syscall.Mprotect(gm.mapped[rounded:], syscall.PROT_NONE); err != nil {
		return nil, err
	}

	// copy user data at the end of mapped page(s)
	gm.Data = gm.mapped[rounded-size:]
	gm.Data = gm.Data[:size:size]
	copy(gm.Data, userdata)

	return &gm, nil
}

// Free releases mapped pages to the system
func (gm *GuardedMemory) Free() error {
	var err error
	if gm.mapped != nil {
		err = syscall.Munmap(gm.mapped)
		gm.mapped = nil
		gm.Data = nil
	}

	return err
}
