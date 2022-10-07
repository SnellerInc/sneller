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
