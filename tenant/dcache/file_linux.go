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

//go:build linux
// +build linux

package dcache

import (
	"os"
	"syscall"
)

// always leave some zeroed space at
// the end of the cache file so that
// we can perform large unaligned reads
// inside assembly code without worrying
// about a SIGBUS because we crossed a page boundary
const slack = 16

func mmap(f *os.File, size int64, ro bool) ([]byte, error) {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	if ro {
		prot = syscall.PROT_READ
	}
	flags := syscall.MAP_SHARED
	return syscall.Mmap(int(f.Fd()), 0, int(size), prot, flags)
}

func unmap(f *os.File, buf []byte) error {
	return syscall.Munmap(buf)
}

func resize(f *os.File, size int64) error {
	err := f.Truncate(size)
	if err != nil {
		return err
	}
	return syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
