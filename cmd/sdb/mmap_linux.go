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

package main

import (
	"io"
	"os"
	"syscall"
)

func mmap(src io.ReaderAt, size int64) ([]byte, bool) {
	f, ok := src.(*os.File)
	if !ok {
		return nil, false
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, false
	}
	return mem, true
}

func unmap(mem []byte) {
	err := syscall.Munmap(mem)
	if err != nil {
		exitf("%s", err)
	}
}
