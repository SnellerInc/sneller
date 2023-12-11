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
