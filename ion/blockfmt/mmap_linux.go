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

package blockfmt

import (
	"fmt"
	"math"
	"os"
	"syscall"
)

func mmap(fp string) ([]byte, error) {
	f, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > math.MaxInt {
		return nil, fmt.Errorf("mapped file size %d exceeds max integer", info.Size())
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return mem, nil
}

func unmap(mem []byte) error {
	return syscall.Munmap(mem)
}
