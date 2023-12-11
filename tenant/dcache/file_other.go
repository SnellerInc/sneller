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

//go:build !linux
// +build !linux

package dcache

import (
	"io"
	"os"
	"strings"
)

const slack = 16

func istmp(f *os.File) bool {
	return strings.HasSuffix(f.Name(), ".tmp")
}

func mmap(f *os.File, size int64, ro bool) ([]byte, error) {
	if istmp(f) {
		return make([]byte, size), nil
	}
	return io.ReadAll(f)
}

func unmap(f *os.File, buf []byte) error {
	if !istmp(f) {
		return nil
	}
	// overwrite the file contents with 'buf'
	err := f.Truncate(0)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

func resize(f *os.File, size int64) error {
	return f.Truncate(size)
}
