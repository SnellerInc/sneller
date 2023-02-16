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
	if !istmp(f.Name()) {
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
