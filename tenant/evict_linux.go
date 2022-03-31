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

package tenant

import (
	"io/fs"
	"os"
	"syscall"
)

func init() {
	atime = linuxatime
	usage = linuxUsage
}

func eventfd() (*os.File, error) {
	const (
		syseventfd2  = 290 // int eventfd(unsigned int count, int flags);
		efdSemaphore = 1
	)
	// we supply EFD_SEMPAHORE in order
	// to guarantee that calls to write() and read()
	// are matched 1-to-1
	rc, _, err := syscall.Syscall(syseventfd2, 0, syscall.O_NONBLOCK|syscall.O_CLOEXEC|efdSemaphore, 0)
	if err != 0 {
		return nil, err
	}
	return os.NewFile(rc, "eventfd"), nil
}

func linuxatime(f fs.FileInfo) int64 {
	return f.Sys().(*syscall.Stat_t).Atim.Nano()
}

// determine the usage of a filesystem;
// the returned values are (bytes used, total bytes)
func linuxUsage(dir string) (int64, int64) {
	var st syscall.Statfs_t
	err := syscall.Statfs(dir, &st)
	if err != nil {
		return 0, 1 // ?
	}
	return int64(st.Blocks-st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize)
}
