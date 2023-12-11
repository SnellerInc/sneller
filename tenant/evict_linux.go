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
		syseventfd2 = 290 // int eventfd2(unsigned int count, int flags);
		// these are from grep -r EFD_ /usr/include/
		flagEFDCLOEXEC   = 02000000
		flagEFDNONBLOCK  = 00004000
		flagEFDSEMAPHORE = 1
	)
	rc, _, err := syscall.Syscall(syseventfd2, 0, flagEFDNONBLOCK|flagEFDCLOEXEC, 0)
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
