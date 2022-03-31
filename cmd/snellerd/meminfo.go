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

package main

import (
	"fmt"
	"os"
	"runtime"
)

// memTotal is the total usable DRAM. On Linux, this
// value is read from /proc/meminfo. On other systems,
// this value remains zero and should be ignored.
var memTotal int64

func init() {
	// Only Linux is supported for now.
	if runtime.GOOS != "linux" {
		return
	}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		panic(err)
	}
	for {
		n, err := fmt.Fscanf(f, "MemTotal: %d kB\n", &memTotal)
		if err != nil {
			panic("/proc/meminfo: " + err.Error())
		}
		if n > 0 {
			memTotal *= 1024
			break
		}
	}
	f.Close()
}
