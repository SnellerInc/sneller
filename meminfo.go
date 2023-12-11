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

package sneller

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
