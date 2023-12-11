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

package tenant

import (
	"errors"
	"io/fs"
	"os"
)

// this is just to keep things
// building on other platforms;
// we don't expect this to work correctly
// anywhere except linux (and the tests
// override these functions anyway)
func init() {
	atime = otherAtime
	usage = otherUsage
}

func eventfd() (*os.File, error) {
	return nil, errors.New("eventfd not supported on platform")
}

func otherAtime(info fs.FileInfo) int64 {
	return info.ModTime().UnixNano()
}

func otherUsage(dir string) (int64, int64) {
	return 0, 1
}
