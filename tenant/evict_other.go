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
