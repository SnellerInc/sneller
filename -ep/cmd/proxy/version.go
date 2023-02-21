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
	"runtime/debug"
)

// BuildInfo returns the build info data of binary.
func BuildInfo() (*debug.BuildInfo, bool) {
	return debug.ReadBuildInfo()
}

// Version returns the version of binary, based on BuildInfo data.
func Version() (string, bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}

	rev, hasRev := findSetting(bi, "vcs.revision")
	date, hasDate := findSetting(bi, "vcs.time")
	if hasRev && hasDate {
		return fmt.Sprintf("date: %s, revision: %s", date, rev), true
	} else if hasRev {
		return fmt.Sprintf("revision: %s", rev), true
	} else if hasDate {
		return fmt.Sprintf("date: %s", date), true
	}

	return "", false
}

func findSetting(bi *debug.BuildInfo, key string) (string, bool) {
	for i := range bi.Settings {
		if bi.Settings[i].Key == key {
			return bi.Settings[i].Value, true
		}
	}

	return "", false
}
