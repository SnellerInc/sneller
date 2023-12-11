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
