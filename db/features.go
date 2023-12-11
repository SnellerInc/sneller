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

package db

// SetFeatures updates b to take into account
// a list of feature strings.
// Unknown feature strings are silently ignored.
//
// See also Definition.Features.
func (c *Config) SetFeatures(lst []string) {
	for _, x := range lst {
		switch x {
		case "legacy-zstd":
			c.Algo = "zstd"
		case "iguana-v0":
			c.Algo = "zion+iguana_v0"
		}
	}
}
