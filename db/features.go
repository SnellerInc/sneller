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
