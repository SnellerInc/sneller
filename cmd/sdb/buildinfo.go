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

	"github.com/SnellerInc/sneller"
)

func init() {
	addApplet(applet{
		name: "version",
		run: func(args []string) bool {
			v, ok := sneller.Version()
			if ok {
				fmt.Println(v)
			} else {
				fmt.Println("version not available, please check -build")
			}
			return true
		},
	})
	addApplet(applet{
		name: "buildinfo",
		run: func(args []string) bool {
			bi, ok := sneller.BuildInfo()
			if ok {
				fmt.Print(bi)
			} else {
				fmt.Println("build info not available")
			}
			return true
		},
	})
}
