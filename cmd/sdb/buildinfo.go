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
