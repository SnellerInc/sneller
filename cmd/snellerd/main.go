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
	"os"
	"strings"

	"github.com/SnellerInc/sneller"

	"golang.org/x/sys/cpu"
)

var version = "development"

// testmode is true when built with the "test" tag.
// This enables unit test specific behavior.
var testmode = false

func main() {
	if !cpu.X86.HasAVX512 {
		fmt.Fprintln(os.Stderr, "CPU doesn't support AVX-512")
		os.Exit(1)
	}

	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "-version":
			v, ok := sneller.Version()
			if ok {
				fmt.Println(v)
			} else {
				fmt.Println("version not available, please check -build")
			}
			return
		case "-build":
			bi, ok := sneller.BuildInfo()
			if ok {
				fmt.Print(bi)
			} else {
				fmt.Println("build info not available")
			}
			return
		}
	}

	ver, ok := sneller.Version()
	if ok {
		version = ver
	}

	useSubCommand := len(args) > 0 && !strings.HasPrefix(args[0], "-")
	if useSubCommand {
		subCommand := args[0]
		args = args[1:]
		switch subCommand {
		case "daemon":
			runDaemon(args)
		case "worker":
			runWorker(args)
		default:
			fmt.Fprintf(os.Stderr, "invalid sub-command '%v'\n", subCommand)
			os.Exit(1)
		}
	} else {
		runDaemon(args)
	}
}
