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
	"os"
	"strings"

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
