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
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/SnellerInc/sneller/ion"
)

func main() {
	flag.Parse()
	o := bufio.NewWriter(os.Stdout)
	args := flag.Args()
	if len(args) == 0 {
		args = []string{"-"}
	}
	var inbuf *bufio.Reader
	for _, arg := range args {
		var err error
		var in *os.File
		if arg == "-" {
			in = os.Stdin
		} else {
			in, err = os.Open(arg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "can't open %q: %s\n", arg, err)
				os.Exit(1)
			}
		}
		if inbuf == nil {
			inbuf = bufio.NewReader(in)
		} else {
			inbuf.Reset(in)
		}
		_, err = ion.ToJSON(o, inbuf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "input %s: %s", arg, err)
			os.Exit(1)
		}
	}
	if err := o.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
