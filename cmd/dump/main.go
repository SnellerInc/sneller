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
