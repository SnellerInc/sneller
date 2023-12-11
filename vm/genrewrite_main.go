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

//go:build none

package main

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/SnellerInc/sneller/vm"
)

var (
	ofile  string
	stdout io.Writer
)

func init() {
	flag.StringVar(&ofile, "o", "-", "output file (- means stdout)")
}

func main() {
	flag.Parse()
	if ofile == "-" {
		ofile = ""
	}

	opnames := readopnames("ssa.go")

	stdout = os.Stdout
	var buf bytes.Buffer
	if ofile != "" {
		stdout = &buf
	}

	vm.GenrewriteMain(stdout, opnames, flag.Args())

	if ofile != "" {
		const rdonly = 0444
		err := os.WriteFile(ofile, buf.Bytes(), rdonly)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func readopnames(fname string) []string {
	var result []string

	f, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	const (
		firstName = "sinvalid"
		lastName  = "_ssamax"
	)

	s := bufio.NewScanner(f)
	collecting := false
	for s.Scan() {
		fields := strings.SplitN(strings.TrimSpace(s.Text()), " ", 2)
		name := fields[0]
		if len(name) == 0 || name[0] != 's' {
			continue
		}

		if collecting {
			if name == lastName {
				break
			}
		} else {
			if name == firstName {
				collecting = true
			} else {
				continue
			}
		}

		result = append(result, name)
	}

	if err := s.Err(); err != nil {
		log.Fatal(err)
	}

	return result
}
