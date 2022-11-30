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
