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

// go:build none

package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"
)

type Mapping struct {
	Const string
	Names []string
}

func (m *Mapping) Name() string {
	return m.Names[0]
}

func main() {
	mapping := extractMapping("builtin.go")
	createFile("builtin_names.go", mapping)
}

func extractMapping(fname string) []Mapping {
	mapping := []Mapping{}
	const (
		SCAN = iota
		PARSE
	)

	mode := SCAN

	err := processLines(fname, func(line string) bool {
		switch mode {
		case SCAN:
			if containsAll(line, "Concat", "BuiltinOp", "iota") {
				m, ok := parseLine(line)
				if ok {
					mapping = append(mapping, m)
				}
				mode = PARSE
			}

			return true
		case PARSE:
			m, ok := parseLine(line)
			if ok {
				if m.Const == "Unspecified" {
					return false
				}
				mapping = append(mapping, m)
			}

			return true
		}

		return false
	})

	checkErr(err)

	return mapping
}

func containsAll(s string, frag ...string) bool {
	for i := range frag {
		if !strings.Contains(s, frag[i]) {
			return false
		}
	}

	return true
}

// syntax: "ConstPascal foo bar // sql:Name1 sql:Name2 extra comment
func parseLine(s string) (m Mapping, ok bool) {
	s, c, comment := strings.Cut(s, "//")

	{
		terms := strings.Split(s, " ")
		if len(terms) == 0 {
			ok = false
			return
		}

		m.Const = strings.TrimSpace(terms[0])
		if len(m.Const) == 0 || !isupper(rune(m.Const[0])) {
			ok = false
			return
		}
	}

	if comment {
		terms := strings.Split(c, " ")
		const prefix = "sql:"
		for _, term := range terms {
			if name, ok := strings.CutPrefix(term, prefix); ok {
				m.Names = append(m.Names, name)
			}
		}
	}

	if len(m.Names) == 0 {
		m.Names = append(m.Names, pascal2snake(m.Const))
	}

	ok = true
	return
}

func pascal2snake(s string) string {
	out := make([]rune, 0, len(s))
	for i, c := range s {
		if i > 0 && isupper(c) {
			out = append(out, '_')
		}
		out = append(out, c)
	}

	return strings.ToUpper(string(out))
}

func isupper(c rune) bool {
	return c >= 'A' && c <= 'Z'
}

func processLines(path string, fn func(line string) bool) error {
	f, err := os.Open(path)
	checkErr(err)
	defer f.Close()
	s := bufio.NewScanner(f)

	for s.Scan() {
		if !fn(s.Text()) {
			break
		}
	}

	return s.Err()
}

func createFile(path string, mapping []Mapping) {
	buf := bytes.NewBuffer(nil)
	generateFileContents(buf, mapping)

	checksum := []byte(fmt.Sprintf("// checksum: %x\n", md5.Sum(buf.Bytes())))
	regenerate := true
	old, err := os.ReadFile(path)
	if err == nil {
		regenerate = !bytes.HasSuffix(old, checksum)
	}

	if regenerate {
		fmt.Printf("Creating %q\n", path)

		f, err := os.Create(path)
		checkErr(err)
		defer f.Close()
		_, err = f.Write(buf.Bytes())
		checkErr(err)
		_, err = f.Write(checksum)
		checkErr(err)
	}
}

const autogenerated = "// Code generated automatically; DO NOT EDIT"

func generateFileContents(f io.Writer, mapping []Mapping) {
	writeln := func(s string, args ...any) {
		fmt.Fprintf(f, s+"\n", args...)
	}

	writeln("package expr")
	writeln("")
	writeln(autogenerated)
	writeln("")
	writeln("var builtin2Name = [%d]string{", len(mapping))

	for i := range mapping {
		writeln("\t%q, // %s", mapping[i].Name(), mapping[i].Const)
	}

	writeln("}")

	writeln("func name2Builtin(s string) BuiltinOp {")
	writeln("\tswitch s {")
	for i := range mapping {
		m := &mapping[i]
		for j := range m.Names {
			writeln("\tcase %q: return %s", m.Names[j], m.Const)
		}
	}
	writeln("\t}") // switch
	writeln("\treturn Unspecified")
	writeln("}")

}

func checkErr(err error) {
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}
}
