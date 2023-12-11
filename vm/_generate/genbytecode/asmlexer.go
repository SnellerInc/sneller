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
)

// Location store location within a file
type Location struct {
	path string
	line int
}

func (l Location) String() string {
	return fmt.Sprintf("%q:%d", l.path, l.line)
}

// AssemblerLineReader reads sources line by line and interpret #include directive.
type AssemblerLineReader struct {
	files map[string][]string // path -> contents of file
}

type lineFunc func(*Location, string) error

func (a *AssemblerLineReader) process(p string, fn lineFunc) error {
	var err error

	type iterator struct {
		location Location
		lines    []string
	}

	top := &iterator{}
	top.lines, err = a.readfile(p)
	top.location.path = p
	if err != nil {
		return err
	}

	stack := []*iterator{top}

	for len(stack) > 0 {
		n := len(stack)
		s := stack[n-1]

		for ; /**/ s.location.line < len(s.lines); s.location.line++ {
			line := strings.TrimSpace(s.lines[s.location.line])
			if include, ok := strings.CutPrefix(line, "#include "); ok {
				k := len(include)
				if k > 2 && include[0] == '"' && include[k-1] == '"' {
					path := include[1 : k-1]
					lines, err := a.readfile(path)
					if err != nil {
						return err
					}
					stack = append(stack, &iterator{
						location: Location{path: path},
						lines:    lines})
					s.location.line++
				} else {
					return fmt.Errorf("%s: malformed include: %q", s.location, line)
				}
				break
			}

			err = fn(&s.location, line)
			if err != nil {
				return nil
			}
		}

		if s.location.line >= len(s.lines) {
			stack = stack[:n-1]
		}
	}

	return nil
}

var systemincludes = []string{"go_asm.h", "funcdata.h", "textflag.h"}
var emptyfile = []string{}

func (a *AssemblerLineReader) readfile(path string) ([]string, error) {
	if a.files == nil {
		a.files = make(map[string][]string)
		for _, s := range systemincludes {
			a.files[s] = emptyfile
		}
	}

	if lines, ok := a.files[path]; ok {
		return lines, nil
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(buf), "\n"), nil
}
