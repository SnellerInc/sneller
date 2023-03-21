// Copyright (C) 2023 Sneller, Inc.
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
			line := s.lines[s.location.line]
			if include, ok := strings.CutPrefix(line, "#include "); ok {
				n := len(include)
				if n > 2 && include[0] == '"' && include[n-1] == '"' {
					path := include[1 : n-1]
					lines, err := a.readfile(path)
					if err != nil {
						return err
					}
					stack = append(stack, &iterator{
						location: Location{path: path},
						lines:    lines})
					s.location.line++
				} else {
					return fmt.Errorf("%s: malformed include: %s", s.location, line)
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
