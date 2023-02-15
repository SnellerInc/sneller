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

package tests

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
)

var sepdash = []byte("---")

// TestCaseSpec holds the pre-parsed testcase content.
type TestCaseSpec struct {
	// List of lines between `---` marks
	Sections [][]string

	// Map of key:value tags extracted from comments
	Tags map[string]string
}

// ReadTestCaseSpecFromFile is a wrapper for ReadTestcase that reads
// testcase from a file.
func ReadTestCaseSpecFromFile(fname string) (*TestCaseSpec, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return readTestcaseSpec(f)
}

// readTestcaseSpec reads parts of a text separated by lines `---`.
//
// Each part is a list of lines.
// The procedure skips empty lines and lines staring with the `#`.
// The procedure collects all key=value settings that start with double '##'.
func readTestcaseSpec(reader io.Reader) (*TestCaseSpec, error) {
	rd := bufio.NewScanner(reader)

	spec := &TestCaseSpec{
		Tags: make(map[string]string),
	}

	sectionID := 0
	spec.Sections = append(spec.Sections, []string{})

	for rd.Scan() {
		line := rd.Bytes()
		if bytes.HasPrefix(line, sepdash) {
			sectionID += 1
			spec.Sections = append(spec.Sections, []string{})
			continue
		}

		if len(line) == 0 {
			continue
		}

		// allow # line comments iff they begin the line
		n := len(line)
		if n > 0 && line[0] == '#' {
			if n > 1 && line[1] == '#' {
				// parse '## key: value'
				if k, v, ok := parseKeyValue(string(line[2:])); ok {
					spec.Tags[k] = v
				}
			}

			continue
		}

		spec.Sections[sectionID] = append(spec.Sections[sectionID], string(line))
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}

	return spec, nil
}

func parseKeyValue(line string) (key string, value string, ok bool) {
	key, value, ok = strings.Cut(line, ":")
	if !ok {
		return
	}

	key = strings.TrimSpace(key)
	key = strings.ToLower(key)
	value = strings.TrimSpace(value)
	ok = len(key) != 0 && len(value) != 0
	return
}
