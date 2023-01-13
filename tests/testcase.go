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
	"os"
)

var sepdash = []byte("---")

// ParseTestcase reads parts of a textfile separated by `---`.
//
// Each part is a list of lines.
// The procedure skips empty lines and lines staring with the `#`.
func ParseTestcase(fname string) ([][]string, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rd := bufio.NewScanner(f)

	partID := 0
	var parts [][]string
	parts = append(parts, []string{})

	for rd.Scan() {
		line := rd.Bytes()
		if bytes.HasPrefix(line, sepdash) {
			partID += 1
			parts = append(parts, []string{})
			continue
		}

		// allow # line comments iff they begin the line
		if len(line) > 0 && line[0] == '#' {
			continue
		}

		if len(line) == 0 {
			continue
		}

		parts[partID] = append(parts[partID], string(line))
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}
