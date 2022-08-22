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

package xsv

import (
	"bufio"
	"bytes"
	"io"

	"golang.org/x/exp/slices"
)

// TsvChopper reads a TSV formatted file
// and splits each line in the individual
// fields. TSV format differs from CSV,
// because it doesn't support quoting to
// allow non-standard characters, but uses
// escape sequences (i.e. \t, \r or \n)
type TsvChopper struct {
	// SkipRecords allows skipping the first
	// N records (useful when headers are used)
	SkipRecords int

	r      io.Reader
	s      *bufio.Scanner
	lineNr int
	starts []int
	ends   []int
	fields []string
}

const tsvSeparator = '\t'

// GetNext fetches one TSV line and
// returns the individual columns. Each
// TSV record is always exactly one line.
func (c *TsvChopper) GetNext(r io.Reader) ([]string, error) {
	c.init(r)

	for {
		if !c.s.Scan() {
			if err := c.s.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}

		c.lineNr++
		if c.lineNr > c.SkipRecords || c.s.Text() == "" {
			break
		}
	}

	line := c.s.Bytes()
	c.fields = c.fields[:0]
	c.starts = c.starts[:0]
	c.ends = c.ends[:0]

	col := 0
	nextEscape := bytes.IndexByte(line, '\\')
	for {
		startCol := col
		nextSeparator := bytes.IndexByte(line[col:], tsvSeparator)
		if nextSeparator == -1 {
			nextSeparator = len(line)
		} else {
			nextSeparator += col
		}
		escapes := 0
		if nextEscape == -1 || nextSeparator < nextEscape {
			// fast-path
			col = nextSeparator
		} else {
			// slow-path
			col = nextEscape
			for ; col < nextSeparator; col++ {
				if line[col] == '\\' && col+1 < nextSeparator {
					if replacement := backslash(line[col+1]); replacement != 0 {
						line[col-escapes] = replacement
						col++
						escapes++
						continue
					}
				}
				line[col-escapes] = line[col]
			}
			nextEscape = bytes.IndexByte(line[col:], '\\')
			if nextEscape != -1 {
				nextEscape += col
			}
		}
		c.starts = append(c.starts, startCol)
		c.ends = append(c.ends, col-escapes)
		if col == len(line) {
			break
		}
		col++
	}

	if cap(c.fields) < len(c.starts) {
		c.fields = slices.Grow(c.fields[:0], len(c.starts))
	}

	// create a single string and slice it to
	// reduce the amount of allocations.
	text := string(line)
	for i := 0; i < len(c.starts); i++ {
		c.fields = append(c.fields, text[c.starts[i]:c.ends[i]])
	}
	return c.fields, nil
}

func (c *TsvChopper) init(r io.Reader) {
	if c.r != r {
		c.r = r
		c.lineNr = 0
		c.s = bufio.NewScanner(c.r)
	}
}

func backslash(c byte) byte {
	switch c {
	case '\\':
		return '\\'
	case 't':
		return '\t'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	default:
		return 0
	}
}
