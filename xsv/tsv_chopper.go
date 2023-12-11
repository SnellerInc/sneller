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

package xsv

import (
	"bufio"
	"bytes"
	"io"
	"slices"
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
