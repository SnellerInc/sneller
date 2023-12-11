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
	"encoding/csv"
	"io"
)

// CsvChopper reads a CSV formatted file
// (RFC 4180) and splits each line in
// the individual fields.
type CsvChopper struct {
	// SkipRecords allows skipping the first
	// N records (useful when headers are used)
	SkipRecords int
	// Separator allows specifying a custom
	// separator (defaults to comma)
	Separator Delim

	r      io.Reader
	cr     *csv.Reader
	lineNr int
}

// GetNext fetches one CSV record and
// returns the individual columns. Due
// to quoting a CSV record may span multiple
// lines of text.
func (c *CsvChopper) GetNext(r io.Reader) ([]string, error) {
	c.init(r)
	for {
		fields, err := c.cr.Read()
		if err != nil {
			return nil, err
		}
		c.lineNr++
		if c.lineNr > c.SkipRecords {
			return fields, nil
		}
	}
}

func (c *CsvChopper) init(r io.Reader) {
	if c.r != r {
		c.r = r
		c.cr = csv.NewReader(c.r)
		c.cr.FieldsPerRecord = -1
		c.cr.ReuseRecord = true
		c.cr.LazyQuotes = true
		if c.Separator != 0 {
			c.cr.Comma = rune(c.Separator)
		}
	}
}
