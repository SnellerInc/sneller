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
