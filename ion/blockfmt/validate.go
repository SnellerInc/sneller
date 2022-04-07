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

package blockfmt

import (
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// Validate validates blockfmt-formatted object in src.
// Any errors encountered are written as distinct
// lines to diag. The caller can presume that no
// content written to diag means that the src had
// no errors.
func Validate(src io.Reader, t *Trailer, diag io.Writer) int {
	d := Decoder{Trailer: t}
	w := checkWriter{dst: diag, blocks: t.Blocks}
	d.Copy(&w, src)
	return w.rows
}

type checkWriter struct {
	dst     io.Writer
	block   int
	chunk   int
	blocks  []Blockdesc
	rows    int
	seenBVM bool

	st   ion.Symtab
	path []string
}

func (c *checkWriter) errorf(f string, args ...interface{}) {
	if len(f) > 0 && f[len(f)-1] != '\n' {
		f += "\n"
	}
	fmt.Fprintf(c.dst, f, args...)
}

func (c *checkWriter) checkRange(tm date.Time, path []string) {
	r := c.blocks[c.block].Ranges
	if len(r) == 0 {
		return
	}
	for i := range r {
		if ts, ok := r[i].(*TimeRange); ok {
			if pathequal(path, ts.Path()) {
				min, max := ts.MinTime(), ts.MaxTime()
				if tm.Before(min) || tm.After(max) {
					c.errorf("block %d chunk %d time %s out of range [%s, %s]",
						c.block, c.chunk, tm, min, max)
				}
				return
			}
		}
	}
}

func (c *checkWriter) walkStruct(fields []byte) {
	var sym ion.Symbol
	var err error
	prevsym := ion.Symbol(0)
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			c.errorf("ion.ReadLabel: %s", err)
			return
		}
		if sym <= prevsym {
			c.errorf("symbol %d < %d", sym, prevsym)
		}
		if int(sym) >= c.st.MaxID() {
			c.errorf("symbol %d not in symbol table", sym)
		}
		var row []byte
		var tm date.Time
		switch ion.TypeOf(fields) {
		case ion.TimestampType:
			tm, fields, err = ion.ReadTime(fields)
			if err != nil {
				c.errorf("ion.ReadDate: %s", err)
				return
			}
			before := len(c.path)
			c.path = append(c.path, c.st.Get(sym))
			c.checkRange(tm, c.path)
			c.path = c.path[:before]
		case ion.StructType:
			before := len(c.path)
			c.path = append(c.path, c.st.Get(sym))
			row, fields = ion.Contents(fields)
			c.walkStruct(row)
			c.path = c.path[:before]
		default:
			fields = fields[ion.SizeOf(fields):]
		}
		prevsym = sym
	}
}

func (c *checkWriter) Write(block []byte) (int, error) {
	// every block should begin with a BVM
	if !c.seenBVM && (len(block) < 4 || !ion.IsBVM(block)) {
		c.errorf("block %d chunk %d doesn't begin with a BVM", c.block, c.chunk)
	} else {
		c.seenBVM = true
	}

	var rest []byte
	var err error
	if ion.IsBVM(block) || ion.TypeOf(block) == ion.AnnotationType {
		rest, err = c.st.Unmarshal(block)
		if err != nil {
			c.errorf("unmarshal symbol table in block %d chunk %d: %s", c.block, c.chunk, err)
			return len(block), nil
		}
	} else {
		rest = block
	}
	for len(rest) > 0 {
		var row []byte
		typ := ion.TypeOf(rest)
		if typ == ion.StructType {
			row, rest = ion.Contents(rest)
			c.walkStruct(row)
			c.rows++
		} else {
			if typ != ion.NullType {
				c.errorf("block %d chunk %d (row %d): not a struct: %s",
					c.block, c.chunk, c.rows, typ)
			}
			size := ion.SizeOf(rest)
			if size <= 0 || size > len(rest) {
				c.errorf("block %d chunk %d (row %d): bad size %d (of %d)",
					c.block, c.chunk, c.rows, size, len(rest))
				break
			}
			rest = rest[size:]
		}
	}
	// update chunk and block;
	// decide if we expect a new BVM
	c.chunk++
	if c.chunk == c.blocks[c.block].Chunks {
		c.chunk = 0
		c.block++
		c.seenBVM = false // expect a new BVM
	}
	return len(block), nil
}
