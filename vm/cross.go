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

package vm

import (
	"bytes"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

// CrossJoin is a RowConsumer that
// cross-joins two tables
type CrossJoin struct {
	dst      QuerySink
	rhstab   *BufferedTable
	lhs, rhs string
}

// Cross is the slowest of the cross-join implementations;
// it simply takes one table incrementally and the other explicitly
// and cross-joins the results as a structure with two bindings
// ('lhs' and 'rhs') that contain the two sides of the cross-join.
//
// (This is essentially the fallback for a comma operator when
// there isn't a faster way to get what we want.)
//
// NOTE: not currently used by the query planner.
// Simply too inefficient to support.
// BUGS: will fail for inputs that cannot easily
// be re-symbolized.
func Cross(dst QuerySink, rhstab *BufferedTable, lhs, rhs string) *CrossJoin {
	return &CrossJoin{
		dst:    dst,
		rhstab: rhstab,
		lhs:    lhs,
		rhs:    rhs,
	}
}

type cross struct {
	parent *CrossJoin
	st     ion.Symtab
	aw     alignedWriter
	out    io.WriteCloser
	rhsbuf bytes.Buffer
	rhssep [][2]int

	lhs, rhs syminfo
}

func (c *CrossJoin) Open() (io.WriteCloser, error) {
	dst, err := c.dst.Open()
	if err != nil {
		return nil, err
	}
	out := &cross{parent: c, out: dst}
	return Splitter(out), nil
}

func (c *CrossJoin) Close() error {
	return c.dst.Close()
}

func objrewrite(lut []ion.Symbol, t ion.Type, mem []byte) error {
	switch t {
	case ion.StructType:
		return structrewrite(lut, mem)
	case ion.ListType, ion.SexpType:
		off := 0
		for off < len(mem) {
			t := ion.TypeOf(mem[off:])
			os, ds := objsize(mem[off:])
			off += int(ds)
			err := objrewrite(lut, t, mem[off:off+int(os)])
			if err != nil {
				return err
			}
			off += int(os)
		}
	case ion.SymbolType:
		u, size := uvint(mem)
		out := lut[u]
		if ion.UVarintSize(uint(out)) != size {
			return fmt.Errorf("cannot accomodate symbol rewrite %x -> %x", u, out)
		}
		ion.UnsafeWriteUVarint(mem, uint(out))
	default:
	}
	return nil
}

// copy a structure recursively, re-writing its symbols
// using the given look-up-table from old symbol ID to new
func structrewrite(lut []ion.Symbol, mem []byte) error {
	// walk each field and re-write every
	// symbol ID
	off := 0
	for off < len(mem) {
		sym, ssize := uvint(mem[off:])
		newsym := lut[sym]
		if ion.UVarintSize(uint(newsym)) != ssize {
			return fmt.Errorf("cannot rewrite symbol 0x%x -> 0x%x", sym, newsym)
		}
		if ion.UnsafeWriteUVarint(mem[off:], uint(newsym)) != ssize {
			panic("ion.UnsafeWriteUVarint")
		}
		off += ssize
		objtype := ion.TypeOf(mem[off:])
		ds, os := objsize(mem[off:])
		off += int(ds)
		if os == 0 {
			// NULL, int(0), etc.
			continue
		}
		err := objrewrite(lut, objtype, mem[off:off+int(os)])
		if err != nil {
			return err
		}
		off += int(os)
	}
	return nil
}

func (c *cross) rewrite(st *ion.Symtab) error {
	c.rhsbuf.Reset()
	c.rhssep = c.rhssep[:0]
	st.CloneInto(&c.st)
	c.st.Intern(c.parent.lhs)
	c.st.Intern(c.parent.rhs)

	var delims [16][2]uint32
	for i := 0; i < c.parent.rhstab.Chunks(); i++ {
		var rhsst ion.Symtab
		mem := c.parent.rhstab.chunk(i)
		body, err := rhsst.Unmarshal(mem)
		if err != nil {
			return fmt.Errorf("cross.rewrite: %w", err)
		}
		off := len(mem) - len(body)

		// create a look-up-table for old symbol values
		// to new symbol values
		lut := make([]ion.Symbol, rhsst.MaxID())
		for i := 0; i < 10; i++ {
			lut[i] = ion.Symbol(i)
		}
		// lut[i] = old symbol -> new symbol
		for i := 10; i < rhsst.MaxID(); i++ {
			lut[i] = c.st.Intern(rhsst.Get(ion.Symbol(i)))
		}
		for off < len(mem) {
			n, next := scan(mem, int32(off), delims[:])
			off = int(next)
			for j := range delims[:n] {
				loff := int(delims[j][0])
				llen := int(delims[j][1])
				inner := mem[loff : loff+llen]
				soff := c.rhsbuf.Len()
				c.rhsbuf.Write(inner)
				c.rhssep = append(c.rhssep, [2]int{soff, len(inner)})
				err := structrewrite(lut, c.rhsbuf.Bytes()[soff:])
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *cross) Symbolize(st *ion.Symtab) error {
	err := c.rewrite(st)
	if err != nil {
		return err
	}
	c.lhs.value, _ = c.st.Symbolize(c.parent.lhs)
	c.rhs.value, _ = c.st.Symbolize(c.parent.rhs)
	c.lhs.encoded, c.lhs.mask, c.lhs.size = encoded(c.lhs.value)
	c.rhs.encoded, c.rhs.mask, c.rhs.size = encoded(c.rhs.value)

	if c.aw.buf == nil {
		c.aw.init(c.out, nil, defaultAlign)
	}
	err = c.aw.setpre(&c.st)
	return err
}

// join a single lhs row with a single rhs row,
// where we have ensured that the rhs row uses
// the superset of the two symbol tables
func (c *cross) join(lhs, rhs []byte) error {
	// pre-compute the left-hand-side symbol + structure
	lhssize := encsize(uint(len(lhs))) + int(c.lhs.size) + len(lhs)
	rhssize := encsize(uint(len(rhs))) + int(c.rhs.size) + len(rhs)
	inner := lhssize + rhssize
	total := encsize(uint(lhssize+rhssize)) + inner
	if c.aw.space() < total {
		_, err := c.aw.flush()
		if err != nil {
			return err
		}
	}
	dst := c.aw.reserve(total)
	w := ion.UnsafeWriteTag(dst, ion.StructType, uint(inner))
	if c.lhs.value < c.rhs.value {
		w += putenc(dst[w:], c.lhs.encoded, c.lhs.size)
		w += ion.UnsafeWriteTag(dst[w:], ion.StructType, uint(len(lhs)))
		w += copy(dst[w:], lhs)
		w += putenc(dst[w:], c.rhs.encoded, c.rhs.size)
		w += ion.UnsafeWriteTag(dst[w:], ion.StructType, uint(len(rhs)))
		w += copy(dst[w:], rhs)
	} else {
		w += putenc(dst[w:], c.rhs.encoded, c.rhs.size)
		w += ion.UnsafeWriteTag(dst[w:], ion.StructType, uint(len(rhs)))
		w += copy(dst[w:], rhs)
		w += putenc(dst[w:], c.lhs.encoded, c.lhs.size)
		w += ion.UnsafeWriteTag(dst[w:], ion.StructType, uint(len(lhs)))
		w += copy(dst[w:], lhs)
	}
	if w != total {
		panic("bad accounting")
	}
	return nil
}

func (c *cross) WriteRows(buf []byte, delims [][2]uint32) error {
	rhsmem := c.rhsbuf.Bytes()
	for i := range delims {
		loff := int(delims[i][0])
		llen := int(delims[i][1])
		lhsmem := buf[loff : loff+llen]
		for j := range c.rhssep {
			roff := c.rhssep[j][0]
			rlen := c.rhssep[j][1]
			err := c.join(lhsmem, rhsmem[roff:roff+rlen])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *cross) Close() error {
	return c.aw.Close()
}
