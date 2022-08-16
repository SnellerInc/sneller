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
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

// manually validate an output buffer
func skipok(buf []byte, t *testing.T) int {
	off := 0
	obj := 0
	for off < len(buf) {
		// at alignment boundaries we expect
		// a BVM and a symbol table
		if off&(defaultAlign-1) == 0 {
			if !ion.IsBVM(buf) {
				t.Fatalf("offset %d missing BVM", off)
			}
			off += 4
			var st ion.Symtab
			rest, err := st.Unmarshal(buf[off:])
			if err != nil {
				t.Fatalf("at offset %d: %s", off, err)
			}
			n := len(buf[off:]) - len(rest)
			off += n
			continue
		}
		ds, skip := objsize(buf[off:])
		if int(ds+skip)+off > len(buf) {
			t.Errorf("obj %x", buf[off:off+int(ds)])
			t.Errorf("off %d (obj %d): length (%d + %d) > len(buf)=%d",
				off, obj, ds, skip, len(buf))
			return obj
		}
		if ion.TypeOf(buf[off:]) == ion.StructType {
			inner := buf[off+int(ds) : off+int(ds)+int(skip)]
			soff := 0
			field := 0
			lastsym := uint(0)
			for soff < len(inner) {
				sym, ssize := uvint(inner[soff:])
				if sym <= lastsym {
					t.Errorf("obj %d: field %d symbol %d <= last symbol %d", obj, field, sym, lastsym)
				}
				lastsym = sym
				soff += ssize
				fds, fskip := objsize(inner[soff:])
				fsize := int(fds + fskip)
				if soff+fsize > len(inner) {
					t.Logf("symbol id: %d", sym)
					t.Logf("encoding: %x", inner[soff:soff+int(fds)])
					t.Logf("structure: %x", inner)
					t.Errorf("obj %d: field %d (off %d) + length (%d + %d) > len(struct)=%d",
						obj, field, soff, fds, fskip, len(inner))
					return obj
				}
				soff += fsize
				field++
			}
			if soff != len(inner) {
				t.Errorf("obj: %x", inner)
				t.Errorf("obj %d: only consumed %d bytes (%d fields) of %d...?", obj, soff, field, len(inner))
			}
			obj++
		} else if ion.TypeOf(buf[off:]) != ion.NullType && ion.TypeOf(buf[off:]) != ion.AnnotationType {
			t.Errorf("unexpected tag bits %x", buf[off:off+1])
		}
		off += int(ds + skip)
	}
	return obj
}

var ticketSelections = []Selection{
	selection("Ticket, Make"),
	selection("Ticket as ticket, Make as mk"),
	selection("Fine"),
	selection("IssueTime as time, IssueData as data, Color as c"),
	selection("Make as m, BodyStyle as b, Color as c"),
	selection("Ticket as t, ViolationDescr as v"),
	// test using 'name' in projection,
	// which should cause re-sorting
	selection("Make as m, Ticket as name"),
}

func TestSelect(t *testing.T) {
	buf := unhex(parkingCitations1KLines)
	tcs := ticketSelections
	for i := range tcs {
		sel := tcs[i]
		t.Run(sel.String(), func(t *testing.T) {
			var out QueryBuffer
			dst := NewProjection(sel, &out)
			err := CopyRows(dst, buftbl(buf), 1)
			if err != nil {
				t.Fatal(err)
			}

			if len(out.Bytes()) == 0 {
				t.Fatal("no output")
			}
			skipok(out.Bytes(), t)

			// test that the number of rows we got
			// is ok
			var c Count
			err = CopyRows(&c, out.Table(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if c.Value() != 1023 {
				t.Errorf("only got %d rows in output?", c.Value())
			}

			// test that we got the right output symbol table
			var outst ion.Symtab
			_, err = outst.Unmarshal(out.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			for j := range sel {
				as := sel[j].Result()
				_, ok := outst.Symbolize(as)
				if !ok {
					t.Errorf("output symbol table missing %q", as)
				}
			}
		})
	}
}

func TestSelectNested(t *testing.T) {
	buf, err := os.ReadFile("../testdata/parking2.ion")
	if err != nil {
		t.Fatal(err)
	}
	tcs := []Selection{
		selection("Ticket as ticket, Issue.Data as data, Issue.Time as time"),
		selection("Issue.Time as time, Issue.Data as data"),
		selection("Coordinates.Lat as lat, Coordinates.Long as long"),
		// test duplicate selection of Ticket in output
		selection("Ticket as t, Issue.Data as data, Ticket as t2"),
		// test selecting a non-existent entry
		selection("Ticket as t, DoesNotExist as d"),
	}
	for _, sel := range tcs {
		t.Run(sel.String(), func(t *testing.T) {
			var out QueryBuffer
			dst := NewProjection(sel, &out)
			err := CopyRows(dst, buftbl(buf), 1)
			if err != nil {
				t.Fatal(err)
			}

			if len(out.Bytes()) == 0 {
				t.Fatal("no output")
			}
			skipok(out.Bytes(), t)

			// test that the number of rows we got
			// is ok
			var c Count
			err = CopyRows(&c, out.Table(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if c.Value() != 1023 {
				t.Errorf("only got %d rows in output?", c.Value())
			}

			// test that we got the right output symbol table
			var outst ion.Symtab
			_, err = outst.Unmarshal(out.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			for j := range sel {
				as := sel[j].Result()
				_, ok := outst.Symbolize(as)
				if !ok {
					t.Errorf("output symbol table missing %q", as)
				}
			}
		})
	}
}

// dumb table that yields the same chunk 'count' times
// (useful for benchmarking)
type looptable struct {
	count int64
	chunk []byte
}

func (l *looptable) Chunks() int { return int(l.count) }

func (l *looptable) run(dst io.Writer) error {
	tmp := Malloc()
	defer Free(tmp)
	tmp = tmp[:copy(tmp, l.chunk)]
	for atomic.AddInt64(&l.count, -1) >= 0 {
		_, err := dst.Write(tmp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *looptable) WriteChunks(dst QuerySink, parallel int) error {
	return SplitInput(dst, parallel, l.run)
}

func BenchmarkSelect(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	tcs := ticketSelections
	for i := range tcs {
		sel := tcs[i]
		b.Run(sel.String(), func(b *testing.B) {
			var c Count
			dst := NewProjection(sel, &c)
			tbl := &looptable{count: int64(b.N), chunk: buf}
			b.SetBytes(int64(len(buf)))
			parallel := runtime.GOMAXPROCS(0)
			b.SetParallelism(parallel)
			err := CopyRows(dst, tbl, parallel)
			if err != nil {
				b.Fatal(err)
			}
		})
	}
}

// This benchmark exists so that you can get a sense
// of what the peak memory bandwidth is on your machine
//
// This is essentially measuring the peak theoretical
// performance of 'SELECT *'
func BenchmarkMemcpy(b *testing.B) {
	b.SetBytes(defaultAlign)
	b.RunParallel(func(pb *testing.PB) {
		in := make([]byte, defaultAlign)
		out := make([]byte, defaultAlign)
		x := 0x01234567abcdef
		for pb.Next() {
			x ^= copy(out, in)
		}
	})
}
