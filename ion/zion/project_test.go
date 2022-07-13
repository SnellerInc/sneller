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

package zion

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/jsonrl"
	"github.com/SnellerInc/sneller/vm"
)

// for a block of ion data, compute the projection
// given by fields and produce the result
func projectToJSON(t *testing.T, buf []byte, fields []string) []byte {
	// select x AS x, y AS y, ...
	sel := make([]expr.Binding, len(fields))
	for i := range sel {
		sel[i].Expr = expr.Identifier(fields[i])
		sel[i].As(fields[i])
	}

	cp := vm.Malloc()
	defer vm.Free(cp)
	n := copy(cp, buf)

	var tmp bytes.Buffer
	jw := ion.NewJSONWriter(&tmp, '\n')
	out := vm.LockedSink(jw)
	p := vm.NewProjection(vm.Selection(sel), out)
	w, err := p.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if e := recover(); e != nil {
			f, _ := os.CreateTemp(".", "corrupt-vm-*.ion")
			f.Write(buf)
			f.Close()
			t.Logf("created output %s", f.Name())
			panic(e)
		}
	}()

	_, err = w.Write(cp[:n])
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = p.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, cp[:n]) {
		t.Fatal("memory corruption")
	}
	return tmp.Bytes()
}

func toNDJSON(t *testing.T, src []byte) []byte {
	var buf bytes.Buffer
	w := ion.NewJSONWriter(&buf, '\n')
	_, err := w.Write(src)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return buf.Bytes()
}

type projectionTester struct {
	blockno int
	t       *testing.T
	fields  []string
	enc     Encoder
	dec     Decoder
}

func (p *projectionTester) Write(block []byte) (int, error) {
	want := projectToJSON(p.t, block, p.fields)
	compressed, err := p.enc.Encode(block, nil)
	if err != nil {
		p.t.Fatal(err)
	}
	decompressed, err := p.dec.Decode(compressed, nil)
	if err != nil {
		if errors.Is(err, errCorrupt) {
			f, _ := os.CreateTemp(".", "corrupt*.ion")
			f.Write(block)
			f.Close()
			p.t.Logf("created output %s", f.Name())
		}
		p.t.Fatal(err)
	}
	got := toNDJSON(p.t, decompressed)
	if !bytes.Equal(got, want) {
		p.t.Logf("buckets: %#x", p.dec.set.buckets)
		p.t.Logf("bits: %#x", p.dec.set.bits)
		for i := range p.dec.st.components {
			c := &p.dec.st.components[i]
			if p.dec.components[i] != c.name {
				panic("???")
			}
			if c.symbol == ^ion.Symbol(0) {
				p.t.Logf("missing symbol %s", c.name)
				continue
			}
			// the encoder's idea of which symbols
			// correspond to which buckets should map 1:1 to the decoder's:
			if int(p.enc.sym2bucket[c.symbol]) != sym2bucket(uint64(p.dec.set.seed), c.symbol) {
				p.t.Fatal("bucket mismatches")
			}
			if !p.dec.set.useBucket(int(p.enc.sym2bucket[c.symbol])) {
				p.t.Fatal("not using a required bucket?")
			}
			p.t.Logf("block %d comp %d %s %d bucket %d", p.blockno, i, c.name, c.symbol, p.enc.sym2bucket[c.symbol])
			p.t.Logf("present: %v", p.dec.set.contains(c.symbol))
		}

		printed := 0
		records := 0
		sep := []byte{'\n'}
		for len(got) > 0 && len(want) > 0 {
			var gotline, wantline []byte
			gotline, got, _ = bytes.Cut(got, sep)
			wantline, want, _ = bytes.Cut(want, sep)
			if !bytes.Equal(wantline, gotline) {
				p.t.Logf("got  %d %s", records, gotline)
				p.t.Logf("want %d %s", records, wantline)
				printed++
				if printed >= 10 {
					break
				}
			}
			records++
		}
		p.t.Fatal("not equal in block", p.blockno)
	}
	p.blockno++
	return len(block), nil
}

// for a given set of fields, test that a SELECT [fields ...]
// produces the same result (in JSON) as decompressing
// just those fields from the input data
func testProjectEquivalent(t *testing.T, fields []string, src *os.File) {
	pt := &projectionTester{
		t:      t,
		fields: fields,
	}
	pt.dec.SetComponents(pt.fields)
	cn := ion.Chunker{
		W:     pt,
		Align: 128 * 1024,
	}
	err := jsonrl.Convert(src, &cn, nil)
	if err != nil {
		t.Fatal(err)
	}
}
