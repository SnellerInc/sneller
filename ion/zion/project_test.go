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

package zion

import (
	"bytes"
	"errors"
	"os"
	"slices"
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
	p, err := vm.NewProjection(vm.Selection(sel), out)
	if err != nil {
		t.Fatal(err)
	}
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

func onePartSortedEqual(d *Decoder, got, want []byte) bool {
	if len(d.components) != 1 {
		return false
	}
	sep := []byte{'\n'}

	gotlines := bytes.Split(got, sep)
	wantlines := bytes.Split(want, sep)

	slices.SortFunc(gotlines, bytes.Compare)
	slices.SortFunc(wantlines, bytes.Compare)
	return slices.EqualFunc(gotlines, wantlines, bytes.Equal)
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
	if !bytes.Equal(got, want) && !onePartSortedEqual(&p.dec, got, want) {
		p.t.Logf("buckets: %#x", p.dec.buckets.BucketBits)
		for i := range p.dec.st.components {
			c := &p.dec.st.components[i]
			if c.symbol == ^ion.Symbol(0) {
				p.t.Logf("missing symbol %s", c.name)
				continue
			}
			// the encoder's idea of which symbols
			// correspond to which buckets should map 1:1 to the decoder's:
			if int(p.enc.sym2bucket[c.symbol]) != p.dec.shape.SymbolBucket(c.symbol) {
				p.t.Fatal("bucket mismatches")
			}
			if p.dec.buckets.BucketBits&(1<<int(p.enc.sym2bucket[c.symbol])) == 0 {
				p.t.Fatal("not using a required bucket?")
			}
			p.t.Logf("block %d comp %d %s %d bucket %d", p.blockno, i, c.name, c.symbol, p.enc.sym2bucket[c.symbol])
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
	err := jsonrl.Convert(src, &cn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}
