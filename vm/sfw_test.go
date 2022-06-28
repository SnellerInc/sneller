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
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

type noClose struct {
	io.Writer
}

func (n *noClose) Close() error { return nil }

func noppad(buf []byte) {
	for len(buf) > 0 {
		wrote, padded := ion.NopPadding(buf, len(buf))
		buf = buf[(wrote + padded):]
	}
}

// See #561
func TestRematerializeIssue561(t *testing.T) {
	row0 := ion.NewStruct(nil,
		[]ion.Field{
			{Label: "foo", Value: ion.Int(0)},
			{Label: "bar", Value: ion.String("the value for bar, row0")},
		},
	)
	row1 := ion.NewStruct(nil,
		// note: fields out-of-order w.r.t. the above
		// will yield a different symbol table
		[]ion.Field{
			{Label: "bar", Value: ion.String("the value for bar, row1")},
			{Label: "foo", Value: ion.Int(1)},
			{Label: "quux", Value: ion.Bool(true)},
		},
	)

	// encode each datum in its own chunk
	// with a different symbol table
	var buf0, buf1 ion.Buffer
	var st ion.Symtab
	var tmp bytes.Buffer
	var out [][]byte
	for _, d := range []ion.Datum{row0, row1} {
		d.Encode(&buf0, &st)
		st.Marshal(&buf1, true)
		tmp.Write(buf1.Bytes())
		tmp.Write(buf0.Bytes())
		out = append(out, tmp.Bytes())
		tmp = bytes.Buffer{}
		st.Reset()
		buf0.Set(nil)
		buf1.Set(nil)
	}

	rc := asRowConsumer(&noClose{&tmp}) // create rematerializer
	splitter := splitter(rc)

	mem := Malloc()
	defer Free(mem)
	for _, chunk := range out {
		size := copy(mem, chunk)
		noppad(mem[size:]) // test that we handle nop pad ok
		_, err := splitter.Write(mem)
		if err != nil {
			t.Fatal(err)
		}
	}
	// test that writing a nop pad alone is ok
	noppad(mem[:150])
	_, err := splitter.Write(mem[:150])
	if err != nil {
		t.Fatal(err)
	}

	err = splitter.Close()
	if err != nil {
		t.Fatal(err)
	}

	orig := tmp.Bytes()
	rd := bufio.NewReader(bytes.NewReader(orig))
	tmp = bytes.Buffer{}
	_, err = ion.ToJSON(&tmp, rd)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"foo": 0, "bar": "the value for bar, row0"}
{"bar": "the value for bar, row1", "foo": 1, "quux": true}`
	got := strings.TrimSpace(tmp.String())
	if got != want {
		t.Errorf("wanted: %s", want)
		t.Errorf("got: %s", got)
	}

	// test that we have exactly two symbol tables
	if !ion.IsBVM(orig) {
		t.Fatalf("data didn't begin with a BVM %x", orig[:4])
	}
	orig = orig[ion.SizeOf(orig):]
	stcount := 1
	for len(orig) > 0 {
		if ion.IsBVM(orig) {
			stcount++
		}
		orig = orig[ion.SizeOf(orig):]
	}
	if stcount != 2 {
		t.Errorf("found %d symbol tables; expected 2", stcount)
	}
}
