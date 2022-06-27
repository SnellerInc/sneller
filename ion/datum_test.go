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

package ion

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDatumEncode(t *testing.T) {
	data := []struct {
		datum Datum
		str   string
	}{
		{UntypedNull{}, "null"},
		{String("foo"), `"foo"`},
		{Int(-1), "-1"},
		{Uint(1000), "1000"},
		{Bool(true), "true"},
		{Bool(false), "false"},
		{
			datum: NewStruct(nil,
				[]Field{
					{"foo", String("foo"), 0},
					{"bar", UntypedNull{}, 0},
					{"inner", NewList(nil, []Datum{
						Int(-1), Uint(0), Uint(1),
					}), 0},
					{"name", String("should-come-first"), 0},
				},
			),
			str: `{"name": "should-come-first", "foo": "foo", "bar": null, "inner": [-1, 0, 1]}`,
		},
	}

	var b, outb Buffer
	var st Symtab
	var text bytes.Buffer
	for i := range data {
		b.Reset()
		outb.Reset()
		st = Symtab{}
		data[i].datum.Encode(&b, &st)
		st.Marshal(&outb, true)
		outb.UnsafeAppend(b.Bytes())
		text.Reset()
		_, err := ToJSON(&text, bufio.NewReader(bytes.NewReader(outb.Bytes())))
		if err != nil {
			t.Errorf("encoding datum %+v: %s", data[i].datum, err)
			continue
		}
		str := text.String()
		want := data[i].str + "\n"
		if str != want {
			t.Errorf("encoding datum %+v: wanted %q; got %q", data[i].datum, want, str)
		}

		out, _, err := ReadDatum(&st, outb.Bytes())
		if err != nil {
			t.Errorf("decoding datum %+v: %s", data[i].datum, err)
			continue
		}
		if !Equal(out, data[i].datum) {
			t.Errorf("got  %#v", out)
			t.Errorf("want %#v", data[i].datum)
		}
	}
}

func TestDatumFromJSON(t *testing.T) {
	var tcs = []string{
		"0",
		"1",
		"true",
		"false",
		`"foo"`,
		`{"foo": {"bar": "baz"}, "quux": 3}`,
		`{"first": 0.02, "arr": [0, false, null, {}]}`,
	}
	for i := range tcs {
		var st Symtab
		var buf Buffer
		d := json.NewDecoder(strings.NewReader(tcs[i]))
		dat, err := FromJSON(&st, d)
		if err != nil {
			t.Errorf("decoding %q: %s", tcs[i], err)
			continue
		}
		st.Marshal(&buf, true)
		dat.Encode(&buf, &st)
		var jsbuf bytes.Buffer
		_, err = ToJSON(&jsbuf, bufio.NewReader(bytes.NewReader(buf.Bytes())))
		if err != nil {
			t.Errorf("%#v ToJSON: %s", dat, err)
			continue
		}
		outstr := jsbuf.String()
		instr := tcs[i] + "\n"
		if outstr != instr {
			t.Errorf("input: %q", instr)
			t.Errorf("output: %q", outstr)
		}
	}
}

func BenchmarkDatumPassthrough(b *testing.B) {
	files := []string{
		"nyc-taxi.block",
		"parking.10n",
		"parking2.ion",
		"parking3.ion",
	}
	for i := range files {
		f := filepath.Join("../testdata/", files[i])
		buf, err := os.ReadFile(f)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(files[i], func(b *testing.B) {
			b.ReportAllocs()
			var st Symtab
			var dst Buffer
			for i := 0; i < b.N; i++ {
				st.Reset()
				dst.Reset()
				body := buf
				for len(body) > 0 {
					d, rest, err := ReadDatum(&st, body)
					if err != nil {
						b.Fatal(err)
					}
					d.Encode(&dst, &st)
					body = rest
				}
			}
		})
	}
}

func FuzzReadDatum(f *testing.F) {
	var tcs = []string{
		"0",
		"1",
		"true",
		"false",
		`"foo"`,
		`{"foo": {"bar": "baz"}, "quux": 3}`,
		`{"first": 0.02, "arr": [0, false, null, {}]}`,
	}
	for i := range tcs {
		var st Symtab
		var buf Buffer
		d := json.NewDecoder(strings.NewReader(tcs[i]))
		dat, err := FromJSON(&st, d)
		if err != nil {
			f.Fatalf("decoding %q: %s", tcs[i], err)
		}
		st.Marshal(&buf, true)
		dat.Encode(&buf, &st)
		f.Add(buf.Bytes())
	}
	f.Fuzz(func(t *testing.T, buf []byte) {
		var st Symtab
		var err error
		var d Datum
		for len(buf) > 0 {
			d, buf, err = ReadDatum(&st, buf)
			if err != nil {
				break
			}
			switch d := d.(type) {
			case *List:
				d.Each(func(d Datum) bool { return true })
			case *Struct:
				d.Each(func(f Field) bool { return true })
			}
		}
	})
}
