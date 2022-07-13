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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/jsonrl"

	"golang.org/x/exp/slices"
)

type testWriter struct {
	t    *testing.T
	enc  Encoder
	dec  Decoder
	dec2 Decoder
}

func (t *testWriter) Write(buf []byte) (int, error) {
	t.enc.SetSeed(0xf00fbeef)
	chunk, err := t.enc.Encode(buf, nil)
	if err != nil {
		return 0, err
	}

	out, err := t.dec.Decode(chunk, nil)
	if err != nil {
		return 0, err
	}

	// we expect that decoding every structure
	// should produce *bit-identical* results to
	// the original input data, with the exception
	// that we don't actually encode the nop pad
	if !bytes.HasPrefix(buf, out) {
		off := 0
		for off < len(buf) && off < len(out) && buf[off] == out[off] {
			off++
		}
		off &^= 7
		t.t.Logf("input:  %x", buf[off:])
		t.t.Logf("output: %x", out[off:])
		t.t.Fatal("output and input not identical")
	}
	tail := buf[len(out):]
	if len(tail) > 0 && ion.TypeOf(tail) != 0 {
		t.t.Fatal("non-matching suffix isn't a nop pad")
	}

	var other bytes.Buffer
	t.dec2.TargetWriteSize = 3 // torture
	n, err := t.dec2.CopyBytes(&other, chunk)
	if err != nil {
		t.t.Fatal(err)
	}
	if !bytes.Equal(other.Bytes(), out) {
		t.t.Error("CopyBytes and Decode disagree")
		if other.Len() != len(out) {
			t.t.Errorf("CopyBytes -> %d bytes; Decode -> %d bytes", other.Len(), len(out))
		}
		words := 0
		for i := 0; i < len(out)-4; i += 4 {
			if i >= other.Len()-4 {
				break
			}
			// display differing words, up to 10 words
			w := out[i : i+4]
			o := other.Bytes()[i : i+4]
			if !bytes.Equal(w, o) {
				t.t.Errorf("offset %d: %#x != %#x", i, w, o)
				words++
				if words >= 10 {
					break
				}
			}
		}
	}
	if int(n) != other.Len() {
		t.t.Errorf("CopyBytes returned %d instead of %d", n, other.Len())
	}
	if t.t.Failed() {
		t.t.FailNow()
	}
	return len(buf), nil
}

func toJSON(t *testing.T, src []byte) string {
	var buf bytes.Buffer
	w := ion.NewJSONWriter(&buf, ',')
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
	return buf.String()
}

func TestSimple(t *testing.T) {
	str := `
{"foo": 0, "bar": {"baz": "quux", "other": null}, "lst": [3, null, false, "xyzabc"]}
`
	tw := &testWriter{t: t}
	cn := ion.Chunker{
		W:     tw,
		Align: 1024,
	}
	err := jsonrl.Convert(strings.NewReader(str), &cn, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cn.Flush()
	if err != nil {
		t.Fatal(err)
	}
}

type testBuffer struct {
	enc    Encoder
	output [][]byte
	input  [][]byte
}

func (t *testBuffer) Write(p []byte) (int, error) {
	t.enc.SetSeed(uint32(len(t.output)) + 0x10000)
	chunk, err := t.enc.Encode(p, nil)
	if err != nil {
		return 0, err
	}
	t.output = append(t.output, chunk)
	t.input = append(t.input, slices.Clone(p))
	return len(p), nil
}

func TestDecodePart(t *testing.T) {
	type testcase struct {
		input, output string
		selection     []string // for SetComponents
		decomps       int      // expected #buckets touched
	}
	cases := []testcase{
		{
			input:     `{"x": 3, "y": 5} {"x": 4, "y": 6}`,
			output:    `[{"x": 3, "y": 5},{"x": 4, "y": 6}]`,
			selection: []string{"x", "y"},
			decomps:   2,
		},
		{
			input:     `{"x": 3, "y": 5} {"x": 4, "y": 6}`,
			output:    `[{"x": 3},{"x": 4}]`,
			selection: []string{"x"},
			decomps:   1,
		},
		{
			input:     `{"x": 3, "y": 5} {"x": 4, "y": 6}`,
			output:    `[{"y": 5},{"y": 6}]`,
			selection: []string{"y"},
			decomps:   1,
		},
		{
			input:     `{"content": {"x": 3, "y": 4, "other": null, "extra": "even more"}, "z": null}`,
			output:    `[{"content": {"x": 3, "y": 4, "other": null, "extra": "even more"}}]`,
			selection: []string{"content"},
			decomps:   1,
		},
		{
			input: `{"content": {"x": 3, "y": 4, "other": null, "extra": "even more"}, "z": null}`,
			// content is inlined into the shape,
			// so we must walk it (but we don't project anything)
			output:    `[{"z": null}]`,
			selection: []string{"z"},
			decomps:   1, // 'content' is inline, so just 'z'
		},
		{
			input:     `{"small": {"x": 0}, "large": "some more field data", "large2": "even more field data!"}`,
			output:    `[{"small": {"x": 0}}]`,
			selection: []string{"small"},
			decomps:   1,
		},
		{
			input: `
{"x": "string zero", "y": "string one"}
{"x": "string zero", "y": "string one"}
{"x": "string two", "y": "string three"}
{"x": "string three", "y": "string two"}
`,
			output:    `[{"x": "string zero"},{"x": "string zero"},{"x": "string two"},{"x": "string three"}]`,
			selection: []string{"x"},
			decomps:   1,
		},
	}
	for i := range cases {
		in := cases[i].input
		out := cases[i].output
		selection := cases[i].selection
		touched := cases[i].decomps
		t.Run(fmt.Sprintf("case%d", i), func(t *testing.T) {
			tb := &testBuffer{}
			cn := ion.Chunker{
				W:     tb,
				Align: 1024,
			}
			err := jsonrl.Convert(strings.NewReader(in), &cn, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = cn.Flush()
			if err != nil {
				t.Fatal(err)
			}
			var dec Decoder
			dec.SetComponents(selection)

			var js bytes.Buffer
			jw := ion.NewJSONWriter(&js, ',')
			for i := range tb.output {
				_, err = dec.CopyBytes(jw, tb.output[i])
				if err != nil {
					t.Fatal(err)
				}
			}
			err = jw.Close()
			if err != nil {
				t.Fatal(err)
			}
			got := js.String()
			if got != out {
				t.Errorf("got  %s", got)
				t.Errorf("want %s", out)
			}
			if dec.decomps != touched {
				t.Errorf("dec.decomps=%d, but wanted %d buckets touched", dec.decomps, touched)
			}
		})
	}
}

type countWriter struct {
	enc   Encoder
	dec   Decoder
	count int
}

func (c *countWriter) Write(p []byte) (int, error) {
	chunk, err := c.enc.Encode(p, nil)
	if err != nil {
		return 0, err
	}
	n, err := c.dec.Count(chunk)
	if err != nil {
		return 0, err
	}
	c.count += n
	return len(p), nil
}

func TestRoundtrip(t *testing.T) {
	files := []struct {
		name   string
		count  int
		fields []string
	}{
		{
			name:  "cloudtrail.json",
			count: 1000,
			fields: []string{
				"eventTime",
				"eventType",
				"hostname",
				"errorCode",
				"managementEvent",
				"eventSource",
				"userIdentity",
				"readOnly",
			},
		},
	}

	testCount := func(t *testing.T, f *os.File, want int) {
		f.Seek(0, 0)
		cw := &countWriter{}
		cn := ion.Chunker{
			W:     cw,
			Align: 256 * 1024,
		}
		err := jsonrl.Convert(f, &cn, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = cn.Flush()
		if err != nil {
			t.Fatal(err)
		}
		if cw.count != want {
			t.Errorf("Decoder.Count=%d, want %d", cw.count, want)
		}
	}

	testAll := func(t *testing.T, f *os.File) {
		tw := testWriter{t: t}
		cn := ion.Chunker{
			W:     &tw,
			Align: 256 * 1024,
		}
		err := jsonrl.Convert(f, &cn, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = cn.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, testcase := range files {
		t.Run(testcase.name, func(t *testing.T) {
			fp := filepath.Join("..", "..", "testdata", testcase.name)
			f, err := os.Open(fp)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			t.Run("all", func(t *testing.T) {
				testAll(t, f)
			})
			f.Seek(0, 0)
			t.Run("count(*)", func(t *testing.T) {
				testCount(t, f, testcase.count)
			})

			for i := range testcase.fields {
				rest := testcase.fields[i:]
				for j := range rest {
					if j == 0 {
						continue
					}
					all := slices.Clone(rest[:j])
					name := strings.Join(all, ",")
					t.Run(name, func(t *testing.T) {
						f.Seek(0, 0)
						testProjectEquivalent(t, all, f)
					})
				}
			}
		})
	}
}

func TestCompressDecompress(t *testing.T) {
	text, err := os.ReadFile("roundtrip_test.go")
	if err != nil {
		t.Fatal(err)
	}
	cmp, err := compress(text, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, skip, err := decompress(cmp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if skip != len(cmp) {
		t.Errorf("skip=%d, should be %d", skip, len(cmp))
	}
	if !bytes.Equal(out, text) {
		t.Fatal("compress->decompress doesn't work")
	}
}

func trimnop(buf []byte) []byte {
	off := 0
	for off < len(buf) {
		if ion.IsBVM(buf[off:]) {
			off += 4
			continue
		}
		if ion.TypeOf(buf[off:]) == ion.NullType {
			break
		}
		off += ion.SizeOf(buf[off:])
	}
	return buf[:off]
}

func BenchmarkDecompressFields(b *testing.B) {
	type benchcase struct {
		file   string
		fields [][]string
	}
	cases := []benchcase{
		{
			file: "cloudtrail.json",
			fields: [][]string{
				{},
				{"eventTime"},
				{"eventTime", "eventType"},
				{"eventTime", "eventID", "hostname"},
				{"eventTime", "eventType", "errorCode"},
			},
		},
	}
	for i := range cases {
		f := cases[i].file
		fields := cases[i].fields
		b.Run(f, func(b *testing.B) {
			fp := filepath.Join("..", "..", "testdata", f)
			f, err := os.Open(fp)
			if err != nil {
				b.Fatal(err)
			}
			info, err := f.Stat()
			if err != nil {
				b.Fatal(err)
			}
			filesize := info.Size()
			defer f.Close()
			tb := &testBuffer{}
			cn := ion.Chunker{
				W:     tb,
				Align: 1024 * 1024,
			}
			err = jsonrl.Convert(f, &cn, nil)
			if err != nil {
				b.Fatal(err)
			}
			err = cn.Flush()
			if err != nil {
				b.Fatal(err)
			}
			size := int64(0)
			for i := range tb.output {
				size += int64(len(tb.output[i]))
			}
			var in []byte
			for i := range tb.input {
				b.Logf("%d -> %d bytes", len(trimnop(tb.input[i])), len(tb.output[i]))
				// trim the nop pad off of the
				// input slices so that we don't
				// count this towards the compression ratio
				in = append(in, trimnop(tb.input[i])...)
			}
			insize := len(in)
			in = enc.EncodeAll(in, nil)
			// benchmark simply (de)compressing the input data directly
			b.Run("baseline", func(b *testing.B) {
				b.Logf("%d -> %d bytes", insize, len(in))
				b.SetBytes(int64(insize)) // bytes of uncompressed ion
				b.ReportMetric(float64(len(in))/float64(insize), "compression-ratio")
				b.ReportMetric(float64(len(in))/float64(filesize), "final-compression-ratio")
				b.RunParallel(func(pb *testing.PB) {
					var out []byte
					var err error
					for pb.Next() {
						out, err = dec.DecodeAll(in, out[:0])
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
			b.Run("count(*)", func(b *testing.B) {
				b.SetBytes(int64(insize))
				b.RunParallel(func(pb *testing.PB) {
					var dec Decoder
					for pb.Next() {
						for j := range tb.output {
							_, err := dec.Count(tb.output[j])
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			})

			for _, sel := range fields {
				name := "all"
				if len(sel) > 0 {
					name = strings.Join(sel, ",")
				}
				b.Run(name, func(b *testing.B) {
					b.SetBytes(int64(insize)) // bytes of uncompressed ion
					b.ReportMetric(float64(size)/float64(insize), "compression-ratio")
					b.ReportMetric(float64(size)/float64(filesize), "final-compression-ratio")
					n := int64(0)
					var err error
					var dec Decoder
					dec.SetComponents(sel)
					for j := range tb.output {
						var nn int64
						nn, err = dec.CopyBytes(io.Discard, tb.output[j])
						if err != nil {
							b.Fatal(err)
						}
						n += nn
					}
					b.ReportMetric(float64(n)/float64(insize), "output-ratio")
					b.RunParallel(func(pb *testing.PB) {
						var err error
						var dec Decoder
						dec.SetComponents(sel)
						for pb.Next() {
							for j := range tb.output {
								_, err = dec.CopyBytes(io.Discard, tb.output[j])
								if err != nil {
									b.Fatal(err)
								}
							}
						}
					})
				})
			}
		})
	}
}
