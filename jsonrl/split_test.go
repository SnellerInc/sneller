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

package jsonrl_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/versify"
	. "github.com/SnellerInc/sneller/jsonrl"
)

func process(t testing.TB, sp *Splitter, r io.ReaderAt, size int64) {
	err := sp.Split(r, size)
	if err != nil {
		t.Fatal(err)
	}
}

type outbuf struct {
	bytes.Buffer
	closed bool
}

func (o *outbuf) Close() error {
	if o.closed {
		return fmt.Errorf("already closed")
	}
	o.closed = true
	return nil
}

const testAlign = 1024 * 1024

func check(t *testing.T, contents []byte, objcount int) {
	if len(contents)&(testAlign-1) != 0 {
		t.Fatalf("output size %d is not aligned", len(contents))
	}
	chunks := len(contents) / testAlign
	var err error
	objn := 0
	for chunk := 0; chunk < chunks; chunk++ {
		mem := contents[chunk*testAlign:]
		mem = mem[:testAlign]
		var st ion.Symtab
		mem, err = st.Unmarshal(mem)
		if err != nil {
			t.Fatalf("chunk %d of %d %s", chunk, chunks, err)
		}
		var dat ion.Datum
		for len(mem) > 0 {
			if ion.IsBVM(mem) {
				t.Errorf("offset %d: unexpected symbol table", testAlign-len(mem))
			}
			off := testAlign - len(mem)
			pre := mem[:4]
			dat, mem, err = ion.ReadDatum(&st, mem)
			if err != nil {
				t.Fatalf("offset %d object %d pre %x %s", off, objn, pre, err)
			}
			if _, ok := dat.(ion.UntypedNull); ok {
				if len(mem) != 0 {
					t.Errorf("offset %d object %d is null but not a nop pad", off, objn)
				}
				break
			}
			if _, ok := dat.(*ion.Struct); !ok {
				t.Errorf("found non-struct datum %T", dat)
			}
			objn++
		}
	}
	if objcount >= 0 && objn != objcount {
		t.Errorf("counted %d non-null objects; wanted %d", objn, objcount)
	}
}

func TestSplit(t *testing.T) {
	files := []struct {
		name    string
		objects int
	}{
		{"parking2.json", 1023},
		{"parking3.json", 60},
	}

	for i := range files {
		t.Run(files[i].name, func(t *testing.T) {
			f, err := os.Open("../testdata/" + files[i].name)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					t.Skip()
				}
				t.Fatal(err)
			}
			defer f.Close()
			fi, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			var sp Splitter
			// we're making the window size
			// extra amll and the parallelism high
			// in order to get more coverage
			sp.WindowSize = 8 * 1024
			sp.MaxParallel = 20
			dst := &outbuf{}
			sp.Output = &SimpleWriter{W: dst}
			sp.Alignment = 1024 * 1024
			process(t, &sp, f, fi.Size())
			check(t, dst.Bytes(), files[i].objects)
		})
	}
}

type testCorpus struct {
	js       []byte // raw json from ion chunks (in order)
	objcount int
}

func synthesize(t testing.TB, corpus string, size int) *testCorpus {
	f, err := os.Open(corpus)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	u, _, err := versify.FromJSON(d)
	if err != nil {
		t.Fatal(err)
	}
	r, w := io.Pipe()
	objcount := 0
	go func() {
		defer w.Close()
		rnd := rand.New(rand.NewSource(0))
		var st ion.Symtab
		var buf, tmp ion.Buffer
		lastid := 10
		written := 0
		for written < size {
			d := u.Generate(rnd)
			d.Encode(&buf, &st)
			objcount++
			if st.MaxID() > lastid {
				tmp.Reset()
				st.Marshal(&tmp, true)
				n, err := w.Write(tmp.Bytes())
				if err != nil {
					panic(err)
				}
				written += n
				lastid = st.MaxID()
			}
			if buf.Size() > 64*1024 || buf.Size() >= size {
				n, err := w.Write(buf.Bytes())
				if err != nil {
					panic(err)
				}
				written += n
				buf.Reset()
			}
		}
		if buf.Size() > 0 {
			_, err := w.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
		}
	}()
	var final bytes.Buffer
	_, err = ion.ToJSON(&final, bufio.NewReader(r))
	if err != nil {
		t.Fatal(err)
	}
	return &testCorpus{js: final.Bytes(), objcount: objcount}
}

func TestSplitVersified(t *testing.T) {
	files := []string{
		"parking2.json",
		"parking3.json",
	}
	for i := range files {
		t.Run(files[i], func(t *testing.T) {
			tc := synthesize(t, filepath.Join("../testdata/", files[i]), 32*1024*1024)
			var sp Splitter
			// we're making the window size
			// extra amll and the parallelism high
			// in order to get more coverage
			sp.WindowSize = 8 * 1024
			sp.MaxParallel = 20
			dst := &outbuf{}
			sp.Output = &SimpleWriter{W: dst}
			sp.Alignment = 1024 * 1024
			process(t, &sp, bytes.NewReader(tc.js), int64(len(tc.js)))
			check(t, dst.Bytes(), tc.objcount)
		})
	}
}

type benchout struct{}

func (b benchout) Open() (io.WriteCloser, error) {
	return b, nil
}

func (b benchout) Close() error { return nil }

func (b benchout) Write(p []byte) (int, error) {
	return len(p), nil
}

func (b benchout) CloseError(err error) {}

func BenchmarkSplit(b *testing.B) {
	files := []string{
		"parking2.json",
		"parking3.json",
	}
	for i := range files {
		b.Run(files[i], func(b *testing.B) {
			const size = 1024 * 1024 * 16
			fp := filepath.Join("../testdata/" + files[i])
			tc := synthesize(b, fp, size)
			insize := int64(len(tc.js))
			rd := bytes.NewReader(tc.js)
			b.SetBytes(insize)
			b.ReportAllocs()
			b.ResetTimer()
			var sp Splitter
			sp.Output = benchout{}
			// NOTE: part of the sensitivity of
			// this test to parallelism is the amount
			// of readahead implicitly being performed,
			// so using a parallelism greater than
			// the number of hardware threads can be faster
			// on fast disks...
			for i := 0; i < b.N; i++ {
				process(b, &sp, rd, insize)
			}
		})
	}
}
