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

package ion_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/versify"
	"github.com/SnellerInc/sneller/jsonrl"
)

type validator struct {
	t      *testing.T
	align  int
	st     ion.Symtab
	recent []ion.Datum
	writes int
	bytes  int
}

func (v *validator) Write(p []byte) (int, error) {
	v.writes++
	v.bytes += len(p)
	if len(p) != v.align {
		return 0, fmt.Errorf("bad write: %d bytes", len(p))
	}
	var err error
	if ion.IsBVM(p) || ion.TypeOf(p) == ion.AnnotationType {
		p, err = v.st.Unmarshal(p)
		if err != nil {
			return 0, err
		}
	}
	objn := 0
	var dat ion.Datum
	for len(p) > 0 {
		dat, p, err = ion.ReadDatum(&v.st, p)
		if err != nil {
			return v.align - len(p), fmt.Errorf("validator.Write: %w", err)
		}
		if _, ok := dat.(ion.UntypedNull); ok {
			// nop pad
			continue
		}
		if !ion.Equal(dat, v.recent[objn]) {
			v.t.Errorf("object %d: got  %#v", objn, dat)
			v.t.Errorf("object %d: want %#v", objn, v.recent[objn])
			return v.align - len(p), fmt.Errorf("unexpected object at index %d", objn)
		}
		objn++
	}
	// for any objects we haven't seen yet,
	// we expect to see them at the beginning
	// of the next chunk
	v.recent = v.recent[:copy(v.recent, v.recent[objn:])]
	return 0, nil
}

func TestChunker(t *testing.T) {
	f, err := os.Open(filepath.Join("../testdata/parking2.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	u, _, err := versify.FromJSON(d)
	if err != nil {
		t.Fatal(err)
	}

	const align = 16 * 1024
	v := &validator{
		t:     t,
		align: align,
	}
	c := ion.Chunker{
		W:     v,
		Align: align,
	}
	src := rand.New(rand.NewSource(0))
	for objects := 0; objects < 100000; objects++ {
		d := u.Generate(src)
		v.recent = append(v.recent, d)
		d.Encode(&c.Buffer, &c.Symbols)
		err := c.Commit()
		if err != nil {
			t.Fatalf("after writing %d objects: %s", objects+1, err)
		}
	}
	err = c.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(v.recent) > 0 {
		t.Errorf("%d remaining expected objects", len(v.recent))
	}
}

func timestamp(s string) ion.Timestamp {
	t, ok := date.Parse([]byte(s))
	if !ok {
		panic("bad timestamp: " + s)
	}
	return ion.Timestamp(t)
}

func TestChunkerRange(t *testing.T) {
	rw := new(rangeWriter)
	c := &ion.Chunker{
		W:     rw,
		Align: 1024 * 1024,
	}

	fooSym := c.Symbols.Intern("foo")
	barSym := c.Symbols.Intern("bar")
	bazSym := c.Symbols.Intern("baz")

	t1 := date.Date(2021, 11, 10, 00, 00, 00, 0)
	t2 := date.Date(2021, 11, 17, 12, 34, 56, 0)

	// Write a bunch of objects like:
	//
	// {
	//   "foo": "date",
	//   "bar": {
	//     "baz": "date"
	//   },
	//   "baz": 123
	// }
	//
	for i := 0; i < 100000; i++ {
		d := time.Duration(i) * time.Second
		c.BeginStruct(-1)
		c.BeginField(fooSym)
		c.Ranges.AddTime(mksymbuf(fooSym), t1.Add(d))
		c.WriteTime(t1.Add(d))
		c.BeginField(barSym)
		c.BeginStruct(-1)
		c.BeginField(bazSym)
		c.Ranges.AddTruncatedTime(mksymbuf(barSym, bazSym), t2.Add(d), ion.TruncToHour)
		c.WriteTruncatedTime(t2.Add(d), ion.TruncToHour)
		c.EndStruct()
		c.BeginField(bazSym)
		c.WriteInt(123)
		c.EndStruct()
		c.Commit()
	}

	c.Flush()

	expected := [][]ranges{{{
		path: []string{"foo"},
		min:  timestamp("2021-11-10T00:00:00Z"),
		max:  timestamp("2021-11-10T11:39:01Z"),
	}, {
		path: []string{"bar", "baz"},
		min:  timestamp("2021-11-17T12:00:00Z"),
		max:  timestamp("2021-11-18T00:00:00Z"),
	}}, {{
		path: []string{"foo"},
		min:  timestamp("2021-11-10T11:39:02Z"),
		max:  timestamp("2021-11-10T23:18:03Z"),
	}, {
		path: []string{"bar", "baz"},
		min:  timestamp("2021-11-18T00:00:00Z"),
		max:  timestamp("2021-11-18T11:00:00Z"),
	}}, {{
		path: []string{"foo"},
		min:  timestamp("2021-11-10T23:18:04Z"),
		max:  timestamp("2021-11-11T03:46:39Z"),
	}, {
		path: []string{"bar", "baz"},
		min:  timestamp("2021-11-18T11:00:00Z"),
		max:  timestamp("2021-11-18T16:00:00Z"),
	}}}

	for i := range rw.allRanges {
		rng := rw.allRanges[i]
		// make sure things are sorted deterministically
		if len(rng) == 2 &&
			len(rng[1].path) == 1 &&
			rng[1].path[0] == "foo" {
			rng[0], rng[1] = rng[1], rng[0]
		}
	}

	if !reflect.DeepEqual(expected, rw.allRanges) {
		t.Errorf("ranges not equal")
		t.Errorf("want: %v", expected)
		t.Errorf("got:  %v", rw.allRanges)
	}
}

func TestChunkerSnapshot(t *testing.T) {
	rw := new(rangeWriter)
	c := &ion.Chunker{
		W:     rw,
		Align: 1024 * 1024,
	}

	foo := c.Symbols.Intern("foo")

	date1 := date.Date(2021, 11, 11, 11, 11, 11, 0)
	date2 := date.Date(2021, 12, 12, 12, 12, 12, 0)

	var snap ion.Snapshot

	// First write
	//   { "foo": /* take snapshot */ date1 }
	// ...then load snapshot and write date2 instead.
	c.BeginStruct(-1)
	c.BeginField(foo)
	c.Save(&snap)
	c.Ranges.AddTime(mksymbuf(foo), date1)
	c.WriteTime(date1)
	c.EndStruct()
	c.Load(&snap)
	c.Ranges.AddTime(mksymbuf(foo), date2)
	c.WriteTime(date2)
	c.EndStruct()

	if err := c.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if want := ([][]ranges{{{
		path: []string{"foo"},
		min:  timestamp("2021-12-12T12:12:12Z"),
		max:  timestamp("2021-12-12T12:12:12Z"),
	}}}); !reflect.DeepEqual(want, rw.allRanges) {
		t.Errorf("ranges not equal")
		t.Errorf("want: %v", want)
		t.Errorf("got:  %v", rw.allRanges)
	}
}

func checkEncoding(t *testing.T, buf *rangeBuf, align int) {
	mem := buf.Bytes()
	var st ion.Symtab
	var err error
	var d ion.Datum
	insize := len(mem)
	for len(mem) > 0 {
		d, mem, err = ion.ReadDatum(&st, mem)
		if err != nil {
			t.Fatal(err)
		}
		s, ok := d.(*ion.Struct)
		if !ok {
			continue
		}
		max := st.MaxID()
		s.Each(func(f ion.Field) bool {
			if int(f.Sym) >= max {
				offset := insize - len(mem)
				t.Logf("field: %v", f)
				t.Logf("offset %d (chunk %d)", offset, offset/align)
				t.Errorf("invalid symbol %d of %d", f.Sym, max)
			}
			return true
		})
	}
}

func TestChunkReadFrom(t *testing.T) {
	files := []string{
		"nyc-taxi.block",
		"parking.10n",
		"parking2.ion",
		"parking3.ion",
	}
	// test at flushing on these
	// multiples of the chunk size
	multiples := []int{
		1, 2, 3, 5, 8, 50,
	}
	for i := range files {
		f := filepath.Join("../testdata/", files[i])
		t.Run(files[i], func(t *testing.T) {
			for _, m := range multiples {
				t.Run(fmt.Sprintf("m=%d", m), func(t *testing.T) {
					fh, err := os.Open(f)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { fh.Close() })
					info, err := fh.Stat()
					if err != nil {
						t.Fatal(err)
					}
					size := info.Size()

					align := 2048
					if files[i] == "nyc-taxi.block" {
						align = 1024
					}
					var buf rangeBuf
					cn := ion.Chunker{
						Align:      align,
						RangeAlign: m * align,
						W:          &buf,
					}

					n, err := cn.ReadFrom(fh)
					if err != nil {
						t.Fatal(err)
					}
					if n != size {
						t.Errorf("returned %d bytes; file is %d bytes", n, size)
					}
					outsize := buf.Buffer.Len()
					if outsize%cn.Align != 0 {
						t.Errorf("outsize is %d?", outsize)
					}
					checkEncoding(t, &buf, cn.Align)
					checkRanges(t, &buf, cn.Align)
					checkChunkerWrite(t, &buf, &cn)
				})
			}
		})
	}
}

type rangeBuf struct {
	bytes.Buffer
	rangeWriter
	boundaries []int // # of chunks per metadata range
	current    int
}

func (r *rangeBuf) Flush() error {
	r.boundaries = append(r.boundaries, r.current)
	r.current = 0
	return r.rangeWriter.Flush()
}

func (r *rangeBuf) Write(p []byte) (int, error) {
	r.current++
	r.rangeWriter.Write(p)
	return r.Buffer.Write(p)
}

type ranges struct {
	path     []string
	min, max ion.Datum
}

// rangeWriter is an io.Writer that discards written
// bytes and exposes SetMinMax for range tracking.
type rangeWriter struct {
	ranges    []ranges // ranges for current chunk
	allRanges [][]ranges
}

func (w *rangeWriter) paths() [][]string {
	eql := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		if len(a) > 0 && &a[0] == &b[0] {
			return true
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	var out [][]string
	for i := range w.allRanges {
		rng := w.allRanges[i]
	rngloop:
		for j := range rng {
			for k := range out {
				if eql(out[k], rng[j].path) {
					continue rngloop
				}
			}
			out = append(out, rng[j].path)
		}
	}
	return out
}

func (w *rangeWriter) SetMinMax(path []string, min, max ion.Datum) {
	w.ranges = append(w.ranges, ranges{
		path: path,
		min:  min,
		max:  max,
	})
}

// Flush implements Flusher
func (w *rangeWriter) Flush() error {
	w.allRanges = append(w.allRanges, w.ranges)
	w.ranges = nil
	return nil
}

func (w *rangeWriter) Write(p []byte) (n int, err error) {
	if w.ranges != nil {
		return 0, fmt.Errorf("%d unflushed ranges", len(w.ranges))
	}
	return len(p), nil
}

// This can be used to generate a statically defined
// slice of ranges from the current state of the
// writer.
//lint:ignore U1000 kept for generating test fixtures
func dumpRanges(allRanges [][]ranges) {
	fmt.Println("expected := [][]ranges{{{")
	for i, rs := range allRanges {
		_ = rs
		if i > 0 {
			fmt.Println("}}, {{")
		}
		for i, r := range rs {
			if i > 0 {
				fmt.Println("}, {")
			}
			min, _ := json.Marshal(date.Time(r.min.(ion.Timestamp)))
			max, _ := json.Marshal(date.Time(r.max.(ion.Timestamp)))
			fmt.Printf("\tpath: %#v,\n", r.path)
			fmt.Printf("\tmin: timestamp(%s),\n", min)
			fmt.Printf("\tmax: timestamp(%s),\n", max)
		}
	}
	fmt.Println("}}}")
}

func mksymbuf(s ...ion.Symbol) ion.Symbuf {
	var b ion.Symbuf
	b.Prepare(len(s))
	for _, s := range s {
		b.Push(s)
	}
	return b
}

func checkRanges(t *testing.T, rn *rangeBuf, align int) {
	contents := rn.Bytes()
	chunks := 0
	for i := range rn.boundaries {
		if rn.boundaries[i] == 0 {
			t.Fatalf("block %d has 0 chunks?", rn.boundaries[i])
		}
		chunks += rn.boundaries[i]
	}
	n := 0
	var st ion.Symtab
	off := 0
	for i := range rn.boundaries {
		size := rn.boundaries[i] * align
		n += checkRange(t, &st, rn.allRanges[i], contents[off:off+size])
		off += size
	}
	t.Logf("check %d values in %d blocks", n, len(rn.allRanges))
}

func checkRange(t *testing.T, st *ion.Symtab, r []ranges, contents []byte) int {
	var dat ion.Datum
	var err error
	n := 0
	for len(contents) > 0 {
		dat, contents, err = ion.ReadDatum(st, contents)
		if err != nil {
			t.Fatal(err)
		}
		s, ok := dat.(*ion.Struct)
		if !ok {
			continue
		}
		n++
		s.Each(func(f ion.Field) bool {
			ts, ok := f.Value.(ion.Timestamp)
			if !ok {
				return true
			}
			found := false
			for j := range r {
				if len(r[j].path) == 1 && r[j].path[0] == f.Label {
					found = true
					min := r[j].min.(ion.Timestamp)
					max := r[j].max.(ion.Timestamp)
					if date.Time(ts).Before(date.Time(min)) {
						t.Errorf("value %s before min for block %s", date.Time(ts), date.Time(min))
					}
					if date.Time(ts).After(date.Time(max)) {
						t.Errorf("value %s after max for block %s", date.Time(ts), date.Time(max))
					}
					break
				}
			}
			if !found {
				for i := range r {
					t.Logf("have range for %s", strings.Join(r[i].path, "."))
				}
				t.Fatalf("no range entry for %s date %s (%d ranges)", f.Label, date.Time(ts), len(r))
			}
			return true
		})
	}
	return n
}

func results(t *testing.T, buf []byte) []*ion.Struct {
	var st ion.Symtab
	var dat ion.Datum
	var err error
	var out []*ion.Struct
	for len(buf) > 0 {
		dat, buf, err = ion.ReadDatum(&st, buf)
		if err != nil {
			t.Fatal(err)
		}
		s, ok := dat.(*ion.Struct)
		if ok {
			out = append(out, s)
		}
	}
	return out
}

func checkEquivalent(t *testing.T, left, right []byte) {
	gotleft := results(t, left)
	gotright := results(t, right)
	t.Helper()
	if len(gotleft) != len(gotright) {
		t.Fatalf("left has %d results, right has %d", len(gotleft), len(gotright))
	}
	for i := range gotleft {
		if !ion.Equal(gotleft[i], gotright[i]) {
			f0 := gotleft[i].Fields(nil)
			f1 := gotright[i].Fields(nil)
			for len(f0) > 0 &&
				len(f1) > 0 &&
				f0[0] == f1[0] {
				f0 = f0[1:]
				f1 = f1[1:]
			}
			t.Errorf("left %v", f0)
			t.Errorf("right %v", f1)
		}
	}
}

// transcoding an ion.Chunker through an ion.Chunker
// should produce semantically identical results
func checkChunkerWrite(t *testing.T, rng *rangeBuf, orig *ion.Chunker) {
	var out rangeBuf
	var cn ion.Chunker
	cn.Align = orig.Align
	cn.W = &out
	cn.RangeAlign = orig.RangeAlign
	cn.WalkTimeRanges = rng.paths()
	tmp := rng.Bytes()
	for len(tmp) > 0 {
		_, err := cn.Write(tmp[:orig.Align])
		if err != nil {
			t.Fatal(err)
		}
		tmp = tmp[orig.Align:]
	}
	err := cn.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// test the result is identical or semantically equivalent
	if !bytes.Equal(out.Bytes(), rng.Bytes()) {
		checkEquivalent(t, out.Bytes(), rng.Bytes())
	}
	// coarse check on # of ranges
	if len(out.allRanges[len(out.allRanges)-1]) != len(rng.allRanges[len(rng.allRanges)-1]) {
		t.Error("didn't get the same number of range entries in the final block?")
	}
	// check that the ranges copied over are valid
	checkRanges(t, &out, cn.Align)
}

func TestSyntheticRanges(t *testing.T) {
	template := `
{"foo": "2022-01-01T00:00:00Z"}
{"foo": "2022-01-02T00:00:00Z"}
{"bar": "2022-01-05T00:01:01Z"}
{"bar": "2022-01-05T00:03:05Z"}
{"foo": {"bar": "2021-01-03T01:02:03Z"}}
{"foo": {"bar": "2021-01-04T05:06:07Z"}}
{"a": {"b": {"quux": "2021-01-03T03:02:01Z"}}}
{"a": {"b": {"quux": "2021-01-03T03:02:05Z"}}}
`
	d := json.NewDecoder(strings.NewReader(template))
	u, _, err := versify.FromJSON(d)
	if err != nil {
		t.Fatal(err)
	}

	var ref rangeBuf
	var tmp bytes.Buffer
	cn := ion.Chunker{
		Align:      2048,
		RangeAlign: 4096,
		W:          &tmp,
	}

	src := rand.New(rand.NewSource(0))
	for i := 0; i < 10000; i++ {
		dat := u.Generate(src)
		dat.Encode(&cn.Buffer, &cn.Symbols)
		err := cn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
	err = cn.Flush()
	if err != nil {
		t.Fatal(err)
	}
	inter := tmp.Bytes()
	origAlign := cn.Align
	cn = ion.Chunker{
		// we're changing the alignment
		// and range multiple, so we have
		// to handle shifting boundaries correctly here
		Align:      3000,
		RangeAlign: 9000,
		W:          &ref,
		WalkTimeRanges: [][]string{
			{"foo"},
			{"bar"},
			{"foo", "bar"},
			{"a", "b", "quux"},
		},
	}
	// copy the data back into a chunker,
	// but this time we should have ranges
	for len(inter) > 0 {
		cn.Write(inter[:origAlign])
		inter = inter[origAlign:]
	}
	err = cn.Flush()
	if err != nil {
		t.Fatal(err)
	}

	var walkRange func([]ion.Field, []string, ion.Datum, ion.Datum)
	walkRange = func(lst []ion.Field, path []string, min, max ion.Datum) {
		for i := range lst {
			if path[0] != lst[i].Label {
				continue
			}
			if len(path) == 1 {
				ts, ok := lst[i].Value.(ion.Timestamp)
				if !ok {
					continue
				}
				min := min.(ion.Timestamp)
				max := max.(ion.Timestamp)
				if date.Time(ts).Before(date.Time(min)) || date.Time(ts).After(date.Time(max)) {
					t.Errorf("value %s %s out of range [%s, %s]", path[0], date.Time(ts), date.Time(min), date.Time(max))
				}
			} else if st, ok := lst[i].Value.(*ion.Struct); ok {
				walkRange(st.Fields(nil), path[1:], min, max)
			}
		}
		if t.Failed() {
			t.Logf("failure in struct %v", lst)
		}
	}

	checkRange := func(st *ion.Symtab, ranges []ranges, mem []byte) {
		var err error
		var dat ion.Datum
		for len(mem) > 0 {
			dat, mem, err = ion.ReadDatum(st, mem)
			if err != nil {
				t.Fatal(err)
			}
			s, ok := dat.(*ion.Struct)
			if !ok {
				continue
			}
			for i := range ranges {
				walkRange(s.Fields(nil), ranges[i].path, ranges[i].min, ranges[i].max)
				if t.Failed() {
					t.Logf("failed @ %v", ranges[i].path)
					return
				}
			}
		}
	}

	contents := ref.Bytes()
	chunks := 0
	for i := range ref.boundaries {
		if ref.boundaries[i] == 0 {
			t.Fatalf("block %d has 0 chunks?", ref.boundaries[i])
		}
		chunks += ref.boundaries[i]
	}
	t.Logf("%d chunks", chunks)
	if chunks != len(contents)/cn.Align {
		t.Fatalf("%d chunks via boundaries, but calculated %d", chunks, len(contents)/cn.Align)
	}
	var st ion.Symtab
	off := 0
	for i := range ref.boundaries {
		size := ref.boundaries[i] * cn.Align
		checkRange(&st, ref.allRanges[i], contents[off:off+size])
		if t.Failed() {
			for i := 10; i < st.MaxID(); i++ {
				t.Logf("symbol %d %s", i, st.Get(ion.Symbol(i)))
			}
			t.Logf("failed @ chunk %d", i)
			break
		}
		off += size
	}
}

func TestChunkerChangingSymbols(t *testing.T) {
	var block0, block1 []byte

	// deterministic structure for row n
	forRow := func(st *ion.Symtab, n int) *ion.Struct {
		return ion.NewStruct(st,
			[]ion.Field{
				{
					Label: "row",
					Value: ion.Uint(n),
				},
				{
					Label: "foo",
					Value: ion.String("value"),
				},
				{
					Label: "bar",
					Value: ion.Uint(0),
				},
				{
					Label: "xyz",
					Value: ion.NewList(nil, []ion.Datum{
						ion.Int(-1), ion.Uint(1),
					}),
				},
				{
					Label: "quux",
					Value: ion.UntypedNull{},
				},
				{
					Label: "timestamp",
					Value: ion.Timestamp(date.Unix(int64(n), 0)),
				},
			},
		)
	}

	recs := []int{
		100, 247, 1000,
	}
	aligns := []int{
		1024, 2048, 3000,
	}
	multiples := []int{
		1, 5, 7,
	}
	testit := func(t *testing.T, records, align, multiple int) {

		var tmp ion.Buffer
		var st ion.Symtab
		for i := 0; i < records; i++ {
			forRow(&st, i).Encode(&tmp, &st)
		}
		off := tmp.Size()
		st.Marshal(&tmp, true)

		block0 = append(tmp.Bytes()[off:], tmp.Bytes()[:off]...)

		st.Reset()
		tmp.Set(nil)
		for i := 0; i < records; i++ {
			rec := forRow(&st, i+records)
			if i == 0 {
				// shift the order in which the symbols
				// are interned; subsequent Encode operations
				// will re-sort the fields to match the order
				// that the first struct ends up in
				fields := rec.Fields(nil)
				rand.Shuffle(len(fields), func(i, j int) {
					fields[i], fields[j] = fields[j], fields[i]
				})
				rec = ion.NewStruct(&st, fields)
			}
			rec.Encode(&tmp, &st)
		}
		off = tmp.Size()
		st.Marshal(&tmp, true)

		block1 = append(tmp.Bytes()[off:], tmp.Bytes()[:off]...)

		var out rangeBuf
		cn := ion.Chunker{
			W:              &out,
			Align:          align,
			RangeAlign:     multiple * align,
			WalkTimeRanges: [][]string{{"timestamp"}},
		}
		_, err := cn.Write(block0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = cn.Write(block1)
		if err != nil {
			t.Fatal(err)
		}

		got := out.Bytes()
		checkRanges(t, &out, cn.Align)

		n := 0
		var outst ion.Symtab
		var dat ion.Datum
		for len(got) > 0 {
			dat, got, err = ion.ReadDatum(&outst, got)
			if err != nil {
				t.Fatal(err)
			}
			s, ok := dat.(*ion.Struct)
			if !ok {
				if _, ok := dat.(ion.UntypedNull); !ok {
					t.Error("got non-null pad datum?")
				}
				continue
			}
			want := forRow(&outst, n)
			n++

			if !ion.Equal(s, want) {
				t.Errorf("first: %#v", s)
				t.Errorf("want : %#v", want)
				t.Fatal("not equal")
			}
		}

	}

	for _, records := range recs {
		for _, align := range aligns {
			for _, multiple := range multiples {
				t.Run(fmt.Sprintf("n=%d/a=%d/m=%d", records, align, multiple), func(t *testing.T) {
					testit(t, records, align, multiple)
				})
			}
		}
	}
}

func BenchmarkChunkerWrite(b *testing.B) {
	files := []string{
		"cloudtrail.json",
	}

	for _, f := range files {
		b.Run(f, func(b *testing.B) {
			f, err := os.Open(filepath.Join("..", "testdata", f))
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			const align = 1024 * 1024
			var tmp bytes.Buffer
			cn := ion.Chunker{
				W:     &tmp,
				Align: align,
			}
			err = jsonrl.Convert(f, &cn, nil)
			if err != nil {
				b.Fatal(err)
			}
			err = cn.Flush()
			if err != nil {
				b.Fatal(err)
			}
			chunk := tmp.Bytes()
			var syms ion.Symtab
			fastchunk, err := syms.Unmarshal(chunk)
			if err != nil {
				b.Fatal(err)
			}
			cn2 := ion.Chunker{
				W:              ioutil.Discard,
				Align:          cn.Align,
				RangeAlign:     100 * cn.Align,
				WalkTimeRanges: [][]string{{"eventTime"}},
			}
			b.SetBytes(int64(len(fastchunk)))
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				mem := fastchunk
				if i%100 == 0 {
					mem = chunk
				}
				_, err = cn2.Write(mem)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
