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

package blockfmt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

func TestPrependMultiSingle(t *testing.T) {
	algos := []string{"zstd", "zion"}
	for _, algo := range algos {
		t.Run(algo, func(t *testing.T) {
			testPrependMultiSingle(t, algo)
		})
	}
}

func testPrependMultiSingle(t *testing.T, algo string) {
	// this is a nasty test
	// because we have two files
	// with disjoint sets of struct fields
	files := []string{
		"parking2.json",
		"cloudtrail.json",
	}
	inputs := func() []Input {
		var in []Input
		for _, file := range files {
			fp := "../../testdata/" + file
			f, err := os.Open(fp)
			if err != nil {
				t.Fatal(err)
			}
			in = append(in, Input{
				R: f,
				F: MustSuffixToFormat(".json"),
			})
		}
		return in
	}
	var out BufferUploader
	align := 2048
	out.PartSize = align * 7
	c := Converter{
		Output:    &out,
		Comp:      algo,
		Inputs:    inputs(),
		Align:     align,
		FlushMeta: 7 * align,
		Parallel:  1,
	}
	if c.MultiStream() {
		t.Fatal("expected MultiStream to be false with Parallel=1")
	}
	err := c.Run()
	if err != nil {
		t.Fatal(err)
	}
	count := check(t, &out)

	// now do it again, but
	// prepend the output
	br := bytes.NewReader(out.Bytes())
	tr, err := ReadTrailer(br, br.Size())
	if err != nil {
		t.Fatal(err)
	}
	var out2 BufferUploader
	c = Converter{
		Output:    &out2,
		Comp:      algo,
		Inputs:    inputs(),
		Align:     align,
		FlushMeta: 7 * align,
		Parallel:  1,
	}
	c.Prepend.R = io.NopCloser(io.LimitReader(br, tr.Offset))
	c.Prepend.Trailer = tr
	err = c.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("original size: %d", len(out.Bytes()))
	t.Logf("doubled size: %d", len(out2.Bytes()))
	// should have doubled the number of objects
	count2 := check(t, &out2)
	if count2 != count*2 {
		t.Errorf("went from %d to %d objects?", count, count2)
	}
}

func TestPrependSingle(t *testing.T) {
	multiples := []int{
		1, 3, 7, 50,
	}
	files := []string{
		"parking2.json",
		"cloudtrail.json",
	}
	for _, m := range multiples {
		for _, file := range files {
			m := m
			file := file
			t.Run(fmt.Sprintf("%s/m=%d", file, m), func(t *testing.T) {
				fp := "../../testdata/" + file
				f, err := os.Open(fp)
				if err != nil {
					t.Fatal(err)
				}
				inputs := []Input{{
					R: f,
					F: MustSuffixToFormat(".json"),
				}}
				var out BufferUploader
				align := 2048
				out.PartSize = align * m
				c := Converter{
					Output:    &out,
					Comp:      "zion",
					Inputs:    inputs,
					Align:     align,
					FlushMeta: m * align,
				}
				if c.MultiStream() {
					t.Fatal("expected MultiStream to be false with 1 input")
				}
				err = c.Run()
				if err != nil {
					t.Fatal(err)
				}
				count := check(t, &out)

				// now do it again, but
				// prepend the output
				br := bytes.NewReader(out.Bytes())
				tr, err := ReadTrailer(br, br.Size())
				if err != nil {
					t.Fatal(err)
				}
				f, err = os.Open(fp)
				if err != nil {
					t.Fatal(err)
				}
				inputs = []Input{{
					R: f,
					F: MustSuffixToFormat(".json"),
				}}
				var out2 BufferUploader
				c = Converter{
					Output: &out2,
					Comp:   "zion",
					Inputs: inputs,
					// Align:     4096, // changing alignment
					// FlushMeta: m * 4096,
					Align:      align,
					FlushMeta:  m * align,
					TargetSize: 2 * align,
				}
				c.Prepend.R = io.NopCloser(io.LimitReader(br, tr.Offset))
				c.Prepend.Trailer = tr
				err = c.Run()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("original size: %d", len(out.Bytes()))
				t.Logf("doubled size: %d", len(out2.Bytes()))
				// should have doubled the number of objects
				count2 := check(t, &out2)
				if count2 != count*2 {
					t.Errorf("went from %d to %d objects?", count, count2)
				}
			})
		}
	}
}

func TestPrependMulti(t *testing.T) {
	multiples := []int{
		1, 3, 7, 50,
	}
	algos := []string{
		"zstd", "zion",
	}
	files := []string{
		"parking2.json",
		"cloudtrail.json",
	}
	inputs := func() []Input {
		var in []Input
		for _, file := range files {
			fp := "../../testdata/" + file
			f, err := os.Open(fp)
			if err != nil {
				t.Fatal(err)
			}
			in = append(in, Input{
				R: f,
				F: MustSuffixToFormat(".json"),
			})
		}
		return in
	}
	for _, algo := range algos {
		for _, m := range multiples {
			m := m
			algo := algo
			t.Run(fmt.Sprintf("%s/%d", algo, m), func(t *testing.T) {
				t.Parallel()
				var out BufferUploader
				align := 2048
				out.PartSize = align * m
				c := Converter{
					Output:    &out,
					Comp:      algo,
					Inputs:    inputs(),
					Align:     align,
					FlushMeta: m * align,
				}
				if !c.MultiStream() {
					t.Fatal("expected MultiStream to be true")
				}
				err := c.Run()
				if err != nil {
					t.Fatal(err)
				}
				count := check(t, &out)

				// now do it again, but
				// prepend the output
				br := bytes.NewReader(out.Bytes())
				tr, err := ReadTrailer(br, br.Size())
				if err != nil {
					t.Fatal(err)
				}
				var out2 BufferUploader
				out2.PartSize = m * align
				c = Converter{
					Output:    &out2,
					Comp:      algo,
					Inputs:    inputs(),
					Align:     align,
					FlushMeta: m * align * 2, // change metadata interval
				}
				c.Prepend.R = io.NopCloser(io.LimitReader(br, tr.Offset))
				c.Prepend.Trailer = tr
				err = c.Run()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("original size: %d", len(out.Bytes()))
				t.Logf("doubled size: %d", len(out2.Bytes()))
				// should have doubled the number of objects
				count2 := check(t, &out2)
				if count2 != count*2 {
					t.Errorf("went from %d to %d objects?", count, count2)
				}
			})
		}
	}
}

func TestPrependGenerated(t *testing.T) {
	t.Parallel() // kind of a slow test
	var rn int
	start := time.Now()

	rows := func(n int, dst *bytes.Buffer) {
		for n > 0 {
			cur := start.Add(time.Duration(rn) * time.Second)
			fmt.Fprintf(dst, `{"timestamp": "%s", "rownum": %d}`,
				cur.Format(time.RFC3339Nano), rn)
			rn++
			n--
		}
	}

	var jsbuf bytes.Buffer
	var trailer *Trailer
	var prepend io.ReadCloser
	for ins := 0; ins < 200; ins++ {
		var obuf BufferUploader
		jsbuf.Reset()
		rows(1000, &jsbuf)
		conv := Converter{
			Comp: "zion",
			Inputs: []Input{{
				Path: fmt.Sprintf("mem://iter%d", ins),
				ETag: fmt.Sprintf("iter-%d-etag", ins),
				R:    io.NopCloser(bytes.NewReader(jsbuf.Bytes())),
				F:    MustSuffixToFormat(".json"),
			}},
			Output:     &obuf,
			Align:      16 * 1024,
			FlushMeta:  10 * 16 * 1024,
			TargetSize: 64 * 1024,
		}
		if trailer != nil {
			conv.Prepend.R = prepend
			conv.Prepend.Trailer = trailer
		}
		err := conv.Run()
		if err != nil {
			t.Fatal(err)
		}

		n := check(t, &obuf)
		if n != (ins+1)*1000 {
			t.Fatalf("%d instead of %d records", n, (ins+1)*1000)
		}
		trailer = conv.Trailer()
		ti := trailer.Sparse.Get([]string{"timestamp"})
		if ti == nil {
			t.Fatal("didn't index timestamp?")
		}
		// records are inserted monotonically,
		// so we should have 100% precise block boundaries:
		if nb := len(trailer.Blocks); ti.StartIntervals() != nb || ti.EndIntervals() != nb {
			t.Fatalf("%d start intervals, %d end intervals; %d blocks", ti.StartIntervals(), ti.EndIntervals(), nb)
		}
		prepend = io.NopCloser(bytes.NewReader(obuf.Bytes()))
	}
}
