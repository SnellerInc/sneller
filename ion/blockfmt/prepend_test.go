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

package blockfmt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestPrependMultiSingle(t *testing.T) {
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
				F: SuffixToFormat[".json"](),
			})
		}
		return in
	}
	var out BufferUploader
	align := 2048
	out.PartSize = align * 7
	c := Converter{
		Output:    &out,
		Comp:      "zstd",
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
		Comp:      "zstd",
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
			t.Run(fmt.Sprintf("%s/m=%d", file, m), func(t *testing.T) {
				fp := "../../testdata/" + file
				f, err := os.Open(fp)
				if err != nil {
					t.Fatal(err)
				}
				inputs := []Input{{
					R: f,
					F: SuffixToFormat[".json"](),
				}}
				var out BufferUploader
				align := 2048
				out.PartSize = align * m
				c := Converter{
					Output:    &out,
					Comp:      "zstd",
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
					F: SuffixToFormat[".json"](),
				}}
				var out2 BufferUploader
				c = Converter{
					Output:    &out2,
					Comp:      "zstd",
					Inputs:    inputs,
					Align:     4096, // changing alignment
					FlushMeta: m * 4096,
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
				F: SuffixToFormat[".json"](),
			})
		}
		return in
	}

	for _, m := range multiples {
		t.Run(fmt.Sprintf("m=%d", m), func(t *testing.T) {
			var out BufferUploader
			align := 2048
			out.PartSize = align * m
			c := Converter{
				Output:    &out,
				Comp:      "zstd",
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
			c = Converter{
				Output:    &out2,
				Comp:      "zstd",
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
