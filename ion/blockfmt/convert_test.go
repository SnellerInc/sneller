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
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func haveParquet2JSON() bool {
	_, err := exec.LookPath("parquet2json")
	return err == nil
}

func testConvertMulti(t *testing.T, algo string, meta int) {
	var inputs []Input
	f, err := os.Open("../../testdata/cloudtrail.json")
	if err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, Input{
		R: f,
		F: MustSuffixToFormat(".json"),
	})
	f, err = os.Open("../../testdata/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, Input{
		R: io.NopCloser(gzipped(f)),
		F: MustSuffixToFormat(".json.gz"),
	})
	f, err = os.Open("../../testdata/parking3.json")
	if err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, Input{
		R: f,
		F: MustSuffixToFormat(".json"),
	})
	if haveParquet2JSON() {
		f, err = os.Open("../../testdata/userdata1.parquet")
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, Input{
			R: f,
			F: MustSuffixToFormat(".parquet"),
		})
	}

	var out BufferUploader
	align := 4096
	out.PartSize = 2 * align
	c := Converter{
		Output:    &out,
		Comp:      algo,
		Inputs:    inputs,
		Align:     align,
		FlushMeta: align * meta,
		Parallel:  2, // 3 inputs + 2 parellelism enables prefetching
	}
	if !c.MultiStream() {
		t.Fatal("expected MultiStream to be true with 2 inputs")
	}
	err = c.Run()
	if err != nil {
		t.Fatal(err)
	}
	if algo != "zion" && c.Trailer().Algo != algo {
		t.Errorf("trailer algo is %q ???", c.Trailer().Algo)
	}
	check(t, &out)
}

func TestConvertMulti(t *testing.T) {
	multiples := []int{
		1, 3, 5, 7, 10,
	}
	algos := []string{"zstd", "zion", "zion+iguana_v0/specialized"}
	for _, algo := range algos {
		for _, m := range multiples {
			t.Run(fmt.Sprintf("algo=%s,m=%d", algo, m), func(t *testing.T) {
				testConvertMulti(t, algo, m)
			})
		}
	}
}
func TestConvertMultiFail(t *testing.T) {
	var inputs []Input

	// first, populate with some good JSON data
	f, err := os.Open("../../testdata/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, Input{
		R: io.NopCloser(gzipped(f)),
		F: MustSuffixToFormat(".json.gz"),
	})
	f, err = os.Open("../../testdata/parking3.json")
	if err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, Input{
		R: f,
		F: MustSuffixToFormat(".json"),
	})

	// now, populate with some bad JSON data
	inputs = append(inputs, Input{
		R: io.NopCloser(strings.NewReader("{\"unterminated\": true")),
		F: MustSuffixToFormat(".json"),
	})

	var out BufferUploader
	out.PartSize = 4096
	c := Converter{
		Output: &out,
		Comp:   "zstd",
		Inputs: inputs,
		Align:  4096,
	}
	if !c.MultiStream() {
		t.Fatal("expected MultiStream to be true with 2 inputs")
	}
	err = c.Run()
	if err == nil {
		t.Fatal("no error?")
	}
	if !IsFatal(c.Inputs[len(c.Inputs)-1].Err) {
		t.Error("expected the last input to have a fatal Err set")
	}
}

func TestConvertSingle(t *testing.T) {
	multiples := []int{
		1, 3, 7, 50,
	}
	formats := []string{
		"zstd", "zion", "zion+iguana_v0/specialized",
	}
	for _, algo := range formats {
		for _, m := range multiples {
			t.Run(fmt.Sprintf("alg=%s,m=%d", algo, m), func(t *testing.T) {
				f, err := os.Open("../../testdata/parking2.json")
				if err != nil {
					t.Fatal(err)
				}
				inputs := []Input{{
					R: io.NopCloser(gzipped(f)),
					F: MustSuffixToFormat(".json.gz"),
				}}
				var out BufferUploader
				align := 2048
				out.PartSize = align * m
				c := Converter{
					Output:    &out,
					Comp:      algo,
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
				if algo != "zion" && c.Trailer().Algo != algo {
					t.Errorf("trailer algo is %q ???", c.Trailer().Algo)
				}
				check(t, &out)
			})
		}
	}
}

func TestConvertEmpty(t *testing.T) {
	inputs := []Input{{
		R: io.NopCloser(strings.NewReader("")),
		F: MustSuffixToFormat(".json"),
	}}
	var out BufferUploader
	out.PartSize = 4096
	c := Converter{
		Output: &out,
		Comp:   "zstd",
		Inputs: inputs,
		Align:  4096,
	}
	if c.MultiStream() {
		t.Fatal("expected MultiStream to be false with 1 input")
	}
	err := c.Run()
	if err != nil {
		t.Fatal(err)
	}
	check(t, &out)
}

// the error produced by trying to convert
// an empty *.gz file should be fatal
func TestConvertEmptyGZ(t *testing.T) {
	inputs := []Input{{
		R: io.NopCloser(strings.NewReader("")),
		F: MustSuffixToFormat(".json.gz"),
	}}
	var out BufferUploader
	out.PartSize = 4096
	c := Converter{
		Output: &out,
		Comp:   "zstd",
		Inputs: inputs,
		Align:  4096,
	}
	err := c.Run()
	if !IsFatal(err) {
		t.Fatalf("error %T is not fatal", err)
	}
}

func gzipped(r io.ReadCloser) io.Reader {
	rp, wp := io.Pipe()
	go func() {
		gw := gzip.NewWriter(wp)
		_, err := io.Copy(gw, r)
		err2 := gw.Close()
		if err == nil {
			err = err2
		}
		if err != nil {
			wp.CloseWithError(err)
			return
		}
		wp.Close()
	}()
	return rp
}

func check(t *testing.T, buf *BufferUploader) int {
	r := bytes.NewReader(buf.Bytes())
	trailer, err := ReadTrailer(r, r.Size())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	var errlog bytes.Buffer
	n := Validate(r, trailer, &errlog)
	if errlog.Len() > 0 {
		t.Helper()
		errlog.Truncate(4096) // don't fill the screen
		t.Fatal(errlog.String())
	}
	return n
}
