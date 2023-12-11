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

package xsv

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

// convert 100 lines of CSV data (69KiB) using a flat output
// that can be emitted directly.
// BenchmarkConvertFlatCSV-16             4334            273827 ns/op         1008018 B/op       2565 allocs/op
func BenchmarkConvertFlatCSV(b *testing.B) {
	benchmarkConvertCSV(b, "bench.csv", "bench1-hints.json")
}

// convert 100 lines of CSV data (69KiB) using more subfields
// that need intermediate parsing.
//
// BenchmarkConvertSubObjectsCSV-16       3998            277794 ns/op          957892 B/op       4540 allocs/op

func BenchmarkConvertSubObjectsCSV(b *testing.B) {
	benchmarkConvertCSV(b, "bench.csv", "bench2-hints.json")
}

func benchmarkConvertCSV(b *testing.B, tsvFile, hintsFile string) {
	// read file into memory to prevent I/O and
	// measure the raw conversion performance.
	f, err := os.ReadFile(testFolder + "/" + tsvFile)
	if err != nil {
		b.Fatalf("cannot open %q: %s", tsvFile, err)
	}

	hf, err := os.ReadFile(testFolder + "/" + hintsFile)
	if err != nil {
		b.Fatalf("cannot read %q: %s", hintsFile, err)
	}
	h, err := ParseHint(hf)
	if err != nil {
		b.Fatalf("cannot parse hints in %q: %s", hintsFile, err)
	}

	dst := ion.Chunker{Align: alignment, W: io.Discard}
	ch := CsvChopper{SkipRecords: h.SkipRecords, Separator: h.Separator}

	b.SetBytes(int64(len(f)))

	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(f)
		err := Convert(r, &dst, &ch, h, nil)
		if err != nil {
			b.Fatalf("cannot convert: %s", err)
		}
	}
}

func TestConvertCSV(t *testing.T) {
	csvFiles, err := fs.Glob(os.DirFS(testFolder), "test*.csv")
	if err != nil {
		t.Fatalf("can't access list files in folder: %v", err)
	}
	for _, csvFile := range csvFiles {
		t.Run(csvFile, func(t *testing.T) {
			base := testFolder + "/" + strings.TrimSuffix(csvFile, filepath.Ext(csvFile))
			hintsFile := base + "-hints.json"
			hf, err := os.ReadFile(hintsFile)
			if err != nil {
				t.Fatalf("cannot read %q: %s", hintsFile, err)
			}
			h, err := ParseHint(hf)
			if err != nil {
				t.Fatalf("cannot parse hints in %q: %s", hintsFile, err)
			}

			ch := CsvChopper{SkipRecords: h.SkipRecords, Separator: h.Separator}
			testConvert(t, csvFile, &ch, h)
		})
	}
}
