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

// convert 100 lines of TSV data (69KiB) using a flat output
// that can be emitted directly.
// BenchmarkConvertFlatTSV-16 (old)       3162            323494 ns/op         1259678 B/op        176 allocs/op
// BenchmarkConvertFlatTSV-16             4334            273827 ns/op         1008018 B/op       2565 allocs/op
func BenchmarkConvertFlatTSV(b *testing.B) {
	benchmarkConvertTSV(b, "bench.tsv", "bench1-hints.json")
}

// convert 100 lines of TSV data (69KiB) using more subfields
// that need intermediate parsing.
//
// BenchmarkConvertSubObjectsTSV-16 (old) 2564            447679 ns/op         1008013 B/op       2155 allocs/op
// BenchmarkConvertSubObjectsTSV-16       3998            277794 ns/op          957892 B/op       4540 allocs/op

func BenchmarkConvertSubObjectsTSV(b *testing.B) {
	benchmarkConvertTSV(b, "bench.tsv", "bench2-hints.json")
}

func benchmarkConvertTSV(b *testing.B, tsvFile, hintsFile string) {
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
	ch := TsvChopper{SkipRecords: h.SkipRecords}

	b.SetBytes(int64(len(f)))

	for n := 0; n < b.N; n++ {
		r := bytes.NewReader(f)
		err := Convert(r, &dst, &ch, h)
		if err != nil {
			b.Fatalf("cannot convert: %s", err)
		}
	}
}

func TestConvertTSV(t *testing.T) {
	tsvFiles, err := fs.Glob(os.DirFS(testFolder), "test*.tsv")
	if err != nil {
		t.Fatalf("can't access list files in folder: %v", err)
	}
	for _, tsvFile := range tsvFiles {
		t.Run(tsvFile, func(t *testing.T) {
			base := testFolder + "/" + strings.TrimSuffix(tsvFile, filepath.Ext(tsvFile))
			hintsFile := base + "-hints.json"
			hf, err := os.ReadFile(hintsFile)
			if err != nil {
				t.Fatalf("cannot read %q: %s", hintsFile, err)
			}
			h, err := ParseHint(hf)
			if err != nil {
				t.Fatalf("cannot parse hints in %q: %s", hintsFile, err)
			}

			ch := TsvChopper{SkipRecords: h.SkipRecords}
			testConvert(t, tsvFile, &ch, h)
		})
	}
}

func FuzzTSV(f *testing.F) {
	f.Add("2022-06-01 21:04:04	2022-06-01 21:04:15	dev-generated-netflow	1143993974	2502817533	31065	58485	6	4873982	9399852	3250	6267	0	65535	65535	PostGresClient	PostGresServer			2022/06/01T21:00:00Z	2251	875	awsv2:000111222333:us-east-2:vpc-54312981872898173	0000:0000:0000:0000:0000:0000:442f:f676	0000:0000:0000:0000:0000:0000:952d:f6fd")
	f.Fuzz(func(t *testing.T, input string) {
		hintsFile := testFolder + "/fuzz-hints.json"
		hf, err := os.ReadFile(hintsFile)
		if err != nil {
			t.Fatalf("cannot read %q: %s", hintsFile, err)
		}
		h, err := ParseHint(hf)
		if err != nil {
			t.Fatalf("cannot parse hints in %q: %s", hintsFile, err)
		}

		f := strings.NewReader(input)
		dst := ion.Chunker{Align: alignment, W: io.Discard}
		ch := TsvChopper{SkipRecords: h.SkipRecords}

		Convert(f, &dst, &ch, h)
	})
}
