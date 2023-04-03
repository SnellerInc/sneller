// Copyright (C) 2023 Sneller, Inc.
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

package iguana

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"testing"

	"golang.org/x/exp/slices"
)

func fetchTestData(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer fz.Close()

	s, err := io.ReadAll(fz)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func TestIguana(t *testing.T) {
	src, err := fetchTestData("testdata/ref.bin.gz")
	if err != nil {
		t.Fatal(err)
	}

	srcLen := len(src)
	t.Logf("srcLen = %d\n", srcLen)

	dst, err := Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		t.Fatal(err)
	}
	dstLen := len(dst)
	t.Logf("comprLen = %d\n", dstLen)
	compressionRatio := 100.0 * (1.0 - float64(dstLen)/float64(srcLen))
	t.Logf("compressed by = %f%%\n", compressionRatio)
	out := make([]byte, 0, 1024*1024)
	out, err = DecompressTo(out, dst)
	if err != nil {
		t.Fatal(err)
	}
	outLen := len(out)

	if srcLen != outLen {
		t.Fatalf("length mismatch: outLen = %d, srcLen = %d\n", outLen, srcLen)
	} else {
		if !slices.Equal(src, out) {
			t.Errorf("content mismatch\n")
			for i := 0; i != srcLen; i++ {
				vs := src[i]
				vo := out[i]

				if vs != vo {
					t.Fatalf("data mismatch at offset %d: vsrc = %02x, vout = %02x\n", i, vs, vo)
				}
			}
		} else {
			t.Logf("OK!\n")
		}
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Fuzz(func(t *testing.T, ref []byte) {
		compressed, err := Compress(ref, nil, DefaultANSThreshold)
		if err != nil {
			return // when would this fail?
		}
		decompressed, err := Decompress(compressed)
		if err != nil {
			t.Fatalf("round-trip failed: %s", err)
		}
		if !bytes.Equal(ref, decompressed) {
			t.Fatal("round trip result is not equal to the input")
		}
	})
}

func BenchmarkRef(b *testing.B) {
	src, err := fetchTestData("testdata/ref.bin.gz")
	if err != nil {
		b.Fatal(err)
	}
	dst, err := Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src, err = DecompressTo(src[:0], dst)
		if err != nil {
			b.Fatal(err)
		}
	}
}
