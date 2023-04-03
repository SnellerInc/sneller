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
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/SnellerInc/sneller/tests"
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

// run a sub-test for each testdata file
func runTestdata[T testing.TB](t T, inner func(T, string, []byte)) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	for i := range entries {
		if entries[i].IsDir() {
			continue
		}
		buf, err := fetchTestData(filepath.Join("testdata", entries[i].Name()))
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		inner(t, entries[i].Name(), buf)
	}
}

func TestRoundtrip(t *testing.T) {
	runTestdata(t, func(t *testing.T, name string, buf []byte) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testRoundtrip(t, buf)
		})
	})
}

func testRoundtrip(t *testing.T, src []byte) {
	srcLen := len(src)
	t.Logf("srcLen = %d\n", srcLen)

	var dec Decoder
	var enc Encoder
	dst, err := enc.Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		t.Fatal(err)
	}

	// test that encoder state is reset correctly
	dst2, err := enc.Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dst, dst2) {
		t.Fatal("second Compress not equivalent?")
	}

	dstLen := len(dst)
	t.Logf("comprLen = %d\n", dstLen)
	compressionRatio := 100.0 * (1.0 - float64(dstLen)/float64(srcLen))
	t.Logf("compressed by = %f%%\n", compressionRatio)
	out := make([]byte, 0, 1024*1024)
	out, err = dec.DecompressTo(out, dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, out) {
		// print the diff of the hexdumps
		delta, ok := tests.Diff(hex.Dump(src), hex.Dump(out))
		if ok {
			t.Log(delta)
		}
		t.Fatal("round-trip encoding+decoding failed")
	}
}

func FuzzRoundTrip(f *testing.F) {
	runTestdata(f, func(f *testing.F, _ string, buf []byte) {
		f.Add(buf)
	})
	f.Fuzz(func(t *testing.T, ref []byte) {
		var dec Decoder
		var enc Encoder
		compressed, err := enc.Compress(ref, nil, DefaultANSThreshold)
		if err != nil {
			return // when would this fail?
		}
		decompressed, err := dec.Decompress(compressed)
		if err != nil {
			t.Fatalf("round-trip failed: %s", err)
		}
		if !bytes.Equal(ref, decompressed) {
			t.Fatal("round trip result is not equal to the input")
		}
		if len(ref) == 0 {
			return
		}
		ref = ref[:len(ref)-1]
		compressed, err = enc.Compress(ref, nil, DefaultANSThreshold)
		if err != nil {
			return // when would this fail?
		}
		decompressed, err = dec.Decompress(compressed)
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
	var enc Encoder
	dst, err := enc.Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		b.Fatal(err)
	}
	var dec Decoder
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src, err = dec.DecompressTo(src[:0], dst)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompress(b *testing.B) {
	src, err := fetchTestData("testdata/ref.bin.gz")
	if err != nil {
		b.Fatal(err)
	}
	var enc Encoder
	dst, err := enc.Compress(src, nil, DefaultANSThreshold)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = enc.Compress(src, dst[:0], DefaultANSThreshold)
		if err != nil {
			b.Fatal(err)
		}
	}
}
