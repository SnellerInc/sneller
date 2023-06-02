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
	"github.com/klauspost/compress/zstd"
)

var enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))

func zstdCompress(buf []byte) []byte {
	return enc.EncodeAll(buf, nil)
}

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
	// try a bunch of short-length strings
	buf := []byte(`this is a short string that we will re-slice for small test-cases`)
	for len(buf) < minOffset*3 {
		buf = append(buf, buf...)
	}
	t.Run("short-strings", func(t *testing.T) {
		for i := range buf {
			testRoundtrip(t, buf[i:])
		}
	})
	buf = bytes.Repeat([]byte{'a'}, 3*minOffset)
	t.Run("short-repeats", func(t *testing.T) {
		for i := range buf {
			testRoundtrip(t, buf[i:])
		}
	})
}

func testRoundtrip(t *testing.T, src []byte) {
	srcLen := len(src)
	t.Logf("srcLen = %d\n", srcLen)

	var dec Decoder
	var enc Encoder
	dst, err := enc.Compress(src, nil, DefaultEntropyRejectionThreshold)
	if err != nil {
		t.Fatal(err)
	}

	// test that encoder state is reset correctly
	dst2, err := enc.Compress(src, nil, DefaultEntropyRejectionThreshold)
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

	zst := zstdCompress(src)
	t.Logf("zstd: %d -> %d", len(src), len(zst))

	// provide a buffer that is perfectly-sized
	// so we can see if there are any oob writes
	out := make([]byte, len(src), len(src)+minLength)
	ret, err := dec.DecompressTo(out[:0:len(src)], dst)
	if err != nil {
		t.Fatal(err)
	}
	tail := out[len(out):cap(out)]
	for i := range tail {
		if tail[i] != 0 {
			t.Logf("%x", tail)
			t.Fatal("wrote garbage to the end of the buffer?")
		}
	}
	if !bytes.Equal(src, ret) {
		// print the diff of the hexdumps
		delta, ok := tests.Diff(hex.Dump(src), hex.Dump(ret))
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
		compressed, err := enc.Compress(ref, nil, DefaultEntropyRejectionThreshold)
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
		compressed, err = enc.Compress(ref, nil, DefaultEntropyRejectionThreshold)
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
	dst, err := enc.Compress(src, nil, DefaultEntropyRejectionThreshold)
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

func BenchmarkTestdata(b *testing.B) {
	b.Run("decompress", func(b *testing.B) {
		runTestdata(b, func(b *testing.B, name string, src []byte) {
			b.Run(name, func(b *testing.B) {
				var enc Encoder
				var dec Decoder
				dst, err := enc.Compress(src, nil, DefaultEntropyRejectionThreshold)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(src)))
				b.ResetTimer()
				var tmp []byte
				for i := 0; i < b.N; i++ {
					tmp, err = dec.DecompressTo(tmp[:0], dst)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	})
	b.Run("compress", func(b *testing.B) {
		runTestdata(b, func(b *testing.B, name string, src []byte) {
			b.Run(name, func(b *testing.B) {
				var enc Encoder
				dst, err := enc.Compress(src, nil, DefaultEntropyRejectionThreshold)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(src)))
				b.ResetTimer()
				b.ReportMetric(float64(len(src)), "input-bytes")
				b.ReportMetric(float64(len(dst)), "output-bytes")
				for i := 0; i < b.N; i++ {
					dst, err = enc.Compress(src, dst[:0], DefaultEntropyRejectionThreshold)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	})
}
