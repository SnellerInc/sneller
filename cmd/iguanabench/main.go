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

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/SnellerInc/sneller/ion/zion/iguana"
)

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}

const iguanaWindowSize = 256 * 1024

func iguanaCompress(src []byte, threshold float64) []byte {
	var out []byte
	var enc iguana.Encoder
	for len(src) > 0 {
		mem := src
		if len(mem) > iguanaWindowSize {
			mem = mem[:iguanaWindowSize]
		}
		src = src[len(mem):]
		lenpos := len(out)
		out = append(out, 0, 0, 0)
		var err error
		out, err = enc.Compress(mem, out, float32(threshold))
		if err != nil {
			panic(err)
		}
		winsize := len(out) - 3 - lenpos
		out[lenpos] = byte(winsize & 0xff)
		out[lenpos+1] = byte((winsize >> 8) & 0xff)
		out[lenpos+2] = byte((winsize >> 16) & 0xff)
	}
	return out
}

func iguanaDecompress(dec *iguana.Decoder, dst, src []byte) ([]byte, error) {
	var err error
	for len(src) >= 4 {
		winsize := int(src[0]) + (int(src[1]) << 8) + (int(src[2]) << 16)
		src = src[3:]
		if len(src) < winsize {
			panic("invalid frame")
		}
		mem := src[:winsize]
		src = src[winsize:]
		dst, err = dec.DecompressTo(dst[:0], mem)
		if err != nil {
			return dst, err
		}
	}
	return dst[:0], nil
}

func main() {
	var threshold float64
	flag.Float64Var(&threshold, "t", 1.0, "entropy coding threshold")
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		fatalf("usage: %s [-t threshold] <file>\n", os.Args[0])
	}

	buf, err := os.ReadFile(args[0])
	if err != nil {
		fatalf("reading file: %s", err)
	}

	comp := iguanaCompress(buf, threshold)
	start := time.Now()
	var tmp []byte
	var min time.Duration
	var dec iguana.Decoder
	deadline := start.Add(3 * time.Second)
	for time.Now().Before(deadline) {
		istart := time.Now()
		tmp, err = iguanaDecompress(&dec, tmp[:0], comp)
		if err != nil {
			fatalf("decompression error: %s", err)
		}
		dur := time.Since(istart)
		if min == 0 || dur < min {
			min = dur
		}
	}
	multiplier := (1e9) / float64(time.Second)
	gibps := (float64(len(buf)) / float64(min)) * multiplier
	fmt.Printf("%dB -> %dB (%.3gx) %.3g GB/s\n", len(buf), len(comp), float64(len(buf))/float64(len(comp)), gibps)
}
