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

package vm

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

// chacha8x4 uses length[lane]==0 as masking
//
//go:noescape
func chacha8bulk(buf []byte, in [][2]uint32, out []uint64) int

//go:noescape
func chacha8bulkseed(buf []byte, in [][2]uint32, seed []uint64) int

// chacha8Bulk computes the hash of every member
// of 'buf' delimited by 'in' and writes the output
// to 'out' (two qwords for each intput), returning
// len(in)
func chacha8Bulk(buf []byte, in [][2]uint32, out []uint64) int {
	if len(out) < 2*len(in) {
		panic("len(out) too small")
	}
	return chacha8bulk(buf, in, out)
}

// chacha8BulkSeed works like Chacha8Bulk except that it
// uses the 'seed' array as the seed *and* the output of
// the hash function
func chacha8BulkSeed(buf []byte, in [][2]uint32, seed []uint64) int {
	if len(seed) < 2*len(in) {
		panic("len(out) too small")
	}
	return chacha8bulkseed(buf, in, seed)
}

// compute the Z-statistic for K trials of N
// with a-priori probability of 0.5;
// approximate the result through the normal distribution
func binomialZ(k, n float64) float64 {
	return (k - (n * 0.5)) / math.Sqrt(n*0.5*0.5)
}

// TODO: improve the testing of dispersion here;
// we could examine the 128x128 correlation matrix
// of bits to see that they are (indistinguishable from) uncorrelated
func TestChaCha8Portable(t *testing.T) {
	bittab := make([]int64, 128)
	buf := make([]byte, 48)
	rand.Read(buf)

	const trials = 1 << 16
	for i := 0; i < trials; i++ {
		chacha8Hash(buf, buf)

		for j := 0; j < 128; j++ {
			k := j / 8
			b := j & 7
			if buf[k]&(1<<b) != 0 {
				bittab[j]++
			}
		}
	}

	min := int64(trials)
	max := int64(0)
	for i := range bittab {
		count := bittab[i]
		if count < min {
			min = count
		} else if count > max {
			max = count
		}
	}
	zmin := binomialZ(float64(min), float64(trials))
	zmax := binomialZ(float64(max), float64(trials))
	t.Logf("min=%d (Z=%g), max=%d (Z=%g) bits in %d trials",
		min, zmin, max, zmax, trials)

	if zmin < -4.41722 || zmax > 4.41722 {
		t.Fatalf("(zmin=%g, zmax=%g) out of safe range", zmin, zmax)
	}
}

func TestChaCha8x4(t *testing.T) {
	buf := make([]byte, 48*4)
	rand.Read(buf)
	inner := func(t *testing.T, ends [4]uint32) {
		t.Helper()
		got := chacha8x4(&buf[0], ends)
		for i := range got {
			want := make([]byte, 16)
			off := 0
			if i > 0 {
				off = int(ends[i-1])
			}
			chacha8Hash(buf[off:ends[i]], want)
			if !bytes.Equal(want, got[i][:]) {
				t.Logf("ends: %d", ends)
				t.Errorf("got[%d] (end %d) is %x", i, ends[i], got[i][:])
				t.Errorf("got[%d] diff %08x", i, want)
			}
		}
	}
	// create a large random test corpus
	// and test the results against the
	// portable reference implementation
	t.Run("random-cases", func(t *testing.T) {
		for rounds := 0; rounds < 1000; rounds++ {
			var ends [4]uint32
			prev := 0
			for i := range ends {
				n := rand.Intn(len(buf)-prev) + prev
				ends[i] = uint32(n)
				prev = n
			}
			inner(t, ends)
			if t.Failed() {
				break
			}
		}
	})
}

func TestChaCha8Bulk(t *testing.T) {
	buf := make([]byte, 512)
	src := make([][2]uint32, 32)
	out := make([]uint64, 2*len(src)+1)

	rand.Read(buf)

	for i := range src {
		src[i][0] = uint32(i)
		src[i][1] = uint32(len(buf) - i)
	}

	n := chacha8Bulk(buf, src, out)
	if n != len(src) {
		t.Errorf("got %d expected %d entries hashed", n, len(src))
	}
	if out[2*len(src)] != 0 {
		t.Fatal("memory corruption")
	}

	for i := range src {
		var h, want [16]byte
		j := i * 2
		h0, h1 := out[j], out[j+1]
		binary.LittleEndian.PutUint64(h[:], h0)
		binary.LittleEndian.PutUint64(h[8:], h1)
		off := src[i][0]
		chacha8Hash(buf[off:off+src[i][1]], want[:])
		if h != want {
			t.Errorf("src %d got %x want %x", i, h[:], want[:])
		}
	}

	for i := range out {
		out[i] = 0
	}

	// test unaligned
	n = chacha8Bulk(buf, src[:27], out)
	if n != 27 {
		t.Errorf("got %b expected %d entries hashed", n, 27)
	}

	// test that we didn't write past the
	// expected location in the output buffer
	outtail := out[27*2:]
	for i := range outtail {
		if outtail[i] != 0 {
			t.Errorf("outtail[%d]: %x", i, outtail[i])
		}
	}
	for i := range src[:27] {
		var h, want [16]byte
		j := i * 2
		h0, h1 := out[j], out[j+1]
		binary.LittleEndian.PutUint64(h[:], h0)
		binary.LittleEndian.PutUint64(h[8:], h1)
		off := src[i][0]
		chacha8Hash(buf[off:off+src[i][1]], want[:])
		if h != want {
			t.Errorf("src %d got %x want %x", i, h[:], want[:])
		}
	}

	// test hashing with seed
	out2 := make([]uint64, len(out))
	copy(out2, out[:27*2])
	n = chacha8BulkSeed(buf, src[:27], out2[:27*2])
	if n != 27 {
		t.Errorf("got %b expected %b entries hashed", n, 27)
	}
	outtail = out2[27*2:]
	for i := range outtail {
		if outtail[i] != 0 {
			t.Errorf("outtail[%d]: %x", i, outtail[i])
		}
	}

	for i := range src[:27] {
		var h, want, seed [16]byte
		j := i * 2
		h0, h1 := out2[j], out2[j+1]
		binary.LittleEndian.PutUint64(h[:], h0)
		binary.LittleEndian.PutUint64(h[8:], h1)
		s0, s1 := out[j], out[j+1]
		binary.LittleEndian.PutUint64(seed[:], s0)
		binary.LittleEndian.PutUint64(seed[8:], s1)

		off := src[i][0]
		chacha8HashSeed(buf[off:off+src[i][1]], want[:], seed[:])
		if h != want {
			t.Errorf("src %d got %x want %x", i, h[:], want[:])
		}
	}
}

func BenchmarkChaCha8x4Core(b *testing.B) {
	buf := make([]byte, (48*4)+8)
	rand.Read(buf)
	ends := [4]uint32{11, 11 + 27, 11 + 27 + 33, 11 + 27 + 33 + 47}
	b.SetBytes(11 + 27 + 33 + 47)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chacha8x4(&buf[0], ends)
	}
}

func BenchmarkChaCha8Bulk(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	locs := make([][2]uint32, 1024)
	n, _ := scan(buf, 0xb7, locs)
	if n == len(locs) {
		b.Errorf("buf len %d undersized", len(locs))
	}
	locs = locs[:n]
	out := make([]uint64, 2*len(locs))
	b.SetBytes(int64(len(buf[0xb7:])))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chacha8Bulk(buf, locs, out)
	}
}

func BenchmarkChaCha8BulkSeed(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	locs := make([][2]uint32, 1024)
	n, _ := scan(buf, 0xb7, locs)
	locs = locs[:n]
	out := make([]uint64, 2*len(locs))
	b.SetBytes(int64(len(buf[0xb7:])))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chacha8BulkSeed(buf, locs, out)
	}
}
