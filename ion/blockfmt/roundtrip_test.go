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

package blockfmt_test

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/ion/versify"
)

type partCollector struct {
	parts [][]byte
	out   io.Writer
}

func (p *partCollector) Write(buf []byte) (int, error) {
	cop := make([]byte, len(buf))
	copy(cop, buf)
	p.parts = append(p.parts, cop)
	return p.out.Write(buf)
}

func (p *partCollector) Flush() error {
	if f, ok := p.out.(ion.Flusher); ok {
		return f.Flush()
	}
	return nil
}

// synthesize a raw []byte containing
// a compressed table with the specified
// number of output data blocks,
// plus the list of decompressed data blocks
func synthesize(t testing.TB, corpus string, blocks int) ([]byte, [][]byte) {
	f, err := os.Open(corpus)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	u, _, err := versify.FromJSON(d)
	if err != nil {
		t.Fatal(err)
	}
	const shift = 12

	var out blockfmt.BufferUploader
	out.PartSize = 1 << shift
	w := blockfmt.CompressionWriter{
		Output:     &out,
		Comp:       blockfmt.CompressorByName("zstd"),
		InputAlign: (1 << shift),
	}

	// we want to collect the decompressed
	// parts so that we can confirm that they
	// are equivalent after reading
	pc := partCollector{out: &w}
	cn := ion.Chunker{
		Align: w.InputAlign,
		W:     &pc,
	}
	src := rand.New(rand.NewSource(0))
	for w.WrittenBlocks() < blocks {
		dat := u.Generate(src)
		dat.Encode(&cn.Buffer, &cn.Symbols)
		err := cn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
	err = cn.Flush()
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	var badbuf bytes.Buffer
	blockfmt.Validate(bytes.NewReader(out.Bytes()), &w.Trailer, &badbuf)
	if badbuf.Len() > 0 {
		t.Fatal(err)
	}
	return out.Bytes(), pc.parts
}

func TestCompressedRoundtrip(t *testing.T) {
	const blockcount = 20

	buf, blocks := synthesize(t, "../../testdata/parking2.json", blockcount)

	r := bytes.NewReader(buf)
	trailer, err := blockfmt.ReadTrailer(r, r.Size())
	if err != nil {
		t.Fatal(err)
	}
	if len(trailer.Blocks) < len(blocks) {
		t.Errorf("reader has %d blocks instead of %d?", len(trailer.Blocks), len(blocks))
	}
	if trailer.BlockShift != 12 {
		t.Fatalf("unexpected block shift %d", trailer.BlockShift)
	}
	out := check(t, buf)
	for i := range blocks {
		if !bytes.Equal(out[:len(blocks[i])], blocks[i]) {
			t.Errorf("block %d not equal", i)
		}
		out = out[len(blocks[i]):]
	}
}

// speed up versification by generating a small random input set
// and then permuting it repeatedly to produce the output
func fastVersify(in versify.Union, src *rand.Rand, dst *ion.Chunker, collect, output int) error {
	datums := make([]ion.Datum, collect)
	for i := range datums {
		datums[i] = in.Generate(src)
	}
	for output > 0 {
		for i := range datums {
			if output == 0 {
				break
			}
			datums[i].Encode(&dst.Buffer, &dst.Symbols)
			err := dst.Commit()
			if err != nil {
				return err
			}
			output--
		}
		src.Shuffle(len(datums), func(i, j int) {
			datums[i], datums[j] = datums[j], datums[i]
		})
	}
	return dst.Flush()
}

func TestMultiRoundtrip(t *testing.T) {
	var buf blockfmt.BufferUploader
	mw := blockfmt.MultiWriter{
		Output: &buf,
		Algo:   "zstd",
		// we're picking very small alignment
		// and target size so we can really exercise
		// the span-merging code
		InputAlign: 4 * 1024,
		TargetSize: 8 * 1024,
	}
	buf.PartSize = mw.TargetSize

	streams := make([]io.WriteCloser, 4)
	var err error
	for i := range streams {
		streams[i], err = mw.Open()
		if err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open("../../testdata/parking2.json")
	if err != nil {
		t.Fatal(err)
	}
	d := json.NewDecoder(f)
	u, _, err := versify.FromJSON(d)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	var wg sync.WaitGroup
	for i := range streams {
		wg.Add(1)
		stream := streams[i]
		go func() {
			defer wg.Done()
			src := rand.New(rand.NewSource(int64(0)))
			cn := ion.Chunker{W: stream, Align: mw.InputAlign, RangeAlign: mw.InputAlign * 10}
			// empirically this is enough objects per stream
			// to create plenty of output blocks:
			err := fastVersify(u, src, &cn, 1000, 50000)
			if err != nil {
				panic(err)
			}
			err = stream.Close()
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	err = mw.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created %d blocks", len(mw.Trailer.Blocks))
	t.Logf("created %d parts", buf.Parts())

	// output block offsets should be sorted
	for i := 0; i < len(mw.Trailer.Blocks)-1; i++ {
		if mw.Trailer.Blocks[i].Offset >= mw.Trailer.Blocks[i+1].Offset {
			t.Errorf("block %d offset %d above next offset %d", i, mw.Trailer.Blocks[i].Offset, mw.Trailer.Blocks[i+1].Offset)
		}
	}
	if mw.Trailer.Blocks[0].Offset != 0 {
		t.Errorf("block 0 offset is %d", mw.Trailer.Blocks[0].Offset)
	}

	contents := buf.Bytes()
	t.Logf("%d bytes output", len(contents))
	check(t, contents)
}

func TestMultiRanges(t *testing.T) {
	var inputs []blockfmt.Input
	for i := 0; i < 3; i++ {
		f, err := os.Open("../../testdata/cloudtrail.json")
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, blockfmt.Input{
			R: f,
			F: blockfmt.SuffixToFormat[".json"](),
		})
	}

	var out blockfmt.BufferUploader
	out.PartSize = 4096
	c := blockfmt.Converter{
		Output: &out,
		Comp:   "zstd",
		Inputs: inputs,
		Align:  4096,
	}
	if !c.MultiStream() {
		t.Fatal("expected MultiStream to be true with 2 inputs")
	}
	err := c.Run()
	if err != nil {
		t.Fatal(err)
	}
	check(t, out.Bytes())
	r := bytes.NewReader(out.Bytes())
	// we know this dataset has an eventTime
	// field in every structure that should be
	// picked up by sparse indexing
	tr, err := blockfmt.ReadTrailer(r, r.Size())
	if err != nil {
		t.Fatal(err)
	}
	if rng := tr.Sparse.Get([]string{"eventTime"}); rng == nil {
		t.Fatal("missing eventTime range")
	}
}

func check(t *testing.T, buf []byte) []byte {
	r := bytes.NewReader(buf)
	trailer, err := blockfmt.ReadTrailer(r, r.Size())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	var bad bytes.Buffer
	blockfmt.Validate(r, trailer, &bad)
	if bad.Len() > 0 {
		t.Fatal(bad.String())
	}
	r.Seek(0, io.SeekStart)
	out := make([]byte, trailer.Decompressed())
	dec := blockfmt.Decoder{}
	dec.Set(trailer, len(trailer.Blocks))
	n, err := dec.Decompress(r, out)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if n != len(out) {
		t.Helper()
		t.Errorf("%d bytes decompressed instead of %d", n, len(out))
	}
	var dst bytes.Buffer
	r.Seek(0, io.SeekStart)
	nn, err := dec.Copy(&dst, io.LimitReader(r, trailer.Offset))
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if int(nn) != len(out) {
		t.Helper()
		t.Errorf("%d bytes decompressed instead of %d", n, len(out))
	}
	if !bytes.Equal(dst.Bytes(), out) {
		t.Error("Decompress and Copy returned different data")
	}
	dst.Reset()
	nn, err = dec.CopyBytes(&dst, buf[:trailer.Offset])
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if int(nn) != len(out) {
		t.Helper()
		t.Errorf("%d bytes decompressed instead of %d", n, len(out))
	}
	if !bytes.Equal(dst.Bytes(), out) {
		t.Error("Decompress and CopyBytes returned different data")
	}
	return out
}
