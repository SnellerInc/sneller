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

package versify

import (
	"math/rand"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// BlockGenerator generates blockfmt-formatted
// tables from a Union.
type BlockGenerator struct {
	// Input is the input stream used to
	// generate the output data
	Input Union
	// Rand is the random source
	// passed to Input.Generate
	Rand *rand.Rand
	// Align is the alignment of uncompressed chunks
	Align int
	// Comp is the compression type
	// passed to blockfmt.Compression
	Comp string
}

// Table produces a compressed table
// containing at least the provided number
// of blocks and returns the encoded table as a []byte.
// Table will return an error if one of
// the generated output objects does not
// fit in b.Align.
func (b *BlockGenerator) Table(blocks int) ([]byte, error) {
	var dst blockfmt.BufferUploader
	comp := b.Comp
	if comp == "" {
		comp = "zstd"
	}
	align := b.Align
	if align == 0 {
		align = 1024 * 1024
	}
	rnd := b.Rand
	if rnd == nil {
		rnd = rand.New(rand.NewSource(0))
	}
	cw := blockfmt.CompressionWriter{
		Output:     &dst,
		Comp:       blockfmt.CompressorByName(comp),
		InputAlign: align,
		TargetSize: align,
	}
	cn := ion.Chunker{
		W:     &cw,
		Align: align,
	}
	for len(cw.Blocks) < blocks {
		d := b.Input.Generate(rnd)
		d.Encode(&cn.Buffer, &cn.Symbols)
		err := cn.Commit()
		if err != nil {
			return nil, err
		}
	}
	err := cn.Flush()
	if err != nil {
		return nil, err
	}
	err = cw.Close()
	if err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}
