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

// Package blockfmt implements routines for reading
// and writing compressed and aligned
// ion blocks to/from backing storage.
//
// The APIs in this package are designed
// with object storage in mind as the primary
// backing store.
//
// The CompressionWriter type can be used
// to write aligned ion blocks (see ion.Chunker)
// to backing storage, and the CompressionReader
// type can provide positional access to compressed
// blocks within the backing storage.
package blockfmt

import (
	"fmt"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// Blockdesc is a descriptor that
// is attached to each block within
// a Trailer.
type Blockdesc struct {
	// Offset is the offset of the *compressed*
	// output data.
	Offset int64
	// Chunks is the number of chunks
	// (with decompressed length equal to
	// 1 << Trailer.BlockShift) within
	// this block
	Chunks int
}

// Trailer is a collection
// of block descriptions.
type Trailer struct {
	// Version is an indicator
	// of the encoded trailer format version
	Version int
	// Offset is the offset of the trailer
	// within the output stream.
	Offset int64
	// Algo is the name of the compression
	// algorithm used to compress blocks
	Algo string
	// BlockShift is the alignment of each block
	// when it is fully decompressed (in bits)
	//
	// For example, BlockShift of 20 means that
	// blocks are 1MB (1 << 20) bytes each.
	BlockShift int
	// Blocks is the list of descriptors
	// for each block.
	Blocks []Blockdesc
	// Sparse contains a lossy secondary index
	// of timestamp ranges and constant fields
	// within Blocks.
	Sparse SparseIndex
}

// Encode encodes a trailer to the provided buffer
// using the provided symbol table.
// Note that Encode may add new symbols to the symbol table.
func (t *Trailer) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)

	// we're encoding trailer version 1
	dst.BeginField(st.Intern("version"))
	dst.WriteInt(int64(1))

	dst.BeginField(st.Intern("offset"))
	dst.WriteInt(t.Offset)

	dst.BeginField(st.Intern("algo"))
	dst.WriteString(t.Algo)

	dst.BeginField(st.Intern("blockshift"))
	dst.WriteInt(int64(t.BlockShift))

	if t.Sparse.blocks != len(t.Blocks) {
		panic("Trailer.Encode: Sparse #blocks don't match trailer blocks")
	}
	// we always encode sparse before blocks
	// so that when we are decoding, we know the
	// *lack* of a sparse field means that we should
	// build Sparse as we decode blocks
	dst.BeginField(st.Intern("sparse"))
	t.Sparse.Encode(dst, st)

	// block offsets are double-differential-encoded
	// (because they tend to be evenly spaced),
	// and chunk counts are delta-encoded (because
	// they tend to be similar)
	dst.BeginField(st.Intern("blocks-delta"))
	dst.BeginList(-1)
	so, do := int64(0), int64(0)
	pc := int64(0)
	for i := range t.Blocks {
		off := t.Blocks[i].Offset
		dst.WriteInt(off - so - do)
		do = off - so
		so = off
		chunks := t.Blocks[i].Chunks
		dst.WriteInt(int64(chunks) - pc)
		pc = int64(chunks)
	}
	dst.EndList()

	dst.EndStruct()
}

func countList(d ion.Datum) (int, error) {
	l, err := d.List()
	if err != nil {
		return 0, err
	}
	return l.Len(), nil
}

// A TrailerDecoder can be used to decode multiple
// trailers containing related information in a more
// memory-efficient way than decoding the trailers
// individually.
type TrailerDecoder struct {
	blockcap int
	blocks   []Blockdesc
	spans    []timespan

	paths      map[string][]string
	ranges     []Range
	rangecap   int
	timeRanges []TimeRange
	algo       string
}

// makeBlocks returns a []Blockdesc of len n, using the
// front of d.blocks if possible.
func (d *TrailerDecoder) makeBlocks(n int) []Blockdesc {
	if n > len(d.blocks) {
		d.blockcap = n + 2*d.blockcap
		d.blocks = make([]Blockdesc, d.blockcap)
	}
	b := d.blocks[:n:n]
	d.blocks = d.blocks[n:]
	return b
}

// path returns an interned []string representation of
// the path represented by v.
func (d *TrailerDecoder) path(v ion.Datum) ([]string, error) {
	data := v.Raw()
	if d.paths == nil {
		d.paths = make(map[string][]string)
	} else if s, ok := d.paths[string(data)]; ok {
		return s, nil
	}
	var path []string
	err := v.UnpackList(func(v ion.Datum) error {
		s, err := v.String()
		if err != nil {
			return err
		}
		path = append(path, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	d.paths[string(data)] = path
	return path, nil
}

// makeRange returns a []Range of len n, using the
// front of d.ranges if possible.
func (d *TrailerDecoder) makeRange(n int) []Range {
	if n > len(d.ranges) {
		d.rangecap = n + 2*d.rangecap
		d.ranges = make([]Range, d.rangecap)
	}
	rs := d.ranges[:n:n]
	d.ranges = d.ranges[n:]
	return rs
}

// timeRange appends a timeRange to d.timeRanges and
// returns a pointer to it.
func (d *TrailerDecoder) timeRange(path []string, min, max date.Time) *TimeRange {
	if len(d.timeRanges) == cap(d.timeRanges) {
		d.timeRanges = make([]TimeRange, 0, 8+2*cap(d.timeRanges))
	}
	d.timeRanges = append(d.timeRanges, TimeRange{
		path: path,
		min:  min,
		max:  max,
	})
	return &d.timeRanges[len(d.timeRanges)-1]
}

func (d *TrailerDecoder) unpackRanges(v ion.Datum) ([]Range, error) {
	n, err := countList(v)
	if n == 0 {
		return nil, err
	}
	ranges := d.makeRange(n)[:0]
	err = v.UnpackList(func(v ion.Datum) error {
		var tmin, tmax struct {
			ts date.Time
			ok bool
		}
		var min, max ion.Datum
		var path []string
		err := v.UnpackStruct(func(f ion.Field) error {
			var err error
			switch f.Label {
			case "min":
				if f.IsTimestamp() {
					tmin.ts, err = f.Timestamp()
					if err != nil {
						return err
					}
					tmin.ok = true
				} else {
					min = f.Datum
				}
			case "max":
				if f.IsTimestamp() {
					tmax.ts, err = f.Timestamp()
					if err != nil {
						return err
					}
					tmax.ok = true
				} else {
					max = f.Datum
				}
			case "path":
				path, err = d.path(f.Datum)
			}
			return err
		})
		if err != nil {
			return err
		}
		if len(path) == 0 {
			return fmt.Errorf("in Block.Ranges: missing Path")
		}
		if tmin.ok && tmax.ok {
			ranges = append(ranges, d.timeRange(path, tmin.ts, tmax.ts))
		} else {
			// NOTE: this should never
			// happen in practice, but
			// handle it anyway...
			if min.IsEmpty() && tmin.ok {
				min = ion.Timestamp(tmin.ts)
			} else {
				min = min.Clone()
			}
			if max.IsEmpty() && tmax.ok {
				max = ion.Timestamp(tmax.ts)
			} else {
				max = max.Clone()
			}
			ranges = append(ranges, NewRange(path, min, max))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

// Decode decodes a trailer.
func (d *TrailerDecoder) Decode(v ion.Datum, dst *Trailer) error {
	seenSparse := false
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "version":
			v, err := f.Int()
			if err != nil {
				return err
			}
			dst.Version = int(v)
		case "offset":
			off, err := f.Int()
			if err != nil {
				return err
			}
			dst.Offset = off
		case "algo":
			alg, err := f.StringShared()
			if err != nil {
				return err
			}
			if len(alg) == 0 {
				return nil
			}
			// usually algo will be the
			// same for all blocks, so we
			// can avoid allocing a string
			if string(alg) != d.algo {
				d.algo = string(alg)
			}
			dst.Algo = d.algo
		case "blockshift":
			shift, err := f.Int()
			if err != nil {
				return err
			}
			dst.BlockShift = int(shift)
		case "sparse":
			seenSparse = true
			return d.decodeSparse(&dst.Sparse, f.Datum)
		case "blocks-delta":
			// smaller delta-encoded block list format
			n, err := countList(f.Datum)
			if err != nil || n == 0 {
				return err
			}
			dst.Blocks = d.makeBlocks(n / 2)[:0]
			dst.unpackBlocks(f.Raw())
		case "blocks":
			// old-format block lists
			n, err := countList(f.Datum)
			if err != nil || n == 0 {
				return err
			}
			dst.Blocks = d.makeBlocks(n)[:0]
			err = f.UnpackList(func(v ion.Datum) error {
				dst.Blocks = dst.Blocks[:len(dst.Blocks)+1]
				blk := &dst.Blocks[len(dst.Blocks)-1]
				// if the 'chunks' field isn't present,
				// then the number of chunks must be 1
				blk.Chunks = 1
				seenRanges := false
				err := v.UnpackStruct(func(f ion.Field) error {
					switch f.Label {
					case "offset":
						off, err := f.Int()
						if err != nil {
							return err
						}
						blk.Offset = off
					case "chunks":
						chunks, err := f.Int()
						if err != nil {
							return err
						}
						blk.Chunks = int(chunks)
					case "ranges":
						if seenSparse {
							panic("ranges and sparse present?")
						}
						seenRanges = true
						// TODO: unpack ranges into Sparse
						ranges, err := d.unpackRanges(f.Datum)
						if err != nil {
							return fmt.Errorf("error unpacking range %d: %w", len(ranges), err)
						}
						dst.Sparse.Push(ranges)
					}
					return nil
				})
				if err != nil {
					return err
				}
				if !seenSparse && !seenRanges {
					dst.Sparse.Push(nil)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("unpacking Block %d: %w", len(dst.Blocks), err)
			}
			return nil
		default:
			// try to be forwards-compatible:
			// if Version != 1, then ignore future fields
			if dst.Version != 1 {
				return nil
			}
			return fmt.Errorf("unexpected field %q", f.Label)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("Trailer.Decode: %w", err)
	}
	return nil
}

func (t *Trailer) unpackBlocks(body []byte) error {
	body, _ = ion.Contents(body)
	var v int64
	var err error
	so, do := int64(0), int64(0)
	pc := int64(0)
	for len(body) > 0 {
		// first value: double-differential offset
		v, body, err = ion.ReadInt(body)
		if err != nil {
			return err
		}
		off := v + so + do
		do = off - so
		so = off
		// second-value: delta-encoded #chunks
		v, body, err = ion.ReadInt(body)
		if err != nil {
			return err
		}
		chunks := v + pc
		pc = chunks
		t.Blocks = append(t.Blocks, Blockdesc{
			Offset: off,
			Chunks: int(chunks),
		})
	}
	return nil
}

// Decode decodes a trailer encoded using Encode.
func (t *Trailer) Decode(st *ion.Symtab, body []byte) error {
	v, _, err := ion.ReadDatum(st, body)
	if err != nil {
		return err
	}
	var d TrailerDecoder
	return d.Decode(v, t)
}

// Decompressed returns the decompressed size
// of all of the data within the trailer blocks.
func (t *Trailer) Decompressed() int64 {
	chunks := 0
	for i := range t.Blocks {
		chunks += t.Blocks[i].Chunks
	}
	return int64(chunks) * int64(1<<t.BlockShift)
}

// BlockRange returns the start and end offsets
// of block [i] within the object.
func (t *Trailer) BlockRange(i int) (start, end int64) {
	start = t.Blocks[i].Offset
	end = t.Offset
	if i+1 < len(t.Blocks) {
		end = t.Blocks[i+1].Offset
	}
	return start, end
}

// BlockSize returns the compressed size of
// block [i] within the object.
func (t *Trailer) BlockSize(i int) int64 {
	start, end := t.BlockRange(i)
	return end - start
}

// DecompressedSize returns the decompressed
// size of block [i] within the object.
func (t *Trailer) DecompressedSize(i int) int64 {
	return int64(t.Blocks[i].Chunks) << t.BlockShift
}
