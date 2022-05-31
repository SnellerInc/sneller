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

	// Ranges is optional value-range metadata
	// associated with columns in this block.
	//
	// NOTE: the only time this should be populated
	// is by CompressionWriter / MultiWriter; this
	// field is not serialized directly.
	// The summary of Blockdesc[*].Range lives in
	// Trailer.Sparse.
	//
	// TODO: remove me entirely
	ranges []Range
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
	// of timestamp ranges within Blocks.
	Sparse SparseIndex
}

// set Sparse state and clear Blocks[*].Ranges
func (t *Trailer) syncRanges() {
	if t.Sparse.blocks == 0 && len(t.Blocks) > 0 {
		t.Sparse.setRanges(t.Blocks)
		for i := range t.Blocks {
			t.Blocks[i].ranges = nil
		}
	}
}

func writeRanges(dst *ion.Buffer, st *ion.Symtab, ranges []Range) {
	symPath := st.Intern("path")
	symMin := st.Intern("min")
	symMax := st.Intern("max")
	dst.BeginList(-1)
	for j := range ranges {
		dst.BeginStruct(-1)
		{
			dst.BeginField(symPath)
			dst.BeginList(-1)
			path := ranges[j].Path()
			for k := range path {
				dst.WriteSymbol(st.Intern(path[k]))
			}
			dst.EndList()
		}
		switch r := ranges[j].(type) {
		case *TimeRange:
			dst.BeginField(symMin)
			dst.WriteTime(r.min)
			dst.BeginField(symMax)
			dst.WriteTime(r.max)
		default:
			if min := r.Min(); min != nil {
				dst.BeginField(symMin)
				min.Encode(dst, st)
			}
			if max := r.Max(); max != nil {
				dst.BeginField(symMax)
				max.Encode(dst, st)
			}
		}
		dst.EndStruct()
	}
	dst.EndList()
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

	dst.BeginField(st.Intern("blocks"))

	// if there are Ranges fields set in the blocks,
	// encode them in the sparse index instead;
	// we have the same sort of backwards-compatibility shim
	// in the decoding path as well
	t.syncRanges()

	symOffset := st.Intern("offset")
	symChunks := st.Intern("chunks")
	dst.BeginList(-1)
	for i := range t.Blocks {
		dst.BeginStruct(-1)
		dst.BeginField(symOffset)
		dst.WriteInt(t.Blocks[i].Offset)
		dst.BeginField(symChunks)
		dst.WriteInt(int64(t.Blocks[i].Chunks))
		dst.EndStruct()
	}
	dst.EndList()

	dst.BeginField(st.Intern("sparse"))
	t.Sparse.Encode(dst, st)

	dst.EndStruct()
}

func countList(body []byte) (int, error) {
	n := 0
	err := unpackList(body, func([]byte) error {
		n++
		return nil
	})
	return n, err
}

func unpackList(body []byte, fn func(field []byte) error) error {
	_, err := ion.UnpackList(body, fn)
	return err
}

func unpackStruct(st *ion.Symtab, body []byte, fn func(name string, field []byte) error) error {
	_, err := ion.UnpackStruct(st, body, fn)
	return err
}

// A TrailerDecoder can be used to decode multiple
// trailers containing related information in a more
// memory-efficient way than decoding the trailers
// individually.
type TrailerDecoder struct {
	// Symbols is the symbol table to use when
	// decoding symbols.
	Symbols *ion.Symtab

	trailers   []Trailer
	blockcap   int
	blocks     []Blockdesc
	paths      map[string][]string
	ranges     []Range
	rangecap   int
	timeRanges []TimeRange
	algo       string
}

func (d *TrailerDecoder) trailer() *Trailer {
	if len(d.trailers) == cap(d.trailers) {
		d.trailers = make([]Trailer, 0, 8+2*cap(d.trailers))
	}
	d.trailers = d.trailers[:len(d.trailers)+1]
	return &d.trailers[len(d.trailers)-1]
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
// the path represented by data.
func (d *TrailerDecoder) path(data []byte) ([]string, error) {
	if d.paths == nil {
		d.paths = make(map[string][]string)
	} else if s, ok := d.paths[string(data)]; ok {
		return s, nil
	}
	var path []string
	err := unpackList(data, func(comp []byte) error {
		sym, _, err := ion.ReadSymbol(comp)
		if err != nil {
			return err
		}
		path = append(path, d.Symbols.Get(sym))
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

func (d *TrailerDecoder) unpackRanges(st *ion.Symtab, field []byte) ([]Range, error) {
	n, err := countList(field)
	if err != nil || n == 0 {
		return nil, err
	}
	ranges := d.makeRange(n)[:0]
	err = unpackList(field, func(field []byte) error {
		var tmin, tmax struct {
			ts date.Time
			ok bool
		}
		var min, max ion.Datum
		var path []string
		err := unpackStruct(d.Symbols, field, func(name string, field []byte) error {
			var err error
			switch name {
			case "min":
				if ion.TypeOf(field) == ion.TimestampType {
					tmin.ts, _, err = ion.ReadTime(field)
					tmin.ok = err == nil
				} else {
					min, _, err = ion.ReadDatum(d.Symbols, field)
				}
			case "max":
				if ion.TypeOf(field) == ion.TimestampType {
					tmax.ts, _, err = ion.ReadTime(field)
					tmax.ok = err == nil
				} else {
					max, _, err = ion.ReadDatum(d.Symbols, field)
				}
			case "path":
				path, err = d.path(field)
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
			if min == nil && tmin.ok {
				min = ion.Timestamp(tmin.ts)
			}
			if max == nil && tmax.ok {
				max = ion.Timestamp(tmax.ts)
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
func (d *TrailerDecoder) Decode(body []byte) (*Trailer, error) {
	t := d.trailer()
	err := d.decode(t, body)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (d *TrailerDecoder) decode(t *Trailer, body []byte) error {
	seenSparse := false
	err := unpackStruct(d.Symbols, body, func(fieldname string, body []byte) error {
		switch fieldname {
		case "version":
			v, _, err := ion.ReadInt(body)
			if err != nil {
				return err
			}
			t.Version = int(v)
		case "offset":
			off, _, err := ion.ReadInt(body)
			if err != nil {
				return err
			}
			t.Offset = off
		case "algo":
			alg, _, err := ion.ReadStringShared(body)
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
			t.Algo = d.algo
		case "blockshift":
			shift, _, err := ion.ReadInt(body)
			if err != nil {
				return err
			}
			t.BlockShift = int(shift)
		case "sparse":
			seenSparse = true
			return t.Sparse.Decode(d.Symbols, body)
		case "blocks":
			n, err := countList(body)
			if err != nil || n == 0 {
				return err
			}
			t.Blocks = d.makeBlocks(n)[:0]
			err = unpackList(body, func(field []byte) error {
				t.Blocks = t.Blocks[:len(t.Blocks)+1]
				blk := &t.Blocks[len(t.Blocks)-1]
				// if the 'chunks' field isn't present,
				// then the number of chunks must be 1
				blk.Chunks = 1
				err := unpackStruct(d.Symbols, field, func(name string, field []byte) error {
					switch name {
					case "offset":
						off, _, err := ion.ReadInt(field)
						if err != nil {
							return err
						}
						blk.Offset = off
					case "chunks":
						chunks, _, err := ion.ReadInt(field)
						if err != nil {
							return err
						}
						blk.Chunks = int(chunks)
					case "ranges":
						// TODO: unpack ranges into Sparse
						ranges, err := d.unpackRanges(d.Symbols, field)
						if err != nil {
							return fmt.Errorf("error unpacking range %d: %w", len(ranges), err)
						}
						blk.ranges = ranges
					}
					return nil
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("unpacking Block %d: %w", len(t.Blocks), err)
			}
			return nil
		default:
			// try to be forwards-compatible:
			// if Version != 1, then ignore future fields
			if t.Version != 1 {
				return nil
			}
			return fmt.Errorf("unexpected field %q", fieldname)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("Trailer.Decode: %w", err)
	}
	if !seenSparse {
		// if this is old-style data, then we
		// should stick the range information
		// in the sparse index and strip it
		// from the blocks themselves
		t.Sparse.setRanges(t.Blocks)
		for i := range t.Blocks {
			t.Blocks[i].ranges = nil
		}
	}
	return nil
}

// Decode decodes a trailer encoded using Encode.
func (t *Trailer) Decode(st *ion.Symtab, body []byte) error {
	d := TrailerDecoder{Symbols: st}
	return d.decode(t, body)
}

// CombineWith combines the current trailer with another one.
// All blocks of the `other` trailer must immediately follow the
// blocks of the current one (we assume the original trailer is
// overwritten).
func (t *Trailer) CombineWith(other *Trailer) error {

	// :-------------------------:-------------------------:
	// : blocks_a  : trailer_a   : blocks_b  : trailer_b   :
	// :-------------------------:-------------------------:
	// =>
	// :------------------------------------:
	// : blocks_a...blocks_b  : new_trailer :
	// :------------------------------------:

	if t.Version != other.Version {
		// TODO: Later: Add code to migrate trailers from previous versions
		return fmt.Errorf("mismatching trailer versions ['%d', '%d']", t.Version, other.Version)
	}
	if t.Algo != other.Algo {
		return fmt.Errorf("mismatching compression algos ['%s', '%s']", t.Algo, other.Algo)
	}
	if t.BlockShift != other.BlockShift {
		return fmt.Errorf("mismatching blockshift ['%d', '%d']", t.BlockShift, other.BlockShift)
	}

	n := len(t.Blocks)
	offset := t.Offset

	t.Offset += other.Offset
	t.Blocks = append(t.Blocks, other.Blocks...)

	// Shift all new blocks using the left-side trailer offset
	for i := range t.Blocks[n:] {
		t.Blocks[n+i].Offset += offset
	}

	return nil
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

// Slice returns a new Trailer corresponding
// to the linear range of blocks t.Blocks[start:end].
func (t *Trailer) Slice(start, end int) *Trailer {
	newt := new(Trailer)
	*newt = *t
	var lastoff int64
	if end == len(t.Blocks) {
		lastoff = t.Offset
	} else {
		lastoff = t.Blocks[end].Offset
	}
	newt.Blocks = t.Blocks[start:end]
	newt.Offset = lastoff
	return newt
}
