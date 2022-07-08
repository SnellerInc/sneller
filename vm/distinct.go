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
	"fmt"
	"io"
	"sync"

	"github.com/SnellerInc/sneller/expr"
)

// DistinctFilter is a QuerySink that deduplicates
// rows using a tuple of input rows
//
// Note that deduplicated rows are returned in their
// entirety, but the contents of the fields that are
// not part of the deduplication condition are left
// unspecified. In other words, when there are duplicate rows,
// the first row to be selected as "distinct" can be any
// of the distinct rows.
type DistinctFilter struct {
	columns []expr.Node
	out     QuerySink

	prog prog
	// dedup is the merged distinct tree;
	// it is the source-of-truth for which
	// distinct values have been located
	// (but we keep a thread-local copy
	// of this tree so that duplicates are
	// filtered without accessing the global tree)
	lock      sync.Mutex
	dedup     *radixTree64
	limit     int64
	remaining int64
}

// NewDistinct creates a new DistinctFilter
// that filters out duplicate rows for
// which the tuple of expressions 'on' are duplicated.
func NewDistinct(on []expr.Node, dst QuerySink) (*DistinctFilter, error) {
	if len(on) == 0 {
		return nil, fmt.Errorf("cannot compute DISTINCT on zero columns")
	}
	df := &DistinctFilter{
		columns: on,
		out:     dst,
	}

	// compute the combined hash
	// of each of the DISTINCT columns
	// and then perform a radix lookup
	// to determine if we have duplicates
	p := &df.prog
	p.Begin()
	var hash, pred *value
	for i := range on {
		val, err := p.serialized(on[i])
		if err != nil {
			return nil, err
		}
		if hash == nil {
			pred = p.mask(val)
			hash = p.hash(val)
		} else {
			pred = p.And(pred, p.mask(val))
			hash = p.hashplus(hash, val)
		}
	}
	// the final state of the bytecode will be
	// the initial base, the hashes, and the final
	// predicate
	p.Return(p.bhk(p.ValidLanes(), hash, pred))
	return df, nil
}

// Limit sets a limit on the number of distinct
// rows to produce. (A limit <= 0 means an unlimited number of rows.)
func (d *DistinctFilter) Limit(n int64) {
	d.limit = n
	d.remaining = n
}

func (d *DistinctFilter) Open() (io.WriteCloser, error) {
	dst, err := d.out.Open()
	if err != nil {
		return nil, err
	}
	return splitter(&deduper{
		parent: d,
		dst:    asRowConsumer(dst),
	}), nil
}

func (d *DistinctFilter) Close() error {
	return d.out.Close()
}

type deduper struct {
	prog   prog
	parent *DistinctFilter
	local  *radixTree64
	dst    rowConsumer
	bc     bytecode

	// temporary buffer for
	// storing computed hashes
	hashes []uint64
	// bytecode slot used for
	// deduplication hashes;
	// is -1 if the columns to match
	// don't exist
	hashslot int
	// closed is set to true
	// if we reach the limit
	// set by the parent
	closed bool
}

func (d *deduper) symbolize(st *symtab) error {
	err := recompile(st, &d.parent.prog, &d.prog, &d.bc)
	if err != nil {
		return err
	}
	// the return value is a bhk tuple
	// instruction, so it will have the
	// immediate field set to the hash register
	// that we are "returning"
	//
	// the return value *can* just be 'false'
	// in which case hashslot should be -1
	var ok bool
	d.hashslot, ok = d.prog.ret.imm.(int)
	if !ok {
		d.hashslot = -1
	}
	return d.dst.symbolize(st)
}

//go:noescape
func evaldedup(bc *bytecode, delims []vmref, hashes []uint64, tree *radixTree64, slot int) int

func (d *deduper) next() rowConsumer { return d.dst }

func (d *deduper) EndSegment() {
	d.bc.dropScratch() // restored in recompile()
}

func (d *deduper) writeRows(delims []vmref) error {
	if d.closed {
		return io.EOF
	}
	if d.hashslot == -1 {
		return nil
	}
	// for the very first set of rows we
	// know that we are likely to encounter
	// distinct rows, so only try to process
	// a few of them, as we are unlikely to
	// compress away many of them...
	if d.local == nil {
		d.local = newRadixTree(0)
		if len(delims) > 16 {
			d.writeRows(delims[:16])
			delims = delims[16:]
		}
	}

	if cap(d.hashes) >= len(delims) {
		d.hashes = d.hashes[:len(delims)]
	} else {
		d.hashes = make([]uint64, len(delims))
	}
	count := evaldedup(&d.bc, delims, d.hashes, d.local, d.hashslot)
	if d.bc.err != 0 {
		return fmt.Errorf("distinct: bytecode error: %w", d.bc.err)
	}
	if count == 0 {
		return nil
	}

	delims = delims[:count]
	hashes := d.hashes[:count]
	outpos := 0
	for i := range hashes {
		_, ok := d.local.insertSlow(hashes[i])
		if ok {
			delims[outpos] = delims[i]
			hashes[outpos] = hashes[i]
			outpos++
		}
	}
	delims = delims[:outpos]
	hashes = hashes[:outpos]

	// we may not insert len(delims) entries
	// (due to duplicates), but we should have
	// inserted at least one entry
	if len(delims) == 0 {
		panic("expected to insert at least one tree entry")
	}

	// perform the same insert, but
	// this time with the global tree
	outpos = 0
	d.parent.lock.Lock()
	all := d.parent.dedup
	if all == nil {
		d.parent.dedup = newRadixTree(0)
		all = d.parent.dedup
	}
	for i := range hashes {
		_, ok := all.insertSlow(hashes[i])
		if ok {
			delims[outpos] = delims[i]
			outpos++
		}
	}
	if d.parent.limit > 0 {
		c := int64(outpos)
		if c >= d.parent.remaining {
			c, d.parent.remaining = d.parent.remaining, 0
			d.closed = true
		} else {
			d.parent.remaining -= c
		}
		outpos = int(c)
	}

	d.parent.lock.Unlock()
	delims = delims[:outpos]
	if len(delims) == 0 {
		return nil
	}
	// delims are absolute now, so write relative to vmm
	return d.dst.writeRows(delims)
}

func (d *deduper) Close() error {
	d.bc.reset()
	return d.dst.Close()
}
