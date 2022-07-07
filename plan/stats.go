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

package plan

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// ExecStats is a collection
// of statistics that are aggregated
// during the execution of a query.
type ExecStats struct {
	// CacheHits and CacheMisses
	// are the sum of the results
	// of CachedTable.Hits() and CachedTable.Misses(),
	// respectively.
	//
	// NOTE: see tenant/dcache.Stats
	// for a detailed description of how
	// we do bookkeeping for cache statistics.
	CacheHits, CacheMisses int64
	// BytesScanned is the number
	// of bytes scanned.
	BytesScanned int64
}

// CachedTable is an interface optionally
// implemented by a vm.Table.
// If a vm.Table returned by TableHandle.Open
// implements CachedTable, then the returned
// Hits and Misses statistics will be added
// to ExecStats.CacheHits and ExecStats.CacheMisses,
// respectively.
type CachedTable interface {
	Hits() int64
	Misses() int64
}

func (e *ExecStats) atomicAdd(tmp *ExecStats) {
	atomic.AddInt64(&e.CacheHits, tmp.CacheHits)
	atomic.AddInt64(&e.CacheMisses, tmp.CacheMisses)
	atomic.AddInt64(&e.BytesScanned, tmp.BytesScanned)
}

func (e *ExecStats) observe(table vm.Table) {
	ct, ok := table.(CachedTable)
	if !ok {
		return
	}
	atomic.AddInt64(&e.CacheHits, ct.Hits())
	atomic.AddInt64(&e.CacheMisses, ct.Misses())
}

func track(into vm.QuerySink) *bytesTracker {
	return &bytesTracker{into: into}
}

// bytesTracker is a vm.QuerySink
// that tracks the number of bytes
// processed by the QuerySink
type bytesTracker struct {
	into    vm.QuerySink
	scanned int64
}

type writeTracker struct {
	w      io.WriteCloser
	parent *bytesTracker
	// cached assertion of w to vm.EndSegmentWriter
	hint vm.EndSegmentWriter
}

func (b *bytesTracker) Open() (io.WriteCloser, error) {
	dst, err := b.into.Open()
	if err != nil {
		return nil, err
	}
	h, _ := dst.(vm.EndSegmentWriter)
	return &writeTracker{
		w:      dst,
		parent: b,
		hint:   h,
	}, nil
}

func (w *writeTracker) EndSegment() {
	if w.hint != nil {
		w.hint.EndSegment()
	}
}

func (w *writeTracker) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	// NOTE: we're considering every byte
	// passed to Write as scanned, because
	// we don't really have a more precise
	// way of tracking how many bytes
	// were actually touched (the core
	// just returns len(p) after processing the block).
	// That's probably precise enough (1MB granularity)
	atomic.AddInt64(&w.parent.scanned, int64(len(p)))
	return n, err
}

func (w *writeTracker) Close() error { return w.w.Close() }

func (b *bytesTracker) Close() error { return b.into.Close() }

// Marshal is identical to Encode except
// that it uses the same symbol table
// that UnmarshalBinary expects will be used.
func (e *ExecStats) Marshal(dst *ion.Buffer) {
	e.Encode(dst, &statsSymtab)
}

// Encode encodes the stats to dst using
// the provided symbol table.
func (e *ExecStats) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	if e.CacheHits != 0 {
		dst.BeginField(st.Intern("hits"))
		dst.WriteInt(e.CacheHits)
	}
	if e.CacheMisses != 0 {
		dst.BeginField(st.Intern("misses"))
		dst.WriteInt(e.CacheMisses)
	}
	if e.BytesScanned != 0 {
		dst.BeginField(st.Intern("scanned"))
		dst.WriteInt(e.BytesScanned)
	}
	dst.EndStruct()
}

func (e *ExecStats) Decode(buf []byte, st *ion.Symtab) error {
	if len(buf) == 0 {
		return fmt.Errorf("plan.ExecStats cannot be 0 encoded bytes")
	}
	if ion.TypeOf(buf) != ion.StructType {
		return fmt.Errorf("plan.ExecStats.Decode: unexpected ion type %s", ion.TypeOf(buf))
	}
	inner, _ := ion.Contents(buf)
	if inner == nil {
		return fmt.Errorf("plan.ExecStats.Decode: invalid TLV bytes")
	}
	var err error
	var sym ion.Symbol
	for len(inner) > 0 {
		sym, inner, err = ion.ReadLabel(inner)
		if err != nil {
			return fmt.Errorf("plan.ExecStats.Decode: %w", err)
		}
		switch st.Get(sym) {
		case "hits":
			e.CacheHits, inner, err = ion.ReadInt(inner)
		case "misses":
			e.CacheMisses, inner, err = ion.ReadInt(inner)
		case "scanned":
			e.BytesScanned, inner, err = ion.ReadInt(inner)
		default:
			inner = inner[ion.SizeOf(inner):]
		}
		if err != nil {
			return fmt.Errorf("plan.ExecStats.Decode: %w", err)
		}
	}
	return nil
}

// static symbol table used for
// encoding the stats structure
// over remote transports;
// we don't need to bother with
// the symbol table overhead if
// we know we're always sending
// exactly one structure...
var statsSymtab ion.Symtab

func init() {
	for _, s := range []string{
		"hits",
		"misses",
		"scanned",
	} {
		statsSymtab.Intern(s)
	}
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (e *ExecStats) UnmarshalBinary(b []byte) error {
	return e.Decode(b, &statsSymtab)
}
