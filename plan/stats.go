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
	Bytes() int64
}

func (e *ExecStats) atomicAdd(tmp *ExecStats) {
	atomic.AddInt64(&e.CacheHits, tmp.CacheHits)
	atomic.AddInt64(&e.CacheMisses, tmp.CacheMisses)
	atomic.AddInt64(&e.BytesScanned, tmp.BytesScanned)
}

func (e *ExecStats) Observe(table vm.Table) {
	ct, ok := table.(CachedTable)
	if !ok {
		return
	}
	atomic.AddInt64(&e.CacheHits, ct.Hits())
	atomic.AddInt64(&e.CacheMisses, ct.Misses())
	atomic.AddInt64(&e.BytesScanned, ct.Bytes())
}

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
	_, err := ion.UnpackStruct(st, buf, func(name string, body []byte) error {
		var err error
		switch name {
		case "hits":
			e.CacheHits, _, err = ion.ReadInt(body)
		case "misses":
			e.CacheMisses, _, err = ion.ReadInt(body)
		case "scanned":
			e.BytesScanned, _, err = ion.ReadInt(body)
		default:
			return errUnexpectedField
		}
		return err
	})

	if err != nil {
		return fmt.Errorf("plan.ExecStats.Decode: %w", err)
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
