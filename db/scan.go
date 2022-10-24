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

package db

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var errStop = errors.New("stop walking")

// Scan performs an incremental append operation
// on a table by listing input objects and adding them
// to the index. Scan returns the number of objects
// added to the table or an error. If Scan returns (0, nil),
// then scanning has already completed and no further
// calls to Scan are necessary to build the table.
//
// Semantically, Scan performs a list operation and a call
// to b.Append on the listed items, taking care to list
// incrementally from the last call to Append.
//
// Scan does not support templated table names.
// The table name in the table definition is
// assumed to be the literal table name.
//
// This method is deprecated and only used in
// tests. It may be removed in the future.
func (b *Builder) Scan(who Tenant, db, table string) (int, error) {
	ifs, err := who.Root()
	if err != nil {
		return 0, err
	}
	root, err := OpenDefinition(ifs, db)
	if err != nil {
		return 0, err
	}
	def := root.Get(table)
	if def == nil {
		return 0, fmt.Errorf("no such table: %s", table)
	}
	st, err := b.open(db, table, def, who)
	if err != nil {
		return 0, err
	}
	idx, err := st.index(nil)
	if err != nil {
		// if the index isn't present
		// or is out-of-date, create a new one
		if shouldRebuild(err) {
			idx = &blockfmt.Index{
				Name: table,
				Algo: "zstd",
			}
		} else {
			return 0, err
		}
	}
	return st.scan(idx, true)
}

// defChanged returns whether st.def has changed
// since the index was built by comparing the
// hash of st.def against the hash in idx.
func (st *tableState) defChanged(idx *blockfmt.Index) bool {
	hash, ok := idx.UserData.Field("definition").Field("hash").Blob()
	return !ok || !bytes.Equal(st.def.Hash(), hash)
}

func (st *tableState) scan(idx *blockfmt.Index, flushOnComplete bool) (int, error) {
	changed := st.defChanged(idx)
	if changed {
		flushOnComplete = true
	}
	if changed || len(idx.Cursors) != len(st.def.Inputs) {
		idx.LastScan = date.Now()
		idx.Cursors = make([]string, len(st.def.Inputs))
		idx.Scanning = true
	}
	if !idx.Scanning {
		return 0, nil
	}
	idx.Inputs.Backing = st.ofs

	var collect []blockfmt.Input
	maxSize := st.conf.MaxScanBytes
	if maxSize <= 0 {
		maxSize = math.MaxInt64
	}
	maxInputs := st.conf.MaxScanObjects
	if maxInputs <= 0 {
		maxInputs = math.MaxInt
	}

	size := int64(0)
	complete := true
	id := st.conf.nextID(idx)
	var mr matcher
	for i := range st.def.Inputs {
		in := &st.def.Inputs[i]
		if len(collect) >= maxInputs || size >= maxSize {
			complete = false
			break
		}
		infs, pat, err := st.owner.Split(in.Pattern)
		if err != nil {
			// invalid definition?
			return 0, err
		}
		format := in.Format
		seek := idx.Cursors[i]
		prefix := infs.Prefix()
		template := st.def.Name
		walk := func(p string, f fs.File, err error) error {
			if err != nil {
				return err
			}
			err = mr.match(pat, p, template)
			if err != nil {
				return err
			}
			if !mr.found || string(mr.result) != st.table {
				return nil
			}
			info, err := f.Stat()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			etag, err := infs.ETag(p, info)
			if err != nil {
				f.Close()
				return err
			}

			full := prefix + p
			ret, err := idx.Inputs.Append(full, etag, id)
			if err != nil {
				// FIXME: on ErrETagChanged, force a rebuild?
				// For now, don't get wedged:
				if errors.Is(err, blockfmt.ErrETagChanged) {
					return nil
				}
				return err
			}
			if !ret {
				// file is not new
				seek = p
				return nil
			}
			fm, err := st.conf.Format(format, p, st.def.Inputs[i].Hints)
			if err != nil {
				return err
			}
			if fm == nil {
				// TODO: insist that definitions contain
				// patterns that make the format of any
				// matching file unambiguous
				return fmt.Errorf("couldn't determine format of file %s", p)
			}
			size += info.Size()
			collect = append(collect, blockfmt.Input{
				Path: full,
				Size: info.Size(),
				ETag: etag,
				R:    f,
				F:    fm,
			})
			seek = p
			if len(collect) >= maxInputs || size >= maxSize {
				return errStop
			}
			return nil
		}
		glob, err := toglob(pat)
		if err != nil {
			return 0, fmt.Errorf("%w: %q", err, pat)
		}
		err = fsutil.WalkGlob(infs, seek, glob, walk)
		idx.Cursors[i] = seek
		if err == errStop {
			complete = false
			break
		} else if err != nil {
			return 0, err
		}
	}
	idx.Scanning = !complete
	if len(collect) == 0 {
		if idx.Scanning {
			panic("should not be possible: idx.Scanning && len(collect) == 0")
		}
		if flushOnComplete {
			return 0, st.flushScanDone(idx.Cursors)
		}
		return 0, nil
	}
	prepend := st.conf.popPrepend(idx)
	err := st.force(idx, prepend, collect, nil)
	if err != nil {
		return 0, err
	}
	return len(collect), nil
}

// mark scanning=false and set the cursors
// to their final values; we do this by re-loading
// the index so that any modifications we made
// along the way (other than scanning for cursors)
// are discarded
func (st *tableState) flushScanDone(cursors []string) error {
	old, err := st.index(nil)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			old = &blockfmt.Index{
				Name: st.table,
				Algo: "zstd",
			}
		} else {
			return fmt.Errorf("scan: flushScanDone: %w", err)
		}
	}
	old.Scanning = false
	old.Cursors = cursors
	return st.flush(old)
}
