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
	"errors"
	"fmt"
	"io/fs"
	"math"
	"time"

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
func (b *Builder) Scan(who Tenant, db, table string) (int, error) {
	st, err := b.open(db, table, who)
	if err != nil {
		return 0, err
	}
	def, err := st.def()
	if err != nil {
		return 0, err
	}
	idx, err := st.index()
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
	return st.scan(def, idx)
}

func (st *tableState) scan(def *Definition, idx *blockfmt.Index) (int, error) {
	// TODO: better detection of change in definition
	if len(idx.Cursors) != len(def.Inputs) {
		idx.LastScan = time.Now()
		idx.Cursors = make([]string, len(def.Inputs))
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
	prepend := st.conf.popPrepend(idx)
	id := len(idx.Contents)
	for i := range def.Inputs {
		if len(collect) >= maxInputs || size >= maxSize {
			break
		}
		infs, pat, err := st.owner.Split(def.Inputs[i].Pattern)
		if err != nil {
			// invalid definition?
			return 0, err
		}
		format := def.Inputs[i].Format
		seek := idx.Cursors[i]
		prefix := infs.Prefix()
		walk := func(p string, f fs.File, err error) error {
			if err != nil {
				return err
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
			fm := st.conf.Format(format, p)
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
		err = fsutil.WalkGlob(infs, seek, pat, walk)
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
			panic("should not be possible: idx.Scannig && len(collect) == 0")
		}
		// re-append the prepended item
		if prepend != nil {
			idx.Contents = append(idx.Contents, *prepend)
		}
		return 0, st.flush(idx)
	}
	err := st.force(idx, prepend, collect)
	if err != nil {
		return 0, err
	}
	return len(collect), nil
}
