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

package sneller

import (
	"fmt"
	"hash"
	"io"
	"path"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"

	"golang.org/x/crypto/blake2b"
)

type CachedEnv interface {
	plan.Env
	CacheValues() ([]byte, time.Time)
}

type savedIndex struct {
	db, table string
	index     *blockfmt.Index
}

type savedList struct {
	db   string
	list []string
}

// FSEnv provides a plan.Env from a db.FS
type FSEnv struct {
	Root db.InputFS

	db     string
	tenant db.Tenant

	recent []savedIndex
	lists  []savedList

	// FIXME: change cachedEnv and don't
	// keep the accumulated state here:
	hash    hash.Hash
	modtime date.Time

	maxscan int64
}

func Environ(t db.Tenant, dbname string) (*FSEnv, error) {
	src, err := t.Root()
	if err != nil {
		return nil, err
	}
	h, _ := blake2b.New256(nil)
	return &FSEnv{
		tenant: t,
		Root:   src,
		db:     dbname,
		hash:   h,
	}, nil
}

func syntax(f string, args ...interface{}) error {
	return &expr.SyntaxError{
		Msg: fmt.Sprintf(f, args...),
	}
}

// CacheValues implements cachedEnv.CacheValues
func (f *FSEnv) CacheValues() ([]byte, time.Time) {
	return f.hash.Sum(nil), f.modtime.Time()
}

var _ plan.Indexer = (*FSEnv)(nil)

func (f *FSEnv) Index(p expr.Node) (plan.Index, error) {
	return f.index(p)
}

func (f *FSEnv) index(e expr.Node) (*blockfmt.Index, error) {
	var dbname, table string
	var err error

	switch e := e.(type) {
	case expr.Ident:
		dbname = f.db
		table = string(e)
	case *expr.Dot:
		id, ok := e.Inner.(expr.Ident)
		if !ok {
			return nil, syntax("trailing path expression %q in table not supported", expr.ToString(e.Inner))
		}
		dbname = string(id)
		table = e.Field
	default:
		return nil, syntax("unexpected table expression %q", expr.ToString(e))
	}
	// if a query references the same table
	// more than once (common with CTEs, nested SELECTs, etc.),
	// then don't load the index more than once; it is expensive
	for i := range f.recent {
		if f.recent[i].db == dbname && f.recent[i].table == table {
			return f.recent[i].index, nil
		}
	}
	index, err := db.OpenPartialIndex(f.Root, dbname, table, f.tenant.Key())
	if err != nil {
		return nil, err
	}
	f.recent = append(f.recent, savedIndex{
		db:    dbname,
		table: table,
		index: index,
	})
	if f.modtime.IsZero() || f.modtime.Before(index.Created) {
		f.modtime = index.Created
	}
	// FIXME: actually use the ETag of the index
	//
	// but, the modtime of the index plus its name
	// ought to be unique per-input
	io.WriteString(f.hash, path.Join(dbname, table))
	io.WriteString(f.hash, index.Created.String())
	return index, nil
}

// MaxScanned returns the maximum number of
// bytes that need to be scanned to satisfy this query.
func (f *FSEnv) MaxScanned() int64 { return f.maxscan }

// Stat implements plan.Env.Stat
func (f *FSEnv) Stat(e expr.Node, h *plan.Hints) (*plan.Input, error) {
	index, err := f.index(e)
	if err != nil {
		return nil, err
	}
	var compiled blockfmt.Filter
	compiled.Compile(h.Filter)
	descs, blocks, size, err := index.Descs(f.Root, &compiled)
	if err != nil {
		return nil, err
	}
	f.maxscan += size
	ds := make([]plan.Descriptor, 0, len(descs))
	for i := range descs {
		ds = append(ds, plan.Descriptor{
			Descriptor: descs[i],
			Blocks:     blocks[i],
		})
	}
	return &plan.Input{
		Descs:  ds,
		Fields: h.Fields,
	}, nil
}

var _ plan.TableLister = (*FSEnv)(nil)

// ListTables implements plan.TableLister.ListTables
func (f *FSEnv) ListTables(dbname string) ([]string, error) {
	if dbname == "" {
		dbname = f.db
	}
	for i := range f.lists {
		if f.lists[i].db == dbname {
			return f.lists[i].list, nil
		}
	}
	li, err := db.ListTables(f.Root, dbname)
	if err != nil {
		return nil, err
	}
	f.lists = append(f.lists, savedList{
		db:   dbname,
		list: li,
	})
	return li, nil
}

var _ plan.UploadEnv = (*FSEnv)(nil)

func (f *FSEnv) Uploader() plan.UploadFS {
	fs, _ := f.Root.(plan.UploadFS)
	return fs
}

func (f *FSEnv) Key() *blockfmt.Key {
	return f.tenant.Key()
}
