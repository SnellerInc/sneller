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
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/fsutil"
)

type multiIndex []Index

// TimeRange returns the union of time ranges for p in
// all the contained indexes.
func (m multiIndex) TimeRange(p []string) (min, max date.Time, ok bool) {
	for i := range m {
		min0, max0, ok := m[i].TimeRange(p)
		if !ok {
			return date.Time{}, date.Time{}, false
		}
		if i == 0 {
			min, max = min0, max0
			continue
		}
		if min0.Before(min) {
			min = min0
		}
		if max0.After(max) {
			max = max0
		}
	}
	return min, max, len(m) > 0
}

func (m multiIndex) HasPartition(x string) bool {
	for i := range m {
		if !m[i].HasPartition(x) {
			return false
		}
	}
	return true
}

// TableLister is an interface an Env or Index can
// optionally implement to support TABLE_GLOB and
// TABLE_PATTERN expressions.
type TableLister interface {
	// ListTables returns the names of tables in
	// the given db. Callers must not modify the
	// returned list.
	ListTables(db string) ([]string, error)
}

func statGlob(tl TableLister, env Env, e *expr.Builtin, h *Hints) (*Input, error) {
	db, m, err := compileGlob(e)
	if err != nil {
		return nil, err
	}
	if m, ok := m.(literalMatcher); ok {
		return env.Stat(mkpath(db, string(m)), h)
	}
	list, err := tl.ListTables(db)
	if err != nil {
		return nil, err
	}
	ret := new(Input)
	matches := 0
	for i := range list {
		if !m.MatchString(list[i]) {
			continue
		}
		in, err := env.Stat(mkpath(db, list[i]), h)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		matches++
		ret.Append(in)
	}
	if matches == 0 {
		return nil, fs.ErrNotExist
	}
	return ret, nil
}

func indexGlob(tl TableLister, idx Indexer, e *expr.Builtin) (Index, error) {
	db, m, err := compileGlob(e)
	if err != nil {
		return nil, err
	}
	if m, ok := m.(literalMatcher); ok {
		return idx.Index(mkpath(db, string(m)))
	}
	list, err := tl.ListTables(db)
	if err != nil {
		return nil, err
	}
	mi := make(multiIndex, 0, len(list))
	for i := range list {
		if !m.MatchString(list[i]) {
			continue
		}
		idx, err := idx.Index(mkpath(db, list[i]))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		} else if idx != nil {
			mi = append(mi, idx)
		}
	}
	switch len(mi) {
	case 0:
		return nil, fs.ErrNotExist
	case 1:
		return mi[0], nil
	default:
		return mi, nil
	}
}

func mkpath(db, tbl string) expr.Node {
	if db == "" {
		return expr.Ident(tbl)
	}
	return &expr.Dot{Inner: expr.Ident(db), Field: tbl}
}

type matcher interface {
	MatchString(string) bool
}

type literalMatcher string

func (m literalMatcher) MatchString(s string) bool { return s == string(m) }

type globMatcher string

func (m globMatcher) MatchString(s string) bool {
	ok, _ := path.Match(string(m), s)
	return ok
}

// compileGlob compiles a matcher from a TABLE_GLOB or
// TABLE_PATTERN builtin.
func compileGlob(bi *expr.Builtin) (db string, m matcher, err error) {
	switch bi.Func {
	case expr.TableGlob:
		db, str, ok := splitGlobArg(bi.Args)
		if !ok {
			return "", nil, fmt.Errorf("invalid argument(s) to TABLE_GLOB")
		}
		if fsutil.MetaPrefix(str) == str {
			return db, literalMatcher(str), nil
		}
		// check syntax
		if _, err := path.Match("", str); err != nil {
			return "", nil, err
		}
		return db, globMatcher(str), nil
	case expr.TablePattern:
		db, str, ok := splitGlobArg(bi.Args)
		if !ok {
			return "", nil, fmt.Errorf("invalid argument(s) to TABLE_PATTERN")
		}
		if !strings.HasPrefix(str, "^") {
			str = "^" + str
		}
		if !strings.HasSuffix(str, "$") {
			str = str + "$"
		}
		m, err := regexp.Compile(str)
		if err != nil {
			return "", nil, err
		}
		if str, full := m.LiteralPrefix(); full {
			return db, literalMatcher(str), nil
		}
		return db, m, nil
	default:
		return "", nil, fmt.Errorf("unsupported builtin: %v", bi.Func)
	}
}

func splitGlobArg(args []expr.Node) (db, str string, ok bool) {
	if len(args) != 1 {
		return "", "", false
	}
	path, ok := expr.FlatPath(args[0])
	if !ok {
		return "", "", false
	}
	if len(path) == 1 {
		return "", path[0], true
	}
	if len(path) != 2 {
		return "", "", false
	}
	return path[0], path[1], true
}
