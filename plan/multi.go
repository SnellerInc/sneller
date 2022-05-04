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
	"io"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type tableHandles []TableHandle

func (h tableHandles) Open() (vm.Table, error) {
	ts := make(tables, len(h))
	for i := range h {
		t, err := h[i].Open()
		if err != nil {
			return nil, err
		}
		ts[i] = t
	}
	return ts, nil
}

func (h tableHandles) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginList(-1)
	for i := range h {
		if err := h[i].Encode(dst, st); err != nil {
			return err
		}
	}
	dst.EndList()
	return nil
}

func decodeHandles(hfn HandleDecodeFn, st *ion.Symtab, mem []byte) (TableHandle, error) {
	var ths tableHandles
	ion.UnpackList(mem, func(mem []byte) error {
		th, err := hfn.decode(st, mem)
		if err != nil {
			return err
		}
		ths = append(ths, th)
		return nil
	})
	return ths, nil
}

type tables []vm.Table

func (t tables) Chunks() int {
	total := 0
	for i := range t {
		n := t[i].Chunks()
		if n == -1 {
			return -1
		}
		total += n
	}
	return total
}

func (t tables) WriteChunks(dst vm.QuerySink, parallel int) error {
	sink, err := newMultiSink(dst, parallel)
	if err != nil {
		return err
	}
	for i := range t {
		if err := t[i].WriteChunks(sink, parallel); err != nil {
			return err
		}
		sink.reset()
	}
	return sink.closeAll()
}

type multiSink struct {
	dst io.Closer
	mw  []multiWriter
	idx int64
}

func newMultiSink(dst vm.QuerySink, parallel int) (*multiSink, error) {
	s := &multiSink{dst: dst}
	if parallel < 1 {
		parallel = 1
	}
	s.mw = make([]multiWriter, 0, parallel)
	for i := 0; i < parallel; i++ {
		wc, err := dst.Open()
		if err != nil {
			s.closeAll()
			return nil, err
		}
		s.mw = append(s.mw, multiWriter{wc: wc})
	}
	return s, nil
}

func (s *multiSink) Open() (io.WriteCloser, error) {
	i := int(atomic.AddInt64(&s.idx, 1)) - 1
	if i >= len(s.mw) {
		return nil, errors.New("too many calls to Open")
	}
	return &s.mw[i], nil
}

func (s *multiSink) Close() error {
	return s.dst.Close()
}

func (s *multiSink) reset() {
	atomic.StoreInt64(&s.idx, 0)
}

func (s *multiSink) closeAll() error {
	var err error
	for i := range s.mw {
		e := s.mw[i].reallyClose()
		if e != nil && err == nil {
			err = e
		}
	}
	return err
}

type multiWriter struct {
	wc io.WriteCloser
}

func (w *multiWriter) Write(b []byte) (n int, err error) {
	return w.wc.Write(b)
}

func (w *multiWriter) reallyClose() error {
	return w.wc.Close()
}

func (w *multiWriter) Close() error {
	return nil
}

// TableLister is an interface an Env can optionally
// implement to support TABLE_GLOB and TABLE_PATTERN
// expressions.
type TableLister interface {
	Env
	// ListTables returns the names of tables in
	// the given db. Callers must not modify the
	// returned list.
	ListTables(db string) ([]string, error)
}

func statGlob(tl TableLister, e *expr.Builtin, flt expr.Node) (TableHandle, error) {
	db, m, err := compileGlob(e)
	if err != nil {
		return nil, err
	}
	if m, ok := m.(literalMatcher); ok {
		return statTable(tl, db, string(m), flt)
	}
	list, err := tl.ListTables(db)
	if err != nil {
		return nil, err
	}
	ts := make(tableHandles, 0, len(list))
	for i := range list {
		if !m.MatchString(list[i]) {
			continue
		}
		th, err := statTable(tl, db, list[i], flt)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		ts = append(ts, th)
	}
	switch len(ts) {
	case 0:
		return nil, fs.ErrNotExist
	case 1:
		return ts[0], nil
	default:
		return ts, nil
	}
}

func statTable(env Env, db, name string, flt expr.Node) (TableHandle, error) {
	var p *expr.Path
	if db != "" {
		p = &expr.Path{First: db, Rest: &expr.Dot{Field: name}}
	} else {
		p = &expr.Path{First: name}
	}
	return env.Stat(p, flt)
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
	p, ok := args[0].(*expr.Path)
	if !ok {
		return "", "", false
	}
	if p.Rest == nil {
		return "", p.First, true
	}
	db = p.First
	dot, ok := p.Rest.(*expr.Dot)
	if !ok || dot.Rest != nil {
		return "", "", false
	}
	return db, dot.Field, true
}
