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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type tableHandles []TableHandle

func (h tableHandles) Open(ctx context.Context) (vm.Table, error) {
	ts := make(tables, len(h))
	for i := range h {
		t, err := h[i].Open(ctx)
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

func decodeHandles(d Decoder, st *ion.Symtab, mem []byte) (TableHandle, error) {
	var ths tableHandles
	ion.UnpackList(mem, func(mem []byte) error {
		th, err := decodeHandle(d, st, mem)
		if err != nil {
			return err
		}
		ths = append(ths, th)
		return nil
	})
	return ths, nil
}

type tables []vm.Table

var _ CachedTable = tables(nil)

func sum(t tables, fn func(ct CachedTable) int64) int64 {
	h := int64(0)
	for i := range t {
		if ct, ok := t[i].(CachedTable); ok {
			h += fn(ct)
		}
	}
	return h
}

func (t tables) Hits() int64   { return sum(t, CachedTable.Hits) }
func (t tables) Misses() int64 { return sum(t, CachedTable.Misses) }
func (t tables) Bytes() int64  { return sum(t, CachedTable.Bytes) }

func (t tables) WriteChunks(dst vm.QuerySink, parallel int) error {
	sink, err := newMultiSink(dst, parallel)
	if err != nil {
		return err
	}
	for i := range t {
		err := t[i].WriteChunks(sink, parallel)
		if err != nil && !errors.Is(err, io.EOF) {
			sink.closeAll()
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
		esw, _ := wc.(vm.EndSegmentWriter)
		s.mw = append(s.mw, multiWriter{wc: wc, esw: esw})
	}
	return s, nil
}

func (s *multiSink) Open() (io.WriteCloser, error) {
	i := int(atomic.AddInt64(&s.idx, 1)) - 1
	if i >= len(s.mw) {
		return nil, fmt.Errorf("too many calls to Open (max %d)", len(s.mw))
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

	// cached assertion of w to vm.EndSegmentWriter
	esw vm.EndSegmentWriter
}

func (w *multiWriter) Write(b []byte) (n int, err error) {
	return w.wc.Write(b)
}

func (w *multiWriter) reallyClose() error {
	return w.wc.Close()
}

func (w *multiWriter) EndSegment() {
	if w.esw != nil {
		w.esw.EndSegment()
	}
}

func (w *multiWriter) Close() error {
	return nil
}

// A teeSink is a vm.QuerySink implementation
// that writes all written rows to each of the
// provided sinks in sequence.
type teeSink struct {
	dst []vm.QuerySink
}

// appendSink appends sink to sinks, producing a
// teeSink for multiple sinks. nil parameters
// are handled appropriately.
func appendSink(sinks, sink vm.QuerySink) vm.QuerySink {
	if sink == nil {
		return sinks
	}
	if sinks == nil {
		return sink
	}
	if ts, ok := sink.(*teeSink); ok {
		for i := range ts.dst {
			sinks = appendSink(sinks, ts.dst[i])
		}
		return sinks
	}
	if ts, ok := sinks.(*teeSink); ok {
		ts.dst = append(ts.dst, sink)
		return ts
	}
	return &teeSink{dst: []vm.QuerySink{sinks, sink}}
}

func (s *teeSink) Open() (io.WriteCloser, error) {
	if len(s.dst) == 0 {
		return eofWriter{}, nil
	}
	w, err := s.dst[0].Open()
	if err != nil {
		return nil, err
	}
	if len(s.dst) == 0 {
		return w, err
	}
	tw := vm.NewTeeWriter(w, func(int64, error) { w.Close() })
	for i := 1; i < len(s.dst); i++ {
		w, err := s.dst[i].Open()
		if err != nil {
			tw.Close()
			return nil, err
		}
		tw.Add(w, func(int64, error) { w.Close() })
	}
	return tw, nil
}

func (s *teeSink) Close() error {
	var err error
	for i := range s.dst {
		err = appenderr(err, s.dst[i].Close())
	}
	return err
}

type eofWriter struct{}

func (eofWriter) Write([]byte) (int, error) { return 0, io.EOF }
func (eofWriter) Close() error              { return nil }
func (eofWriter) EndSegment()               {}

// TableLister is an interface an Env or Index can
// optionally implement to support TABLE_GLOB and
// TABLE_PATTERN expressions.
type TableLister interface {
	// ListTables returns the names of tables in
	// the given db. Callers must not modify the
	// returned list.
	ListTables(db string) ([]string, error)
}

func statGlob(tl TableLister, env Env, e *expr.Builtin, h *Hints) (TableHandle, error) {
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
	ts := make(tableHandles, 0, len(list))
	for i := range list {
		if !m.MatchString(list[i]) {
			continue
		}
		th, err := env.Stat(mkpath(db, list[i]), h)
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

func appenderr(outerr, err error) error {
	if outerr == nil {
		return err
	}
	if err == nil {
		return outerr
	}
	return fmt.Errorf("%w and %s", outerr, err)
}

// appenderrs calls appenderr for each of errs.
func appenderrs(outerr error, errs []error) error {
	for i := range errs {
		outerr = appenderr(outerr, errs[i])
	}
	return outerr
}
