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
	"fmt"
	"io"
	"io/fs"
	"runtime"
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"

	"golang.org/x/exp/slices"
)

// Runner is the caller-provided interface through
// which table data is actually written into a vm.QuerySink.
// This interface exists in order to allow callers to have
// fine-grained control over data access patterns during
// query plan execution (for e.g. caching, etc.)
//
// See also [ExecParams].
type Runner interface {
	// Run should write the contents of src into dst
	// and update ep.Stats as it does so.
	Run(dst vm.QuerySink, src *Input, ep *ExecParams) error
}

// FSRunner is an implementation of Runner over
// a file system.
type FSRunner struct {
	fs.FS
}

// Run implements Runner.Run
func (r *FSRunner) Run(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	in := make([]readerInput, len(src.Descs))
	for i := range src.Descs {
		in[i].t = &src.Descs[i].Trailer
		f, err := r.Open(src.Descs[i].Path)
		if err != nil {
			return err
		}
		defer f.Close()
		in[i].src = f
	}
	tbl := readerTable{
		in:     in,
		fields: src.Fields,
		blocks: src.Blocks,
	}
	err := tbl.WriteChunks(dst, ep.Parallel)
	ep.Stats.Observe(&tbl)
	return err
}

type readerInput struct {
	t   *blockfmt.Trailer
	src io.ReadCloser
}

type readerTable struct {
	in []readerInput

	fields []string

	blocks  []blockfmt.Block
	block   int64
	scanned int64
}

// IfMatcher can be implemented by a file that
// supports ETag matching using semantics
// compatible with the HTTP "If-Match" header.
type IfMatcher interface {
	// IfMatch is called to specify that the file
	// should be checked against the given ETag
	// and return an error if it doesn't match.
	//
	// If the ETag does not match, the error may
	// be returned at the time IfMatch is called,
	// on read, or at close time.
	IfMatch(etag string) error
}

// RangeReader can be implemented by a file that
// supports requesting a subrange of the file.
// This can be more efficient than using utilies
// from the io package to do the same thing, for
// example due to the implementation's ability
// to specify a "Range" HTTP header.
type RangeReader interface {
	RangeReader(off, width int64) (io.ReadCloser, error)
}

// SectionReader produces a reader which reads
// from [src] at the given offset [off] limited
// to [n] bytes.
//
// This function is exported to support [Runner]
// implementations that make use of optional
// interfaces in the same way that [FSRunner]
// does and may disappear in the future.
func SectionReader(src io.ReadCloser, off, n int64) (io.ReadCloser, error) {
	if rr, ok := src.(RangeReader); ok {
		return rr.RangeReader(off, n)
	}
	if rd, ok := src.(io.ReaderAt); ok {
		return io.NopCloser(io.NewSectionReader(rd, off, n)), nil
	}
	if src, ok := src.(io.ReadSeekCloser); ok {
		_, err := src.Seek(off, io.SeekStart)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(io.LimitReader(src, n)), nil
	}
	return nil, fmt.Errorf("cannot make section reader from %T", src)
}

func vmMalloc(size int) []byte {
	if size > vm.PageSize {
		panic("size > vm.PageSize")
	}
	return vm.Malloc()[:size]
}

func (f *readerTable) write(dst io.Writer) error {
	var d blockfmt.Decoder
	d.Malloc = vmMalloc
	d.Free = vm.Free
	d.Fields = f.fields
	for {
		i := atomic.AddInt64(&f.block, 1) - 1
		if i >= int64(len(f.blocks)) {
			break
		}
		idx := f.blocks[i].Index
		off := f.blocks[i].Offset
		pos := f.in[idx].t.Blocks[off].Offset
		d.BlockShift = f.in[idx].t.BlockShift
		d.Algo = f.in[idx].t.Algo
		d.Offset = pos
		end := f.in[idx].t.Offset
		if off < len(f.in[idx].t.Blocks)-1 {
			end = f.in[idx].t.Blocks[off+1].Offset
		}
		size := int64(f.in[idx].t.Blocks[off].Chunks) << d.BlockShift
		src, err := SectionReader(f.in[idx].src, pos, end-pos)
		if err != nil {
			return err
		}
		_, err = d.Copy(dst, src)
		src.Close()
		if err != nil {
			return err
		}
		atomic.AddInt64(&f.scanned, size)
	}
	return nil
}

var _ CachedTable = &readerTable{}

func (f *readerTable) Hits() int64   { return 0 }
func (f *readerTable) Misses() int64 { return 0 }
func (f *readerTable) Bytes() int64 {
	return f.scanned
}

func (f *readerTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	return vm.SplitInput(dst, parallel, f.write)
}

// ExecParams is a collection of all the
// runtime parameters for a query.
type ExecParams struct {
	// Plan is the query plan being executed.
	Plan *Tree
	// Output is the destination of the query output.
	Output io.Writer
	// Stats are stats that are collected
	// during query execution.
	Stats ExecStats
	// Parallel determines the (local) parallelism
	// of plan execution. If Parallel is unset, then
	// runtime.GOMAXPROCS(0) is used instead.
	Parallel int
	// Rewriter is a rewrite that should be applied
	// to each expression in the query plan before
	// the query begins execution.
	Rewriter expr.Rewriter
	// Context indicates the cancellation scope
	// of the query. Transports are expected to
	// stop processing queries after Context is canceled.
	Context context.Context
	// Runner is the local execution environment
	// for the query. If Runner is nil, then query
	// execution will fail.
	Runner Runner
	// FS is the file system to read inputs from.
	// This may implement UploadFS, which is
	// required to enable support for SELECT INTO.
	FS fs.FS

	get func(i int) *Input
}

type multiRewriter struct {
	parent, self expr.Rewriter
}

func (m *multiRewriter) Walk(e expr.Node) expr.Rewriter {
	parent := m.parent.Walk(e)
	self := m.self.Walk(e)
	if parent == nil {
		return self
	} else if self == nil {
		return parent
	}
	if parent == m.parent && self == m.self {
		return m
	}
	return &multiRewriter{parent: parent, self: self}
}

func (m *multiRewriter) Rewrite(e expr.Node) expr.Node {
	return m.self.Rewrite(m.parent.Rewrite(e))
}

// AddRewrite adds a rewrite to ep.Rewriter.
// Each rewrite added via AddRewrite is executed
// on the results produced by rewriters added from
// preceding calls to AddRewrite.
func (ep *ExecParams) AddRewrite(r expr.Rewriter) {
	if ep.Rewriter == nil {
		ep.Rewriter = r
		return
	}
	ep.Rewriter = &multiRewriter{parent: ep.Rewriter, self: r}
}

// PopRewrite removes the most-recently-added Rewriter
// added via ep.AddRewrite.
func (ep *ExecParams) PopRewrite() {
	if ep.Rewriter == nil {
		return
	}
	if mr, ok := ep.Rewriter.(*multiRewriter); ok {
		ep.Rewriter = mr.parent
	} else {
		ep.Rewriter = nil
	}
}

func (ep *ExecParams) rewrite(x expr.Node) expr.Node {
	if ep.Rewriter == nil || x == nil {
		return x
	}
	return expr.Rewrite(ep.Rewriter, expr.Copy(x))
}

func (ep *ExecParams) rewriteAll(lst []expr.Node) []expr.Node {
	if ep.Rewriter == nil {
		return lst
	}
	newlst := slices.Clone(lst)
	for i := range newlst {
		newlst[i] = expr.Rewrite(ep.Rewriter, expr.Copy(newlst[i]))
	}
	return newlst
}

func (ep *ExecParams) rewriteAgg(v vm.Aggregation) vm.Aggregation {
	if ep.Rewriter == nil {
		return v
	}
	nv := slices.Clone(v)
	for i := range nv {
		c := expr.Copy(nv[i].Expr)
		nv[i].Expr = expr.Rewrite(ep.Rewriter, c).(*expr.Aggregate)
	}
	return nv
}

func (ep *ExecParams) rewriteBind(lst []expr.Binding) []expr.Binding {
	if ep.Rewriter == nil {
		return lst
	}
	newlst := slices.Clone(lst)
	for i := range newlst {
		newlst[i].Expr = expr.Rewrite(ep.Rewriter, expr.Copy(newlst[i].Expr))
	}
	return newlst
}

// clone everything except ep.Stats
func (ep *ExecParams) clone() *ExecParams {
	return &ExecParams{
		Plan:     ep.Plan,
		Output:   ep.Output,
		Parallel: ep.Parallel,
		Context:  ep.Context,
		Rewriter: ep.Rewriter,
		Runner:   ep.Runner,
		FS:       ep.FS,
		get:      ep.get,
	}
}

// Exec executes a query plan using the
// parameters provided in [ep].
func Exec(ep *ExecParams) error {
	if ep.Parallel == 0 {
		ep.Parallel = runtime.GOMAXPROCS(0)
	}
	if ep.Context == nil {
		ep.Context = context.Background()
	}
	err := (&LocalTransport{}).Exec(ep)
	return err
}

// LocalTransport is a Transport
// that executes queries locally.
type LocalTransport struct {
	// Threads is the number of threads
	// used for query evaluation.
	// If Threads is <= 0, then runtime.GOMAXPROCS
	// is used.
	Threads int
}

func (l *LocalTransport) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	settype("local", dst, st)
	if l.Threads > 0 {
		dst.BeginField(st.Intern("threads"))
		dst.WriteInt(int64(l.Threads))
	}
	dst.EndStruct()
}

// Exec implements Transport.Exec
func (l *LocalTransport) Exec(ep *ExecParams) error {
	s := vm.LockedSink(ep.Output)
	if ep.Parallel == 0 {
		ep.Parallel = l.Threads
	}
	if ep.Parallel == 0 {
		ep.Parallel = runtime.GOMAXPROCS(0)
	}
	return ep.Plan.exec(s, ep)
}

// Transport models the exection environment
// of a query plan.
//
// See LocalTransport for executing queries locally.
// See Client for executing queries remotely.
type Transport interface {
	// Exec executes the provided query plan,
	// streaming the output of the query to ep.Output
	// (ep.Output may not be nil).
	// Each call to ep.Output.Write should contain exactly
	// one "chunk" of ion-encoded data, which will
	// begin with an ion BVM and be followed by zero
	// or more ion structures.
	//
	// The ep.Rewrite provided via ExecParams, if non-nil,
	// determines how table expressions are re-written
	// before they are provided to Transport.
	Exec(ep *ExecParams) error
}
