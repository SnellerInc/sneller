// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package plan

import (
	"context"
	"io"
	"io/fs"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
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
		in[i].desc = &src.Descs[i].Descriptor
		in[i].blks = src.Descs[i].Blocks.Clone()
	}
	tbl := readerTable{
		fs:     r.FS,
		in:     in,
		fields: src.Fields,
	}
	// fast-path for local files: use mmap for reading
	if dfs, ok := r.FS.(*blockfmt.DirFS); ok {
		for i := range in {
			in[i].mapped, _ = dfs.Mmap(in[i].desc.Path)
		}
		defer func() {
			for i := range in {
				if in[i].mapped != nil {
					dfs.Unmap(in[i].mapped)
				}
			}
		}()
	}
	err := tbl.WriteChunks(dst, ep.Parallel)
	ep.Stats.Observe(&tbl)
	return err
}

type readerInput struct {
	desc   *blockfmt.Descriptor
	blks   ints.Intervals
	mapped []byte
}

type readerTable struct {
	fs      fs.FS
	in      []readerInput
	fields  []string
	idx     int
	lock    sync.Mutex
	scanned int64
}

func (f *readerTable) next() (in *readerInput, off int) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for f.idx < len(f.in) {
		in = &f.in[f.idx]
		if off, ok := in.blks.Next(); ok {
			return in, off
		}
		f.idx = f.idx + 1
	}
	return nil, 0
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
		in, off := f.next()
		if in == nil {
			break
		}
		d.Set(&in.desc.Trailer)
		pos := in.desc.Trailer.Blocks[off].Offset
		end := in.desc.Trailer.Offset
		if off < len(in.desc.Trailer.Blocks)-1 {
			end = in.desc.Trailer.Blocks[off+1].Offset
		}
		size := int64(in.desc.Trailer.Blocks[off].Chunks) << d.BlockShift
		var err error
		if in.mapped != nil {
			_, err = d.CopyBytes(dst, in.mapped[pos:end])
		} else {
			var src io.ReadCloser
			src, err = fsutil.OpenRange(f.fs, in.desc.Path, in.desc.ETag, pos, end-pos)
			if err != nil {
				return err
			}
			_, err = d.Copy(dst, src)
			src.Close()
		}
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
