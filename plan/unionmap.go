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
	"sync"
	"sync/atomic"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// UnionMap is an op that gathers
// a collection of sub-operations and
// yields their results (in any order,
// and without deduplication)
type UnionMap struct {
	Nonterminal

	// Geometry determines how table handle inputs
	// are distributed onto the constituent partials
	Geometry *Geometry
}

var (
	transLock     sync.Mutex
	transDecoders map[string]func() TransportDecoder
)

func init() {
	AddTransportDecoder("local", func() TransportDecoder {
		return new(LocalTransport)
	})
}

// AddTransportDecoder adds function which
// returns a new TransportDecoder for the given
// type name.
func AddTransportDecoder(typ string, fn func() TransportDecoder) {
	transLock.Lock()
	defer transLock.Unlock()
	if transDecoders == nil {
		transDecoders = make(map[string]func() TransportDecoder)
	}
	transDecoders[typ] = fn
}

// TransportDecoder is a transport that can be
// decoded from an ion struct.
type TransportDecoder interface {
	ion.FieldSetter
	Transport
}

func getDecoder(name string) (TransportDecoder, bool) {
	transLock.Lock()
	defer transLock.Unlock()
	fn := transDecoders[name]
	if fn == nil {
		return nil, false
	}
	return fn(), true
}

// EncodeTransport attempts to encode t to buf. If t
// cannot be serialized, this returns an error.
func EncodeTransport(t Transport, st *ion.Symtab, buf *ion.Buffer) error {
	type encoder interface {
		Encode(dst *ion.Buffer, st *ion.Symtab)
	}
	enc, ok := t.(encoder)
	if !ok {
		return fmt.Errorf("cannot serialize %T", t)
	}
	enc.Encode(buf, st)
	return nil
}

// DecodeTransport decodes a transport encoded with
// EncodeTransport.
func DecodeTransport(d ion.Datum) (Transport, error) {
	return ion.UnpackTyped(d, getDecoder)
}

func (l *LocalTransport) SetField(f ion.Field) error {
	switch f.Label {
	case "threads":
		i, err := f.Int()
		if err != nil {
			return err
		}
		l.Threads = int(i)
	default:
		return errUnexpectedField
	}
	return nil
}

func (u *UnionMap) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	if u.Geometry == nil {
		return fmt.Errorf("plan.UnionMap: Geometry is nil")
	}
	in := src.HashSplit(len(u.Geometry.Peers))
	w, err := dst.Open()
	if err != nil {
		return err
	}
	s := vm.Locked(w)

	// NOTE: the heuristic here at the momement
	// is that the reduction step of sub-queries
	// does not benefit substantially from having
	// parallelism, so we union all the output bytes
	// into a single thread here
	errors := make([]error, len(in))
	var wg sync.WaitGroup
	for i := range in {
		if in[i] == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tp := u.Geometry.Peers[i]
			// wrap the rest of the query in a Tree;
			// this makes it look to the Transport
			// like we are executing a sub-query, which
			// is approximately true
			subep := ep.clone()
			subep.Plan = &Tree{
				Inputs: in[i : i+1],
				Data:   ep.Plan.Data,
				Root: Node{
					Op:    u.From,
					Input: 0,
				},
			}
			subep.Output = s
			// subep.get will be clobbered by Exec here:
			errors[i] = tp.Exec(subep)
			ep.Stats.atomicAdd(&subep.Stats)
		}(i)
	}
	wg.Wait()
	for i := range errors {
		if errors[i] != nil {
			err = errors[i]
			break
		}
	}
	err2 := w.Close()
	err3 := dst.Close()
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	return err
}

func (u *UnionMap) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("unionmap", dst, st)
	if u.Geometry != nil {
		dst.BeginField(st.Intern("geometry"))
		if err := u.Geometry.encode(dst, st); err != nil {
			return err
		}
	}
	dst.EndStruct()
	return nil
}

func (u *UnionMap) SetField(f ion.Field) error {
	switch f.Label {
	case "geometry":
		g, err := decodeGeometry(f.Datum)
		if err != nil {
			return err
		}
		u.Geometry = g
	default:
		return errUnexpectedField
	}
	return nil
}

func (u *UnionMap) String() string { return "UNION MAP" }

type UnionPartition struct {
	Nonterminal
	By []string
}

func (u *UnionPartition) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("union_partition", dst, st)
	dst.BeginField(st.Intern("by"))
	dst.BeginList(-1)
	for i := range u.By {
		dst.WriteString(u.By[i])
	}
	dst.EndList()
	dst.EndStruct()
	return nil
}

func (u *UnionPartition) SetField(f ion.Field) error {
	switch f.Label {
	case "by":
		return f.UnpackList(func(d ion.Datum) error {
			str, err := d.String()
			if err != nil {
				return err
			}
			u.By = append(u.By, str)
			return nil
		})
	default:
		return errUnexpectedField
	}
}

// tablePart represents part of a table
// split on a particular set of partitions.
//
// tablePart implements expr.Rewriter in order
// to rewrite PARTITION_VALUE() expresions into
// the corresponding Parts constants.
type tablePart struct {
	contents *Input
	parts    []ion.Datum
}

func (t *tablePart) Walk(e expr.Node) expr.Rewriter {
	return t
}

func (t *tablePart) Rewrite(e expr.Node) expr.Node {
	b, ok := e.(*expr.Builtin)
	if !ok || b.Func != expr.PartitionValue {
		return e
	}
	id := int(b.Args[0].(expr.Integer))
	return mustConst(t.parts[id])
}

func (t *tablePart) size() int64 {
	n := int64(0)
	for i := range t.contents.Descs {
		n += t.contents.Descs[i].Descriptor.Size
	}
	return n
}

func (u *UnionPartition) String() string {
	return fmt.Sprintf("UNION PARTITION %v", u.By)
}

func (u *UnionPartition) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	if len(u.By) == 0 {
		return fmt.Errorf("plan: UnionPartition: 0 partitions to split?")
	}
	groups, ok := src.Partition(u.By)
	if !ok {
		return fmt.Errorf("plan: UnionPartition: input cannot be partitioned")
	}
	if groups.Groups() == 0 {
		// we can get 0 parts if a predicate
		// removes all the blocks
		nop := NoOutput{}
		return nop.exec(dst, src, ep)
	}
	var parts []tablePart
	groups.Each(func(lst []ion.Datum, i *Input) {
		parts = append(parts, tablePart{
			contents: i,
			parts:    lst,
		})
	})

	var wg sync.WaitGroup
	errs := make([]error, len(parts))
	parallel := distribute(parts, ep.Parallel)
	for i := range parts {
		mw, err := newOpenSink(dst, parallel[i])
		if err != nil {
			if i > 0 {
				dst.Close()
			}
			return err
		}
		subep := ep.clone()
		subep.Parallel = mw.Len()
		subep.Output = nil
		// stack the PARTITION_VALUE() rewrite
		subep.AddRewrite(&parts[i])
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := u.From.exec(mw, parts[i].contents, subep)
			errs[i] = err
			ep.Stats.atomicAdd(&subep.Stats)
		}(i)
	}
	wg.Wait()
	var err error
	for i := range errs {
		if errs[i] != nil && !errors.Is(errs[i], io.EOF) {
			err = errs[i]
			break
		}
	}
	err2 := dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

// produce a histogram with a sum equal to value
// with each element proportional to h[i]
func distribute(h []tablePart, value int) []int {
	hist := make([]int64, len(h))
	out := make([]int, len(h))
	sum := int64(0)
	for i := range h {
		sz := h[i].size()
		if sz == 0 {
			sz = 1
		}
		hist[i] = sz
		sum += hist[i]
	}
	total := value
	for i := range out {
		// distribute proportional share of value
		n := int((hist[i] * int64(value)) / sum)
		if n <= 0 {
			n = 1
		}
		out[i] += n
		total -= n
	}
	if total > 0 {
		// distribute any remaining bits
		out[len(out)-1] += total
	}
	return out
}

// openSink is a QuerySink that collects a list
// of io.WriteClosers from src (up to max) and stores
// them internally. If max is less than or equal to zero,
// then max is reset to 1.
type openSink struct {
	cache []io.WriteCloser
	pos   atomic.Int32
}

// Len returns the number of internal cached io.WriteClosers.
func (m *openSink) Len() int { return len(m.cache) }

// Open returns the next cached io.WriteCloser,
// or (nil, io.EOF) if Open has been called more
// than m.Len times.
func (m *openSink) Open() (io.WriteCloser, error) {
	n := m.pos.Add(1) - 1
	if int(n) >= len(m.cache) {
		return nil, io.EOF
	}
	return m.cache[n], nil
}

// Close closes any cached io.WriteClosers
// that were not returned from Open.
func (m *openSink) Close() error {
	n := int(m.pos.Load())
	for i := n; i < len(m.cache); i++ {
		m.cache[i].Close()
	}
	return nil
}

func newOpenSink(src vm.QuerySink, max int) (*openSink, error) {
	if max <= 0 {
		max = 1
	}
	var cache []io.WriteCloser
	for i := 0; i < max; i++ {
		w, err := src.Open()
		if err != nil {
			if len(cache) == 0 {
				return nil, err
			}
			break
		}
		cache = append(cache, w)
	}
	return &openSink{cache: cache}, nil
}
