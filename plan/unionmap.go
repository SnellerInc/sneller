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

func doSplit(th TableHandle) (Subtables, error) {
	if sp, ok := th.(SplitHandle); ok {
		return sp.Split()
	}
	hs, ok := th.(tableHandles)
	if !ok {
		return SubtableList{{
			Transport: &LocalTransport{},
			Handle:    th,
		}}, nil
	}
	var out Subtables
	for i := range hs {
		sub, err := doSplit(hs[i])
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = sub
		} else {
			out = out.Append(sub)
		}
	}
	return out, nil
}

func (u *UnionMap) wrap(dst vm.QuerySink, ep *ExecParams) func(TableHandle) error {
	// we need to capture the rewriting state,
	// since we can't actually perform rewriting
	// until we have split the incoming handle
	orig := ep
	ep = ep.clone()

	// on execution, split the handle and then dispatch to u.From
	return func(h TableHandle) error {
		tbls, err := doSplit(h)
		if err != nil {
			return err
		}
		if tbls.Len() == 0 {
			// write no data
			var b ion.Buffer
			var st ion.Symtab
			st.Marshal(&b, true)
			return writeIon(&b, dst)
		}
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
		errors := make([]error, tbls.Len())
		var wg sync.WaitGroup
		wg.Add(tbls.Len())
		for i := 0; i < tbls.Len(); i++ {
			go func(i int) {
				defer wg.Done()
				var sub Subtable
				tbls.Subtable(i, &sub)
				// wrap the rest of the query in a Tree;
				// this makes it look to the Transport
				// like we are executing a sub-query, which
				// is approximately true
				stub := &Tree{
					Inputs: []Input{{
						Handle: sub.Handle,
					}},
					Root: Node{
						Op:    u.From,
						Input: 0,
					},
				}
				subep := ep.clone()
				subep.Output = s
				// subep.get will be clobbered by Exec here:
				errors[i] = sub.Exec(stub, subep)
				orig.Stats.atomicAdd(&subep.Stats)
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
}

func (u *UnionMap) encode(dst *ion.Buffer, st *ion.Symtab, _ expr.Rewriter) error {
	dst.BeginStruct(-1)
	settype("unionmap", dst, st)
	dst.EndStruct()
	return nil
}

func (u *UnionMap) setfield(d Decoder, f ion.Field) error {
	return errUnexpectedField
}

func (u *UnionMap) String() string { return "UNION MAP" }

type UnionPartition struct {
	Nonterminal
	By []string
}

func (u *UnionPartition) encode(dst *ion.Buffer, st *ion.Symtab, _ expr.Rewriter) error {
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

func (u *UnionPartition) setfield(d Decoder, f ion.Field) error {
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

// TablePart represents part of a table
// split on a particular set of partitions.
//
// TablePart implements expr.Rewriter in order
// to rewrite PARTITION_VALUE() expresions into
// the corresponding Parts constants.
type TablePart struct {
	Handle TableHandle
	Parts  []ion.Datum
}

func (t *TablePart) Walk(e expr.Node) expr.Rewriter {
	return t
}

func (t *TablePart) Rewrite(e expr.Node) expr.Node {
	b, ok := e.(*expr.Builtin)
	if !ok || b.Func != expr.PartitionValue {
		return e
	}
	id := int(b.Args[0].(expr.Integer))
	return mustConst(t.Parts[id])
}

// PartitionHandle is an optional interface implemented
// by TableHandle for partitions.
type PartitionHandle interface {
	// SplitBy should split the table on the given partition(s)
	// and return one TablePart for each unique partition tuple.
	// Each TablePart[*].Parts datum should correspond to
	// the list of parts provided to SplitBy.
	// The returned slice of TableParts should have at least one element.
	SplitBy(parts []string) ([]TablePart, error)
}

func (u *UnionPartition) String() string {
	return fmt.Sprintf("UNION PARTITION %v", u.By)
}

func (u *UnionPartition) wrap(dst vm.QuerySink, ep *ExecParams) func(TableHandle) error {
	if len(u.By) == 0 {
		return delay(fmt.Errorf("plan: UnionPartition: 0 partitions to split?"))
	}
	return func(h TableHandle) error {
		ph, ok := h.(PartitionHandle)
		if !ok {
			return fmt.Errorf("plan: UnionPartition: handle %T cannot be partitioned", h)
		}
		parts, err := ph.SplitBy(u.By)
		if err != nil {
			return err
		}
		if len(parts) == 0 {
			return fmt.Errorf("plan: UnionPartition: handle %T by %v produced 0 parts?", h, u.By)
		}
		var wg sync.WaitGroup
		errs := make([]error, len(parts))
		for i := range parts {
			w, err := dst.Open()
			if err != nil {
				return err
			}
			subep := &ExecParams{
				Output:   w,
				Parallel: ep.Parallel,
				Context:  ep.Context,
				Rewriter: ep.Rewriter,
			}
			// stack the PARTITION_VALUE() rewrite
			subep.AddRewrite(&parts[i])
			into := vm.LockedSink(w)
			inner := u.From.wrap(into, subep)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := inner(parts[i].Handle)
				err2 := w.Close()
				if err == nil || errors.Is(err, io.EOF) {
					err = err2
				}
				errs[i] = err
				ep.Stats.atomicAdd(&subep.Stats)
			}(i)
		}
		wg.Wait()
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
}
