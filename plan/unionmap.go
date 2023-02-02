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
	"fmt"
	"sync"

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
				subep := &ExecParams{
					Output:   s,
					Parallel: ep.Parallel, // ...meaningful?
					Context:  ep.Context,
				}
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
				errors[i] = sub.Exec(stub, subep)
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
}

func (u *UnionMap) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("unionmap", dst, st)
	dst.EndStruct()
	return nil
}

func (u *UnionMap) setfield(d Decoder, f ion.Field) error {
	return errUnexpectedField
}

func (u *UnionMap) String() string { return "UNION MAP" }
