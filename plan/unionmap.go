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

	Orig *expr.Table
	Sub  Subtables
}

var (
	transLock     sync.Mutex
	transDecoders map[string]TransportDecoder
)

func init() {
	AddTransportDecoder("local", decodeLocal)
}

// AddTransportDecoder adds a decoding function
// for Tranport objects.
//
// When Decode needs to decode a Transport,
// it examines the first field of the ion
// structure, which should be of the form 'type: `name`'.
// If the value of that field matches name,
// then the provided decode function will be passed
// the bytes consisting of the body of the transport
// structure.
func AddTransportDecoder(name string, decoder TransportDecoder) {
	transLock.Lock()
	defer transLock.Unlock()
	if transDecoders == nil {
		transDecoders = make(map[string]TransportDecoder)
	}
	transDecoders[name] = decoder
}

// TransportDecoder is a function that decodes
// a Transport object from a set of ion struct fields.
//
// A TransportDecoder function must be safe to call
// from multiple goroutines simultaneously.
type TransportDecoder func(*ion.Symtab, []byte) (Transport, error)

func getDecoder(name string) TransportDecoder {
	transLock.Lock()
	defer transLock.Unlock()
	if transDecoders == nil {
		return nil
	}
	return transDecoders[name]
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
func DecodeTransport(st *ion.Symtab, body []byte) (Transport, error) {
	if ion.TypeOf(body) != ion.StructType {
		return nil, fmt.Errorf("ion object of type %s cannot be a transport", ion.TypeOf(body))
	}
	body, _ = ion.Contents(body)
	if body == nil {
		return nil, fmt.Errorf("transport body: invalid TLV bytes")
	}
	typsym, _ := st.Symbolize("type")
	sym, body, err := ion.ReadLabel(body)
	if err != nil {
		return nil, err
	}
	if sym != typsym {
		return nil, fmt.Errorf("first transport field %q not \"type\"", st.Get(sym))
	}
	sym, body, err = ion.ReadSymbol(body)
	if err != nil {
		return nil, err
	}
	name := st.Get(sym)
	if name == "" {
		return nil, fmt.Errorf("symbol %d not in symbol table", sym)
	}
	dec := getDecoder(name)
	if dec == nil {
		return nil, fmt.Errorf("no transport decoder for name %q", name)
	}
	return dec(st, body)
}

func decodeLocal(st *ion.Symtab, body []byte) (Transport, error) {
	var sym ion.Symbol
	var err error
	t := &LocalTransport{}
	for len(body) > 0 {
		sym, body, err = ion.ReadLabel(body)
		if err != nil {
			return nil, err
		}
		switch st.Get(sym) {
		case "threads":
			i, _, err := ion.ReadInt(body)
			if err != nil {
				return nil, err
			}
			t.Threads = int(i)
		}
	}
	return t, nil
}

func (u *UnionMap) exec(dst vm.QuerySink, parallel int, stats *ExecStats) error {
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
	errors := make([]error, u.Sub.Len())
	var wg sync.WaitGroup
	wg.Add(u.Sub.Len())
	for i := 0; i < u.Sub.Len(); i++ {
		go func(i int) {
			defer wg.Done()
			var sub Subtable
			u.Sub.Subtable(i, &sub)
			rw := func(in *expr.Table, handle TableHandle) (*expr.Table, TableHandle) {
				if in.Equals(u.Orig) {
					return sub.Table, sub.Handle
				}
				return in, handle
			}
			// wrap the rest of the query in a Tree;
			// this makes it look to the Transport
			// like we are executing a sub-query, which
			// is approximately true
			stub := &Tree{Op: u.From}
			errors[i] = sub.Exec(stub, rw, s, stats)
		}(i)
	}
	wg.Wait()

	// for now, just yield the first error;
	// it's not clear what we would do differently
	// if we had a whole bunch of them turn up
	for i := range errors {
		if errors[i] != nil {
			err = errors[i]
			break
		}
	}
	if err != nil {
		w.Close()
		dst.Close()
		return err
	}
	err = w.Close()
	err2 := dst.Close()
	if err == nil {
		err = err2
	}
	return err
}

func (u *UnionMap) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("unionmap", dst, st)
	dst.BeginField(st.Intern("orig"))
	u.Orig.Encode(dst, st)
	// subtables are encoded as
	//   [[transport, table-expr] ...]
	dst.BeginField(st.Intern("sub"))
	if err := u.Sub.Encode(st, dst); err != nil {
		return err
	}
	dst.EndStruct()
	return nil
}

func (u *UnionMap) setfield(d Decoder, name string, st *ion.Symtab, body []byte) error {
	switch name {
	case "orig":
		nod, _, err := expr.Decode(st, body)
		if err != nil {
			return err
		}
		t, ok := nod.(*expr.Table)
		if !ok {
			return fmt.Errorf("UnionMap.Orig: cannot use node of type %T", nod)
		}
		u.Orig = t
	case "sub":
		sub, err := DecodeSubtables(d, st, body[:ion.SizeOf(body)])
		if err != nil {
			return err
		}
		u.Sub = sub
	}
	return nil
}

func tableStrings(lst Subtables) []string {
	out := make([]string, lst.Len())
	for i := range out {
		var sub Subtable
		lst.Subtable(i, &sub)
		out[i] = expr.ToString(sub)
	}
	return out
}

func (u *UnionMap) String() string {
	return fmt.Sprintf("UNION MAP %s %v", expr.ToString(u.Orig), tableStrings(u.Sub))
}
