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

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

// Decoder wraps environment specific methods used
// during plan decoding. Implementations may also
// implement interfaces such as SubtableDecoder and
// UploadEnv to provide additional functionality.
type Decoder interface {
	// DecodeHandle is used to decode a TableHandle
	// produced by TableHandle.Encode. If a list is
	// encountered when decoding a TableHandle,
	// this function will be called for each item
	// in the list to produce a concatenated table.
	DecodeHandle(st *ion.Symtab, mem []byte) (TableHandle, error)
}

// SubtableDecoder can optionally be implemented by
// Decoder to handle decoding subtables using a more
// sophisticated representation than the default.
type SubtableDecoder interface {
	// DecodeSubtables decodes Subtables produced
	// by Subtables.Encode.
	DecodeSubtables(st *ion.Symtab, mem []byte) (Subtables, error)
}

// UploaderDecoder can optionally be implemented by a
// Decoder to handle decoding an UploadFS, which is
// required to enable support for SELECT INTO.
//
// See also: UploadEnv
type UploaderDecoder interface {
	DecodeUploader(st *ion.Symtab, mem []byte) (UploadFS, error)
}

// decodeHandle calls d.DecodeHandle with special
// handling for lists.
func decodeHandle(d Decoder, st *ion.Symtab, mem []byte) (TableHandle, error) {
	if ion.TypeOf(mem) == ion.ListType {
		return decodeHandles(d, st, mem)
	}
	return d.DecodeHandle(st, mem)
}

// decode decodes an input structure.
func (i *Input) decode(d Decoder, st *ion.Symtab, mem []byte) error {
	if ion.TypeOf(mem) != ion.StructType {
		return fmt.Errorf("plan.Decode: unexpected ion type %s for Input", ion.TypeOf(mem))
	}
	_, err := ion.UnpackStruct(st, mem, func(field string, buf []byte) error {
		switch field {
		case "table":
			e, _, err := expr.Decode(st, buf)
			if err != nil {
				return err
			}
			t, ok := e.(*expr.Table)
			if !ok {
				return fmt.Errorf("plan.Decode: input expr %T not a table", e)
			}
			i.Table = t
		case "handle":
			th, err := decodeHandle(d, st, buf[:ion.SizeOf(buf)])
			if err != nil {
				return err
			}
			i.Handle = th
		}
		return nil
	})
	return err
}

// Decode decodes an ion-encoded tree
// from the provided symbol table and buffer.
// During decoding, each Leaf op in the Tree
// will have its TableHandle populated with env.Stat.
// See also: Plan.Encode, Plan.EncodePart.
func Decode(d Decoder, st *ion.Symtab, buf []byte) (*Tree, error) {
	t := &Tree{}
	_, err := ion.UnpackStruct(st, buf, func(field string, inner []byte) error {
		switch field {
		case "inputs":
			return unpackList(inner, func(field []byte) error {
				t.Inputs = append(t.Inputs, Input{})
				return t.Inputs[len(t.Inputs)-1].decode(d, st, field)
			})
		case "root":
			return t.Root.decode(d, st, inner)
		default:
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (n *Node) decode(d Decoder, st *ion.Symtab, buf []byte) error {
	_, err := ion.UnpackStruct(st, buf, func(field string, inner []byte) error {
		switch field {
		case "op":
			if ion.TypeOf(inner) != ion.ListType {
				return fmt.Errorf("plan.Decode: expected op to be a list; found %s", ion.TypeOf(inner))
			}
			var body []byte
			var err error
			body, _ = ion.Contents(inner)
			n.Op, err = decodeOps(d, st, body)
			return err
		case "inputs":
			return unpackList(inner, func(field []byte) error {
				n.Inputs = append(n.Inputs, Input{})
				return n.Inputs[len(n.Inputs)-1].decode(d, st, field)
			})
		case "children":
			return unpackList(inner, func(field []byte) error {
				nn := &Node{}
				err := nn.decode(d, st, field)
				if err != nil {
					return err
				}
				n.Children = append(n.Children, nn)
				return nil
			})
		default:
			return nil
		}
	})
	if err != nil {
		return err
	}
	if n.Op == nil {
		return fmt.Errorf("plan.Decode: no Op field present")
	}
	return nil
}

// Decode decodes a query plan from 'buf'
// using the ion symbol table 'st' and the
// environment 'env.'
//
// See also: Encode
//
// During decoding, each *Leaf plan that references
// a table has its TableHandle populated with env.Stat.
func decodeOps(d Decoder, st *ion.Symtab, buf []byte) (Op, error) {
	typesym, ok := st.Symbolize("type")
	if !ok {
		return nil, fmt.Errorf("plan.Decode: symbol table missing \"type\" symbol")
	}
	var inner []byte
	var err error
	var top Op
	var lbl, typ ion.Symbol
	count := 0
	for len(buf) > 0 {
		if ion.TypeOf(buf) != ion.StructType {
			return nil, fmt.Errorf("plan.Decode: field %d not a structure; got %s", count, ion.TypeOf(buf))
		}
		inner, buf = ion.Contents(buf)
		if inner == nil {
			return nil, fmt.Errorf("plan.Decode: invalid TLV bytes in field %d", count)
		}
		// "type" should be the first symbol
		lbl, inner, err = ion.ReadLabel(inner)
		if err != nil {
			return nil, err
		}
		if lbl != typesym {
			return nil, fmt.Errorf("plan.Decode: first field of plan op is %q; expected \"type\"", st.Get(lbl))
		}
		typ, inner, err = ion.ReadSymbol(inner)
		if err != nil {
			return nil, fmt.Errorf("plan.Decode: reading \"type\" symbol: %w", err)
		}
		op, err := decodetyp(d, st.Get(typ), st, inner)
		if err != nil {
			return nil, fmt.Errorf("plan.Decode: reading op %d: %w", count, err)
		}
		if top == nil {
			top = op
		} else {
			op.setinput(top)
			top = op
		}
		count++
	}
	return top, nil
}

func empty(name string) Op {
	switch name {
	case "agg":
		return &SimpleAggregate{}
	case "leaf":
		return &Leaf{}
	case "none":
		return NoOutput{}
	case "dummy":
		return DummyOutput{}
	case "limit":
		return &Limit{}
	case "count(*)":
		return &CountStar{}
	case "hashagg":
		return &HashAggregate{}
	case "order":
		return &OrderBy{}
	case "distinct":
		return &Distinct{}
	case "project":
		return &Project{}
	case "filter":
		return &Filter{}
	case "unnest":
		return &Unnest{}
	case "unionmap":
		return &UnionMap{}
	case "outpart":
		return &OutputPart{}
	case "outidx":
		return &OutputIndex{}
	}
	return nil
}

func decodetyp(d Decoder, name string, st *ion.Symtab, body []byte) (Op, error) {
	op := empty(name)
	if op == nil {
		return nil, fmt.Errorf("plan.Decode: unrecognized type name %q", name)
	}
	var lbl ion.Symbol
	var err error
	for len(body) > 0 {
		lbl, body, err = ion.ReadLabel(body)
		if err != nil {
			return nil, err
		}
		err = op.setfield(d, st.Get(lbl), st, body)
		if err != nil {
			return nil, fmt.Errorf("decoding %T: %w", op, err)
		}
		body = body[ion.SizeOf(body):]
	}
	return op, nil
}
