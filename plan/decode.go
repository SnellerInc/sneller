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
	DecodeHandle(ion.Datum) (TableHandle, error)
}

// UploaderDecoder can optionally be implemented by a
// Decoder to handle decoding an UploadFS, which is
// required to enable support for SELECT INTO.
//
// See also: UploadEnv
type UploaderDecoder interface {
	DecodeUploader(ion.Datum) (UploadFS, error)
}

// decodeHandle calls d.DecodeHandle with special
// handling for lists.
func decodeHandle(d Decoder, v ion.Datum) (TableHandle, error) {
	if v.IsList() {
		return decodeHandles(d, v)
	}
	return d.DecodeHandle(v)
}

// decode decodes an input structure.
func (i *Input) decode(d Decoder, v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "table":
			e, err := expr.Decode(f.Datum)
			if err != nil {
				return err
			}
			t, ok := e.(*expr.Table)
			if !ok {
				return fmt.Errorf("input expr %T not a table", e)
			}
			i.Table = t
		case "handle":
			th, err := decodeHandle(d, f.Datum)
			if err != nil {
				return err
			}
			i.Handle = th
		default:
			return errUnexpectedField
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("plan.Decode: %w", err)
	}
	return err
}

// Decode decodes an ion-encoded tree
// from the provided symbol table and buffer.
// During decoding, each Leaf op in the Tree
// will have its TableHandle populated with env.Stat.
// See also: Plan.Encode, Plan.EncodePart.
func Decode(d Decoder, st *ion.Symtab, buf []byte) (*Tree, error) {
	v, _, err := ion.ReadDatum(st, buf)
	if err != nil {
		return nil, err
	}
	return DecodeDatum(d, v)
}

func DecodeDatum(d Decoder, v ion.Datum) (*Tree, error) {
	t := &Tree{}
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "inputs":
			return f.UnpackList(func(v ion.Datum) error {
				t.Inputs = append(t.Inputs, Input{})
				return t.Inputs[len(t.Inputs)-1].decode(d, v)
			})
		case "root":
			return t.Root.decode(d, f.Datum)
		default:
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (n *Node) decode(d Decoder, v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "op":
			var err error
			n.Op, err = decodeOps(d, f.Datum)
			return err
		case "input":
			v, err := f.Int()
			if err == nil {
				n.Input = int(v)
			}
			return err
		default:
			return errUnexpectedField
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

type decOp struct {
	op Op
	d  Decoder
}

func (d *decOp) SetField(f ion.Field) error {
	return d.op.setfield(d.d, f)
}

// Decode decodes a query plan from 'buf'
// using the ion symbol table 'st' and the
// environment 'env.'
//
// See also: Encode
//
// During decoding, each *Leaf plan that references
// a table has its TableHandle populated with env.Stat.
func decodeOps(d Decoder, v ion.Datum) (Op, error) {
	var top Op
	i := 0
	dec := decOp{d: d}
	err := v.UnpackList(func(v ion.Datum) error {
		_, err := ion.UnpackTyped(v, func(typ string) (*decOp, bool) {
			dec.op = empty(typ)
			if dec.op != nil {
				return &dec, true
			}
			return nil, false
		})
		if err != nil {
			return err
		}
		if top == nil {
			top = dec.op
		} else {
			dec.op.setinput(top)
			top = dec.op
		}
		i++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("plan.Decode: item #%d: %w", i, err)
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
	case "union_partition":
		return &UnionPartition{}
	case "outpart":
		return &OutputPart{}
	case "outidx":
		return &OutputIndex{}
	case "unpivot":
		return &Unpivot{}
	case "unpivotatdistinct":
		return &UnpivotAtDistinct{}
	case "explain":
		return &Explain{}
	case "substitute":
		return &Substitute{}
	}
	return nil
}

var (
	errUnexpectedField error = errors.New("unexpected field")
)
