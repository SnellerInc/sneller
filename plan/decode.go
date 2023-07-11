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

	"github.com/SnellerInc/sneller/ion"
)

// Decode decodes an ion-encoded tree
// from the provided symbol table and buffer.
// During decoding, each Leaf op in the Tree
// will have its TableHandle populated with env.Stat.
// See also: Plan.Encode, Plan.EncodePart.
func Decode(st *ion.Symtab, buf []byte) (*Tree, error) {
	v, _, err := ion.ReadDatum(st, buf)
	if err != nil {
		return nil, err
	}
	return DecodeDatum(v)
}

func DecodeDatum(v ion.Datum) (*Tree, error) {
	t := &Tree{}
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "id":
			var err error
			t.ID, err = f.String()
			return err
		case "inputs":
			return f.UnpackList(func(v ion.Datum) error {
				t.Inputs = append(t.Inputs, &Input{})
				return t.Inputs[len(t.Inputs)-1].decode(v)
			})
		case "data":
			t.Data = f.Datum.Clone()
		case "root":
			return t.Root.decode(f.Datum)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (n *Node) decode(v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "op":
			var err error
			n.Op, err = decodeOps(f.Datum)
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

// Decode decodes a query plan from 'buf'
// using the ion symbol table 'st' and the
// environment 'env.'
//
// See also: Encode
//
// During decoding, each *Leaf plan that references
// a table has its TableHandle populated with env.Stat.
func decodeOps(v ion.Datum) (Op, error) {
	var top Op
	i := 0
	err := v.UnpackList(func(v ion.Datum) error {
		op, err := ion.UnpackTyped(v, empty)
		if err != nil {
			return err
		}
		if top == nil {
			top = op
		} else {
			op.setinput(top)
			top = op
		}
		i++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("plan.Decode: item #%d: %w", i, err)
	}
	return top, nil
}

func empty(name string) (Op, bool) {
	var op Op
	switch name {
	case "agg":
		op = &SimpleAggregate{}
	case "leaf":
		op = &Leaf{}
	case "none":
		op = NoOutput{}
	case "dummy":
		op = DummyOutput{}
	case "limit":
		op = &Limit{}
	case "count(*)":
		op = &CountStar{}
	case "hashagg":
		op = &HashAggregate{}
	case "order":
		op = &OrderBy{}
	case "distinct":
		op = &Distinct{}
	case "project":
		op = &Project{}
	case "filter":
		op = &Filter{}
	case "unnest":
		op = &Unnest{}
	case "unionmap":
		op = &UnionMap{}
	case "union_partition":
		op = &UnionPartition{}
	case "outpart":
		op = &OutputPart{}
	case "outidx":
		op = &OutputIndex{}
	case "unpivot":
		op = &Unpivot{}
	case "unpivotatdistinct":
		op = &UnpivotAtDistinct{}
	case "explain":
		op = &Explain{}
	case "substitute":
		op = &Substitute{}
	default:
		return nil, false
	}
	return op, true
}

var (
	errUnexpectedField error = errors.New("unexpected field")
)
