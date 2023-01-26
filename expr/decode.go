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

package expr

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/SnellerInc/sneller/ion"
)

func Decode(st *ion.Symtab, msg []byte) (Node, []byte, error) {
	d, rest, err := ion.ReadDatum(st, msg)
	if err != nil {
		return nil, nil, err
	}
	n, err := FromDatum(d)
	return n, rest, err
}

func FromDatum(d ion.Datum) (Node, error) {
	node, err := fromDatum(d)
	if err != nil {
		err = fmt.Errorf("expr.Decode: %w", err)
	}
	return node, err
}

func fromDatum(d ion.Datum) (Node, error) {
	if d.IsEmpty() {
		return nil, fmt.Errorf("no input data")
	}
	switch d.Type() {
	case ion.NullType:
		return Null{}, nil
	case ion.BoolType:
		b, err := d.Bool()
		return Bool(b), err
	case ion.UintType:
		u, err := d.Uint()
		return Integer(u), err
	case ion.IntType:
		i, err := d.Int()
		return Integer(i), err
	case ion.FloatType:
		f, err := d.Float()
		return Float(f), err
	case ion.StringType:
		s, err := d.String()
		return String(s), err
	case ion.StructType:
		s, err := d.Struct()
		if err != nil {
			return nil, err
		}
		return decodeStruct(s)
	case ion.SymbolType:
		s, err := d.String()
		return Ident(s), err
	case ion.TimestampType:
		d, err := d.Timestamp()
		return &Timestamp{Value: d}, err
	default:
		return nil, fmt.Errorf("cannot decode ion %s", d.Type())
	}
}

var (
	errUnexpectedField = errors.New("unexpected field")
)

func decodeStruct(s ion.Struct) (composite, error) {
	var node composite

	settype := func(typename string) error {
		node = getEmpty(typename)
		if node == nil {
			return fmt.Errorf("unknown structure %q", typename)
		}

		return nil
	}

	setfield := func(f ion.Field) error {
		return node.setfield(f)
	}

	err := s.UnpackTyped(settype, setfield)

	return node, err
}

type composite interface {
	Node
	setfield(f ion.Field) error
}

func getEmpty(name string) composite {
	switch name {
	case "aggregate":
		return &Aggregate{}
	case "rat":
		return (*Rational)(new(big.Rat))
	case "star":
		return Star{}
	case "dot":
		return &Dot{}
	case "index":
		return &Index{}
	case "cmp":
		return &Comparison{}
	case "stringmatch":
		return &StringMatch{}
	case "not":
		return &Not{}
	case "logical":
		return &Logical{}
	case "builtin":
		return &Builtin{}
	case "unaryArith":
		return &UnaryArith{}
	case "arith":
		return &Arithmetic{}
	case "append":
		return &Appended{}
	case "is":
		return &IsKey{}
	case "select":
		return &Select{}
	case "on":
		return &OnEquals{}
	case "join":
		return &Join{}
	case "missing":
		return Missing{}
	case "table":
		return &Table{}
	case "case":
		return &Case{}
	case "cast":
		return &Cast{}
	case "member":
		return &Member{}
	case "lookup":
		return &Lookup{}
	case "struct":
		return &Struct{}
	case "list":
		return &List{}
	case "unpivot":
		return &Unpivot{}
	case "union":
		return &Union{}
	default:
		return nil
	}
}
