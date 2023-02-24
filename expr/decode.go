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

func Decode(d ion.Datum) (Node, error) {
	node, err := decode(d)
	if err != nil {
		err = fmt.Errorf("expr.Decode: %w", err)
	}
	return node, err
}

func decode(d ion.Datum) (Node, error) {
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
		return decodeStruct(d)
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

func decodeStruct(d ion.Datum) (composite, error) {
	return ion.UnpackTyped(d, getEmpty)
}

type composite interface {
	Node
	ion.FieldSetter
}

func getEmpty(name string) (composite, bool) {
	switch name {
	case "aggregate":
		return &Aggregate{}, true
	case "rat":
		return (*Rational)(new(big.Rat)), true
	case "star":
		return Star{}, true
	case "dot":
		return &Dot{}, true
	case "index":
		return &Index{}, true
	case "cmp":
		return &Comparison{}, true
	case "stringmatch":
		return &StringMatch{}, true
	case "not":
		return &Not{}, true
	case "logical":
		return &Logical{}, true
	case "builtin":
		return &Builtin{}, true
	case "unaryArith":
		return &UnaryArith{}, true
	case "arith":
		return &Arithmetic{}, true
	case "append":
		return &Appended{}, true
	case "is":
		return &IsKey{}, true
	case "select":
		return &Select{}, true
	case "join":
		return &Join{}, true
	case "missing":
		return Missing{}, true
	case "table":
		return &Table{}, true
	case "case":
		return &Case{}, true
	case "cast":
		return &Cast{}, true
	case "member":
		return &Member{}, true
	case "lookup":
		return &Lookup{}, true
	case "struct":
		return &Struct{}, true
	case "list":
		return &List{}, true
	case "unpivot":
		return &Unpivot{}, true
	case "union":
		return &Union{}, true
	default:
		return nil, false
	}
}
