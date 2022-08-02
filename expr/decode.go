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
	"fmt"
	"math/big"

	"github.com/SnellerInc/sneller/ion"
)

func Decode(st *ion.Symtab, msg []byte) (Node, []byte, error) {
	node, rest, err := decode(st, msg)
	if err != nil {
		err = fmt.Errorf("expr.Decode: %w", err)
	}
	return node, rest, err
}

func decode(st *ion.Symtab, msg []byte) (Node, []byte, error) {
	if len(msg) == 0 {
		return nil, nil, fmt.Errorf("no input data")
	}
	switch ion.TypeOf(msg) {
	case ion.NullType:
		return Null{}, msg[ion.SizeOf(msg):], nil
	case ion.BoolType:
		b, rest, err := ion.ReadBool(msg)
		return Bool(b), rest, err
	case ion.UintType:
		u, rest, err := ion.ReadUint(msg)
		return Integer(u), rest, err
	case ion.IntType:
		i, rest, err := ion.ReadInt(msg)
		return Integer(i), rest, err
	case ion.FloatType:
		f, rest, err := ion.ReadFloat64(msg)
		return Float(f), rest, err
	case ion.StringType:
		s, rest, err := ion.ReadString(msg)
		return String(s), rest, err
	case ion.StructType:
		return decodeStruct(st, msg)
	case ion.TimestampType:
		d, rest, err := ion.ReadTime(msg)
		return &Timestamp{Value: d}, rest, err
	default:
		if len(msg) > 8 {
			msg = msg[:8]
		}
		return nil, nil, fmt.Errorf("cannot decode ion %x", msg)
	}
}

func decodeStruct(st *ion.Symtab, msg []byte) (composite, []byte, error) {
	var node composite
	rest, err := ion.UnpackStruct(st, msg, func(name string, field []byte) error {
		if node != nil {
			return node.setfield(name, st, field)
		}
		// expect the type field to be the
		// first field in the struct
		if name != "type" {
			return fmt.Errorf("missing type field, found %q", name)
		}
		sym, _, err := ion.ReadSymbol(field)
		if err != nil {
			return fmt.Errorf("reading \"type\": %w", err)
		}
		str := st.Get(sym)
		if str == "" {
			return fmt.Errorf("symbol %d not in symbol table", sym)
		}
		node = getEmpty(str)
		if node == nil {
			return fmt.Errorf("unknown structure %q", str)
		}
		return nil
	})
	if node == nil {
		err = fmt.Errorf("missing type field")
	}
	return node, rest, err
}

type composite interface {
	Node
	setfield(name string, st *ion.Symtab, buf []byte) error
}

func getEmpty(name string) composite {
	switch name {
	case "aggregate":
		return &Aggregate{}
	case "rat":
		return (*Rational)(new(big.Rat))
	case "star":
		return Star{}
	case "path":
		return &Path{}
	case "cmp":
		return &Comparison{}
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
	case "struct":
		return &Struct{}
	case "list":
		return &List{}
	case "unpivot":
		return &Unpivot{}
	default:
		return nil
	}
}
