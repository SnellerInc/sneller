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
	if len(msg) == 0 {
		return nil, nil, fmt.Errorf("expr.Decode: no input data")
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
		t, rest, err := ion.ReadTime(msg)
		return &Timestamp{Value: t}, rest, err
	default:
		if len(msg) > 8 {
			msg = msg[:8]
		}
		return nil, nil, fmt.Errorf("expr.Decode: cannot decode ion %x", msg)
	}
}

func decodeStruct(st *ion.Symtab, msg []byte) (Node, []byte, error) {
	typel, ok := st.Symbolize("type")
	if !ok {
		return nil, nil, fmt.Errorf("expr.Decode: symbol table doesn't have \"type\"")
	}
	contents, rest := ion.Contents(msg)

	body := contents
	var err error
	var lbl ion.Symbol
	for len(body) > 0 {
		lbl, body, err = ion.ReadLabel(body)
		if err != nil {
			return nil, nil, fmt.Errorf("expr.Decode: %w", err)
		}
		if lbl == typel {
			sym, _, err := ion.ReadSymbol(body)
			if err != nil {
				return nil, nil, fmt.Errorf("expr.Decode: reading \"type\": %w", err)
			}
			str := st.Get(sym)
			if str == "" {
				return nil, nil, fmt.Errorf("expr.Decode: symbol %d not in symbol table", sym)
			}
			empty := getEmpty(str)
			if empty == nil {
				return nil, nil, fmt.Errorf("expr.Decode: unknown structure %q", str)
			}
			err = decodeinto(empty, st, contents)
			if err != nil {
				return nil, rest, fmt.Errorf("reading into %T: %w", empty, err)
			}
			return empty, rest, nil
		}
		body = body[ion.SizeOf(body):]
	}
	return nil, rest, fmt.Errorf("expr.Decode: unrecognized structure %x", msg)
}

func decodeinto(dst composite, st *ion.Symtab, src []byte) error {
	var err error
	var lbl ion.Symbol
	for len(src) > 0 {
		lbl, src, err = ion.ReadLabel(src)
		if err != nil {
			return err
		}
		str := st.Get(lbl)
		if str == "" {
			return fmt.Errorf("expr.Decode: symbol %d not valid", lbl)
		}
		err = dst.setfield(str, st, src)
		if err != nil {
			return err
		}
		src = src[ion.SizeOf(src):]
	}
	return nil
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
	default:
		return nil
	}
}
