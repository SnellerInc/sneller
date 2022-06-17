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

package ion

import (
	"fmt"
)

// take a buffer (must be valid, sorted symbols, etc.)
// and resymbolize it starting with the empty symbol table,
// and set st to the new (hopefully smaller) symbol table
func resymbolize(dst *Buffer, rng *Ranges, st *Symtab, buf []byte) {
	var newst Symtab
	for len(buf) > 0 {
		buf = resymValue(dst, &newst, st, buf)
	}

	// resymbolize ranges:
	var new Symbuf
	newm := make(map[symstr]dataRange)
	newp := rng.paths[:0]
	for oldstr, r := range rng.m {
		strs := oldstr.resolve(st)
		new.Prepare(len(strs))
		for i := range strs {
			new.Push(newst.Intern(strs[i]))
		}
		newstr := symstr(new)
		newm[newstr] = r
		newp = append(newp, newstr)
	}
	rng.m = newm
	rng.paths = newp
	newst.CloneInto(st)
}

func resymValue(dst *Buffer, new, old *Symtab, buf []byte) []byte {
	switch TypeOf(buf) {
	case AnnotationType:
		panic("unexpected annotation")
	case ListType:
		return resymList(dst, new, old, buf)
	case StructType:
		return resymStruct(dst, new, old, buf)
	case SymbolType:
		sym, rest, err := ReadSymbol(buf)
		if err != nil {
			panic(err)
		}
		// we are deliberately decompressing
		// the old interned value, because we
		// may or may not want to re-compress it now;
		// additionally, inserting symbols in DFS order
		// may disturb the field name order
		dst.WriteString(old.Get(sym))
		return rest
	default:
		size := SizeOf(buf)
		if size == 0 {
			panic("invalid value")
		}
		dst.UnsafeAppend(buf[:size])
		return buf[size:]
	}
}

func resymList(dst *Buffer, new, old *Symtab, buf []byte) []byte {
	dst.BeginList(-1)
	self, rest := Contents(buf)
	for len(self) > 0 {
		self = resymValue(dst, new, old, self)
	}
	dst.EndList()
	return rest
}

func resymStruct(dst *Buffer, new, old *Symtab, buf []byte) []byte {
	var sym Symbol
	var err error
	self, rest := Contents(buf)
	dst.BeginStruct(-1)
	ord := Symbol(0)
	for len(self) > 0 {
		sym, self, err = ReadLabel(self)
		if err != nil {
			panic(err)
		}
		// if the old symbols were ordered,
		// then the new symbols should be ordered as well;
		// we are traversing the symbols in the same order
		// as they were originally parsed (in-order traversal)
		if int(sym) >= old.MaxID() {
			panic(fmt.Sprintf("symbol %d not in symtab(%d)", sym, old.MaxID()))
		}
		newsym := new.Intern(old.Get(sym))
		if newsym < ord {
			panic("symbols out-of-order")
		}
		dst.BeginField(newsym)
		self = resymValue(dst, new, old, self)
	}
	dst.EndStruct()
	return rest
}
