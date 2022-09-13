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

// Package core contains core functions and data
// types shared by the sneller and snellerd
// executables.
package core

import (
	"context"
	"fmt"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/vm"
)

func (f *FilterHandle) Open(ctx context.Context) (vm.Table, error) {
	panic("bare filterHandle.Open")
	return nil, nil
}

func (f *FilterHandle) Filter(e expr.Node) plan.TableHandle {
	o := *f
	o.Expr = e
	return &o
}

func (f *FilterHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	if f.Expr != nil {
		dst.BeginField(st.Intern("filter"))
		f.Expr.Encode(dst, st)
	}
	if len(f.Fields) > 0 {
		dst.BeginField(st.Intern("fields"))
		dst.BeginList(-1)
		for i := range f.Fields {
			dst.WriteString(f.Fields[i])
		}
		dst.EndList()
	}
	if f.AllFields {
		dst.BeginField(st.Intern("all_fields"))
		dst.WriteBool(true)
	}
	dst.BeginField(st.Intern("blobs"))
	f.Blobs.Encode(dst, st)
	dst.EndStruct()
	return nil
}

func (f *FilterHandle) Decode(st *ion.Symtab, mem []byte) error {
	if len(mem) == 0 {
		return fmt.Errorf("filterHandle.decode: no data?")
	}
	if ion.TypeOf(mem) != ion.StructType {
		return fmt.Errorf("unexpected filterHandle type: %s", ion.TypeOf(mem))
	}
	mem, _ = ion.Contents(mem)
	var err error
	var sym ion.Symbol
	for len(mem) > 0 {
		sym, mem, err = ion.ReadLabel(mem)
		if err != nil {
			return err
		}
		switch st.Get(sym) {
		case "filter":
			f.Expr, mem, err = expr.Decode(st, mem)
		case "blobs":
			skip := ion.SizeOf(mem)
			f.Blobs, err = blob.DecodeList(st, mem)
			mem = mem[skip:]
		case "fields":
			mem, err = ion.UnpackList(mem, func(field []byte) error {
				var str string
				str, _, err = ion.ReadString(field)
				if err != nil {
					return err
				}
				f.Fields = append(f.Fields, str)
				return nil
			})
		case "all_fields":
			f.AllFields, mem, err = ion.ReadBool(mem)
		default:
			return fmt.Errorf("unrecognized filterHandle field %q", st.Get(sym))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// FilterHandle is a plan.TableHandle
// implementation that stores a list of blobs
// with associated filter and scanning hints.
type FilterHandle struct {
	// Expr is the filter expression.
	Expr expr.Node
	// Fields are the fields that will be accessed
	// during scanning, to be used as a hint
	// during blob decompression.
	Fields []string
	// AllFields indicates that all fields are
	// used during scanning.
	AllFields bool
	// Blobs is the list of blobs that make up the
	// table this handle refers to.
	Blobs *blob.List

	// cached result of compileFilter(Expr)
	compiled Filter
}

// CompileFilter compiles the filter expression
// in h.Expr, returning a cached filter if it
// has already been compiled. If h.Expr is nil,
// this returns (nil, true). This will only
// return false if there is a filter expression
// present and it fails to compile.
func (f *FilterHandle) CompileFilter() (Filter, bool) {
	if f.compiled != nil {
		return f.compiled, true
	}
	if f.Expr == nil {
		return nil, true
	}
	flt, ok := compileFilter(f.Expr)
	if !ok {
		return nil, false
	}
	f.compiled = flt
	return flt, true
}
