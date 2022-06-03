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

package main

import (
	"context"
	"fmt"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/vm"
)

var canVMOpen = false

func (f *filterHandle) Open(ctx context.Context) (vm.Table, error) {
	panic("bare filterHandle.Open")
	return nil, nil
}

func (f *filterHandle) Filter(e expr.Node) plan.TableHandle {
	o := *f
	o.filter = e
	return &o
}

func (f *filterHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	if f.filter != nil {
		dst.BeginField(st.Intern("filter"))
		f.filter.Encode(dst, st)
	}
	dst.BeginField(st.Intern("blobs"))
	f.blobs.Encode(dst, st)
	dst.EndStruct()
	return nil
}

func (f *filterHandle) decode(st *ion.Symtab, mem []byte) error {
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
			f.filter, mem, err = expr.Decode(st, mem)
		case "blobs":
			skip := ion.SizeOf(mem)
			f.blobs, err = blob.DecodeList(st, mem)
			mem = mem[skip:]
		default:
			return fmt.Errorf("unrecognized filterHandle field %q", st.Get(sym))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type filterHandle struct {
	filter expr.Node
	blobs  *blob.List

	// cached result of compileFilter(filter)
	compiled filter
}
