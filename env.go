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

package sneller

import (
	"context"
	"fmt"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
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
	if f.Splitter != nil {
		dst.BeginField(st.Intern("splitter"))
		f.Splitter.encode(dst, st)
	}
	dst.EndStruct()
	return nil
}

func (f *FilterHandle) Decode(d ion.Datum) error {
	err := d.UnpackStruct(func(sf ion.Field) error {
		var err error
		switch sf.Label {
		case "filter":
			f.Expr, err = expr.Decode(sf.Datum)
		case "blobs":
			f.Blobs, err = blob.DecodeList(sf.Datum)
		case "fields":
			err = sf.UnpackList(func(d ion.Datum) error {
				str, err := d.String()
				if err != nil {
					return err
				}
				f.Fields = append(f.Fields, str)
				return nil
			})
		case "all_fields":
			f.AllFields, err = sf.Bool()
		case "splitter":
			s := new(Splitter)
			err = sf.UnpackStruct(s.setField)
			if err != nil {
				return err
			}
			f.Splitter = s
		default:
			return fmt.Errorf("unrecognized field %q", sf.Label)
		}

		return err
	})

	if err != nil {
		return fmt.Errorf("decoding FilterHandle: %w", err)
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

	// Splitter is used to split blobs
	Splitter *Splitter

	// cached result of compileFilter(Expr)
	compiled blockfmt.Filter
}

// CompileFilter compiles the filter expression
// in h.Expr, returning a cached filter if it
// has already been compiled. If h.Expr is nil,
// this returns (nil, true). This will only
// return false if there is a filter expression
// present and it fails to compile.
func (f *FilterHandle) CompileFilter() (*blockfmt.Filter, bool) {
	if f.Expr == nil {
		return nil, true
	}
	f.compiled.Compile(f.Expr)
	if f.compiled.Trivial() {
		return nil, false
	}
	return &f.compiled, true
}

func blobsSize(lst []blob.Interface) int64 {
	n := int64(0)
	for i := range lst {
		switch c := lst[i].(type) {
		case *blob.Compressed:
			n += c.Trailer.Decompressed()
		case *blob.CompressedPart:
			n += c.Decompressed()
		default:
			info, _ := c.Stat()
			if info != nil {
				n += info.Size
			}
		}
	}
	return n
}

func (f *FilterHandle) Size() int64 {
	if f.Blobs == nil {
		return 0
	}
	return blobsSize(f.Blobs.Contents)
}
