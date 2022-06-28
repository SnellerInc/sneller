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
	"io"
	"testing"
)

func TestPathLess(t *testing.T) {
	ord := [][]Symbol{
		{0},
		{0, 1},
		{0, 1, 1},
		{0, 2},
		{0, 2, 1},
		{0, 2, 2},
		{0, 2, 3},
		{0, 3},
		{1, 0},
		{2},
	}
	for i := range ord[:len(ord)-1] {
		tail := ord[i+1:]
		if pathLess(ord[i], ord[i]) {
			t.Errorf("%v less than itself?", ord)
		}
		for j := range tail {
			if !pathLess(ord[i], tail[j]) {
				t.Errorf("%v not less than %v?", ord[i], tail[j])
			}
			if pathLess(tail[j], ord[i]) {
				t.Errorf("%v < %v ?", tail[j], ord[i])
			}
		}
	}
}

func TestUniqueFields(t *testing.T) {
	fields := []Field{
		{Value: Uint(1000)},
		{Value: String("foobarbaz")},
		{Value: UntypedNull{}},
		{Value: nil}, // set below
	}
	fields2 := []Field{
		{Value: String("inner")},
		{Value: Float(3.5)},
	}
	cn := Chunker{
		W:     io.Discard,
		Align: 2048,
	}
	// test that infinite unique fields
	// do not cause the symbol table to explode
	for i := 0; i < 1000; i++ {
		fields[0].Label = fmt.Sprintf("f0_%d", i)
		fields[1].Label = fmt.Sprintf("f1_%d", i)
		fields[2].Label = fmt.Sprintf("f2_%d", i)
		fields[3].Label = fmt.Sprintf("f3_%d", i)
		fields2[0].Label = fmt.Sprintf("f0_%d", i-1)
		fields2[1].Label = fmt.Sprintf("f1_%d", i+1)
		fields[3].Value = NewStruct(nil, fields2)
		NewStruct(nil, fields).Encode(&cn.Buffer, &cn.Symbols)
		err := cn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
}
