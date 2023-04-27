// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
		{Datum: Uint(1000)},
		{Datum: String("foobarbaz")},
		{Datum: Null},
		{Datum: Empty}, // set below
	}
	fields2 := []Field{
		{Datum: String("inner")},
		{Datum: Float(3.5)},
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
		fields[3].Datum = NewStruct(nil, fields2).Datum()
		NewStruct(nil, fields).Encode(&cn.Buffer, &cn.Symbols)
		err := cn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
}
