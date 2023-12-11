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

package vm

import (
	"bytes"
	"slices"
	"testing"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion"
	"github.com/SnellerInc/sneller/ion/zion/zll"
)

func TestZionFlatten(t *testing.T) {
	s0 := ion.NewStruct(nil, []ion.Field{{
		Label: "row",
		Datum: ion.Int(0),
	}, {
		Label: "not_projected",
		Datum: ion.String("a string!"),
	}, {
		Label: "value",
		Datum: ion.String("foo"),
	}, {
		Label: "ignore_me_0",
		Datum: ion.Null,
	}})
	s1 := ion.NewStruct(nil, []ion.Field{{
		Label: "row",
		Datum: ion.Int(1),
	}, {
		Label: "value",
		Datum: ion.String("bar"),
	}, {
		Label: "ignore_me_1",
		Datum: ion.Null,
	}})
	s2 := ion.NewStruct(nil, []ion.Field{{
		Label: "not_projected",
		Datum: ion.String("another string"),
	}, {
		Label: "another not projected",
		Datum: ion.String("yet another string"),
	}})

	var st ion.Symtab
	var buf ion.Buffer
	s0.Encode(&buf, &st)
	s1.Encode(&buf, &st)
	s2.Encode(&buf, &st)
	pos := buf.Size()
	st.Marshal(&buf, true)

	body := append(buf.Bytes()[pos:], buf.Bytes()[:pos]...)

	var enc zion.Encoder
	encoded, err := enc.Encode(body, nil)
	if err != nil {
		t.Fatal(err)
	}
	var shape zll.Shape
	var buckets zll.Buckets

	st.Reset()
	shape.Symtab = &st
	rest, err := shape.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	buckets.Reset(&shape, rest)
	buckets.Decompressed = Malloc()[:0]
	buckets.SkipPadding = true
	defer Free(buckets.Decompressed)

	count, err := shape.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("unexpected count %d", count)
	}
	tape := []ion.Symbol{st.Intern("row"), st.Intern("value")}
	slices.Sort(tape)
	err = buckets.SelectSymbols(tape)
	if err != nil {
		t.Fatal(err)
	}

	flat := make([]vmref, zionStride*len(tape))
	in, out := zionflatten(shape.Bits[shape.Start:], &buckets, flat, tape)
	if in != len(shape.Bits[shape.Start:]) {
		t.Fatalf("consumed %d of %d shape bytes?", in, len(shape.Bits[shape.Start:]))
	}
	if out != 3 {
		t.Fatalf("out = %d", out)
	}

	// check that the fields were transposed correctly:
	cmp := func(a, b []byte) {
		if !bytes.Equal(a, b) {
			t.Helper()
			t.Errorf("%x != %x", a, b)
		}
	}

	// "row" values
	cmp(flat[0].mem(), []byte{0x20})
	cmp(flat[1].mem(), []byte{0x21, 0x01})
	cmp(flat[2].mem(), []byte{})

	// "value" values
	flat = flat[zionStride:]
	cmp(flat[0].mem(), []byte{0x83, 'f', 'o', 'o'})
	cmp(flat[1].mem(), []byte{0x83, 'b', 'a', 'r'})
	cmp(flat[2].mem(), []byte{})
}
