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

package zll

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestInt8Vec(t *testing.T) {
	vec := make([]int8, 1024)
	for i := range vec {
		vec[i] = int8(i)
	}
	for i := range vec {
		rand.Shuffle(len(vec), func(i, j int) {
			vec[i], vec[j] = vec[j], vec[i]
		})
		testInt8(t, vec[:i])
	}
}

func testInt8(t *testing.T, vec []int8) {
	var st ion.Symtab
	var buf ion.Buffer
	sym := st.Intern("label")
	buf.BeginStruct(-1)
	buf.BeginField(sym)
	buf.BeginList(-1)
	for i := range vec {
		buf.WriteInt(int64(vec[i]))
	}
	buf.EndList()
	buf.EndStruct()

	src, _ := ion.Contents(buf.Bytes())
	comp, ok := tryInt8Vector(src, nil)
	if !ok {
		t.Fatalf("couln't int8vec compress %x\n", src)
	}
	t.Logf("%d bytes -> %d bytes", len(src), len(comp))
	ret, err := decodeInt8Vec(nil, comp)
	if err != nil {
		t.Errorf("output: %x\n", comp)
		t.Fatal(err)
	}
	if !bytes.Equal(ret, src) {
		t.Errorf("input : %x\n", src)
		t.Errorf("output: %x\n", ret)
		t.Fatal("didn't decompress identical bytes")
	}
}
