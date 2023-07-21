// Copyright (C) 2023 Sneller, Inc.
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
