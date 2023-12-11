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
	"io"
	"os"
	"testing"

	"github.com/SnellerInc/sneller/expr"
)

func path(t testing.TB, s string) expr.Node {
	p, err := expr.ParsePath(s)
	if err != nil {
		if t == nil {
			panic(err)
		}
		t.Helper()
		t.Fatal(err)
	}
	return p
}

func readVM(t testing.TB, name string) []byte {
	f, err := os.Open(name)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	size := info.Size()
	if size > PageSize {
		t.Helper()
		t.Fatalf("file %q larger than page size", name)
	}
	buf := Malloc()
	t.Cleanup(func() { Free(buf) })
	_, err = io.ReadFull(f, buf[:size])
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return buf
}

func TestSplat(t *testing.T) {
	var bc bytecode
	var p prog
	var st symtab
	defer st.free()

	buf := readVM(t, "../testdata/parking3.ion")
	rest, err := st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	delims := make([]vmref, 128)
	n, _ := scanvmm(rest, delims)
	if n != 60 {
		t.Fatalf("got %d rows at top level?", n)
	}

	p.begin()
	field := p.dot("Entries", p.validLanes())
	list := p.ssa2(stolist, field, field)
	p.returnScalar(p.initMem(), list, p.mask(list))
	p.symbolize(&st, &auxbindings{})
	err = p.compile(&bc, &st, "TestSplat")
	if err != nil {
		t.Fatal(err)
	}

	// # of entries in the first
	// sixteen rows of the
	entrylengths := []int{
		15, 12, 3, 50,
		4, 8, 1, 70,
		14, 39, 5, 88,
		1, 2, 18, 1,
	}
	totalentries := 0
	for i := range entrylengths {
		totalentries += entrylengths[i]
	}

	// there are 1023 inner records, so we
	// should expect this to be able to process everything:
	outdelims := make([]vmref, 1024)
	outperm := make([]int32, len(outdelims))
	in, out := evalsplat(&bc, delims[:n], outdelims, outperm)
	if in != n {
		t.Errorf("in = %d (expected %d)", in, n)
	}
	if out != 1023 {
		t.Errorf("out = %d (expected 1023)", out)
	}

	// test that the output permutation
	// is what we expect given the know
	// entry lengths
	j := 0
	for i := range entrylengths {
		span := entrylengths[i]
		for k := 0; k < span; k++ {
			if int(outperm[j+k]) != i {
				t.Errorf("outperm[%d]=%d, want %d", j+k, outperm[j+k], i)
			}
		}
		j += span
	}

	// test providing just enough space
	// for entries [0, ...]
	want := 0
	for j, elen := range entrylengths {
		want += elen
		in, out = evalsplat(&bc, delims[:n], outdelims[:want], outperm[:want])
		if in != j+1 {
			t.Errorf("with %d outdelims available expected %d in; got %d", want, j+1, in)
		}
		if out != want {
			t.Errorf("with %d outdelims available expected %d out; got %d", want, want, out)
		}
	}

	// test that delimiter object sizes
	// are sane
	for i := range outdelims[:out] {
		os, ds := objsize(vmref(outdelims[i]).mem())
		outsize := int(os) + int(ds)
		if outsize != int(outdelims[i][1]) {
			t.Errorf("delim %d: size %d, but computed %d", i, outdelims[i][1], outsize)
		}
	}
}
