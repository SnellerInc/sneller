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

package vm

import (
	"encoding/binary"
	"math/bits"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"

	"github.com/dchest/siphash"
)

// walk the tree and check that each value
// has the prefix implied by its tree location
func checklevel(tr *radixTree64, tab int, shift int, prefix uint64, t testing.TB) {
	entries := tr.index[tab]
	mask := (uint64(1) << (shift + 4)) - 1
	for i := range entries {
		pre := prefix | (uint64(i) << shift)
		if entries[i] > 0 {
			if int(entries[i]) >= len(tr.index) {
				t.Errorf("table %d entry %d: invalid index %d", tab, i, entries[i])
				continue
			}
			checklevel(tr, int(entries[i]), shift+4, pre, t)
		} else if entries[i] < 0 {
			vid := ^entries[i]
			if int(vid) >= len(tr.values) || vid%int32(tr.vsize) != 0 {
				t.Errorf("table %d entry %d: invalid vptr %d", tab, i, vid)
				continue
			}
			buf := tr.values[vid:]
			hash := binary.LittleEndian.Uint64(buf)
			if (hash & mask) != pre {
				t.Errorf("bad indexing of %16x", hash)
				t.Errorf("   needs prefix %16x", pre)
				rev := bits.RotateLeft64(hash, 32)
				if (rev & mask) == pre {
					t.Error("(found at reversed location?)")
				}
			}
		}
	}
}

func checktable(tr *radixTree64, t testing.TB) {
	checklevel(tr, 0, 0, 0, t)
}

func TestRadixBytecodeFind(t *testing.T) {
	var st symtab
	defer st.free()
	orig := unhex(parkingCitations1KLines)
	buf := Malloc()
	defer Free(buf)
	buf = buf[:copy(buf, orig)]
	_, err := st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	var agt aggtable
	agt.tree = newRadixTree(8)

	// compute GROUP BY Make
	p := &agt.prog
	p.begin()
	makeval := p.dot("Make", p.validLanes())
	mem, err := p.store(p.initMem(), makeval, 0)
	if err != nil {
		panic(err)
	}
	b := p.aggbucket(mem, p.hash(makeval), makeval)
	p.returnValue(p.aggregateSlotCount(mem, b, makeval, 0))
	err = p.symbolize(&st, &auxbindings{})
	if err != nil {
		t.Fatal(err)
	}
	err = p.compile(&agt.bc, &st, "TestRadixBytecodeFind")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bytecode: %s\n", agt.bc.String())

	delims := make([]vmref, 1024)
	n, _ := scanvmm(buf, delims)
	if n != 1023 {
		t.Fatal("expected 1023 delims; found", n)
	}
	first16 := delims[:16]

	// insert all of the delimiter values
	makesym, _ := st.Symbolize("Make")
	var hashes []uint64
	for i := range first16 {
		start := first16[i][0]
		end := start + first16[i][1]
		mem := vmref{start, end}.mem()
		var sym ion.Symbol
		var obj []byte
		for len(mem) > 0 {
			sym, mem, err = ion.ReadLabel(mem)
			if err != nil {
				panic(err)
			}
			if sym == makesym {
				obj = mem[:ion.SizeOf(mem)]
				break
			}
			mem = mem[ion.SizeOf(mem):]
		}
		if obj == nil {
			continue
		}
		h64, _ := siphash.Hash128(0, 0, obj)
		hashes = append(hashes, h64)
		t.Logf("row %d: hash %x", i, h64)
		_, _ = agt.tree.Insert(h64)
		if agt.tree.Find(h64) == nil {
			t.Fatalf("Find(%x) failed", h64)
		}
	}
	t.Logf("index[0]: %d", agt.tree.index[0])
	checktable(agt.tree, t)

	abort := uint16(0)
	n = agt.fasteval(first16, &abort)
	if n != 16 {
		slot := agt.bc.errinfo >> 3
		hashmem := agt.bc.vstack[slot : slot+16]
		t.Logf("hashes: %x", hashmem)
		t.Fatalf("n = %d", n)
	}
	if abort != 0 {
		t.Errorf("abort = %x", abort)
	}

	counts := make(map[uint64]int)
	for i := range hashes {
		counts[hashes[i]]++
	}

	for i := range hashes {
		h := hashes[i]
		buf := agt.tree.Find(h)
		if buf == nil {
			t.Errorf("couldn't find hashes[%d]", i)
			continue
		}
		loc := agt.tree.Offset(h)
		c := binary.LittleEndian.Uint64(buf)
		if int(c) != counts[h] {
			t.Errorf("count[%x] = loc %d = %d, should be %d", h, loc, c, counts[h])
		}
	}
}

func TestRadixBytecodeInsert(t *testing.T) {
	var st symtab
	defer st.free()
	orig := unhex(parkingCitations1KLines)
	buf := Malloc()
	defer Free(buf)
	buf = buf[:copy(buf, orig)]
	_, err := st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	var agt aggtable
	agt.tree = newRadixTree(8)

	// compute GROUP BY Make
	p := &agt.prog
	p.begin()
	makeval := p.dot("Make", p.validLanes())
	mem, err := p.store(p.initMem(), makeval, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := p.aggbucket(mem, p.hash(makeval), makeval)
	p.returnValue(p.aggregateSlotCount(mem, b, makeval, 0))
	err = p.symbolize(&st, &auxbindings{})
	if err != nil {
		t.Fatal(err)
	}
	err = p.compile(&agt.bc, &st, "TestRadixBytecodeInsert")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bytecode: %s\n", agt.bc.String())

	// This is ugly, but the parent is normally present;
	// we rely on its presence in aggtable.WriteRows().
	// Instead of making it possible to not have the
	// parent we just add some dummy parent here. At the
	// moment we are only interested in parent.by[] data.
	agt.parent = &HashAggregate{
		by: []expr.Binding{
			{},
		},
		aggregateOps: []AggregateOp{AggregateOp{fn: AggregateOpCount}},
	}
	agt.aggregateOps = agt.parent.aggregateOps

	delims := make([]vmref, 1024)
	n, _ := scanvmm(buf, delims)
	if n != 1023 {
		t.Fatal("expected 1023 delims; found", n)
	}
	delims = delims[:n]
	err = agt.writeRows(delims, &rowParams{})
	if err != nil {
		t.Fatal(err)
	}

	if len(agt.pairs) != 59 {
		t.Errorf("distinct=%d, wanted %d", len(agt.pairs), 59)
	}
	names := make(map[string]bool)
	results := make(map[string]int)
	for i := range agt.pairs {
		hash := agt.hashof(&agt.pairs[i])
		repr := agt.reproff(&agt.pairs[i], 0)
		str, _, err := ion.ReadString(repr)
		if err != nil {
			t.Errorf("%x not an ion string...?", repr)
		}
		if names[str] {
			t.Errorf("grouped name %q twice", str)
		}
		names[str] = true
		h, _ := siphash.Hash128(0, 0, repr)
		// note: hashof can return either hash(repr) or rotl(hash(repr), 32)
		// depending on how the item was inserted into the table
		if h != hash && h != bits.RotateLeft64(hash, 32) {
			t.Errorf("hash(%q) = %x but found %x", str, h, hash)
		}

		buf := agt.tree.Find(hash)
		if buf == nil {
			t.Fatalf("tree.Find(%x) failed", hash)
		}
		results[str] = int(binary.LittleEndian.Uint64(buf))
	}
	wantresults := map[string]int{
		"ACUR": 15,
		"AUDI": 12,
		"BENZ": 3,
		"BMW":  50,
		"BUIC": 4,
		"CADI": 8,
		"CHEC": 1,
		"CHEV": 70,
		"CHRY": 14,
		"DODG": 39,
		"FIAT": 5,
		"FORD": 88,
		"FREI": 1,
		"FRHT": 2,
		"GMC":  18,
		"HINO": 1,
		"HOND": 122,
		"HYUN": 33,
		"INFI": 18,
		"ISU":  1,
		"JAGR": 1,
		"JAGU": 1,
		"JEEP": 28,
		"KIA":  22,
		"KW":   1,
		"LEXS": 9,
		"LEXU": 19,
		"LINC": 8,
		"LIND": 1,
		"LROV": 4,
		"MASE": 1,
		"MAZD": 15,
		"MBNZ": 14,
		"MERC": 3,
		"MERZ": 18,
		"MITS": 11,
		"MNNI": 5,
		"NISS": 80,
		"OLDS": 2,
		"OTHR": 4,
		"PLYM": 2,
		"PONT": 7,
		"PORS": 3,
		"PTRB": 3,
		"RROV": 1,
		"SAA":  1,
		"SATU": 3,
		"SCIO": 2,
		"STRN": 2,
		"SUBA": 13,
		"SUZI": 1,
		"SUZU": 3,
		"TESL": 1,
		"TOYO": 96,
		"TOYT": 83,
		"TSMR": 1,
		"UNK":  3,
		"VOLK": 36,
		"VOLV": 6,
	}

	for k, v := range wantresults {
		if results[k] != v {
			t.Errorf("%q - got %d, wanted %d", k, results[k], v)
		}
	}

	// test merging; compute a second tree
	// using the same data and merge the
	// results to see that we get exactly
	// double the wanted counts...
	var agt2 aggtable
	// reset aggbucket and hashvaluep ops
	// so that we can re-compile the output
	// without the register allocation code
	// panic-ing because registers have already been assigned
	for _, v := range agt.prog.values {
		if v.op == saggbucket || v.op == shashvaluep {
			v.imm = nil
		}
	}
	agt2.tree = newRadixTree(8)
	agt2.parent = agt.parent
	agt2.aggregateOps = agt.parent.aggregateOps
	err = agt.prog.compile(&agt2.bc, &st, "TestRadixBytecodeInsert")
	if err != nil {
		t.Fatal(err)
	}

	err = agt2.writeRows(delims, &rowParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(agt2.pairs) != 59 {
		t.Errorf("len(pairs)=%d, wanted 59", len(agt2.pairs))
	}

	agt.merge(&agt2)
	if len(agt.pairs) != 59 {
		t.Errorf("after merge, len(pairs)=%d, wanted 59", len(agt.pairs))
	}
	// re-compute results
	results = make(map[string]int)
	for i := range agt.pairs {
		hash := agt.hashof(&agt.pairs[i])
		repr := agt.reproff(&agt.pairs[i], 0)
		str, _, err := ion.ReadString(repr)
		if err != nil {
			t.Errorf("%x not an ion string...?", repr)
		}
		h, _ := siphash.Hash128(0, 0, repr)
		// note: hashof can return either hash(repr) or rotl(hash(repr), 32)
		// depending on how the item was inserted into the table
		if h != hash && h != bits.RotateLeft64(hash, 32) {
			t.Errorf("hash(%q) = %x but found %x", str, h, hash)
		}
		buf := agt.tree.Find(hash)
		if buf == nil {
			t.Fatalf("tree.Find(%x) failed", hash)
		}
		results[str] = int(binary.LittleEndian.Uint64(buf))
	}

	// test that the count has been doubled
	for k, v := range wantresults {
		if results[k] != 2*v {
			t.Errorf("%q - got %d, wanted %d", k, results[k], 2*v)
		}
	}
}

func BenchmarkAggregate(b *testing.B) {
	var st symtab
	defer st.free()
	orig := unhex(parkingCitations1KLines)
	buf := Malloc()
	buf = buf[:copy(buf, orig)]
	_, err := st.Unmarshal(buf)
	if err != nil {
		b.Fatal(err)
	}

	var agt aggtable
	agt.tree = newRadixTree(8)
	agt.parent = &HashAggregate{
		by: []expr.Binding{
			{},
		},
	}

	// compute GROUP BY Make
	p := &agt.prog
	p.begin()
	makeval := p.dot("Make", p.validLanes())
	mem, err := p.store(p.initMem(), makeval, 0)
	if err != nil {
		b.Fatal(err)
	}
	bucket := p.aggbucket(mem, p.hash(makeval), makeval)
	p.returnValue(p.aggregateSlotCount(mem, bucket, makeval, 0))
	err = p.symbolize(&st, &auxbindings{})
	if err != nil {
		b.Fatal(err)
	}
	err = p.compile(&agt.bc, &st, "BenchmarkAggregate")
	if err != nil {
		b.Fatal(err)
	}

	delims := make([]vmref, 1024)
	n, _ := scanvmm(buf, delims)
	if n != 1023 {
		b.Fatal("expected 1023 delims; found", n)
	}
	delims = delims[:n]
	b.SetBytes(int64(delims[1022][0] + delims[1022][1]))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = agt.writeRows(delims, &rowParams{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Logf("distinct: %d", len(agt.pairs))
}
