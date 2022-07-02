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
	"fmt"
	"math"
	"math/bits"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/ion"
)

// NOTE: the assembly knows about these constants;
// be sure to change the constants there too
const (
	radix   = 4
	tabsize = 1 << radix
	tabmask = tabsize - 1
)

type radixTree64 struct {
	// NOTE: memory layout known by assembly!

	// index is a radix tree of indices;
	// positive indices refer to child tables,
	// and negative indices refer to values
	index  [][tabsize]int32
	values []byte

	// pre-computed [vsize*i] for use by SIMD
	helptable [17]uint32

	vsize int
}

func newRadixTree(datasize int) *radixTree64 {
	rt := &radixTree64{
		index:  make([][tabsize]int32, 1),
		vsize:  aggregateTagSize + datasize,
		values: make([]byte, 0, (aggregateTagSize+datasize)*16),
	}
	for i := 0; i <= 16; i++ {
		rt.helptable[i] = uint32(rt.vsize) * uint32(i)
	}
	return rt
}

// use 'h' to find the leaf index pointer
// returns (pointer, radix)
func (t *radixTree64) find(h uint64) (*int32, int) {
	i := &t.index[0][h&tabmask]
	rad := 0
	for *i > 0 {
		h >>= radix
		i = &t.index[*i][h&tabmask]
		rad += radix
	}
	return i, rad
}

func (t *radixTree64) Offset(h uint64) int32 {
	if len(t.index) == 0 {
		return -1
	}
	idx0, _ := t.find(h)
	idx1, _ := t.find(bits.RotateLeft64(h, 32))
	if *idx0 != 0 {
		buf := t.ref(*idx0)
		if binary.LittleEndian.Uint64(buf) == h {
			return ^*idx0
		}
	}
	if *idx1 != 0 {
		buf := t.ref(*idx1)
		if binary.LittleEndian.Uint64(buf) == bits.RotateLeft64(h, 32) {
			return ^*idx1
		}
	}
	return -1
}

// Find finds the data associated with the given
// hash value, or returns nil if no such entry
// has been inserted into the table
func (t *radixTree64) Find(h uint64) []byte {
	if len(t.index) == 0 {
		return nil
	}
	idx0, _ := t.find(h)
	idx1, _ := t.find(bits.RotateLeft64(h, 32))
	if *idx0 != 0 {
		buf := t.ref(*idx0)
		if binary.LittleEndian.Uint64(buf) == h {
			return buf[aggregateTagSize:]
		}
	}
	if *idx1 != 0 {
		buf := t.ref(*idx1)
		if binary.LittleEndian.Uint64(buf) == bits.RotateLeft64(h, 32) {
			return buf[aggregateTagSize:]
		}
	}
	return nil
}

// get reference data from a (negative) reference integer
func (t *radixTree64) ref(r int32) []byte {
	idx := int(-r - 1)
	buf := t.values[idx:]
	return buf[:t.vsize]
}

// newtable pushes a new table to the index
func (t *radixTree64) newtable() int {
	idx := len(t.index)
	t.index = t.index[:idx+1]
	return idx
}

// guarantee that there are at least 16 free value slots
func (t *radixTree64) reserve() {
	if len(t.values)+(t.vsize*16) > cap(t.values) {
		t.values = slices.Grow(t.values,
			(len(t.values)+(t.vsize*16))*2)
	}
}

// append a new value associated with the given hash
// to the values array and return its reference number
// and its data buffer
func (t *radixTree64) newvalue(h uint64) int32 {
	t.reserve()
	idx := len(t.values)
	t.values = t.values[:idx+t.vsize]
	buf := t.values[idx:]
	binary.LittleEndian.PutUint64(buf, h)
	return int32(-idx - 1)
}

func (t *radixTree64) walk(idx int, fn func(h uint64, data []byte)) int {
	if idx < 0 {
		buf := t.ref(int32(idx))
		fn(binary.LittleEndian.Uint64(buf), buf[aggregateTagSize:])
		return 0
	}
	tab := &t.index[idx]
	max := 0
	for i := range tab[:] {
		if tab[i] == 0 {
			continue
		}
		d := t.walk(int(tab[i]), fn)
		if d > max {
			max = d
		}
	}
	return max + 1
}

func (t *radixTree64) Walk(fn func(h uint64, data []byte)) int {
	return t.walk(0, fn)
}

// insertSlow inserts an entry for 'h' into the tree
// and returns the assigned t.values offset and
// whether or not an entry for the hash was already present
func (t *radixTree64) insertSlow(h uint64) (int32, bool) {
	// make sure that we have enough memory that we
	// could tolerate a full 64-bit collision during insertion
	if cap(t.index) < len(t.index)+(64>>radix) {
		old := t.index
		t.index = make([][tabsize]int32, len(t.index), 2*(len(t.index)+(64>>radix)))
		copy(t.index, old)
	}

	// try inserts that don't grow the tree
	// TODO: the following, but in assembly
	// (need to pre-allocate up to 16 new values)
	hrev := bits.RotateLeft64(h, 32)
	i0, h0depth := t.find(h)
	var h0, h1 uint64
	if *i0 != 0 {
		buf := t.ref(*i0)
		h0 = binary.LittleEndian.Uint64(buf)
		if h0 == h {
			return ^*i0, false
		}
	}
	i1, h1depth := t.find(hrev)
	if *i1 != 0 {
		buf := t.ref(*i1)
		h1 = binary.LittleEndian.Uint64(buf)
		if h1 == hrev {
			return ^*i1, false
		}
	}

	if *i0 == 0 {
		ref := t.newvalue(h)
		*i0 = ref
		return ^ref, true
	}
	if *i1 == 0 {
		ref := t.newvalue(hrev)
		*i1 = ref
		return ^ref, true
	}

	// --- slow path ---

	// try a cuckoo shuffle to see if we can avoid growing;
	// this gives us another opportunity to minimize table depth
	if i2, _ := t.find(bits.RotateLeft64(h0, 32)); *i2 == 0 {
		*i2 = *i0
		binary.LittleEndian.PutUint64(t.ref(*i0), bits.RotateLeft64(h0, 32))
		ref := t.newvalue(h)
		*i0 = ref
		return ^ref, true
	}
	if i3, _ := t.find(bits.RotateLeft64(h1, 32)); *i3 == 0 {
		*i3 = *i1
		binary.LittleEndian.PutUint64(t.ref(*i1), bits.RotateLeft64(h1, 32))
		ref := t.newvalue(hrev)
		*i1 = ref
		return ^ref, true
	}

	// extra slow path: insert a new table and indirect ref;
	// pick the insertion with the smallest collision
	dist := bits.TrailingZeros64(h ^ h0)
	self := h
	depth := h0depth
	neighbits := h0 >> h0depth
	neighp := i0
	if bits.TrailingZeros64(hrev^h1) < dist {
		self = hrev
		depth = h1depth
		neighbits = h1 >> h1depth
		neighp = i1
	}

	neigh := *neighp
	selfbits := self >> depth
	var tab *[16]int32
	for neighbits&tabmask == selfbits&tabmask {
		ntidx := t.newtable()
		*neighp = int32(ntidx)
		tab = &t.index[ntidx]
		neighbits >>= radix
		selfbits >>= radix
		neighp = &tab[neighbits&tabmask]
	}
	if tab == nil {
		panic("corrupt table")
	}

	ref := t.newvalue(self)
	tab[neighbits&tabmask] = neigh
	tab[selfbits&tabmask] = ref
	return ^ref, true
}

func (t *radixTree64) Insert(h uint64) ([]byte, bool) {
	i, ins := t.insertSlow(h)
	_, buf := t.value(i)
	return buf, ins
}

// value returns the value at a particular value offset
// (you can get this offset from Find or Insert)
func (t *radixTree64) value(i int32) (uint64, []byte) {
	buf := t.values[i : int(i)+t.vsize]
	return binary.LittleEndian.Uint64(buf), buf[aggregateTagSize:]
}

// MaxAggregateBuckets is the maximum cardinality
// of a hash aggregate (SUM(...) ... GROUP BY ...);
// this is chosen somewhat arbitrarily to prevent
// ridiculous memory consumption and the higher
// likelihood of hash collissions
const MaxAggregateBuckets = 1 << 24

type hpair struct {
	reprloc int32
	hloc    int32
}

type aggtable struct {
	parent *HashAggregate

	tree *radixTree64

	// ssa program and compiled bytecode
	// for updating buckets in the tree
	prog prog
	bc   bytecode

	// Kinds of aggregate operations - required to be able to reserve
	// the correct number of bytes for each kind, and to be able to
	// aggergate partially aggregated results.
	aggregateKinds []AggregateKind

	// distinct ion values, concatenated;
	// pointed to by pairs[].reprloc
	//
	// Each `reprloc` stores an index to the first value. We
	// don't store offsets to next values, however, such offsets
	// can be computed on the fly with the help of `ion.SizeOf()`.
	repr []byte

	// each distinct aggregate entry
	// has an hpair entry that holds
	// the representation of each value
	pairs []hpair
}

// for an aggtable, get the hash of the value
func (a *aggtable) hashof(p *hpair) uint64 {
	return binary.LittleEndian.Uint64(a.tree.values[p.hloc:])
}

// for an aggtable, turn an hpair into its ion representation
// at the given byte offset
func (a *aggtable) reproff(p *hpair, off int) []byte {
	mem := a.repr[int(p.reprloc)+off:]
	return mem[:ion.SizeOf(mem)]
}

func (a *aggtable) repridx(p *hpair, idx int) []byte {
	mem := a.repr[p.reprloc:]
	for idx > 0 {
		mem = mem[ion.SizeOf(mem):]
		idx--
	}
	return mem[:ion.SizeOf(mem)]
}

// fullrepr returns the serialized representation of 'columns'
// grouping columns
func (a *aggtable) fullrepr(p *hpair, columns int) []byte {
	mem := a.repr[p.reprloc:]
	width := 0
	for i := 0; i < columns; i++ {
		width += ion.SizeOf(mem[width:])
	}
	return mem[:width]
}

// for an aggtable, turn an hpair into the aggregate memory
func (a *aggtable) valueof(p *hpair) []byte {
	mem := a.tree.values[aggregateTagSize+int(p.hloc):]
	return mem[:a.tree.vsize]
}

func le64(x []byte) uint64 {
	return binary.LittleEndian.Uint64(x)
}

func fp64(x []byte) float64 {
	return math.Float64frombits(le64(x))
}

func cmpInt64(left, right []byte) int {
	lu := int64(le64(left))
	ru := int64(le64(right))
	if lu < ru {
		return -1
	}
	if lu == ru {
		return 0
	}
	return 1
}

func cmpCount(left, right []byte) int {
	lu := le64(left)
	ru := le64(right)
	if lu < ru {
		return -1
	}
	if lu == ru {
		return 0
	}
	return 1
}

// partiql floating-point comparison:
// compare ordered, but sort NaN first
func cmpPartiqlfp(leftf, rightf float64) int {
	if leftf < rightf {
		return -1
	}
	if math.IsNaN(leftf) {
		if math.IsNaN(rightf) {
			return 0 // NaN and NaN are un-ordered
		}
		return -1 // sort NaN before real number
	}
	if leftf == rightf {
		return 0
	}
	return 1
}

func cmpFloat(left, right []byte) int {
	return cmpPartiqlfp(fp64(left), fp64(right))
}

func cmpAvgInt64(left, right []byte) int {
	lcnt := le64(left[8:])
	rcnt := le64(right[8:])

	if lcnt == 0 {
		if rcnt == 0 {
			return 0
		}
		return 1
	} else if rcnt == 0 {
		return -1
	}

	lavg := int64(le64(left)) / int64(lcnt)
	ravg := int64(le64(right)) / int64(rcnt)

	if lavg < ravg {
		return -1
	}
	if lavg > ravg {
		return 1
	}
	return 0
}

func cmpAvgFloat64(left, right []byte) int {
	lcnt := le64(left[8:])
	rcnt := le64(right[8:])

	if lcnt == 0 {
		if rcnt == 0 {
			return 0
		}
		return 1
	} else if rcnt == 0 {
		return -1
	}

	lavg := fp64(left) / float64(lcnt)
	ravg := fp64(right) / float64(rcnt)
	return cmpPartiqlfp(lavg, ravg)
}

var agg2cmp = [...](func([]byte, []byte) int){
	AggregateKindNone:  nil,
	AggregateKindSumF:  cmpFloat,
	AggregateKindAvgF:  cmpAvgFloat64,
	AggregateKindMinF:  cmpFloat,
	AggregateKindMaxF:  cmpFloat,
	AggregateKindSumI:  cmpInt64,
	AggregateKindSumC:  cmpInt64,
	AggregateKindAvgI:  cmpAvgInt64,
	AggregateKindMinI:  cmpInt64,
	AggregateKindMaxI:  cmpInt64,
	AggregateKindAndI:  cmpInt64,
	AggregateKindOrI:   cmpInt64,
	AggregateKindXorI:  cmpInt64,
	AggregateKindAndK:  cmpInt64,
	AggregateKindOrK:   cmpInt64,
	AggregateKindMinTS: cmpInt64,
	AggregateKindMaxTS: cmpInt64,
	AggregateKindCount: cmpCount,
}

// return an integer that can be used to sort
// the results of the aggregate expression
// (< 0 for less, 0 for equal, > 0 for greater)
func aggcmp(kind AggregateKind, left, right []byte) int {
	return agg2cmp[kind](left, right)
}

func (a *aggtable) initentry(buf []byte) {
	copy(buf, a.parent.initialData)
}

//go:noescape
func evalhashagg(bc *bytecode, delims []vmref, tree *radixTree64, abort *uint16) int

func (a *aggtable) fasteval(delims []vmref, abort *uint16) int {
	if a.bc.compiled == nil {
		panic("aggtable.bc.compiled == nil")
	}

	return evalhashagg(&a.bc, delims, a.tree, abort)
}

func (a *aggtable) symbolize(st *symtab) error {
	err := recompile(st, &a.parent.prog, &a.prog, &a.bc)
	if err != nil {
		return err
	}
	return nil
}

func (b *bytecode) getVRegOffsetAndSize(base, index int) (uint32, uint32) {
	// TODO: I think this code is ugly. Really wondering whether we cannot
	// just use some IO to just read 32-bit quantities, that would avoid
	// `shift` and deciding between even/odd value.
	element := index >> 1
	shift := 32 * (index & 1)

	lo := uint32(b.vstack[base+element+0] >> shift)
	hi := uint32(b.vstack[base+element+8] >> shift)

	return lo, hi
}

func (a *aggtable) writeRows(delims []vmref) error {
	// Number of projected fields that we GROUP BY. This
	// specifies how many concatenated values will be stored
	// in a.repr[] for each aggregated item.
	projectedGroupByCount := len(a.parent.by)
	vRegSizeInUInt64Units := int(vRegSize >> 3)
	var abort uint16
	for len(delims) > 0 {
		n := a.fasteval(delims, &abort)
		if a.bc.err != 0 && a.bc.err != bcerrNeedRadix {
			errorf("error pc %d", a.bc.errpc)
			errorf("bytecode:\n%s\n", a.bc.String())
			return fmt.Errorf("hash aggregate: bytecode error: errpc %d: %w", a.bc.errpc, a.bc.err)
		}
		delims = delims[n:]
		if len(delims) != 0 && abort == 0 {
			panic("did not expect early return with abort==0")
		}

		// for each value that did not have a location,
		// create a newly-initialized slot and then
		// restart the loop with the larger table
		//
		// TODO: when the number of restarts is high,
		// consider allocating table space more aggressively?
		step := 16 - bits.LeadingZeros16(abort)
		hashmem := a.bc.hashmem[a.bc.errinfo>>3:]
		for i := 0; i < step; i++ {
			if abort&(1<<i) == 0 {
				continue
			}

			// FIXME: we're assuming the result is
			// allocated in hash slot 0 because the
			// only other hash slot usage would be
			// an IN(...) expression that shouldn't
			// be part of the aggregation...
			if len(a.bc.hashmem) != 32 {
				panic("more than 1 hash slot?")
			}
			h := hashmem[i*2]
			off, ok := a.tree.insertSlow(h)
			if ok {
				// new distinct value
				if len(a.pairs) >= MaxAggregateBuckets {
					return fmt.Errorf("cannot create more than %d aggregate pairs", len(a.pairs))
				}

				// start of the index in `a.repr` where all GROUP BY fields will be appended.
				reprloc := int32(len(a.repr))

				for n := 0; n < projectedGroupByCount; n++ {
					lo, hi := a.bc.getVRegOffsetAndSize(n*vRegSizeInUInt64Units, i)
					if hi == 0 {
						// TODO: Should we specify the value to provide more info in case it happens?
						panic("abort bit set on a MISSING value")
					}
					mem := vmref{lo, hi}.mem()
					// must be a single valid ion object
					if ion.SizeOf(mem) != len(mem) {
						panic(fmt.Sprintf("column %d vmref 0x%x has invalid size", n, mem))
					}
					a.repr = append(a.repr, mem...)
				}
				a.pairs = append(a.pairs, hpair{
					reprloc: reprloc,
					hloc:    off,
				})
				a.initentry(a.tree.values[off+8:])
			}
		}
	}
	return nil
}

func (a *aggtable) Close() error {
	a.bc.reset()
	parent := a.parent
	parent.lock.Lock()

	// a little clever:
	// when another thread finished earlier,
	// grab its result, drop the lock,
	// perform the merge, and then write
	// the result back (assuming there
	// are no other new results);
	// otherwise grab the next result
	// and merge again!
	//
	// this ends up being more memory-hungry
	// than doing a single merge, but it is
	// faster since we are potentially performing
	// multiple merges simultaneously
	for parent.final != nil {
		tmp := parent.final
		parent.final = nil
		parent.lock.Unlock()
		a.merge(tmp)
		parent.lock.Lock()
	}

	parent.final = a
	if atomic.AddInt64(&parent.children, -1) < 0 {
		panic("duplicate aggtable.Close()")
	}
	parent.lock.Unlock()
	return nil
}

// merge the right-hand-side table into
// the left-hand-side table by walking
// all of the right-hand-side entries
// and inserting/merging them via the slow path
func (a *aggtable) merge(r *aggtable) {
	for i := range r.pairs {
		p := &r.pairs[i]
		// get value from rhs
		hash := r.hashof(p)
		repr := r.fullrepr(p, len(a.parent.by))
		value := r.valueof(p)

		// regular insert slow path for lhs
		off, ok := a.tree.insertSlow(hash)
		if ok {
			reprloc := int32(len(a.repr))
			a.repr = append(a.repr, repr...)
			a.pairs = append(a.pairs, hpair{
				reprloc: reprloc,
				hloc:    off,
			})
			a.initentry(a.tree.values[off+8:])
		}

		mergeAggregatedValues(a.tree.values[off+8:], value, a.aggregateKinds)
	}
}
