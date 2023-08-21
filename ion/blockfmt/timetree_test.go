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

package blockfmt

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func (t *TimeIndex) Equal(o *TimeIndex) bool {
	return slices.Equal(t.min, o.min) &&
		slices.Equal(t.max, o.max)
}

func (t *TimeIndex) Decode(st *ion.Symtab, buf []byte) error {
	d, _, err := ion.ReadDatum(st, buf)
	if err != nil {
		return err
	}
	*t = TimeIndex{}
	var td TrailerDecoder
	return td.decodeTimes(t, d)
}

func testTimeIndexRoundtrip(t *testing.T, ti *TimeIndex) {
	var buf ion.Buffer
	var st ion.Symtab
	var cmp TimeIndex
	ti.Encode(&buf, &st)
	err := cmp.Decode(&st, buf.Bytes())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	if len(ti.max) != len(cmp.max) {
		t.Helper()
		t.Fatalf("%d -> %d?", len(ti.max), len(cmp.max))
	}
	if len(ti.min) != len(cmp.min) {
		t.Helper()
		t.Fatalf("%d -> %d?", len(ti.min), len(cmp.min))
	}
	for i := range ti.max {
		if ti.max[i] != cmp.max[i] {
			t.Helper()
			t.Fatalf("max %d: %v != %v", i, ti.max[i], cmp.max[i])
		}
	}
	for i := range ti.min {
		if ti.min[i] != cmp.min[i] {
			t.Helper()
			t.Fatalf("min %d: %v != %v", i, ti.min[i], cmp.min[i])
		}
	}
}

func TestTimeTree(t *testing.T) {
	start := date.Now().Truncate(time.Microsecond)

	ti := &TimeIndex{}
	testTimeIndexRoundtrip(t, ti)

	// we are going to Append these to
	// see if they are equal to ti after
	// the append operation
	var lo, hi TimeIndex

	// first, do only monotonic insertions
	// and tests that things are recorded correctly:
	for i := 0; i < 1000; i++ {
		if ti.Blocks() != i {
			t.Fatalf("iter %d: %d blocks", i, ti.Blocks())
		}

		next := start.Add((time.Second * 3) / 4)
		ti.Push(start, next)

		cur := &lo
		if i >= 500 {
			cur = &hi
		}
		cur.Push(start, next)

		// since we use non-overlapping ranges,
		// these should yield equivalent results
		if ti.Start(start) != ti.Start(next) {
			t.Fatalf("block %d: Start(min)=%d, Start(max)=%d", i, ti.Start(start), ti.Start(next))
		}
		if ti.End(start) != ti.End(next) {
			t.Fatalf("block %d: End(min)=%d, End(max)=%d", i, ti.End(start), ti.End(next))
		}
		if !ti.Contains(start.Add(next.Time().Sub(start.Time()) / 2)) {
			t.Fatal("doesn't contain midpoint?")
		}
		// there is a hole at 3/4 of a second,
		// so 0.75s + 1ns should be a position
		// that doesn't have any assigned range
		// (i.e. Start == End)
		if ti.Contains(start.Add(1 + (time.Second*3)/4)) {
			t.Fatal("contains hole?")
		}

		// Start(min) should start
		// at the previous block offset (or 0)
		pos := ti.Start(start)
		if pos != i {
			t.Fatalf("block %d Start(min) = %d", i, pos)
		}
		// End(start) should include this block
		pos = ti.End(next)
		if pos != i+1 {
			t.Fatalf("block %d End(max) = %d", i, pos)
		}

		start = next.Add(time.Microsecond)
	}
	lo.Append(&hi)
	if !ti.Equal(&lo) {
		// lo.Append(&hi) should produce a sparse index
		// that is exactly equivalent to the one we built
		t.Error("lo not equal to ti after Append")
	}

	testTimeIndexRoundtrip(t, ti)
	testTimeIndexRoundtrip(t, &hi)

	ti.Reset()

	times := make([]date.Time, 1000)
	start = date.Now().Truncate(time.Microsecond)
	for i := range times {
		times[i] = start.Add(time.Second * time.Duration(i))
	}
	min := times[0]
	max := times[len(times)-1].Add(time.Second / 2)

	shuffle := func(lst []date.Time) {
		rand.Shuffle(len(lst), func(i, j int) {
			lst[i], lst[j] = lst[j], lst[i]
		})
	}
	// add 1000 elements in 10 monotonic intervals
	for i := 0; i < len(times); i += 100 {
		shuffle(times[i : i+100])
	}

	for i := range times {
		if ti.Blocks() != i {
			t.Fatalf("iter %d: %d blocks?", i, ti.Blocks())
		}
		start := times[i]
		end := start.Add(time.Second / 2)
		ti.Push(start, end)

		// want blocks[base:] or more precise
		pos := ti.Start(times[i])
		base := (i / 100) * 100
		if pos < base || pos > i {
			t.Fatalf("block %d: start %d not within span [%d:]", i, pos, base)
		}

		// want blocks[:pos+1] or more precise
		limit := ti.End(times[i])
		if limit <= pos || limit > (base+100) {
			t.Fatalf("block %d: end %d not within span [%d:]", i, limit, base)
		}
	}
	if ti.Blocks() != len(times) {
		t.Fatalf("%d blocks? (expected %d)", ti.Blocks(), len(times))
	}

	// there should be ten recognized intervals (or more)
	if ti.StartIntervals() < 10 {
		t.Errorf("%d left intervals", ti.StartIntervals())
	}
	if ti.EndIntervals() < 10 {
		t.Errorf("%d right intervals", ti.EndIntervals())
	}

	testTimeIndexRoundtrip(t, ti)
	gotmin, ok := ti.Min()
	if !ok {
		t.Fatal("Min() failed")
	}
	if !gotmin.Equal(min) {
		t.Errorf("min %s != %s", min, gotmin)
	}
	gotmax, ok := ti.Max()
	if !ok {
		t.Fatal("Max() failed")
	}
	if !gotmax.Equal(max) {
		t.Errorf("max %s != %s", max, gotmax)
	}
}

func TestTimetreeTrim(t *testing.T) {
	base, err := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	hour := func(i int) date.Time {
		return date.FromTime(base).Add(time.Duration(i) * time.Hour)
	}
	hp := func(hr, offset int) timespan {
		return timespan{when: hour(hr), offset: offset}
	}
	grp := func(args ...timespan) []timespan {
		return args
	}

	run := func(i, j int, min, max []timespan, wantmin, wantmax []timespan) {
		t.Helper()
		ts := TimeIndex{min, max}
		got := ts.trim(i, j)
		want := TimeIndex{wantmin, wantmax}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("want %#v", want)
			t.Errorf("got  %#v", got)
		}
	}

	run(0, 1,
		grp(hp(0, 0), hp(1, 1)),
		grp(hp(2, 2)),
		grp(hp(0, 0)),
		grp(hp(2, 1)))
	run(0, 2,
		grp(hp(0, 0), hp(1, 1), hp(2, 2)),
		grp(hp(3, 2), hp(4, 3)),
		grp(hp(0, 0), hp(1, 1)),
		grp(hp(3, 2)))
	run(0, 50,
		grp(hp(0, 3), hp(1, 49), hp(2, 50)),
		grp(hp(44, 48), hp(47, 49), hp(48, 51)),
		grp(hp(0, 3), hp(1, 49)),
		grp(hp(44, 48), hp(47, 49), hp(48, 50)))
	run(1, 2,
		grp(hp(0, 0), hp(1, 1), hp(2, 2)),
		grp(hp(3, 2), hp(4, 3)),
		grp(hp(1, 0)),
		grp(hp(3, 1)))
	run(10, 50,
		grp(hp(0, 3), hp(1, 49), hp(2, 50)),
		grp(hp(44, 48), hp(47, 49), hp(48, 51)),
		grp(hp(1, 39)),
		grp(hp(44, 38), hp(47, 39), hp(48, 40)))
}

func FuzzTimeIndexAppendBlocks(f *testing.F) {
	now := date.Now().Truncate(time.Minute)
	// generate produces a series of time ranges
	// using the values in a []byte provided by
	// the fuzzer, and returns whether the
	// resulting time intervals are monotonic
	generate := func(b []byte) (out [][2]date.Time, mono bool) {
		out = make([][2]date.Time, len(b))
		mono = true
		base := now
		for i := range b {
			d1 := b[i] & 0b00001111
			d2 := b[i] & 0b11110000 >> 4
			out[i][0] = base
			out[i][1] = base.Add(time.Duration(d1) * time.Minute)
			base = base.Add(time.Duration(d2) * time.Minute)
			if d2 <= d1 {
				mono = false
			}
		}
		return out, mono
	}
	// appendall is a trivially correct version of
	// append that works on the whole range
	appendall := func(t, next *TimeIndex) {
		n := t.Blocks()
		for i := range next.min {
			t.pushMin(next.min[i].when, next.min[i].offset+n)
		}
		for i := range next.max {
			t.pushMax(next.max[i].when, next.max[i].offset+n)
		}
	}
	mkindex := func(ts [][2]date.Time) TimeIndex {
		var out TimeIndex
		for i := range ts {
			out.Push(ts[i][0], ts[i][1])
		}
		return out
	}
	f.Add([]byte{0x1a}, []byte{0x01, 0x10})
	f.Add([]byte{0x01, 0x23}, []byte{0xab, 0xcd})
	f.Fuzz(func(t *testing.T, b0, b1 []byte) {
		if len(b0) > 20 || len(b1) > 20 {
			t.Skip("skipping large input")
		}
		if len(b1) < 1 {
			t.Skip("skipping empty range")
		}
		ts0, mono0 := generate(b0)
		ts1, mono1 := generate(b1)
		next := mkindex(ts1)
		blocks := next.Blocks()
		run := func(i, j int) {
			t.Helper()
			t.Run(fmt.Sprintf("i=%d/j=%d", i, j), func(t *testing.T) {
				got := mkindex(ts0)
				got.appendBlocks(&next, i, j)
				// check the block count
				if n0, n1 := len(ts0)+j-i, got.Blocks(); n0 != n1 {
					t.Errorf("wrong number of blocks: %d != %d", n0, n1)
				}
				// if both inputs are monotonic, the
				// result can be checked against the
				// index rebuilt from the result of
				// appending ts1[i:j] to t0
				if mono0 && mono1 {
					want := mkindex(append(ts0, ts1[i:j]...))
					if !reflect.DeepEqual(want, got) {
						t.Errorf("did not match (appended inputs)")
						t.Errorf("  want: %v", want)
						t.Errorf("  got:  %v", got)
					}
				}
				// also check against trim + append
				// which should handle time indexes
				// built from non-monotonic inputs the
				// same way
				trimmed := mkindex(ts1)
				trimmed = trimmed.trim(i, j)
				want := mkindex(ts0)
				appendall(&want, &trimmed)
				if !reflect.DeepEqual(want, got) {
					t.Errorf("  want: %v", want)
					t.Errorf("  got:  %v", got)
				}
			})
		}
		if !mono0 || !mono1 {
			// skip most non-monotonic series for now
			run(0, blocks)
			return
		}
		// try appending every combination
		for i := 0; i < blocks; i++ {
			for j := i + 1; j <= blocks; j++ {
				run(i, j)
			}
		}
	})
}
