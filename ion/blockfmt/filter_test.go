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
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"

	"golang.org/x/exp/slices"
)

func TestFilter(t *testing.T) {
	var f Filter
	var si SparseIndex
	run := func(filt string, ranges [][2]int) {
		t.Helper()
		qbytes := []byte(fmt.Sprintf("SELECT * WHERE %s", filt))
		q, err := partiql.Parse(qbytes)
		if err != nil {
			t.Fatal(err)
		}
		q.Body = expr.Simplify(q.Body, expr.HintFn(expr.NoHint))
		f.Compile(q.Body.(*expr.Select).Where)
		var out [][2]int
		f.Visit(&si, func(start, end int) {
			out = append(out, [2]int{start, end})
		})
		if !slices.Equal(out, ranges) {
			t.Errorf("got %v; wanted %v", out, ranges)
		}
		empty := true
		for i := range ranges {
			start := ranges[i][0]
			end := ranges[i][1]
			if start == end {
				continue // empty range
			}
			empty = false
			if !f.Overlaps(&si, 0, start+1) {
				t.Errorf("doesn't overlap [%d %d]", 0, start+1)
			}
			if end > 0 && !f.Overlaps(&si, end-1, si.Blocks()) {
				t.Errorf("doesn't overlap [%d %d]", end-1, si.Blocks())
			}
		}
		if f.MatchesAny(&si) == empty {
			t.Error("MatchesAny and empty do not match")
		}
	}
	testFilter(t, &f, &si, run)
}

func BenchmarkFilter(b *testing.B) {
	var f Filter
	var si SparseIndex
	num := 0
	run := func(filt string, args [][2]int) {
		b.Run(fmt.Sprintf("case-%d", num), func(b *testing.B) {
			qbytes := []byte(fmt.Sprintf("SELECT * WHERE %s", filt))
			q, err := partiql.Parse(qbytes)
			if err != nil {
				b.Fatal(err)
			}
			q.Body = expr.Simplify(q.Body, expr.HintFn(expr.NoHint))
			where := q.Body.(*expr.Select).Where
			b.Logf("query: %s", expr.ToString(where))
			b.Run("compile", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					f.Compile(where)
				}
			})
			b.Run("visit", func(b *testing.B) {
				b.ReportAllocs()
				var out [][2]int
				for i := 0; i < b.N; i++ {
					out = out[:0]
					f.Visit(&si, func(start, end int) {
						out = append(out, [2]int{start, end})
					})
				}
			})
		})
		num++
	}
	testFilter(b, &f, &si, run)
}

func testFilter(t testing.TB, f *Filter, si *SparseIndex, run func(filt string, ranges [][2]int)) {
	if !f.Trivial() {
		t.Error("zero value of Filter should be Trivial")
	}

	// produce uniformly-spaced blocks where
	// block N has time base + N minutes up to
	// (but not including) minute N+1
	base := date.Now().Truncate(time.Minute)
	for i := 0; i < 60; i++ {
		start := base.Add(time.Minute * time.Duration(i))
		end := start.Add(time.Minute - time.Microsecond)
		rng := NewRange([]string{"timestamp"},
			(&expr.Timestamp{start}).Datum(),
			(&expr.Timestamp{end}).Datum())
		si.Push([]Range{rng})
	}
	// double-check index looks right:
	for i := 0; i < 60; i++ {
		m := base.Add(time.Minute * time.Duration(i))
		ti := si.Get([]string{"timestamp"})
		if ti.Start(m) != i ||
			ti.End(m) != i+1 {
			t.Fatalf("minute %d: start/end = %d/%d", i, ti.Start(m), ti.End(m))
		}
	}
	minute := func(i int) string {
		return "`" + base.Add(time.Minute*time.Duration(i)).Time().Format(time.RFC3339Nano) + "`"
	}
	unixminute := func(i int) int64 {
		return base.Add(time.Minute * time.Duration(i)).Unix()
	}
	unixmicro := func(i int) int64 {
		return base.Add(time.Minute * time.Duration(i)).UnixMicro()
	}
	sprintf := fmt.Sprintf
	run(sprintf("x = 'foo'"), [][2]int{{0, 60}})
	run(sprintf("timestamp < %s", minute(0)), [][2]int{{0, 0}})
	run(sprintf("timestamp >= %s", minute(60)), [][2]int{{0, 0}})
	run(sprintf("timestamp < %s", minute(1)), [][2]int{{0, 1}})
	run(sprintf("to_unix_epoch(timestamp) < %d", unixminute(1)), [][2]int{{0, 1}})
	run(sprintf("to_unix_micro(timestamp) < %d", unixmicro(1)), [][2]int{{0, 1}})
	run(sprintf("%s > timestamp", minute(1)), [][2]int{{0, 1}})
	run(sprintf("timestamp <= %s", minute(1)), [][2]int{{0, 2}})
	run(sprintf("unknown and timestamp < %s", minute(1)), [][2]int{{0, 1}})
	run(sprintf("timestamp > %s", minute(1)), [][2]int{{1, 60}})
	// overlapping ranges should be coalesced:
	run(sprintf("timestamp > %s and timestamp > %s", minute(1), minute(2)), [][2]int{{2, 60}})
	run(sprintf("timestamp >= %s", minute(1)), [][2]int{{1, 60}})
	run(sprintf("timestamp > %s and unknown", minute(1)), [][2]int{{1, 60}})
	run(sprintf("timestamp < %s and timestamp > %s", minute(1), minute(1)), [][2]int{{0, 0}})
	run(sprintf("timestamp < %s or timestamp > %s", minute(1), minute(1)), [][2]int{{0, 60}})
	run(sprintf("timestamp < %s or timestamp > %s", minute(30), minute(30)), [][2]int{{0, 60}})
	run(sprintf("timestamp < %s or timestamp >= %s", minute(1), minute(59)), [][2]int{{0, 1}, {59, 60}})
	run(sprintf("timestamp = %s", minute(1)), [][2]int{{1, 2}})
	run(sprintf("to_unix_epoch(timestamp) = %d", unixminute(1)), [][2]int{{1, 2}})
}