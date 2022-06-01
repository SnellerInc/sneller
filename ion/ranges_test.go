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
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func TestRangesSaveLoad(t *testing.T) {
	var rs Ranges
	var snap Snapshot

	p1 := mksymstr(1)
	p2 := mksymstr(2)
	p3 := mksymstr(3)
	ts := date.Date(2021, 11, 11, 11, 0, 0, 0)

	rs.AddTime(mksymbuf(1), ts)
	rs.save(&snap)
	rs.AddTime(mksymbuf(2), ts)
	rs.AddTime(mksymbuf(3), ts)

	if want := (Ranges{
		paths: []symstr{p1, p2, p3},
		m: map[symstr]dataRange{
			p1: newTimeRange(ts),
			p2: newTimeRange(ts),
			p3: newTimeRange(ts),
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch before load")
		t.Errorf("want: %#v", want)
		t.Errorf("got:  %#v", rs)
	}

	rs.load(&snap)

	if want := (Ranges{
		paths: []symstr{p1},
		m: map[symstr]dataRange{
			p1: newTimeRange(ts),
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch after load")
		t.Errorf("want: %#v", want)
		t.Errorf("got:  %#v", rs)
	}

	rs.AddTime(mksymbuf(3), ts)

	if want := (Ranges{
		paths: []symstr{p1, p3},
		m: map[symstr]dataRange{
			p1: newTimeRange(ts),
			p3: newTimeRange(ts),
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch after addTime")
		t.Errorf("want: %#v", want)
		t.Errorf("got:  %#v", rs)
	}
}

func TestRangesCommit(t *testing.T) {
	var rs Ranges

	p1 := mksymstr(1)
	p2 := mksymstr(2)
	t1 := date.Date(2021, 11, 11, 11, 0, 0, 0)
	t2 := t1.Add(time.Minute)

	rs.AddTime(mksymbuf(1), t1)
	rs.AddTime(mksymbuf(2), t1)
	rs.commit()
	rs.AddTime(mksymbuf(1), t2)

	if want := (Ranges{
		paths: []symstr{p1, p2},
		m: map[symstr]dataRange{
			p1: &timeRange{
				min:        t1,
				max:        t1,
				hasRange:   true,
				pending:    t2,
				hasPending: true,
				commits:    1,
			},
			p2: &timeRange{
				min:        t1,
				max:        t1,
				hasRange:   true,
				pending:    t1, // not cleared
				hasPending: false,
				commits:    1,
			},
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch before flush")
		t.Errorf("want: %#v", want)
		t.Errorf("got:  %#v", rs)
	}

	rs.flush()

	if want := (Ranges{
		paths: []symstr{p1},
		m: map[symstr]dataRange{
			p1: &timeRange{
				min:        t1, // not cleared
				max:        t1, // not cleared
				hasRange:   false,
				pending:    t2,
				hasPending: true,
				commits:    0,
			},
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch after flush")
		t.Errorf("want = %#v", want)
		t.Errorf("got  = %#v", rs)
	}

	rs.commit()

	if want := (Ranges{
		paths: []symstr{p1},
		m: map[symstr]dataRange{
			p1: &timeRange{
				min:        t2,
				max:        t2,
				hasRange:   true,
				pending:    t2, // not cleared
				hasPending: false,
				commits:    1,
			},
		},
	}); !rangesEqual(want, rs) {
		t.Errorf("mismatch after commit")
		t.Errorf("want = %#v", want)
		t.Errorf("got  = %#v", rs)
	}
}

// This can be run to make sure that range tracking is
// not super alloc-y.
func BenchmarkRanges(b *testing.B) {
	t := date.Date(2021, 11, 10, 0, 0, 0, 0)

	paths := [...]Symbuf{
		mksymbuf(1),
		mksymbuf(1, 2),
		mksymbuf(1, 2, 3),
		mksymbuf(1, 2, 3, 4),
	}

	var r Ranges

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := paths[i%len(paths)]
		r.AddTime(p, t)
	}

	if want := len(paths); b.N >= want {
		if got := len(r.paths); got != want {
			b.Errorf("wrong number of paths: %d != %d", got, want)
		}
		if got := len(r.m); got != want {
			b.Errorf("wrong number of ranges: %d != %d", got, want)
		}
	}
}

func mksymbuf(s ...Symbol) Symbuf {
	var b Symbuf
	b.Prepare(len(s))
	for _, s := range s {
		b.Push(s)
	}
	return b
}

func mksymstr(s ...Symbol) symstr {
	return symstr(mksymbuf(s...))
}

func rangesEqual(r1, r2 Ranges) bool {
	return reflect.DeepEqual(r1.paths, r2.paths) &&
		reflect.DeepEqual(r1.m, r2.m)
}
