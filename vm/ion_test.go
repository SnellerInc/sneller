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
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestSymbolEncoded(t *testing.T) {
	tcs := []int{
		1,
		127,
		128,
		129,
		255,
		256,
		257,
		511,
		512,
		1023,
		1024,
		1025,
	}

	var buf [4]byte
	for i := range tcs {
		buf = [4]byte{}
		sym := ion.Symbol(tcs[i])

		enc, mask, size := encoded(sym)
		if enc&^mask != 0 {
			t.Errorf("encoded(%d): mask %x?", sym, mask)
		}
		binary.LittleEndian.PutUint32(buf[:], enc)

		val, esize := uvint(buf[:])
		if val != uint(sym) {
			t.Errorf("encoded(%d): %x", sym, buf[:])
			t.Errorf("decoded as %d", val)
		}
		if mask != (1<<(esize*8))-1 {
			t.Errorf("esize %d but mask %x", esize, mask)
		}
		if esize != int(size) {
			t.Errorf("got size %d but expected %d", size, esize)
		}
	}
}

func TestCopyObject(t *testing.T) {
	buf := unhex(parkingCitations1KLines)
	if ion.IsBVM(buf) {
		buf = buf[4:]
	}

	out := make([]byte, len(buf))

	src := buf
	off := 0
	for off < len(src) {
		off += copyobj(out[off:], src[off:])
	}
	if !bytes.Equal(buf, out) {
		t.Fatal("didn't produce an identical object?")
	}
}

func TestParseSimplfiedTimestamp(t *testing.T) {

	ts := date.Date(2021, 8, 22, 14, 42, 32, 0)

	testcases := []struct {
		trunc ion.TimeTrunc
		val   uint64
	}{
		{trunc: ion.TruncToSecond,
			val: 0x800fe588968eaaa0},
		{trunc: ion.TruncToMinute,
			val: 0x800fe588968eaa80},
		{trunc: ion.TruncToHour,
			val: 0x800fe588968e8080},
		{trunc: ion.TruncToDay,
			val: 0x800fe58896808080},
		{trunc: ion.TruncToMonth,
			val: 0x800fe58880808080},
		{trunc: ion.TruncToYear,
			val: 0x800fe58080808080},
	}

	var buf ion.Buffer
	for i := range testcases {
		buf.Reset()
		// given
		buf.WriteTruncatedTime(ts, testcases[i].trunc)

		// when
		val, ok := ionParseSimplifiedTimestamp(buf.Bytes())
		if !ok {
			t.Fatalf("Expected % 02x to be interpreted as simplified timestamp", buf.Bytes())
		}

		// then
		if val != testcases[i].val {
			t.Logf("%016x != %016x", val, testcases[i].val)
			t.Errorf("case #%d: wrongly decoded", i)
		}
	}
}

func TestSimplifiedTimestampToTime(t *testing.T) {

	ts := date.Date(2021, 8, 22, 14, 42, 32, 0)

	// given
	var buf ion.Buffer
	buf.WriteTime(ts)

	// when
	val, ok := ionParseSimplifiedTimestamp(buf.Bytes())
	if !ok {
		t.Fatalf("Expected % 02x to be interpreted as simplified timestamp", buf.Bytes())
	}

	// then
	if ts != simplifiedTimestampToTime(val) {
		t.Logf("%s != %016x", ts, simplifiedTimestampToTime(val))
		t.Errorf("wrongly decoded")
	}
}

func TestSimplfiedTimestampComparison(t *testing.T) {
	// given
	var buf ion.Buffer
	for i := 0; i < 6; i++ {
		year := 2021
		month := 8
		day := 22
		hour := 14
		minute := 42
		second := 32
		ts1 := date.Date(year, month, day, hour, minute, second, 0)

		switch i {
		case 0:
			year += 1
		case 1:
			month += 1
		case 2:
			day += 1
		case 3:
			hour += 1
		case 4:
			minute += 1
		case 5:
			second += 1
		}

		ts2 := date.Date(year, month, day, hour, minute, second, 0)

		buf.Reset()
		buf.WriteTime(ts1)

		val1, ok1 := ionParseSimplifiedTimestamp(buf.Bytes())
		if !ok1 {
			t.Fatalf("Expected % 02x to be interpreted as simplified timestamp", buf.Bytes())
		}

		buf.Reset()
		buf.WriteTime(ts2)

		val2, ok2 := ionParseSimplifiedTimestamp(buf.Bytes())
		if !ok2 {
			t.Fatalf("Expected % 02x to be interpreted as simplified timestamp", buf.Bytes())
		}

		if ts1.Before(ts2) != (val1 < val2) {
			t.Errorf("wrong comparison result: %v != %v", ts1.Before(ts2), val1 < val2)
		}
	}
}

func BenchmarkCopy1KObjects(b *testing.B) {
	buf := unhex(parkingCitations1KLines)
	if ion.IsBVM(buf) {
		buf = buf[4:]
	}
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		out := make([]byte, len(buf))
		for pb.Next() {
			off := 0
			for off < len(buf) {
				off += copyobj(out[off:], buf[off:])
			}
		}
	})
}
