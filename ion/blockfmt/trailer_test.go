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
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestTrailerEncode(t *testing.T) {
	time0 := date.Now().Truncate(time.Microsecond)
	samples := []Trailer{
		{Version: 1},
		{Version: 1, Offset: 0x12345, Algo: "zstd", BlockShift: 20},
		{
			Version:    1,
			Offset:     0x12345,
			Algo:       "lz4",
			BlockShift: 20,
			Blocks: []Blockdesc{
				{
					Offset: 0,
					Chunks: 700,
				},
				{
					Offset: 1 << 20,
					Chunks: 1,
				},
			},
			Sparse: mksparse([]ion.Field{
				{Label: "foo", Value: ion.String("foo")},
				{Label: "bar", Value: ion.Int(100)},
			}, []TimeRange{
				{[]string{"foo"}, time0, time0.Add(time.Second)},
				{[]string{"foo"}, time0.Add(time.Second), time0.Add(time.Minute)},
			}),
		},
	}

	for i := range samples {
		var st ion.Symtab
		var buf ion.Buffer
		samples[i].Encode(&buf, &st)

		var out Trailer
		err := out.Decode(&st, buf.Bytes())
		if err != nil {
			t.Fatalf("case %d: %s", i, err)
		}
		// FIXME: reflect.DeepEqual doesn't play
		// well with ion.Struct because the symbol
		// tables won't match, so just check the
		// constants separately then copy them in
		if !ion.Equal(out.Sparse.consts.Datum(), samples[i].Sparse.consts.Datum()) {
			t.Error("constants are not equal")
		}
		samples[i].Sparse.consts = out.Sparse.consts
		if !reflect.DeepEqual(samples[i], out) {
			t.Error("results not equivalent")
		}
	}
}
