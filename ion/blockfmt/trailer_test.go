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
				{Label: "foo", Datum: ion.String("foo")},
				{Label: "bar", Datum: ion.Int(100)},
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
