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

	"github.com/SnellerInc/sneller/ion"
)

func TestTrailerEncode(t *testing.T) {
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
					Ranges: []Range{
						NewRange(
							[]string{"x", "y"},
							ion.Uint(1000),
							ion.Uint(2000),
						),
					},
				},
				{
					Offset: 1 << 20,
					Chunks: 1,
					Ranges: []Range{
						NewRange(
							[]string{"x", "y"},
							ion.Uint(0),
							ion.Uint(1000),
						),
					},
				},
			},
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
		if !reflect.DeepEqual(samples[i], out) {
			t.Error("results not equivalent")
		}
	}
}

func TestTrailerCombineWith(t *testing.T) {
	a := Trailer{
		Version:    1,
		Offset:     1000,
		Algo:       "lz4",
		BlockShift: 20,
		Blocks: []Blockdesc{
			{
				Offset: 0,
				Ranges: []Range{
					NewRange(
						[]string{"x", "y"},
						ion.Uint(1000),
						ion.Uint(2000),
					),
				},
			},
			{
				Offset: 500,
				Ranges: []Range{
					NewRange(
						[]string{"z", "z"},
						ion.Uint(0),
						ion.Uint(1000),
					),
				},
			},
		},
	}
	b := Trailer{
		Version:    1,
		Offset:     2000,
		Algo:       "lz4",
		BlockShift: 20,
		Blocks: []Blockdesc{
			{
				Offset: 0,
				Ranges: []Range{
					NewRange(
						[]string{"a", "b"},
						ion.Uint(1337),
						ion.Uint(1338),
					),
				},
			},
			{
				Offset: 1000,
				Ranges: []Range{
					NewRange(
						[]string{"c", "d"},
						ion.Uint(42),
						ion.Uint(24),
					),
				},
			},
		},
	}
	expected := Trailer{
		Version:    1,
		Offset:     3000,
		Algo:       "lz4",
		BlockShift: 20,
		Blocks: []Blockdesc{
			{
				Offset: 0,
				Ranges: []Range{
					NewRange(
						[]string{"x", "y"},
						ion.Uint(1000),
						ion.Uint(2000),
					),
				},
			},
			{
				Offset: 500,
				Ranges: []Range{
					NewRange(
						[]string{"z", "z"},
						ion.Uint(0),
						ion.Uint(1000),
					),
				},
			},
			{
				Offset: 1000,
				Ranges: []Range{
					NewRange(
						[]string{"a", "b"},
						ion.Uint(1337),
						ion.Uint(1338),
					),
				},
			},
			{
				Offset: 2000,
				Ranges: []Range{
					NewRange(
						[]string{"c", "d"},
						ion.Uint(42),
						ion.Uint(24),
					),
				},
			},
		},
	}

	err := a.CombineWith(&b)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(a, expected) {
		t.Error("results not equivalent")
	}
}
