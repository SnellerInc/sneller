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

package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
)

func BenchmarkDecodeTree(b *testing.B) {
	blocks := []int{
		1, 100, 10000, 100000,
	}
	for _, count := range blocks {
		b.Run(fmt.Sprintf("%d-blocks", count), func(b *testing.B) {
			var buf ion.Buffer
			var st ion.Symtab
			tree := mkplan(b, count)
			tree.Encode(&buf, &st)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Decode(&st, buf.Bytes())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func mkplan(b *testing.B, n int) *Tree {
	query := `SELECT * FROM '../testdata/parking.10n' LIMIT 1`
	s, err := partiql.Parse([]byte(query))
	if err != nil {
		b.Fatal(err)
	}
	tree, err := New(s, &benchenv{blocks: n})
	if err != nil {
		b.Fatal(err)
	}
	return tree
}

type benchenv struct {
	blocks int
}

func (b *benchenv) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("open not allowed")
}

func (b *benchenv) Stat(_ expr.Node, _ *Hints) (*Input, error) {
	// produce N fake compressed blobs
	// with data that is reasonably sized
	descs := make([]blockfmt.Descriptor, b.blocks)
	blocks := make([]blockfmt.Block, b.blocks)
	for i := range descs {
		descs[i] = blockfmt.Descriptor{
			ObjectInfo: blockfmt.ObjectInfo{
				Path:         "a-very-long/path-to-the-object/finally.ion.zst",
				ETag:         "\"abc123xyzandmoreetagstringhere\"",
				LastModified: date.Now(),
				Format:       "zion+zstd",
				Size:         1234567,
			},
			Trailer: blockfmt.Trailer{
				Version:    1,
				Offset:     1234500,
				Algo:       "zstd",
				BlockShift: 20,
				// common case for the new format
				// will be ~100 chunks and one block descriptor
				Blocks: []blockfmt.Blockdesc{{
					Offset: 0,
					Chunks: 100,
				}},
			},
		}
	}
	for i := range blocks {
		blocks[i].Index = i
	}
	return &Input{
		Descs:  descs,
		Blocks: blocks,
	}, nil
}
