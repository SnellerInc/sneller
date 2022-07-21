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
	"reflect"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
)

func TestFlattenTree(t *testing.T) {
	root := mkplan(t, 1)
	sub1 := mkplan(t, 1)
	sub2 := mkplan(t, 1)
	root.Children = []*Tree{sub1, sub1, sub2, sub1}
	flat := root.flatten()
	if len(flat) != 3 {
		t.Fatalf("expected 3 flattened trees, got %d", len(flat))
	}
	got, err := reconstitute(flat)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, root) {
		t.Fatal("reconstituted tree did not match")
	}
	if got.Children[0] != got.Children[1] {
		t.Fatal("children are not identical")
	}
}

func BenchmarkDecodeTree(b *testing.B) {
	blocks := []int{
		1, 100, 10000, 100000,
	}
	for _, count := range blocks {
		b.Run(fmt.Sprintf("%d-blocks", count), func(b *testing.B) {
			var buf ion.Buffer
			var st ion.Symtab
			var be benchenv
			tree := mkplan(b, count)
			tree.Encode(&buf, &st)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Decode(&be, &st, buf.Bytes())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func mkplan(tb testing.TB, n int) *Tree {
	query := `SELECT * FROM '../testdata/parking.10n' LIMIT 1`
	s, err := partiql.Parse([]byte(query))
	if err != nil {
		tb.Fatal(err)
	}
	tree, err := New(s, &benchenv{blocks: n})
	if err != nil {
		tb.Fatal(err)
	}
	return tree
}

type benchenv struct {
	blocks int
}

func (b *benchenv) DecodeHandle(st *ion.Symtab, mem []byte) (TableHandle, error) {
	l, err := blob.DecodeList(st, mem)
	if err != nil {
		return nil, err
	}
	return &blobHandle{l}, nil
}

func (b *benchenv) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("open not allowed")
}

type blobHandle struct {
	*blob.List
}

func (b *blobHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("Open() not allowed")
}

func (b *blobHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	b.List.Encode(dst, st)
	return nil
}

func (b *benchenv) Stat(_ expr.Node, _ *Hints) (TableHandle, error) {
	// produce N fake compressed blobs
	// with data that is reasonably sized
	lst := make([]blob.Interface, b.blocks)
	for i := range lst {
		lst[i] = &blob.Compressed{
			From: &blob.URL{
				Value: "https://s3.amazonaws.com/a-very-long/path-to-the-object/finally.ion.zst",
				Info: blob.Info{
					ETag:         "\"abc123xyzandmoreetagstringhere\"",
					Size:         1234567,
					Align:        1024 * 1024,
					LastModified: date.Now(),
				},
			},
			Trailer: &blockfmt.Trailer{
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
	return &blobHandle{&blob.List{lst}}, nil
}
