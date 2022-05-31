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
	"crypto/rand"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
)

func TestIndirectTree(t *testing.T) {
	var all []Descriptor
	idx := Index{Algo: "zstd"}

	dir := NewDirFS(t.TempDir())

	// force generation of new indirect refs
	// so we get coverage of larger ref lists
	oldRefSize := targetRefSize
	targetRefSize = 2048
	t.Cleanup(func() {
		targetRefSize = oldRefSize
	})

	now := func() date.Time {
		return date.Now().Truncate(time.Microsecond)
	}

	start := now()
	newdesc := func(iter, blocks int) Descriptor {
		name := "packed-" + uuid()
		d := Descriptor{
			ObjectInfo: ObjectInfo{
				Path:         path.Join("db", "foo", "bar", name),
				ETag:         "etag-for-" + name,
				LastModified: now(),
				Format:       Version,
				Size:         123456,
			},
			Trailer: &Trailer{
				Version:    1,
				Offset:     345123,
				BlockShift: 20,
				Algo:       "zstd",
			},
		}
		// descriptors are 1 hour apart; blocks are 1 minute apart
		min := start.Add(time.Duration(iter) * time.Hour)
		for i := 0; i < blocks; i++ {
			lo := min.Add(time.Duration(i) * time.Minute)
			hi := lo.Add(time.Minute)
			d.Trailer.Blocks = append(d.Trailer.Blocks, Blockdesc{
				Offset: int64(i) * 98246,
				Chunks: 50,
			})
			d.Trailer.Sparse.push([]string{"timestamp"}, lo, hi)
			d.Trailer.Sparse.bump()
		}
		return d
	}

	allRefs := func(idx *Index) []Descriptor {
		tail := idx.Inline
		head, err := idx.Indirect.Search(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		return append(head, tail...)
	}

	var key Key
	rand.Read(key[:])
	indexmem, err := Sign(&key, &idx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		idx, err := DecodeIndex(&key, indexmem, 0)
		if err != nil {
			t.Fatal(err)
		}
		// remove garbage early so that allRefs will fail
		// if something was added to ToDelete that shouldn't have been...
		for i := range idx.ToDelete {
			dir.Remove(idx.ToDelete[i].Path)
		}
		idx.ToDelete = nil

		d := newdesc(i, 30)
		ds := d.Trailer.Decompressed()
		all = append(all, d)
		idx.Inline = append(idx.Inline, d)

		prev := len(idx.Indirect.Refs)
		// force all but the latest object
		// to be flushed to the indirect list
		err = idx.SyncOutputs(dir, path.Join("db", "foo", "bar"), ds, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(idx.Inline) != 1 {
			t.Fatalf("iter %d: expected 1 inline object; found %d", i, len(idx.Inline))
		}
		after := len(idx.Indirect.Refs)
		if prev == after && prev != 0 {
			// we should have generated garbage
			if len(idx.ToDelete) != 1 {
				t.Error("didn't generate any garbage?")
			}
		} else if prev != 0 && after != prev+1 {
			// otherwise, we should have added 1 new ref
			t.Errorf("%d -> %d indirect refs?", prev, after)
		}
		if idx.Indirect.Objects()+len(idx.Inline) != len(all) {
			t.Errorf("Indirect.Objects() = %d, len(idx.Inline) = %d, len(all) = %d",
				idx.Indirect.Objects(), len(idx.Inline), len(all))
		}

		gotAll := allRefs(idx)
		if len(all) != len(gotAll) {
			t.Fatalf("got %d instead of %d refs", len(gotAll), len(all))
		}
		if !reflect.DeepEqual(all, gotAll) {
			for j := len(all) - 1; j >= 0; j-- {
				if all[j].Path != gotAll[j].Path {
					t.Errorf("index %d: %s != %s", j, all[j].Path, gotAll[j].Path)
					continue
				}
				if !reflect.DeepEqual(all[j].Trailer.Sparse, gotAll[j].Trailer.Sparse) {
					t.Errorf("%#v", all[j].Trailer.Sparse)
					t.Fatalf("%#v", gotAll[j].Trailer.Sparse)
				}
			}
			t.Fatalf("iter %d: results not equal", i)
		}
		indexmem, err = Sign(&key, idx)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("final refs: %d, objects: %d", len(idx.Indirect.Refs), idx.Indirect.Objects())
}
