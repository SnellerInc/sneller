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
	"bytes"
	"crypto/rand"
	"path"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
)

func TestIndirectTree(t *testing.T) {
	var all []Descriptor

	dir := NewDirFS(t.TempDir())
	dir.MinPartSize = 1

	now := func() date.Time {
		return date.Now().Truncate(time.Microsecond)
	}

	assertEquivalent := func(a, b []Descriptor) {
		for i := range a {
			blocks := a[i].Trailer.Blocks
			var srcblocks []Blockdesc
			var sparse *SparseIndex

			offset := int64(0)
			for len(srcblocks) < len(blocks) && len(b) > 0 {
				if sparse == nil {
					s := b[0].Trailer.Sparse.Clone()
					sparse = &s
				} else {
					sparse.Append(&b[0].Trailer.Sparse)
				}
				lst := b[0].Trailer.Blocks
				for j := range lst {
					blk := lst[j]
					blk.Offset += offset
					srcblocks = append(srcblocks, blk)
				}
				offset += b[0].Trailer.Offset
				b = b[1:]
			}
			if !slices.Equal(blocks, srcblocks) {
				t.Helper()
				if len(srcblocks) != len(blocks) {
					t.Errorf("len(srcblocks)=%d, len(blocks)=%d", len(srcblocks), len(blocks))
				}
				for i := range blocks {
					if srcblocks[i] != blocks[i] {
						t.Errorf("%d: %#v != %#v", i, srcblocks[i], blocks[i])
					}
				}
				t.Fatal("block lists not equivalent")
			}
			if !reflect.DeepEqual(&a[i].Trailer.Sparse, sparse) {
				t.Helper()
				t.Fatal("sparse not equal")
			}
		}
	}

	start := now()
	newdesc := func(iter, blocks int) Descriptor {
		name := "packed-" + uuid()
		d := Descriptor{
			ObjectInfo: ObjectInfo{
				Path:         path.Join("db", "foo", "bar", name),
				LastModified: now(),
				Format:       Version,
				Size:         16,
			},
			Trailer: Trailer{
				Version:    1,
				Offset:     11,
				BlockShift: 20,
				Algo:       "zstd",
			},
		}
		// descriptors are 1 hour apart; blocks are 1 minute apart
		min := start.Add(time.Duration(iter) * time.Hour)
		for i := 0; i < blocks; i++ {
			// make ranges disjoint so that we can
			// do precise queries for specific blocks:
			lo := min.Add(time.Duration(i) * time.Minute)
			hi := lo.Add(time.Minute - time.Microsecond)
			d.Trailer.Blocks = append(d.Trailer.Blocks, Blockdesc{
				Offset: int64(i) * 98246,
				Chunks: 50,
			})
			d.Trailer.Sparse.push([]string{"timestamp"}, lo, hi)
			d.Trailer.Sparse.bump()
		}

		// we don't ever inspect the contents of these files,
		// so just write out some garbage to be concatenated
		body := bytes.Repeat([]byte{0xff}, int(d.Size))
		etag, err := dir.WriteFile(d.Path, body)
		if err != nil {
			t.Fatal(err)
		}
		d.ETag = etag
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

	latestAbove := func(idx *Index, iter int) []Descriptor {
		var f Filter
		min := start.Add(time.Duration(iter) * time.Hour)
		exp := expr.Compare(expr.GreaterEquals, expr.Identifier("timestamp"), &expr.Timestamp{min})
		f.Compile(exp)
		tail, err := idx.Indirect.Search(dir, &f)
		if err != nil {
			t.Fatal(err)
		}
		return tail
	}

	latestBelow := func(idx *Index, iter int) []Descriptor {
		var f Filter
		min := start.Add(time.Duration(iter)*time.Hour - 1)
		exp := expr.Compare(expr.Less, expr.Identifier("timestamp"), &expr.Timestamp{min})
		f.Compile(exp)
		tail, err := idx.Indirect.Search(dir, &f)
		if err != nil {
			t.Fatal(err)
		}
		return tail
	}

	var key Key
	rand.Read(key[:])
	empty := Index{Algo: "zstd"}
	indexmem, err := Sign(&key, &empty)
	if err != nil {
		t.Fatal(err)
	}
	var idx *Index
	for i := 0; i < 100; i++ {
		// periodically re-load (must re-load @ 0)
		idx2, err := DecodeIndex(&key, indexmem, 0)
		if err != nil {
			t.Fatal(err)
		}
		if idx == nil {
			idx = idx2
		} else {
			if !idx.Inputs.oldroot.Equal(idx2.Inputs.oldroot) {
				t.Fatal("input trees not equivalent")
			}
			idx2.Inputs.oldroot = idx.Inputs.oldroot
			if !reflect.DeepEqual(idx, idx2) {
				t.Errorf("have: %+v", idx)
				t.Errorf("data: %+v", idx2)
				t.Fatal("index not equal")
			}
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

		// force all but the latest object
		// to be flushed to the indirect list
		c := IndexConfig{
			MaxInlined: ds * 10,
			TargetSize: ds * 10,
			// force generation of new indirect refs
			// so we get coverage of larger ref lists
			TargetRefSize: 2048,
		}
		err = c.SyncOutputs(idx, dir, path.Join("db", "foo", "bar"))
		if err != nil {
			t.Fatal(err)
		}
		if idx.Indirect.OrigObjects()+len(idx.Inline) != len(all) {
			t.Errorf("Indirect.Objects() = %d, len(idx.Inline) = %d, len(all) = %d",
				idx.Indirect.OrigObjects(), len(idx.Inline), len(all))
		}

		gotAll := allRefs(idx)
		assertEquivalent(gotAll, all)
		if idx.Indirect.OrigObjects() > 0 {
			field := []string{"timestamp"}
			tr := idx.Indirect.Sparse.Get(field)
			if tr == nil {
				t.Fatalf("iter %d no [timestamp] index?", i)
			}
			if min, ok := tr.Min(); !ok || !min.Equal(start) {
				t.Errorf("iter %d min [timestamp] = %s not %s?", i, min, start)
			}
			wantmax := start.Add((time.Duration(idx.Indirect.OrigObjects()-1) * time.Hour) + 30*time.Minute - time.Microsecond)
			if max, ok := tr.Max(); !ok || !max.Equal(wantmax) {
				t.Errorf("iter %d max [timestamp] = %s not %s?", i, max, wantmax)
			}

			// check that sparse indexing information
			// is updated appropriately on each insert
			last := latestAbove(idx, i-1)
			assertEquivalent(last, all[len(all)-2:len(all)-1])
			below := latestBelow(idx, i)
			assertEquivalent(below, all[:len(all)-1])
		}
		indexmem, err = Sign(&key, idx)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("final refs: %d, orig objects %d, objects: %d", len(idx.Indirect.Refs), idx.Indirect.OrigObjects(), idx.Objects())
}
