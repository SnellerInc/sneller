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

package db

import (
	"fmt"
	"io/fs"
	"path"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// FS is the filesystem implementation
// that is required for turning a table
// reference into a consistent list of blobs.
type FS interface {
	InputFS

	// URL should return a URL for the
	// file at the given full path and with
	// the provided fs.FileInfo. If etag does
	// not match the current ETag of the object,
	// or if the URL doesn't fetch specifically
	// the object with the given ETag, then
	// URL should return an error indicating that
	// the object has been overwritten.
	URL(name string, info fs.FileInfo, etag string) (string, error)
}

// descInfo is an implementation of fs.FileInfo
// for blockfmt.Descriptor when Size is present
type descInfo blockfmt.Descriptor

var _ fs.FileInfo = &descInfo{}

func (d *descInfo) Name() string      { return path.Base(d.Path) }
func (d *descInfo) Size() int64       { return (*blockfmt.Descriptor)(d).Size }
func (d *descInfo) Mode() fs.FileMode { return 0644 }

func (d *descInfo) ModTime() time.Time {
	// S3 stores only second-precision modtimes,
	// so if we generate a table in less than 1 second,
	// the modtime is actually *before* the index commit time
	// (before rounding); we have to round up so that the
	// If-Not-Modified-Since precondition will pass.
	return d.LastModified.Time().Add(time.Second - 1).Truncate(time.Second)
}

func (d *descInfo) IsDir() bool      { return false }
func (d *descInfo) Sys() interface{} { return (*blockfmt.Descriptor)(d) }

func noIfMatch(fs FS) bool {
	if _, ok := fs.(*DirFS); ok {
		return ok
	}
	return false
}

// Blobs collects the list of objects
// from an index and returns them as
// a list of blobs against which queries can be run
// along with the number of (decompressed) bytes that
// comprise the returned blobs. If [keep] is non-nil,
// then the returned blob list will be comprised only
// of blobs for which the filter condition is satisfied
// by at least one row in the data pointed to by the blob.
//
// Note that the returned blob.List may consist
// of zero blobs if the index has no contents.
func Blobs(src FS, idx *blockfmt.Index, keep *blockfmt.Filter) (*blob.List, int64, error) {
	out := &blob.List{}
	var size int64
	var err error
	for i := range idx.Inline {
		if idx.Inline[i].Format != blockfmt.Version {
			return nil, 0, fmt.Errorf("don't know how to convert format %q into a blob", idx.Inline[i].Format)
		}
		out.Contents, err = descToBlobs(src, &idx.Inline[i], keep, out.Contents, &size)
		if err != nil {
			return nil, 0, err
		}
	}
	var descs []blockfmt.Descriptor
	descs, err = idx.Indirect.Search(src, keep)
	if err != nil {
		return out, size, err
	}
	for i := range descs {
		out.Contents, err = descToBlobs(src, &descs[i], keep, out.Contents, &size)
		if err != nil {
			return out, size, err
		}
	}
	return out, size, nil
}

func descToBlobs(src FS, b *blockfmt.Descriptor, keep *blockfmt.Filter, into []blob.Interface, size *int64) ([]blob.Interface, error) {
	var self *blob.Compressed
	info := (*descInfo)(b)
	uri, err := src.URL(b.Path, info, b.ETag)
	if err != nil {
		return into, err
	}
	visit := func(start, end int) {
		if start == end {
			return
		}
		if self == nil {
			self = &blob.Compressed{
				From: &blob.URL{
					Value: uri,
					// when we are testing with a DirFS,
					// don't send the If-Match header,
					// since http.FileServer doesn't handle it
					UnsafeNoIfMatch: noIfMatch(src),
					Info: blob.Info{
						// Note: blob.URL.ReadAt automatically
						// inserts the If-Match header to ensure
						// that the object can't change while
						// we are reading it.
						ETag: b.ETag,
						Size: info.Size(),
						// Note: the Align of blob.Compressed.From
						// is ignored, since blob.Compressed reads
						// the trailer to figure out the alignment.
						Align: 1,
						// LastModified should match info.ModTime exactly
						LastModified: date.FromTime(info.ModTime()),
						Ephemeral:    b.Size < DefaultMinMerge,
					},
				},
				Trailer: b.Trailer,
			}
		}
		// for now, just map blocks -> blobs 1:1
		for i := start; i < end; i++ {
			*size += int64(b.Trailer.Blocks[i].Chunks << b.Trailer.BlockShift)
			into = append(into, &blob.CompressedPart{
				Parent:     self,
				StartBlock: i,
				EndBlock:   i + 1,
			})
		}
	}
	if keep == nil {
		visit(0, len(b.Trailer.Blocks))
	} else {
		keep.Visit(&b.Trailer.Sparse, visit)
	}
	return into, nil
}
