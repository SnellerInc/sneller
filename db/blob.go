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

func keepAny(t *blockfmt.Trailer, filt *blockfmt.Filter) bool {
	if filt == nil || filt.Trivial() {
		return true
	}
	return filt.MatchesAny(&t.Sparse)
}

// Blobs collects the list of objects
// from an index and returns them as
// a list of blobs against which queries can be run.
// Only blobs that reference objects for which
// at least one block satisfies keep(Blockdesc.Ranges)
// are returned.
//
// Note that the returned blob.List may consist
// of zero blobs if the index has no contents.
func Blobs(src FS, idx *blockfmt.Index, keep *blockfmt.Filter) (*blob.List, error) {
	out := &blob.List{}
	for i := range idx.Inline {
		if idx.Inline[i].Format != blockfmt.Version {
			return nil, fmt.Errorf("don't know how to convert format %q into a blob", idx.Inline[i].Format)
		}
		if !keepAny(&idx.Inline[i].Trailer, keep) {
			continue
		}
		canExpire := idx.Inline[i].Size < DefaultMinMerge && i == len(idx.Inline)-1
		b, err := descToBlob(src, &idx.Inline[i], canExpire)
		if err != nil {
			return nil, err
		}
		out.Contents = append(out.Contents, b)
	}
	descs, err := idx.Indirect.Search(src, keep)
	if err != nil {
		return out, err
	}
	for i := range descs {
		b, err := descToBlob(src, &descs[i], false)
		if err != nil {
			return out, err
		}
		out.Contents = append(out.Contents, b)
	}
	return out, nil
}

func descToBlob(src FS, b *blockfmt.Descriptor, canExpire bool) (*blob.Compressed, error) {
	info := (*descInfo)(b)
	uri, err := src.URL(b.Path, info, b.ETag)
	if err != nil {
		return nil, err
	}
	return &blob.Compressed{
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
				Ephemeral:    canExpire,
			},
		},
		Trailer: b.Trailer,
	}, nil
}
