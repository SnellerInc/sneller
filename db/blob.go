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
	fs.FS
	// Prefix indicates the expected
	// prefix of index contents within
	// this path. For example, if the FS
	// implementation is an S3 bucket,
	// Prefix should return "s3://<bucket>/"
	Prefix() string

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
// a list of blobs against which queries can be run.
//
// Note that the returned blob.List may consist
// of zero blobs if the index has no contents.
func Blobs(src FS, idx *blockfmt.Index) (*blob.List, error) {
	out := &blob.List{}
	for i := range idx.Contents {
		p := idx.Contents[i].Path
		if idx.Contents[i].Format != blockfmt.Version {
			return nil, fmt.Errorf("don't know how to convert format %q into a blob", idx.Contents[i].Format)
		}
		info := (*descInfo)(&idx.Contents[i])
		b, err := infoToBlob(src, &idx.Contents[i], info, p)
		if err != nil {
			return nil, err
		}
		out.Contents = append(out.Contents, b)
	}
	return out, nil
}

func infoToBlob(src FS, b *blockfmt.Descriptor, info fs.FileInfo, p string) (blob.Interface, error) {
	etag := b.ETag
	modtime := b.LastModified.Time()
	// annoyingly, S3 does not use very precise
	// LastModified times (it truncates the precision);
	// just guarantee that the object is no more than one
	// second newer...
	delta := info.ModTime().Sub(modtime)
	if delta > time.Second {
		return nil, fmt.Errorf("object %s has been modified (index says %s; file says %s)", p, modtime, info.ModTime())
	}
	if infs, ok := src.(blockfmt.InputFS); ok {
		curETag, err := infs.ETag(p, info)
		if err == nil && etag != curETag {
			return nil, fmt.Errorf("object %s has been modified (index says ETag %s; current ETag %s)", p, etag, curETag)
		}
	}
	uri, err := src.URL(p, info, etag)
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
				ETag: etag,
				Size: info.Size(),
				// Note: the Align of blob.Compressed.From
				// is ignored, since blob.Compressed reads
				// the trailer to figure out the alignment.
				Align: 1,
				// LastModified should match info.ModTime exactly
				LastModified: date.FromTime(info.ModTime()),
			},
		},
		Trailer: b.Trailer,
	}, nil
}
