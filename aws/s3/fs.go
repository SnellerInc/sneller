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

package s3

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/aws"
)

// BucketFS implements fs.FS,
// fs.ReadDirFS, and fs.SubFS.
type BucketFS struct {
	Key    *aws.SigningKey
	Bucket string
	Client *http.Client

	// DelayGet, if true, causes the
	// Open call to use a HEAD operation
	// rather than a GET operation.
	// The first call to fs.File.Read will
	// cause the full GET to be performed.
	DelayGet bool
}

func (b *BucketFS) sub(name string) *Prefix {
	return &Prefix{
		Key:    b.Key,
		Client: b.Client,
		Bucket: b.Bucket,
		Path:   name,
	}
}

func badpath(op, name string) error {
	return &fs.PathError{
		Op:   op,
		Path: name,
		Err:  fs.ErrInvalid,
	}
}

// Put performs a PutObject operation at the object key 'where'
// and returns the ETag of the newly-created object.
func (b *BucketFS) Put(where string, contents []byte) (string, error) {
	where = path.Clean(where)
	if !fs.ValidPath(where) {
		return "", badpath("s3 PUT", where)
	}
	_, base := path.Split(where)
	if base == "." {
		// don't allow a path that is
		// nominally a directory
		return "", badpath("s3 PUT", where)
	}
	req, err := http.NewRequest(http.MethodPut, uri(b.Key, b.Bucket, where), nil)
	if err != nil {
		return "", err
	}
	b.Key.SignV4(req, contents)
	client := b.Client
	if client == nil {
		client = &DefaultClient
	}
	res, err := flakyDo(client, req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return "", fmt.Errorf("s3 PUT: %s %s", res.Status, extractMessage(res.Body))
	}
	etag := res.Header.Get("ETag")
	return etag, nil
}

// Sub implements fs.SubFS.Sub.
func (b *BucketFS) Sub(dir string) (fs.FS, error) {
	dir = path.Clean(dir)
	if !fs.ValidPath(dir) {
		return nil, badpath("sub", dir)
	}
	if dir == "." {
		return b, nil
	}
	return b.sub(dir + "/"), nil
}

// Open implements fs.FS.Open
//
// The returned fs.File will be either a *File
// or a *Prefix depending on whether name refers
// to an object or a common path prefix that
// leads to multiple objects.
// If name does not refer to an object or a path prefix,
// then Open returns an error matching fs.ErrNotExist.
func (b *BucketFS) Open(name string) (fs.File, error) {
	// interpret a trailing / to mean
	// a directory
	isDir := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return nil, badpath("open", name)
	}
	// opening the "root directory"
	if name == "." {
		return b.sub("."), nil
	}
	if !isDir {
		// try a HEAD or GET operation; these
		// are cheaper and faster than
		// full listing operations
		if b.DelayGet {
			rd, err := Stat(b.Key, b.Bucket, name)
			if err == nil {
				return &File{Reader: rd}, nil
			}
		} else {
			f, err := Open(b.Key, b.Bucket, name)
			if err == nil {
				return f, nil
			}
		}
	}

	p := b.sub(name)
	ret, err := p.readDirAt(1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(ret) > 0 {
		// listing produced an object matching the full path:
		if f, ok := ret[0].(*File); ok && !isDir && f.object == name {
			return f, nil
		}
		// listing produced a prefix matching the full path:
		if p2, ok := ret[0].(*Prefix); ok && p2.Path == p.Path+"/" {
			return p2, nil
		}
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

// ReadDir implements fs.ReadDirFS
func (b *BucketFS) ReadDir(name string) ([]fs.DirEntry, error) {
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return nil, badpath("readdir", name)
	}
	if name == "." {
		return b.sub(".").ReadDir(-1)
	}
	return b.sub(name + "/").ReadDir(-1)
}

// Prefix implements fs.File, fs.ReadDirFile,
// and fs.DirEntry, and fs.FS.
type Prefix struct {
	// Key is the signing key used to sign requests.
	Key *aws.SigningKey
	// Bucket is the bucket at the root of the "filesystem"
	Bucket string
	// Path is the path of this prefix.
	// The value of Path should always be
	// a valid path (see fs.ValidPath) plus
	// a trailing forward slash to indicate
	// that this is a pseudo-directory prefix.
	Path   string
	Client *http.Client

	// listing token;
	// "" means start from the beginning
	token string
}

func (p *Prefix) join(extra string) string {
	if p.Path == "." {
		// root of bucket
		return extra
	}
	return path.Join(p.Path, extra)
}

// Open opens the object or pseudo-directory
// at the provided path.
// The returned fs.File will be a *File if
// the combined Prefix and path lead to an object;
// if the combind prefix and path produce another
// complete object prefix, then a *Prefix will
// be returned. If the combined prefix and path
// do not produce a prefix that is present within
// the target bucket, then an error matching
// fs.ErrNotExist is returned.
func (p *Prefix) Open(file string) (fs.File, error) {
	file = path.Clean(file)
	if file == "." {
		return p, nil
	}
	if !fs.ValidPath(file) {
		return nil, badpath("open", file)
	}
	fullp := &Prefix{
		Key:    p.Key,
		Bucket: p.Bucket,
		Path:   p.join(file),
		Client: p.Client,
	}
	ret, err := fullp.readDirAt(1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, fs.ErrNotExist
	}
	if f, ok := ret[0].(*File); ok && f.object == fullp.Path {
		return f, nil
	}
	// common prefixes end with a trailing delimiter,
	// so searching at 'foo/bar' will produce a prefix 'foo/bar/'
	if p, ok := ret[0].(*Prefix); ok && p.Path == fullp.Path+"/" {
		return p, nil
	}
	return nil, fs.ErrNotExist
}

// Name implements fs.DirEntry.Name
func (p *Prefix) Name() string {
	return path.Base(p.Path)
}

// Type implements fs.DirEntry.Type
func (p *Prefix) Type() fs.FileMode {
	return fs.ModeDir
}

// Info implements fs.DirEntry.Info
func (p *Prefix) Info() (fs.FileInfo, error) {
	return p.Stat()
}

// IsDir implements fs.FileInfo.IsDir
func (p *Prefix) IsDir() bool { return true }

// ModTime implements fs.FileInfo.ModTime
//
// Note: currently ModTime returns the zero time.Time,
// as S3 prefixes don't have a meaningful modification time.
func (p *Prefix) ModTime() time.Time { return time.Time{} }

// Mode implements fs.FileInfo.Mode
func (p *Prefix) Mode() fs.FileMode { return fs.ModeDir | 0755 }

// Sys implements fs.FileInfo.Sys
func (p *Prefix) Sys() interface{} { return nil }

// Size implements fs.FileInfo.Size
func (p *Prefix) Size() int64 { return 0 }

// Stat implements fs.File.Stat
func (p *Prefix) Stat() (fs.FileInfo, error) {
	return p, nil
}

// Read implements fs.File.Read.
//
// Read always returns an error.
func (p *Prefix) Read(_ []byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: p.Path,
		Err:  fs.ErrInvalid,
	}
}

// Close implements fs.File.Close
func (p *Prefix) Close() error {
	return nil
}

// File implements fs.File
type File struct {
	// Reader is a reader that points to
	// the associated s3 object.
	*Reader
	body io.ReadCloser // populated lazily
}

// Name implements fs.FileInfo.Name
func (f *File) Name() string {
	return path.Base(f.Reader.object)
}

// Path returns the full path to the
// S3 object within its bucket.
// See also blockfmt.NamedFile
func (f *File) Path() string {
	return f.Reader.object
}

// Mode implements fs.FileInfo.Mode
func (f *File) Mode() fs.FileMode { return 0644 }

// Read implements fs.File.Read
//
// Note: Read is not safe to call from
// multiple goroutines simultaneously.
// Use ReadAt for parallel reads.
func (f *File) Read(p []byte) (int, error) {
	if f.body == nil {
		var err error
		f.body, err = f.Reader.RangeReader(0, f.Reader.Size())
		if err != nil {
			return 0, err
		}
	}
	return f.body.Read(p)
}

// Info implements fs.DirEntry.Info
//
// Info returns exactly the same thing as f.Stat
func (f *File) Info() (fs.FileInfo, error) {
	return f.Stat()
}

// Type implements fs.DirEntry.Type
//
// Type returns exactly the same thing as f.Mode
func (f *File) Type() fs.FileMode { return f.Mode() }

// Close implements fs.File.Close
func (f *File) Close() error {
	if f.body == nil {
		return nil
	}
	err := f.body.Close()
	f.body = nil
	return err
}

// IsDir implements fs.DirEntry.IsDir.
// IsDir always returns false.
func (f *File) IsDir() bool { return false }

// ModTime implements fs.DirEntry.ModTime.
// This returns the same value as f.Reader.LastModified.
func (f *File) ModTime() time.Time { return f.Reader.LastModified }

// Sys implements fs.FileInfo.Sys.
func (f *File) Sys() interface{} { return nil }

// Stat implements fs.File.Stat
func (f *File) Stat() (fs.FileInfo, error) {
	return f, nil
}

// ReadDir implements fs.ReadDirFile
//
// Every returned fs.DirEntry will be either
// a Prefix or a File struct.
func (p *Prefix) ReadDir(n int) ([]fs.DirEntry, error) {
	return p.readDirAt(n)
}

func (p *Prefix) readDirAt(n int) ([]fs.DirEntry, error) {
	if !ValidBucket(p.Bucket) {
		return nil, badBucket(p.Bucket)
	}
	parts := []string{
		"delimiter=%2F",
		"list-type=2",
	}
	if p.Path != "" && p.Path != "." {
		parts = append(parts, "prefix="+queryEscape(p.Path))
	}
	if n > 0 {
		if p.Path != "." && p.Path[len(p.Path)-1] == '/' && n == 1 {
			// an extra entry will be returned for *this* path;
			// we should ignore it
			n++
		}
		parts = append(parts, fmt.Sprintf("max-keys=%d", n))
	}
	if p.token != "" {
		parts = append(parts, "continuation-token="+url.QueryEscape(p.token))
	}
	sort.Strings(parts)
	query := "?" + strings.Join(parts, "&")
	req, err := http.NewRequest(http.MethodGet, rawURI(p.Key, p.Bucket, query), nil)
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}
	p.Key.SignV4(req, nil)
	res, err := flakyDo(p.client(), req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("s3 list objects: %s", res.Status)
	}
	ret := struct {
		IsTruncated bool `xml:"IsTruncated"`
		Contents    []struct {
			ETag         string    `xml:"ETag"`
			Name         string    `xml:"Key"`
			LastModified time.Time `xml:"LastModified"`
			Size         int64     `sml:"Size"`
		} `xml:"Contents"`
		CommonPrefixes []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
		EncodingType string `xml:"EncodingType"`
		NextToken    string `xml:"NextContinuationToken"`
	}{}
	err = xml.NewDecoder(res.Body).Decode(&ret)
	if err != nil {
		return nil, fmt.Errorf("xml decoding response: %w", err)
	}
	out := make([]fs.DirEntry, 0, len(ret.Contents)+len(ret.CommonPrefixes))
	exists := p.Path == "."
	for i := range ret.Contents {
		if ret.Contents[i].Name == p.Path && p.Path[len(p.Path)-1] == '/' {
			// this "folder" is returned itself;
			// ignore it because it is not part of
			// its own directory
			exists = true
			continue
		}
		out = append(out, &File{
			Reader: &Reader{
				Key:          p.Key,
				Client:       p.client(),
				ETag:         ret.Contents[i].ETag,
				LastModified: ret.Contents[i].LastModified,
				size:         ret.Contents[i].Size,
				bucket:       p.Bucket,
				object:       ret.Contents[i].Name,
			},
		})
	}
	for i := range ret.CommonPrefixes {
		out = append(out, &Prefix{
			Key:    p.Key,
			Bucket: p.Bucket,
			Client: p.Client,
			Path:   ret.CommonPrefixes[i].Prefix,
		})
	}
	// if we didn't find anything that indicates
	// that this prefix is actually a real prefix
	// (no common object prefixes; no 'self' folder, etc.),
	// then this list operation was performed on a directory
	// that simply doesn't exist
	if !exists && len(out) == 0 {
		return nil, &fs.PathError{Op: "readdir", Path: p.Path, Err: fs.ErrNotExist}
	}
	if len(out) == 0 && n > 0 {
		p.token = ""
		return out, io.EOF
	}
	p.token = ret.NextToken
	return out, nil
}

func (p *Prefix) client() *http.Client {
	if p.Client == nil {
		return &DefaultClient
	}
	return p.Client
}
