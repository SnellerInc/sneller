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
	"context"
	"encoding/xml"
	"errors"
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
	"github.com/SnellerInc/sneller/fsutil"
	"golang.org/x/exp/slices"
)

// BucketFS implements fs.FS,
// fs.ReadDirFS, and fs.SubFS.
type BucketFS struct {
	Key    *aws.SigningKey
	Bucket string
	Client *http.Client
	Ctx    context.Context

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
		Ctx:    b.Ctx,
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
	return b.put(where, contents)
}

func (b *BucketFS) put(where string, contents []byte) (string, error) {
	req, err := http.NewRequestWithContext(b.Ctx, http.MethodPut, uri(b.Key, b.Bucket, where), nil)
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
		f, err := Open(b.Key, b.Bucket, name, !b.DelayGet)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return f, err
		}
	}

	return b.sub(name).openDir()
}

// VisitDir implements fs.VisitDirFS
func (b *BucketFS) VisitDir(name, seek, pattern string, walk fsutil.VisitDirFn) error {
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return badpath("visitdir", name)
	}
	if name == "." {
		return b.sub(".").VisitDir(".", seek, pattern, walk)
	}
	return b.sub(name+"/").VisitDir(".", seek, pattern, walk)
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
	ret, err := b.sub(name + "/").ReadDir(-1)
	if err != nil {
		return ret, err
	}
	if len(ret) == 0 {
		// *almost always* because name doesn't actually exist;
		// we should double-check
		f, err := b.sub(name + "/").openDir()
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return ret, nil
}

// Prefix implements fs.File, fs.ReadDirFile,
// and fs.DirEntry, and fs.FS.
type Prefix struct {
	// Key is the signing key used to sign requests.
	Key *aws.SigningKey `xml:"-"`
	// Bucket is the bucket at the root of the "filesystem"
	Bucket string `xml:"-"`
	// Path is the path of this prefix.
	// The value of Path should always be
	// a valid path (see fs.ValidPath) plus
	// a trailing forward slash to indicate
	// that this is a pseudo-directory prefix.
	Path   string          `xml:"Prefix"`
	Client *http.Client    `xml:"-"`
	Ctx    context.Context `xml:"-"`

	// listing token;
	// "" means start from the beginning
	token string
	// if true, ReadDir returns io.EOF
	dirEOF bool
}

func (p *Prefix) join(extra string) string {
	if p.Path == "." {
		// root of bucket
		return extra
	}
	return path.Join(p.Path, extra)
}

func (p *Prefix) sub(name string) *Prefix {
	return &Prefix{
		Key:    p.Key,
		Client: p.Client,
		Bucket: p.Bucket,
		Path:   p.join(name),
		Ctx:    p.Ctx,
	}
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
	return p.sub(file).openDir()
}

func (p *Prefix) openDir() (fs.File, error) {
	if p.Path == "" || p.Path == "." {
		// the root directory trivially exists
		return p, nil
	}
	ret, err := p.list(1, "", "", "")
	if err != nil {
		return nil, err
	}
	// if we got anything at all, it exists
	if len(ret.Contents) == 0 && len(ret.CommonPrefixes) == 0 {
		return nil, &fs.PathError{Op: "open", Path: p.Path, Err: fs.ErrNotExist}
	}
	if strings.HasSuffix(p.Path, "/") {
		return p, nil
	}
	path := p.Path + "/"
	return &Prefix{
		Key:    p.Key,
		Bucket: p.Bucket,
		Client: p.Client,
		Path:   path,
		Ctx:    p.Ctx,
	}, nil
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
	Reader

	ctx  context.Context // from parent bucket
	body io.ReadCloser   // actual body; populated lazily
	pos  int64           // current read offset
}

// Name implements fs.FileInfo.Name
func (f *File) Name() string {
	return path.Base(f.Reader.Path)
}

// Path returns the full path to the
// S3 object within its bucket.
// See also blockfmt.NamedFile
func (f *File) Path() string {
	return f.Reader.Path
}

// Mode implements fs.FileInfo.Mode
func (f *File) Mode() fs.FileMode { return 0644 }

// Open implements fsutil.Opener
func (f *File) Open() (fs.File, error) { return f, nil }

// Read implements fs.File.Read
//
// Note: Read is not safe to call from
// multiple goroutines simultaneously.
// Use ReadAt for parallel reads.
//
// Also note: the first call to Read performs
// an HTTP request to S3 to read the entire
// contents of the object starting at the
// current read offset (zero by default, or
// another offset set via Seek).
// If you need to read a sub-range of the
// object, consider using f.Reader.RangeReader
func (f *File) Read(p []byte) (int, error) {
	if f.body == nil {
		err := f.ctx.Err()
		if err != nil {
			return 0, err
		}
		f.body, err = f.Reader.RangeReader(f.pos, f.Size()-f.pos)
		if err != nil {
			return 0, err
		}
	}
	n, err := f.body.Read(p)
	f.pos += int64(n)
	return n, err
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
	f.pos = 0
	return err
}

// Seek implements io.Seeker
//
// Seek rejects offsets that are beyond
// the size of the underlying object.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	var newpos int64
	switch whence {
	case io.SeekStart:
		newpos = offset
	case io.SeekCurrent:
		newpos = f.pos + offset
	case io.SeekEnd:
		newpos = f.Reader.Size + offset
	default:
		panic("invalid seek whence")
	}
	if newpos < 0 || newpos > f.Reader.Size {
		return f.pos, fmt.Errorf("invalid seek offset %d", newpos)
	}
	// current data is invalid
	// if the position has changed
	if newpos != f.pos && f.body != nil {
		f.body.Close()
	}
	f.pos = newpos
	return f.pos, nil
}

func (f *File) Size() int64 {
	return f.Reader.Size
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

// split a glob pattern on the first meta-character
// so that we can list from the most specific prefix
func splitMeta(pattern string) (string, string) {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '\\', '[':
			return pattern[:i], pattern[i:]
		default:
		}
	}
	return pattern, ""
}

// VisitDir implements fs.VisitDirFS
func (p *Prefix) VisitDir(name, seek, pattern string, walk fsutil.VisitDirFn) error {
	if !ValidBucket(p.Bucket) {
		return badBucket(p.Bucket)
	}
	subp := p.sub(name)
	if !strings.HasSuffix(subp.Path, "/") {
		subp.Path += "/"
	}
	token := ""
	for {
		d, tok, err := subp.readDirAt(-1, token, seek, pattern)
		if err != nil && err != io.EOF {
			return &fs.PathError{Op: "visit", Path: subp.Path, Err: err}
		}
		// despite being called "start-after", the
		// S3 API includes the seek key in the list
		// response, which is not consistent with
		// fsutil.VisitDir, so filter it out here...
		if len(d) > 0 && d[0].Name() == seek {
			d = d[1:]
		}
		for i := range d {
			err := walk(d[i])
			if err != nil {
				if err == fs.SkipDir {
					err = nil
				}
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		token = tok
	}
}

// ReadDir implements fs.ReadDirFile
//
// Every returned fs.DirEntry will be either
// a Prefix or a File struct.
func (p *Prefix) ReadDir(n int) ([]fs.DirEntry, error) {
	if p.dirEOF {
		return nil, io.EOF
	}
	d, next, err := p.readDirAt(n, p.token, "", "")
	if err == io.EOF {
		p.dirEOF = true
		if len(d) > 0 || n < 0 {
			// the spec for fs.ReadDirFile says
			// ReadDir(-1) shouldn't produce an explicit EOF
			err = nil
		}
	}
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: p.Path, Err: err}
	}
	p.token = next
	return d, nil
}

type listResponse struct {
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []File   `xml:"Contents"`
	CommonPrefixes []Prefix `xml:"CommonPrefixes"`
	EncodingType   string   `xml:"EncodingType"`
	NextToken      string   `xml:"NextContinuationToken"`
}

func (p *Prefix) list(n int, token, seek, prefix string) (*listResponse, error) {
	if !ValidBucket(p.Bucket) {
		return nil, badBucket(p.Bucket)
	}
	parts := []string{
		"delimiter=%2F",
		"list-type=2",
	}
	// make sure there's a '/' at the end and
	// append the prefix
	path := p.Path
	if path != "" && path != "." {
		if !strings.HasSuffix(path, "/") {
			path += "/" + prefix
		} else {
			path += prefix
		}
	} else {
		// NOTE: if p.Path was "." this will replace
		// it with prefix which may be ""; this is
		// the intended behavior
		path = prefix
	}
	if path != "" {
		parts = append(parts, "prefix="+queryEscape(path))
	}
	// the seek parameter is only meaningful
	// if it is "larger" than the prefix being listed;
	// otherwise we should reject it
	// (AWS S3 accepts redundant start-after params,
	// but Minio rejects them)
	if seek != "" && (seek < prefix || !strings.HasPrefix(seek, prefix)) {
		return nil, fmt.Errorf("seek %q not compatible with prefix %q", seek, prefix)
	}
	if seek != "" {
		parts = append(parts, "start-after="+queryEscape(p.join(seek)))
	}
	if n > 0 {
		parts = append(parts, fmt.Sprintf("max-keys=%d", n))
	}
	if token != "" {
		parts = append(parts, "continuation-token="+url.QueryEscape(token))
	}
	sort.Strings(parts)
	query := "?" + strings.Join(parts, "&")
	req, err := http.NewRequestWithContext(p.Ctx, http.MethodGet, rawURI(p.Key, p.Bucket, query), nil)
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
		if res.StatusCode == 404 {
			// this can actually mean the bucket doesn't exist,
			// but for practical purposes we can treat it
			// as an empty filesystem
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("s3 list objects s3://%s/%s: %s", p.Bucket, p.Path, res.Status)
	}
	ret := &listResponse{}
	err = xml.NewDecoder(res.Body).Decode(ret)
	if err != nil {
		return nil, fmt.Errorf("xml decoding response: %w", err)
	}
	return ret, nil
}

func patmatch(pattern, name string) (bool, error) {
	if pattern == "" {
		return true, nil
	}
	return path.Match(pattern, name)
}

func ignoreKey(key string, dirOK bool) bool {
	name := path.Base(key)
	return key == "" ||
		!dirOK && key[len(key)-1] == '/' ||
		name == "." || name == ".."
}

// readDirAt reads n entries (or all if n < 0)
// from a directory using the given continuation
// token, returning the directory entries, the
// next continuation token, and any error.
//
// If seek is provided, this will be appended to
// the prefix path passed as the start-after
// parameter to the list call.
//
// If pattern is provided, the returned entries
// will be filtered against this pattern, and
// the prefix before the first meta-character
// will be used to determine a prefix that will
// be appended to the path passed as the prefix
// parameter to the list call.
//
// If the full directory listing was read in one
// call, this returns the list of directory
// entries, an empty continuation token, and
// io.EOF. Note that this behavior differs from
// fs.ReadDirFile.ReadDir.
func (p *Prefix) readDirAt(n int, token, seek, pattern string) (d []fs.DirEntry, next string, err error) {
	prefix, _ := splitMeta(pattern)
	ret, err := p.list(n, token, seek, prefix)
	if err != nil {
		return nil, "", err
	}
	out := make([]fs.DirEntry, 0, len(ret.Contents)+len(ret.CommonPrefixes))
	for i := range ret.Contents {
		if ignoreKey(ret.Contents[i].Path(), false) {
			continue
		}
		name := ret.Contents[i].Name()
		match, err := patmatch(pattern, name)
		if err != nil {
			return nil, "", err
		} else if !match {
			continue
		}
		ret.Contents[i].Key = p.Key
		ret.Contents[i].Client = p.client()
		ret.Contents[i].Bucket = p.Bucket
		ret.Contents[i].ctx = p.Ctx
		out = append(out, &ret.Contents[i])
	}
	for i := range ret.CommonPrefixes {
		if ignoreKey(ret.CommonPrefixes[i].Path, true) {
			continue
		}
		name := ret.CommonPrefixes[i].Name()
		match, err := patmatch(pattern, name)
		if err != nil {
			return nil, "", err
		} else if !match {
			continue
		}
		ret.CommonPrefixes[i].Key = p.Key
		ret.CommonPrefixes[i].Bucket = p.Bucket
		ret.CommonPrefixes[i].Client = p.Client
		ret.CommonPrefixes[i].Ctx = p.Ctx
		out = append(out, &ret.CommonPrefixes[i])
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) bool {
		return a.Name() < b.Name()
	})
	if !ret.IsTruncated {
		err = io.EOF
	}
	return out, ret.NextToken, err
}

func (p *Prefix) client() *http.Client {
	if p.Client == nil {
		return &DefaultClient
	}
	return p.Client
}
