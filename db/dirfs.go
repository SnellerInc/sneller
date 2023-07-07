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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var _ InputFS = &DirFS{}

// DirFS is an InputFS implementation
// that can be used for local testing.
// It includes a local HTTP server
// bound to the loopback interface
// that will serve the directory contents.
type DirFS struct {
	*blockfmt.DirFS

	start    sync.Once
	server   *http.Server
	listener net.Listener
	addr     net.Addr
	err      error
}

// NewDirFS constructs a new DirFS.
func NewDirFS(dir string) *DirFS {
	return &DirFS{
		DirFS: blockfmt.NewDirFS(dir),
	}
}

// Close closes the http server
// associated with the DirFS.
func (d *DirFS) Close() error {
	if d.server != nil {
		d.listener.Close()
		d.listener = nil
		err := d.server.Close()
		d.server = nil
		return err
	}
	return nil
}

func (d *DirFS) startOnce() error {
	d.start.Do(func() {
		d.server = &http.Server{
			Handler: d,
		}
		l, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			d.err = err
			return
		}
		d.listener = l
		d.addr = l.Addr()
		go d.server.Serve(l)
	})
	return d.err
}

// ServeHTTP is a basic implementation of a file
// server which serves from this DirFS.
func (d *DirFS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	f, err := d.Open(name)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, fs.ErrNotExist) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	defer f.Close()
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		http.Error(w, "file is not seekable", http.StatusInternalServerError)
		return
	}
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if fi.IsDir() {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	etag, err := d.ETag(name, fi)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Etag", etag)
	http.ServeContent(w, r, "", fi.ModTime(), rs)
}

// Prefix is "file://"
func (d *DirFS) Prefix() string {
	return "file://"
}

// Encode writes the URL for the server to dst.
// This can be used by DecodeClientFS to access
// the DirFS remotely.
func (d *DirFS) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	cfs, err := d.Start()
	if err != nil {
		return err
	}
	return cfs.Encode(dst, st)
}

// Start begins serving files and returns a
// ClientFS.
func (d *DirFS) Start() (*ClientFS, error) {
	if err := d.startOnce(); err != nil {
		return nil, err
	}
	return &ClientFS{
		DirFS: d.DirFS,
		URL:   "http://" + d.addr.String(),
	}, nil
}

// ClientFS is a client for a DirFS. This is
// meant to be used for testing purposes only.
type ClientFS struct {
	*blockfmt.DirFS
	// URL is the URL returned by DirFS.Start.
	URL string
}

// DecodeClientFS produces a ClientFS from the
// datum encoded by DirFS.Encode.
func DecodeClientFS(d ion.Datum) (*ClientFS, error) {
	var root, url string
	err := d.UnpackStruct(func(f ion.Field) error {
		var err error
		switch f.Label {
		case "root":
			root, err = f.String()
		case "url":
			url, err = f.String()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &ClientFS{
		DirFS: blockfmt.NewDirFS(root),
		URL:   url,
	}, nil
}

func (c *ClientFS) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("root"))
	dst.WriteString(c.DirFS.Root)
	dst.BeginField(st.Intern("url"))
	dst.WriteString(c.URL)
	dst.EndStruct()
	return nil
}

// OpenRange implements fsutil.OpenRangeFS.OpenRange
func (c *ClientFS) OpenRange(name, etag string, off, width int64) (io.ReadCloser, error) {
	info, _ := fs.Stat(c.DirFS, name)
	url, err := c.url(name)
	if err != nil {
		return nil, err
	}
	return &clientFile{
		info:     info,
		url:      url,
		hasrange: true,
		etag:     etag,
		start:    off,
		end:      off + width,
	}, nil
}

func (c *ClientFS) Open(name string) (fs.File, error) {
	info, _ := fs.Stat(c.DirFS, name)
	url, err := c.url(name)
	if err != nil {
		return nil, err
	}
	return &clientFile{
		info: info,
		url:  url,
	}, nil
}

type clientFile struct {
	info       fs.FileInfo
	url        string
	rc         io.ReadCloser
	etag       string
	lastmod    time.Time // FIXME: unused
	hasrange   bool
	start, end int64
}

func (c *clientFile) Stat() (fs.FileInfo, error) {
	if c.info != nil {
		return c.info, nil
	}
	return nil, fmt.Errorf("Stat not supported")
}

// IfMatch implements plan.ETagChecker.
func (c *clientFile) IfMatch(etag string) error {
	c.etag = etag
	return nil
}

func (c *clientFile) IfUnmodifiedSince(mod time.Time) error {
	c.lastmod = mod
	return nil
}

// RangeReader implements plan.RangeReader.
func (c *clientFile) RangeReader(off, n int64) (io.ReadCloser, error) {
	if c.hasrange {
		return nil, fmt.Errorf("RangeReader already called")
	}
	if off < 0 || n <= 0 {
		return nil, fmt.Errorf("out of range (off=%d n=%d)", off, n)
	}
	c.start, c.end = off, off+n
	c.hasrange = true
	return c, nil
}

func (c *clientFile) Read(b []byte) (n int, err error) {
	err = c.open()
	if err != nil {
		return 0, err
	}
	return c.rc.Read(b)
}

func (c *clientFile) Close() error {
	if c.rc != nil {
		return c.rc.Close()
	}
	return nil
}

func (c *clientFile) open() error {
	if c.rc != nil {
		return nil
	}
	req, err := http.NewRequest(http.MethodGet, c.url, nil)
	if err != nil {
		return err
	}
	if c.hasrange {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end))
	}
	if c.etag != "" {
		req.Header.Set("If-Match", c.etag)
	}
	// TODO: send If-Modified-Since header

	res, err := flakyGet(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusPartialContent {
		res.Body.Close()
		return fmt.Errorf("unexpected HTTP response status %d", res.StatusCode)
	}

	// if we got an ETag back, let's check it
	et := res.Header.Get("ETag")
	if et != "" && c.etag != "" && et != c.etag {
		res.Body.Close()
		return fmt.Errorf("unexpected ETag in response %q", et)
	}
	// NOTE: we're doing this here because when
	// you send both If-Match and If-Unmodified-Since to S3,
	// then S3 prefers If-Match, so we can't enforce both up front
	lm := res.Header.Get("Last-Modified")
	if lm != "" && !c.lastmod.IsZero() {
		t, err := time.Parse(time.RFC1123, lm)
		if err != nil {
			res.Body.Close()
			return fmt.Errorf("parsing Last-Modified: %s", err)
		}
		// FIXME: re-enable this check;
		// See issue #790
		if false && t.After(c.lastmod) {
			res.Body.Close()
			return fmt.Errorf("Last-Modified time %s after descriptor LastModified %s", lm, c.lastmod)
		}
	}
	c.rc = res.Body
	return nil
}

func (c *ClientFS) url(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", fmt.Errorf("invalid path: %s", name)
	}
	if name == "." {
		return c.URL, nil
	}
	url := strings.TrimSuffix(c.URL, "/")
	return url + "/" + name, nil
}

func redactQuery(err error) error {
	ue, ok := err.(*url.Error)
	if !ok {
		return err
	}
	u, _ := url.Parse(ue.URL)
	if u == nil {
		return err
	}
	u.RawQuery = ""
	u.RawFragment = ""
	ue.URL = u.String()
	return ue
}

func flakyGet(req *http.Request) (*http.Response, error) {
	res, err := http.DefaultClient.Do(req)
	if req.Body != nil ||
		(err == nil && res.StatusCode != 500 && res.StatusCode != 503) {
		return res, redactQuery(err)
	}
	// force re-dialing, which will hopefully
	// lead to a load balancer picking a healthy backend...?
	http.DefaultClient.CloseIdleConnections()
	res, err = http.DefaultClient.Do(req)
	return res, redactQuery(err)
}
