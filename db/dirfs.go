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
	"net"
	"net/http"
	"sync"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var _ FS = &DirFS{}

// DirFS is an FS implementation
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

// NewDirFS constructs
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
			Handler: http.FileServer(http.FS(d)),
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

// Prefix is "file://"
func (d *DirFS) Prefix() string {
	return "file://"
}

// URL implements FS.URL
func (d *DirFS) URL(fp string, info fs.FileInfo, etag string) (string, error) {
	if err := d.startOnce(); err != nil {
		return "", err
	}
	if !fs.ValidPath(fp) {
		return "", fmt.Errorf("getting URL for %s: %w", fp, fs.ErrInvalid)
	}
	if info.Mode().IsDir() {
		return "", fmt.Errorf("path %s is a directory; can't provide it as a URL", fp)
	}
	uri := "http://" + d.addr.String() + "/" + fp
	return uri, nil
}
