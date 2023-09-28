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

//go:build linux

package debug

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDebug(t *testing.T) {
	tmpdir := t.TempDir()
	sock := filepath.Join(tmpdir, "sock")
	var outbuf bytes.Buffer
	lg := log.New(&outbuf, "", log.Lshortfile)
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	f, err := l.(*net.UnixListener).File()
	if err != nil {
		t.Fatal(err)
	}
	Fd(int(f.Fd()), lg)

	local, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	go func() {
		for {
			conn, err := local.Accept()
			if err != nil {
				return
			}
			other, err := net.Dial("unix", sock)
			if err != nil {
				panic(err)
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, other)
			}()
			go func() {
				defer other.Close()
				io.Copy(other, conn)
			}()
		}
	}()
	hostport := local.Addr().String()
	res, err := http.Get("http://" + hostport + "/debug/pprof/cmdline")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Errorf("got status code %d", res.StatusCode)
	}
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got cmdline %s", buf)
}

func TestPathDebug(t *testing.T) {
	tmpdir := t.TempDir()
	sock := filepath.Join(tmpdir, "sock")
	var outbuf bytes.Buffer
	lg := log.New(&outbuf, "", log.Lshortfile)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(outbuf.String())
		}
	})

	// bind to a local socket path
	ok := func(ucred *syscall.Ucred) bool {
		t.Logf("got ucred %+v", ucred)
		return int(ucred.Pid) == syscall.Getpid()
	}
	Path(sock, ok, lg)

	c := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}
	res, err := c.Get("http://localprofile/debug/pprof/cmdline")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("got status code %d %s", res.StatusCode, buf)
	}
	t.Logf("got cmdline %s", buf)
}
