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
