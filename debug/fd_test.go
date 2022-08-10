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

package debug

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
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
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got cmdline %s", buf)
}
