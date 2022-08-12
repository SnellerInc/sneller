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

// Package debug provides remote debugging tools
package debug

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
)

// Fd binds an http server to the
// provided file descriptor and starts
// it asynchronously. If the server ever
// stops running, the error returned
// from http.Serve is passed to errorln.
func Fd(fd int, lg *log.Logger) {
	f := os.NewFile(uintptr(fd), "debug_sock")
	l, err := net.FileListener(f)
	f.Close()
	if err != nil {
		lg.Printf("warning: unable to set up debug fd: %s", err)
		return
	}
	lg.Printf("binding pprof handlers to fd=%d", fd)
	go func() {
		defer l.Close()
		lg.Printf("debug fd: %s", http.Serve(l, nil))
	}()
}
