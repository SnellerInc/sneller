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

// Package debug provides remote debugging tools
package debug

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
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

type ruleListener struct {
	net.Listener
	ok func(*syscall.Ucred) bool
}

func (r *ruleListener) Accept() (net.Conn, error) {
	type sysconn interface {
		SyscallConn() (syscall.RawConn, error)
	}
	for {
		c, err := r.Listener.Accept()
		if err != nil {
			return nil, err
		}
		sc, err := c.(sysconn).SyscallConn()
		if err != nil {
			return nil, err
		}
		var inner error
		var ucred *syscall.Ucred
		err = sc.Control(func(fd uintptr) {
			ucred, inner = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		})
		if err != nil {
			return nil, err
		}
		if inner != nil {
			return nil, err
		}
		if r.ok(ucred) {
			return c, nil
		}
		// ignore and continue if !ok
		c.Close()
	}
}

// Path creates a unix socket at path and listens on it
// for debug connections. The ok() function is used to
// filter connections based on the credentials of the
// process on the other end of the connection.
//
// See also Fd, which uses a local file descriptor
// rather than a local unix socket path.
func Path(path string, ok func(*syscall.Ucred) bool, lg *log.Logger) {
	l, err := net.Listen("unix", path)
	if err != nil {
		lg.Printf("unable to listen: %s", err)
		return
	}
	rl := &ruleListener{
		Listener: l,
		ok:       ok,
	}
	lg.Printf("binding pprof handlers to unix socket %s", path)
	go func() {
		defer rl.Close()
		lg.Printf("debug fd: %s", http.Serve(rl, nil))
	}()
}
