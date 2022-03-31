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

package main

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

type delayedHijack struct {
	laddr    net.Addr
	req      *http.Request
	res      http.ResponseWriter
	hijacked bool
}

type sysconn interface {
	SyscallConn() (syscall.RawConn, error)
}

func (d *delayedHijack) SyscallConn() (syscall.RawConn, error) {
	d.hijacked = true
	d.res.Header().Add("Transfer-Encoding", "chunked")
	d.res.WriteHeader(http.StatusOK)
	flush(d.res)
	conn, ok := d.req.Context().Value(rawConnKey).(net.Conn)
	if !ok {
		return nil, fmt.Errorf("no rawConn value?")
	}
	sc, ok := conn.(sysconn)
	if !ok {
		return nil, fmt.Errorf("can't use %T as sysconn", conn)
	}
	return sc.SyscallConn()
}

func (d *delayedHijack) Write(p []byte) (int, error) {
	panic("not expecting Write to delayedHijack")
}

func (d *delayedHijack) Read(p []byte) (int, error) {
	panic("not expected Read from delayedHijack")
}

func (d *delayedHijack) Close() error {
	return nil
}

func (d *delayedHijack) LocalAddr() net.Addr {
	return d.laddr
}

func (d *delayedHijack) RemoteAddr() net.Addr {
	raddr := d.req.RemoteAddr
	host, port, _ := net.SplitHostPort(raddr)
	ip := net.ParseIP(host)
	portnum, _ := strconv.Atoi(port)
	return &net.TCPAddr{
		IP:   ip,
		Port: portnum,
	}
}

func (d *delayedHijack) SetDeadline(t time.Time) error      { return nil }
func (d *delayedHijack) SetReadDeadline(t time.Time) error  { return nil }
func (d *delayedHijack) SetWriteDeadline(t time.Time) error { return nil }
