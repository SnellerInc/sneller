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
