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

//go:build !linux && !freebsd && !openbsd && !netbsd && !aix && !dragonfly
// +build !linux,!freebsd,!openbsd,!netbsd,!aix,!dragonfly

package usock

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
)

const Implemented = false

func notImplemented(name string) error {
	return fmt.Errorf("usock.%s not implemented on %s", name, runtime.GOOS)
}

func Fd(io.Closer) int { return -1 }

func SocketPair() (*net.UnixConn, *net.UnixConn, error) {
	return nil, nil, notImplemented("SocketPair")
}

func WriteWithFile(dst *net.UnixConn, msg []byte, handle *os.File) (int, error) {
	return 0, notImplemented("WriteWithFile")
}

func WriteWithConn(dst *net.UnixConn, msg []byte, conn net.Conn) (int, error) {
	return 0, notImplemented("WriteWithConn")
}

func ReadWithFile(src *net.UnixConn, msg []byte) (int, *os.File, error) {
	return 0, nil, notImplemented("ReadWithFile")
}

func ReadWithConn(src *net.UnixConn, msg []byte) (int, net.Conn, error) {
	return 0, nil, notImplemented("ReadWithConn")
}
