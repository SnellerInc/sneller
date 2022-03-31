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
