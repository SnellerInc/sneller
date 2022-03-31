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

//go:build linux || netbsd || openbsd || solaris || freebsd || aix || darwin || dragonfly
// +build linux netbsd openbsd solaris freebsd aix darwin dragonfly

// Package usock implements a wrapper
// around the unix(7) SCM_RIGHTS API,
// which allows processes to exchange
// file handles over a unix(7) control socket.
package usock

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
)

const Implemented = true

// this needs to be large enough
// to accept a control message that
// passes a single file descriptor
const scmBufSize = 32

type sysconn interface {
	SyscallConn() (syscall.RawConn, error)
}

// Fd returns the file descriptor
// associated with an io.Closer.
// The io.Closer should be either
// an *os.File or a net.Conn backed
// by a real socket file descriptor.
// If the argument to Fd is not backed
// by a file descriptor, Fd returns -1.
//
// Note that the returned file descriptor
// isn't valid for any longer than the
// provided io.Closer remains open.
// Please only use Fd for informational purposes.
func Fd(c io.Closer) int {
	sc, ok := c.(sysconn)
	if !ok {
		return -1
	}
	conn, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	var out int
	err = conn.Control(func(fd uintptr) {
		out = int(fd)
	})
	if err != nil {
		return -1
	}
	return out
}

// SocketPair returns a pair of connected unix sockets.
func SocketPair() (*net.UnixConn, *net.UnixConn, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	left, err := fd2unix(fds[0])
	if err != nil {
		syscall.Close(fds[0])
		syscall.Close(fds[1])
		return nil, nil, err
	}
	right, err := fd2unix(fds[1])
	if err != nil {
		left.Close()
		syscall.Close(fds[1])
		return nil, nil, err
	}
	return left, right, nil
}

func fd2unix(fd int) (*net.UnixConn, error) {
	osf := os.NewFile(uintptr(fd), "")
	if osf == nil {
		return nil, fmt.Errorf("bad file descriptor %d", fd)
	}
	defer osf.Close() // net.FileConn will dup(2) the fd
	fc, err := net.FileConn(osf)
	if err != nil {
		return nil, err
	}
	uc, ok := fc.(*net.UnixConn)
	if !ok {
		fc.Close()
		return nil, fmt.Errorf("couldn't convert %T to net.UnixConn", fc)
	}
	return uc, nil
}

func writeWithSysconn(dst *net.UnixConn, msg []byte, rc syscall.RawConn) (int, error) {
	var reterr error
	var n int
	// capture the input file descriptor for
	// long enough that the call to sendmsg(2) completes
	err := rc.Control(func(fd uintptr) {
		oob := syscall.UnixRights(int(fd))
		n, _, reterr = dst.WriteMsgUnix(msg, oob, nil)
	})
	if err != nil {
		return 0, err
	}
	return n, reterr
}

// WriteWithFile writes a message to dst,
// including the provided file handle in an
// out-of-band control message.
func WriteWithFile(dst *net.UnixConn, msg []byte, handle *os.File) (int, error) {
	rc, err := handle.SyscallConn()
	if err != nil {
		return 0, err
	}
	return writeWithSysconn(dst, msg, rc)
}

// WriteWithConn is similar to WriteWithFile,
// except that it sends the file descriptor associated
// with a net.Conn rather than an os.File.
func WriteWithConn(dst *net.UnixConn, msg []byte, conn net.Conn) (int, error) {
	sc, ok := conn.(sysconn)
	if !ok {
		return 0, fmt.Errorf("cannot write connection of type %T", conn)
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return 0, err
	}
	return writeWithSysconn(dst, msg, rc)
}

// ReadWithFile reads data from src,
// and if it includes an out-of-band control message,
// it will try to turn it into a file handle.
func ReadWithFile(src *net.UnixConn, dst []byte) (int, *os.File, error) {
	oob := make([]byte, scmBufSize)
	n, oobn, _, _, err := src.ReadMsgUnix(dst, oob)
	if err != nil {
		return n, nil, err
	}
	oob = oob[:oobn]
	if len(oob) > 0 {
		scm, err := syscall.ParseSocketControlMessage(oob)
		if err != nil {
			return n, nil, err
		}
		if len(scm) != 1 {
			return n, nil, fmt.Errorf("%d socket control messages", len(scm))
		}
		fds, err := syscall.ParseUnixRights(&scm[0])
		if err != nil {
			return n, nil, fmt.Errorf("parsing unix rights: %s", err)
		}
		if len(fds) == 1 {
			// try to set this fd as non-blocking
			syscall.SetNonblock(fds[0], true)
			return n, os.NewFile(uintptr(fds[0]), "<socketconn>"), nil
		}
		if len(fds) > 1 {
			for i := range fds {
				syscall.Close(fds[i])
			}
			return n, nil, fmt.Errorf("control message sent %d fds", len(fds))
		}
		// fallthrough; len(fds) == 0
	}
	return n, nil, nil
}

// ReadWithConn is like ReadWithFile,
// except that it converts the in-band file descriptor
// to a net.Conn rather than an os.File.
func ReadWithConn(src *net.UnixConn, dst []byte) (int, net.Conn, error) {
	n, f, err := ReadWithFile(src, dst)
	if err != nil {
		return n, nil, err
	}
	var conn net.Conn
	if f != nil {
		defer f.Close()
		conn, err = net.FileConn(f)
		if err != nil {
			return n, nil, err
		}
	}
	return n, conn, nil
}
