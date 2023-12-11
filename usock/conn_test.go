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

//go:build linux || freebsd || openbsd || netbsd || solaris || aix || dragonfly
// +build linux freebsd openbsd netbsd solaris aix dragonfly

package usock

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestFdLeak(t *testing.T) {
	nfds := func() int {
		dirents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		return len(dirents)
	}

	start := nfds()
	t.Logf("beginning fds: %d", start)

	one, two, err := SocketPair()
	if err != nil {
		t.Fatal(err)
	}

	during := nfds()
	t.Logf("during: have %d fds", during)
	if during != start+2 {
		t.Errorf("now have %d fds?", during)
	}

	one.Close()
	two.Close()

	final := nfds()
	if final != start {
		t.Errorf("final: have %d; wanted %d", final, start)
	}
}

func TestConn(t *testing.T) {
	msg := []byte("hello, world")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("pipe read fd: %d", r.Fd())
	t.Logf("pipe write fd: %d", w.Fd())

	outer, inner, err := SocketPair()
	if err != nil {
		t.Fatal(err)
	}

	err = outer.SetDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		t.Fatalf("SetDeadline: %s", err)
	}
	_, err = WriteWithFile(outer, msg, r)
	if err != nil {
		t.Fatal(err)
	}
	// writing the fd message should have
	// created a new handle reference, so
	// we can close the original read handle
	r.Close()

	outmsg := make([]byte, 2*len(msg))
	n, f, err := ReadWithFile(inner, outmsg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(outmsg[:n], msg) {
		t.Errorf("%q != %q", outmsg[:n], msg)
	}
	if f == nil {
		t.Error("no file descriptor returned?")
	}
	t.Logf("output file descriptor: %d", f.Fd())

	// now write through the shared pipe
	_, err = w.Write(msg)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	out, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, msg) {
		t.Errorf("%q != %q", out, msg)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
}
