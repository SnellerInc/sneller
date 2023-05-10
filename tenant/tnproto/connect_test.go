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

package tnproto

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/usock"
)

func randpair() (id ID, key Key) {
	rand.Read(id[:])
	rand.Read(key[:])
	return
}

func TestAttach(t *testing.T) {
	r, w := net.Pipe()

	id, key := randpair()
	go func() {
		err := Attach(w, id, key)
		if err != nil {
			panic(err)
		}
		w.Close()
	}()
	defer r.Close()
	outid, outkey, err := ReadHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	if id != outid {
		t.Fatalf("got id %x; wanted %x", outid, id)
	}
	if key != outkey {
		t.Fatalf("got key %x; wanted %x", outkey, key)
	}
}

const largeSize = 500000

var largeInput = []*plan.Input{{
	Descs: []blockfmt.Descriptor{{
		ObjectInfo: blockfmt.ObjectInfo{
			Path: strings.Repeat("A", largeSize),
			Size: largeSize,
		},
	}},
}}

// See #381
//
// Make sure that a call to recvmsg
// plus a call to read(2) can read
// a very large plan body
func TestDirectExecHugeBody(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip()
	}
	here, there, err := usock.SocketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer here.Close()
	defer there.Close()
	myconn, thereconn, err := usock.SocketPair()
	if err != nil {
		t.Fatal(err)
	}
	thereconn.Close()
	defer myconn.Close()

	var outerwg sync.WaitGroup
	outerwg.Add(1)
	go func() {
		var b Buffer
		b.Prepare(&plan.Tree{
			Inputs: largeInput,
			Root: plan.Node{
				Input: 0,
				Op: &plan.Leaf{
					Orig: &expr.Table{
						Binding: expr.Bind(expr.Identifier("foo"), ""),
					},
				},
			},
		}, OutputRaw)
		rc, err := b.DirectExec(there, myconn)
		if err != nil {
			panic(err)
		}
		rc.Close()
		outerwg.Done()
	}()

	var header [8]byte
	n, copysock, err := usock.ReadWithConn(here, header[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatalf("read %d bytes?", n)
	}
	defer copysock.Close()
	size := int(binary.LittleEndian.Uint32(header[3:]))
	buf := make([]byte, size)
	here.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(here, buf)
	if err != nil {
		t.Fatal(err)
	}
	// tell the other end to detach
	p, err := detach(here)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	outerwg.Wait()
}
