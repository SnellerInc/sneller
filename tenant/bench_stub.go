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

//go:build none
// +build none

// This is a fake tenant process
// that we are using for testing.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"

	// make sure we know how to decode blob.List
	_ "github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/tenant/tnproto"
)

type Env struct {
	cache   *dcache.Cache
	eventfd *os.File
	evbuf   [8]byte
}

func (e *Env) post() {
	binary.LittleEndian.PutUint64(e.evbuf[:], 1)
	e.eventfd.Write(e.evbuf[:])
}

// handle implements plan.Handle and cache.Segment
type Handle struct {
	env  *Env
	body []byte
}

func (h *Handle) Align() int  { return len(h.body) }
func (h *Handle) Size() int64 { return int64(len(h.body)) }

func (h *Handle) ETag() string {
	w := sha256.New()
	w.Write(h.body)
	return base64.URLEncoding.EncodeToString(w.Sum(nil))
}

func (h *Handle) Read(p []byte) (int, error) {
	n := copy(p, h.body)
	if n == len(h.body) {
		return n, io.EOF
	}
	return n, nil
}

func (h *Handle) Close() error { return nil }

func (h *Handle) Open(ctx context.Context) (io.ReadCloser, error) {
	return h, nil
}

func (h *Handle) Decode(dst io.Writer, src []byte) error {
	_, err := dst.Write(src)
	return err
}

type tableHandle Handle

func (t *tableHandle) Open() (vm.Table, error) {
	return t.env.cache.Table((*Handle)(t), 0), nil
}

func (t *tableHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

func mkhandle(e *Env) *tableHandle {
	// emit a dummy record
	var tmp ion.Buffer
	var st ion.Symtab
	st.Marshal(&tmp, true)
	tmp.BeginStruct(-1)
	tmp.EndStruct()
	return &tableHandle{
		env:  e,
		body: tmp.Bytes(),
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(111)
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "worker" {
		die(errors.New("expected to run in worker mode"))
	}

	workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
	workerTenant := workerCmd.String("t", "", "tenant identifier")
	workerControlSocket := workerCmd.Int("c", -1, "control socket")
	eventfd := workerCmd.Int("e", -1, "eventfd")
	if workerCmd.Parse(os.Args[2:]) != nil {
		die(errors.New("invalid arguments passed to stub"))
	}

	if *workerControlSocket == -1 {
		die(errors.New("no control socket file descriptor"))
	}
	if *eventfd == -1 {
		die(errors.New("no eventfd file descriptor"))
	}

	_ = *workerTenant

	f := os.NewFile(uintptr(*workerControlSocket), "<ctlsock>")
	conn, err := net.FileConn(f)
	if err != nil {
		die(err)
	}
	f.Close()
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		die(fmt.Errorf("unexpected fd type %T", conn))
	}

	evfd := os.NewFile(uintptr(*eventfd), "<eventfd>")

	cachedir := os.Getenv("CACHEDIR")
	if cachedir == "" {
		die(errors.New("no CACHEDIR variable set"))
	}

	defer uc.Close()
	env := Env{eventfd: evfd}
	env.cache = dcache.New(cachedir, env.post)
	err = tnproto.Serve(uc, func(st *ion.Symtab, mem []byte) (plan.TableHandle, error) {
		return mkhandle(&env), nil
	})
	if err != nil {
		die(err)
	}
}
