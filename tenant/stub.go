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
	"strconv"

	"github.com/SnellerInc/sneller/cgroup"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/vm"
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

var _ plan.UploaderDecoder = (*Env)(nil)

func (e *Env) DecodeUploader(st *ion.Symtab, buf []byte) (plan.UploadFS, error) {
	return db.DecodeDirFS(st, buf)
}

// handle implements plan.Handle and cache.Segment
type Handle struct {
	filename string
	size     int64
	env      *Env
	repeat   int
	ctx      context.Context
	hang     bool
}

func (h *Handle) Merge(other dcache.Segment) {
	h2 := other.(*Handle)
	if h.ETag() != h2.ETag() {
		panic("merging bad etags")
	}
}

func (h *Handle) Align() int { return 1024 * 1024 }

func (h *Handle) Size() int64 { return h.size * int64(h.repeat) }

func (h *Handle) ETag() string {
	w := sha256.New()
	io.WriteString(w, h.filename)
	io.WriteString(w, strconv.Itoa(h.repeat))
	return base64.URLEncoding.EncodeToString(w.Sum(nil))
}

func (h *Handle) Ephemeral() bool { return false }

type repeatReader struct {
	ctx   context.Context
	file  *os.File
	count int
}

func (r *repeatReader) Read(dst []byte) (int, error) {
	for {
		if r.count <= 0 {
			return 0, io.EOF
		}
		n, err := r.file.Read(dst)
		if n > 0 || err != io.EOF {
			return n, err
		}
		r.count--
		r.file.Seek(0, io.SeekStart)
	}
}

func (r *repeatReader) Close() error { return r.file.Close() }

func (h *Handle) Open() (io.ReadCloser, error) {
	f, err := os.Open(h.filename)
	if err != nil {
		return nil, err
	}
	return &repeatReader{
		ctx:   h.ctx,
		file:  f,
		count: h.repeat,
	}, nil
}

func (h *Handle) Decode(dst io.Writer, src []byte) error {
	if h.Align() > vm.PageSize {
		return fmt.Errorf("align %d > vm.PageSize %d", h.Align(), vm.PageSize)
	}
	if h.hang {
		<-h.ctx.Done()
		return h.ctx.Err()
	}
	buf := vm.Malloc()
	defer vm.Free(buf)
	for off := int64(0); off < h.Size(); off += int64(h.Align()) {
		mem := src[off:]
		if len(mem) > h.Align() {
			mem = mem[:h.Align()]
		}
		_, err := dst.Write(buf[:copy(buf, mem)])
		if err != nil {
			return err
		}
	}
	return nil
}

type tableHandle struct {
	Handle
}

func (t *tableHandle) Open(ctx context.Context) (vm.Table, error) {
	o := t.Handle
	o.ctx = ctx
	return t.env.cache.Table(&o, 0), nil
}

func (t *tableHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("repeat"))
	dst.WriteInt(int64(t.repeat))
	dst.BeginField(st.Intern("filename"))
	dst.WriteString(t.filename)
	dst.BeginField(st.Intern("hang"))
	dst.WriteBool(t.hang)
	dst.EndStruct()
	return nil
}

// the calling code expects splitting to duplicate the data 4x
func (t *tableHandle) Split() (plan.Subtables, error) {
	ret := make(plan.SubtableList, 4)
	for i := 0; i < 4; i++ {
		ret[i].Handle = t
		ret[i].Transport = &plan.LocalTransport{}
	}
	return ret, nil
}

func (e *Env) DecodeHandle(st *ion.Symtab, buf []byte) (plan.TableHandle, error) {
	if len(buf) == 0 {
		return nil, fmt.Errorf("no TableHandle present")
	}
	th := &tableHandle{
		Handle: Handle{
			env:    e,
			repeat: 1,
		},
	}
	_, err := ion.UnpackStruct(st, buf, func(name string, field []byte) error {
		switch name {
		case "repeat":
			n, _, err := ion.ReadInt(field)
			if err != nil {
				return err
			}
			th.repeat = int(n)
		case "filename":
			str, _, err := ion.ReadString(field)
			if err != nil {
				return err
			}
			if str == "/dev/null" {
				return fmt.Errorf("%q cannot be source of data", str)
			}
			fi, err := os.Stat(str)
			if err != nil {
				return err
			}
			th.filename = str
			th.size = fi.Size()
		case "hang":
			hang, _, err := ion.ReadBool(field)
			if err != nil {
				return err
			}
			th.hang = hang
		default:
			return fmt.Errorf("unrecognized field %q", name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return th, nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(111)
}

// if we are in a recognized cgroup2 hierarchy
// *and* we are not in the root cgroup, check
// that our cgroup ends with the tenant ID
func testCgroupOK() {
	want := os.Getenv("WANT_CGROUP")
	if want == "" {
		return
	}
	cur, err := cgroup.Self()
	if err != nil {
		die(fmt.Errorf("reading /proc/self/cgroup: %s", err))
	}
	if string(cur) != want {
		die(fmt.Errorf("got cgroup %s but want %s", string(cur), want))
	}
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

	testCgroupOK()

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
	err = tnproto.Serve(uc, &env)
	if err != nil {
		die(err)
	}
}
