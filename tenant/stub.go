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

//go:build none
// +build none

// This is a fake tenant process
// that we are using for testing.

package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/SnellerInc/sneller/cgroup"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/vm"
)

var testdata = blockfmt.NewDirFS("../testdata")

type Env struct {
	cache   *dcache.Cache
	eventfd *os.File
	evbuf   [8]byte
}

func (e *Env) post() {
	binary.LittleEndian.PutUint64(e.evbuf[:], 1)
	e.eventfd.Write(e.evbuf[:])
}

// the calling code expects splitting to duplicate the data 4x
func (e *Env) Geometry() *plan.Geometry {
	peers := make([]plan.Transport, 4)
	for i := 0; i < 4; i++ {
		peers[i] = &plan.LocalTransport{}
	}
	return &plan.Geometry{
		Peers: peers,
	}
}

func (e *Env) Run(dst vm.QuerySink, src *plan.Input, ep *plan.ExecParams) error {
	if len(src.Descs) == 1 {
		switch src.Descs[0].Path {
		case "bad":
			return fmt.Errorf("deliberate error")
		case "hang":
			time.Sleep(time.Hour)
			return fmt.Errorf("hang timeout")
		}
	}
	r := plan.FSRunner{FS: testdata}
	return r.Run(dst, src, ep)
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
	srv := tnproto.Server{
		Server: plan.Server{Runner: &env},
		Logf: func(f string, args ...any) {
			fmt.Fprintf(os.Stderr, f+"\n", args...)
		},
	}
	err = srv.Serve(uc)
	if err != nil {
		die(err)
	}
}
