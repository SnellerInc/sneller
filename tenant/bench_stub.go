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
	"errors"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/vm"
)

type Env struct{}

func (e *Env) Run(dst vm.QuerySink, src *plan.Input, ep *plan.ExecParams) error {
	return nil
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
	defer uc.Close()
	srv := tnproto.Server{Runner: &Env{}}
	err = srv.Serve(uc)
	if err != nil {
		die(err)
	}
}
