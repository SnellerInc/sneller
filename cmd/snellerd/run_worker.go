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

//go:build linux

package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/debug"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/vm"
)

func nfds() int {
	d, _ := os.ReadDir("/proc/self/fd")
	return len(d) - 1
}

func runWorker(args []string) {
	log.Default().SetOutput(os.Stdout)
	sneller.CanVMOpen = true

	workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
	workerTenant := workerCmd.String("t", "", "tenant identifier")
	workerControlSocket := workerCmd.Int("c", -1, "control socket")
	eventfd := workerCmd.Int("e", -1, "eventfd")
	if workerCmd.Parse(args) != nil {
		os.Exit(1)
	}

	if *workerControlSocket == -1 {
		panic("no control socket file descriptor")
	}
	if *workerTenant == "" {
		panic("unknown tenant")
	}
	if *eventfd == -1 {
		panic("no eventfd passed")
	}
	logger := log.New(os.Stdout, "", 0)

	// capture vm errors associated with this tenant
	vm.Errorf = logger.Printf
	start := nfds()
	defer func() {
		http.DefaultClient.CloseIdleConnections()
		end := nfds()
		if end > start {
			logger.Printf("warning: file descriptor leak: exiting with %d > %d", end, start)
		}
	}()
	f := os.NewFile(uintptr(*workerControlSocket), "<ctlsock>")
	conn, err := net.FileConn(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		panic(fmt.Errorf("unexpected fd type %T", conn))
	}
	defer uc.Close()
	err = syscall.SetNonblock(int(*eventfd), true)
	if err != nil {
		logger.Printf("warning: couldn't set eventfd to nonblocking: %s", err)
	}
	evfd := os.NewFile(uintptr(*eventfd), "eventfd")

	run := sneller.TenantRunner{
		Events: evfd,
	}
	if cachedir := os.Getenv("CACHEDIR"); cachedir != "" {
		info, err := os.Stat(cachedir)
		if err != nil || !info.IsDir() {
			logger.Printf("ignoring invalid cache dir %s", cachedir)
		} else {
			run.Cache = dcache.New(cachedir, run.Post)
			run.Cache.Logger = logger

			// for now, only allow root to debug us
			ok := func(ucred *syscall.Ucred) bool {
				return ucred.Uid == 0
			}
			debug.Path(filepath.Join(cachedir, "debug.sock"), ok, logger)
		}
	}

	// use a dedicated http client configuration for aws s3
	// so that we can limit the number of idle conns;
	// see #3055
	s3client := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: 5 * time.Second,
			MaxIdleConnsPerHost:   5,
			MaxIdleConns:          runtime.GOMAXPROCS(0) * 2,
			DisableCompression:    true,
			DialContext: (&net.Dialer{
				Timeout: 2 * time.Second,
			}).DialContext,
		},
	}
	initfs := func(d ion.Datum) (fs.FS, error) {
		if testmode {
			return db.DecodeClientFS(d)
		}
		s3fs, err := db.DecodeS3FS(d)
		if err != nil {
			return nil, err
		}
		s3fs.Client = s3client
		return s3fs, nil
	}
	srv := tnproto.Server{
		Server: plan.Server{
			Runner: &run,
			InitFS: initfs,
		},
		Logf: logger.Printf,
	}
	err = srv.Serve(uc)
	if err != nil {
		logger.Fatalf("cannot serve: %v", err)
	}
}
