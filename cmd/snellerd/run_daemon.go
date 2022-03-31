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

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/auth"
	"github.com/SnellerInc/sneller/tenant"
)

func runDaemon(args []string) {
	daemonCmd := flag.NewFlagSet("daemon", flag.ExitOnError)
	authEndpoint := daemonCmd.String("a", "", "authorization specification (file://, http://, https://, empty uses environment)")
	daemonEndpoint := daemonCmd.String("e", "127.0.0.1:8000", "endpoint to listen on (REST API)")
	remoteEndpoint := daemonCmd.String("r", "127.0.0.1:9000", "endpoint to listen on for remote requests (inter-node)")
	peerExec := daemonCmd.String("x", "", "command to exec for fetching peers")
	if daemonCmd.Parse(args) != nil {
		os.Exit(1)
	}

	exe, err := os.Readlink("/proc/self/exe")
	if err != nil {
		panic("unable to determine current executable")
	}

	server := &server{
		logger:    log.New(os.Stderr, "", log.Lshortfile),
		sandbox:   tenant.CanSandbox(),
		tenantcmd: []string{exe, "worker"},
		peers:     noPeers{},
	}
	if *peerExec != "" {
		server.peers = &peerCmd{
			cmd: strings.Fields(*peerExec),
		}
	}
	err = server.peers.Start(5*time.Second, server.logger.Printf)
	if err != nil {
		server.logger.Fatal(err)
	}

	provider, err := auth.Parse(*authEndpoint)
	if err != nil {
		server.logger.Fatal(err)
	}
	server.auth = provider

	httpl, err := net.Listen("tcp", *daemonEndpoint)
	if err != nil {
		server.logger.Fatal(err)
	}
	var tenantl net.Listener
	if *remoteEndpoint != "" {
		tenantl, err = net.Listen("tcp", *remoteEndpoint)
		if err != nil {
			server.logger.Fatal(err)
		}
	}

	if dir := os.Getenv("CACHEDIR"); dir != "" {
		server.cachedir = dir
	} else {
		server.cachedir = "/tmp"
	}
	if server.sandbox {
		server.logger.Println("sandboxing enabled")
	}

	go func() {
		server.logger.Printf("Sneller daemon %s listening on %v\n", version, httpl.Addr())
		err := server.Serve(httpl, tenantl)
		if err != nil {
			server.logger.Fatal(err)
		}
	}()

	c := make(chan os.Signal, 1)

	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal
	<-c

	// Create a deadline to wait for
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Doesn't block if no connections, but will otherwise wait until the timeout deadline
	server.Shutdown(ctx)
}
