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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/SnellerInc/sneller/tenant/tnproto"
)

const cmdTimeout = 30 * time.Second

type peerlist interface {
	Start(interval time.Duration, logf func(f string, args ...interface{})) error
	Get() []*net.TCPAddr
	Stop()
}

type noPeers struct{}

func (n noPeers) Get() []*net.TCPAddr                                     { return nil }
func (n noPeers) Start(time.Duration, func(string, ...interface{})) error { return nil }
func (n noPeers) Stop()                                                   {}

type peerCmd struct {
	cmd    []string
	recent atomic.Value
	ticker *time.Ticker
	logf   func(f string, args ...interface{})
	stop   chan struct{}
}

type peerDesc struct {
	Addr string `json:"addr"`
}

type peerJSON struct {
	Peers []peerDesc `json:"peers"`
}

func (p *peerCmd) Start(interval time.Duration, logf func(f string, args ...interface{})) error {
	p.logf = logf
	err := p.run()
	if err != nil {
		return err
	}
	p.ticker = time.NewTicker(interval)
	p.stop = make(chan struct{})
	go func() {
		for {
			select {
			case <-p.ticker.C:
				err := p.run()
				if err != nil {
					logf("getting peers: %s", err)
				}
			case <-p.stop:
				return
			}
		}
	}()
	return nil
}

func (p *peerCmd) Stop() {
	p.ticker.Stop()
	close(p.stop)
}

func (p *peerCmd) Get() []*net.TCPAddr {
	return p.recent.Load().([]*net.TCPAddr)
}

func (p *peerCmd) run() error {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, p.cmd[0], p.cmd[1:]...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			cmd.Process.Kill()
			return fmt.Errorf("peer command timed-out (killed): %s", stderr.String())
		}

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("cmdline %v exited with code %d: %s", p.cmd, exitErr.ProcessState.ExitCode(), stderr.String())
		}

		return fmt.Errorf("failed running command %q: %s", p.cmd[0], err)
	}

	var ret peerJSON
	err = json.Unmarshal(stdout.Bytes(), &ret)
	if err != nil {
		return err
	}
	lst := make([]*net.TCPAddr, 0, len(ret.Peers))
	dl := net.Dialer{
		Timeout: time.Second,
	}
	for i := range ret.Peers {
		addr := ret.Peers[i].Addr
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("couldn't parse peer %d: %w", i, err)
		}
		portnum, err := strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("couldn't parse peer %d port number: %w", i, err)
		}
		ip := net.ParseIP(host)
		if len(ip) == 0 {
			return fmt.Errorf("couldn't parse peer %d IP: %w", i, err)
		}
		tcpaddr := &net.TCPAddr{IP: ip, Port: portnum}
		conn, err := dl.Dial("tcp", tcpaddr.String())
		if err != nil {
			p.logf("discarding peer %s: %s", addr, err)
			continue
		}
		err = tnproto.Ping(conn)
		conn.Close()
		if err != nil {
			p.logf("discarding peer (ping) %s: %s", addr, err)
			continue
		}
		lst = append(lst, tcpaddr)
	}
	p.recent.Store(lst)
	return nil
}
