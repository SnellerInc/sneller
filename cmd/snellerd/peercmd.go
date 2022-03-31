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
	stop   chan struct{}
}

type peerDesc struct {
	Addr string `json:"addr"`
}

type peerJSON struct {
	Peers []peerDesc `json:"peers"`
}

func (p *peerCmd) Start(interval time.Duration, logf func(f string, args ...interface{})) error {
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
			return fmt.Errorf("running peer exited with code %d: %s", exitErr.ProcessState.ExitCode(), stderr.String())
		}

		return fmt.Errorf("failed running command %q: %s", p.cmd[0], err)
	}

	var ret peerJSON
	err = json.Unmarshal(stdout.Bytes(), &ret)
	if err != nil {
		return err
	}
	lst := make([]*net.TCPAddr, 0, len(ret.Peers))
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
		lst = append(lst, &net.TCPAddr{
			IP:   ip,
			Port: portnum,
		})
	}
	p.recent.Store(lst)
	return nil
}
