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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"time"
)

const maxWaitForHost = 10 * time.Second // maximum time to wait until host is known

var (
	headlessServiceName string
	portnum             int
)

func init() {
	flag.StringVar(&headlessServiceName, "s", "", "headless service name")
	flag.IntVar(&portnum, "p", 8001, "fixed port number")
}

type peerDesc struct {
	Addr string `json:"addr"`
}

type peerJSON struct {
	Peers []peerDesc `json:"peers"`
}

func main() {
	flag.Parse()
	if headlessServiceName == "" {
		flag.Usage()
		os.Exit(1)
	}

	start := time.Now()
retry:
	ips, err := net.LookupIP(headlessServiceName)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound && time.Since(start) < maxWaitForHost {
			time.Sleep(250 * time.Millisecond)
			goto retry
		}
		fmt.Fprintf(os.Stderr, "net.LookupIP(%q): %s", headlessServiceName, err)
		os.Exit(1)
	}

	endPoints := make([]*net.TCPAddr, 0, len(ips))
	for _, ip := range ips {
		endPoints = append(endPoints, &net.TCPAddr{
			IP:   ip,
			Port: portnum,
		})
	}

	sort.Slice(endPoints, func(i, j int) bool {
		return bytes.Compare(endPoints[i].IP, endPoints[j].IP) < 0
	})

	var ret peerJSON
	for i := range endPoints {
		ret.Peers = append(ret.Peers, peerDesc{
			Addr: endPoints[i].String(),
		})
	}
	json.NewEncoder(os.Stdout).Encode(&ret)
}
