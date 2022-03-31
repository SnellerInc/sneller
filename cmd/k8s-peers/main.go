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
