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
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testPeers is a wrapper around the
// "production" peerCmd implementation
// that writes a static list to a file
// and makes the command implementation
// just "cat <file>"
type testPeers struct {
	peerCmd
	tt   *testing.T
	list []*net.TCPAddr
}

func makePeers(t *testing.T, peers ...*net.TCPAddr) *testPeers {
	return &testPeers{list: peers, tt: t}
}

func (t *testPeers) Start(interval time.Duration, logf func(string, ...interface{})) error {
	dir := t.tt.TempDir()
	var body peerJSON
	for i := range t.list {
		body.Peers = append(body.Peers, peerDesc{
			Addr: t.list[i].String(),
		})
	}
	buf, err := json.Marshal(&body)
	if err != nil {
		t.tt.Fatal(err)
	}
	name := filepath.Join(dir, "peers.json")
	err = os.WriteFile(name, buf, 0644)
	if err != nil {
		t.tt.Fatal(err)
	}
	t.cmd = []string{"cat", name}
	return t.peerCmd.Start(interval, logf)
}
