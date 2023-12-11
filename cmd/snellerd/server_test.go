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
