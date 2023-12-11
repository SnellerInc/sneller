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

package sneller

import (
	"net"
	"time"

	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
)

type Splitter struct {
	WorkerID  tnproto.ID
	WorkerKey tnproto.Key
	Peers     []*net.TCPAddr
	SelfAddr  string
}

func (s *Splitter) Geometry() *plan.Geometry {
	peers := make([]plan.Transport, len(s.Peers))
	for i := range peers {
		peers[i] = s.transport(i)
	}
	return &plan.Geometry{Peers: peers}
}

func (s *Splitter) transport(i int) plan.Transport {
	nodeID := s.Peers[i].String()
	if nodeID == s.SelfAddr {
		return &plan.LocalTransport{}
	}
	return &tnproto.Remote{
		ID:      s.WorkerID,
		Key:     s.WorkerKey,
		Net:     "tcp",
		Addr:    nodeID,
		Timeout: 3 * time.Second,
	}
}
