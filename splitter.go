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
