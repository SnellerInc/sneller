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
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/dchest/siphash"
)

const DefaultSplitSize = int64(100 * 1024 * 1024)

type Splitter struct {
	SplitSize int64
	WorkerID  tnproto.ID
	WorkerKey tnproto.Key
	Peers     []*net.TCPAddr
	SelfAddr  string
}

func (s *Splitter) encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("SplitSize"))
	dst.WriteInt(s.SplitSize)
	dst.BeginField(st.Intern("WorkerID"))
	dst.WriteBlob(s.WorkerID[:])
	dst.BeginField(st.Intern("WorkerKey"))
	dst.WriteBlob(s.WorkerKey[:])
	dst.BeginField(st.Intern("Peers"))
	dst.BeginList(-1)
	for i := range s.Peers {
		dst.WriteString(s.Peers[i].String())
	}
	dst.EndList()
	dst.BeginField(st.Intern("SelfAddr"))
	dst.WriteString(s.SelfAddr)
	dst.EndStruct()
}

func parseAddr(x string) (*net.TCPAddr, error) {
	host, port, err := net.SplitHostPort(x)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse %q: %w", x, err)
	}
	portnum, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse port number %q: %w", port, err)
	}
	ip := net.ParseIP(host)
	if len(ip) == 0 {
		return nil, fmt.Errorf("couldn't parse IP %q: %w", host, err)
	}
	return &net.TCPAddr{IP: ip, Port: portnum}, nil
}

func (s *Splitter) setField(f ion.Field) error {
	var err error
	var buf []byte
	switch f.Label {
	case "SplitSize":
		s.SplitSize, err = f.Int()
	case "WorkerID":
		buf, err = f.BlobShared()
		if err == nil {
			copy(s.WorkerID[:], buf)
		}
	case "WorkerKey":
		buf, err = f.BlobShared()
		if err == nil {
			copy(s.WorkerKey[:], buf)
		}
	case "Peers":
		err = f.UnpackList(func(d ion.Datum) error {
			str, err := d.String()
			if err != nil {
				return err
			}
			addr, err := parseAddr(str)
			if err != nil {
				return err
			}
			s.Peers = append(s.Peers, addr)
			return nil
		})
	case "SelfAddr":
		s.SelfAddr, err = f.String()
	default:
		err = fmt.Errorf("Splitter: unexpected field %q", f.Label)
	}
	return err
}

func (s *Splitter) split(fh *TenantHandle) (plan.Subtables, error) {
	blobs := make([]blob.Interface, 0, len(fh.Blobs.Contents))
	splits := make([]split, len(s.Peers))
	for i := range splits {
		splits[i].tp = s.transport(i)
	}
	insert := func(b blob.Interface) error {
		i, err := s.partition(b)
		if err != nil {
			return err
		}
		splits[i].blobs = append(splits[i].blobs, len(blobs))
		blobs = append(blobs, b)
		return nil
	}
	for _, b := range fh.Blobs.Contents {
		if err := insert(b); err != nil {
			return nil, err
		}
	}
	return &Subtables{
		parent:    fh.parent,
		table:     expr.Null{},
		splits:    compact(splits),
		blobs:     fh.Blobs.Contents,
		fields:    fh.Fields,
		allFields: fh.AllFields,
		filter:    nil, // pushed down later
	}, nil
}

// compact compacts splits so that any splits with no
// blobs are removed from the list.
func compact(splits []split) []split {
	out := splits[:0]
	for i := range splits {
		if len(splits[i].blobs) > 0 {
			out = append(out, splits[i])
		}
	}
	return out
}

// partition returns the index of the peer which should
// handle the specified blob.
func (s *Splitter) partition(b blob.Interface) (int, error) {
	info, err := b.Stat()
	if err != nil {
		return 0, err
	}

	// just two fixed random values
	key0 := uint64(0x5d1ec810)
	key1 := uint64(0xfebed702)

	hash := siphash.Hash(key0, key1, []byte(info.ETag))
	maxUint64 := ^uint64(0)
	idx := hash / (maxUint64 / uint64(len(s.Peers)))
	return int(idx), nil
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

type split struct {
	tp    plan.Transport
	blobs []int
}

// Subtables is the plan.Subtables implementation
// returned by TenantHandle.Split
type Subtables struct {
	parent *TenantEnv // for local execution
	splits []split
	table  expr.Node
	blobs  []blob.Interface

	// from plan.Hints:
	filter    expr.Node
	fields    []string
	allFields bool

	next *Subtables // set if combined
}

// Len implements plan.Subtables.Len.
func (s *Subtables) Len() int {
	n := len(s.splits)
	if s.next != nil {
		n += s.next.Len()
	}
	return n
}

// Subtable implements plan.Subtables.Subtable.
func (s *Subtables) Subtable(i int, sub *plan.Subtable) {
	if s.next != nil && i >= len(s.splits) {
		s.next.Subtable(i-len(s.splits), sub)
		return
	}
	sp := &s.splits[i]
	blobs := make([]blob.Interface, len(sp.blobs))
	for i, bi := range sp.blobs {
		blobs[i] = s.blobs[bi]
	}
	*sub = plan.Subtable{
		Transport: sp.tp,
		Handle: &TenantHandle{
			parent: s.parent,
			FilterHandle: &FilterHandle{
				// NOTE: we're not setting Splitter here
				// because we don't expect a second splitting...
				Blobs:     &blob.List{Contents: blobs},
				Fields:    s.fields,
				AllFields: s.allFields,
				Expr:      s.filter,
			},
		},
	}
}

// Filter implements plan.Subtables.Filter.
func (s *Subtables) Filter(e expr.Node) {
	s.filter = e
	if s.next != nil {
		s.next.Filter(e)
	}
}

// Append implements plan.Subtables.Append.
func (s *Subtables) Append(sub plan.Subtables) plan.Subtables {
	end := s
	for end.next != nil {
		end = end.next
	}
	end.next = sub.(*Subtables)
	return s
}
