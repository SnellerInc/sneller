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
	"errors"
	"fmt"
	"net"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/dchest/siphash"
)

const defaultSplitSize = int64(100 * 1024 * 1024)

type splitter struct {
	SplitSize int64
	workerID  tnproto.ID
	peers     []*net.TCPAddr
	selfAddr  string

	// compute total size of input blobs
	// and the maximum # of bytes scanned after
	// sparse indexing has been applied
	total, maxscan int64
}

func (s *server) newSplitter(workerID tnproto.ID, peers []*net.TCPAddr) *splitter {
	split := &splitter{
		SplitSize: s.splitSize,
		workerID:  workerID,
		peers:     peers,
	}
	if s.remote != nil {
		split.selfAddr = s.remote.String()
	}
	return split
}

func (s *splitter) Split(table *expr.Table, handle plan.TableHandle) ([]plan.Subtable, error) {
	if table.Value == nil {
		return nil, errors.New("table hasn't been populated yet via the environment")
	}

	// distribute blobs over available nodes
	blobList := table.Value.(*blob.List)

	splitSize := s.SplitSize
	if s.SplitSize == 0 {
		splitSize = defaultSplitSize
	}

	// compile the filter expression if provided
	var flt filter
	if fh, ok := handle.(*filterHandle); ok {
		flt = compileFilter(fh.filter)
	}

	// determine all ranges for this table
	nodeBlobLists := make(map[string]*blob.List)

	insert := func(b blob.Interface) error {
		endPoint, err := s.partition(b)
		if err != nil {
			return err
		}
		nodeID := endPoint.String()
		if nodeBlobLists[nodeID] == nil {
			nodeBlobLists[nodeID] = &blob.List{}
		}
		nodeBlobLists[nodeID].Contents = append(nodeBlobLists[nodeID].Contents, b)
		return nil
	}

	for _, b := range blobList.Contents {
		stat, err := b.Stat()
		if err != nil {
			return nil, err
		}
		s.total += stat.Size
		c, ok := b.(*blob.Compressed)
		if !ok {
			// we can only really do interesting
			// splitting stuff with blob.Compressed
			if err := insert(b); err != nil {
				return nil, err
			}
			s.maxscan += stat.Size
			continue
		}
		ret, err := c.Split(int(splitSize))
		if err != nil {
			return nil, err
		}
		if flt != nil {
			// only insert blobs that satisfy
			// the predicate pushdown conditions
			scan := int64(0)
			for _, b := range ret.Contents {
				b, scan = filterBlob(b, flt, scan)
				if b == nil {
					continue
				}
				if err := insert(b); err != nil {
					return nil, err
				}
			}
			s.maxscan += scan
			continue
		}
		s.maxscan += stat.Size
		for _, b := range ret.Contents {
			if err := insert(b); err != nil {
				return nil, err
			}
		}
	}

	subtables := make([]plan.Subtable, 0, len(nodeBlobLists))
	for nodeID, blobList := range nodeBlobLists {
		bind := table.Binding
		bind.As(fmt.Sprintf("part.%d", len(subtables)))
		subtable := plan.Subtable{
			Transport: s.transport(nodeID),
			Table: &expr.Table{
				Binding: bind,
				Value:   blobList,
			},
		}
		subtables = append(subtables, subtable)
	}
	return subtables, nil
}

// filterBlob applies a filter to a blob. If the entire
// blob is excluded by the filter, this returns (nil, 0);
// otherwise, it returns the filtered blob and the number
// of bytes to be scanned in the blob
func filterBlob(b blob.Interface, f filter, size int64) (blob.Interface, int64) {
	c, ok := b.(*blob.Compressed)
	if !ok {
		return b, size
	}
	t := c.Trailer
	self := int64(0)
	any := false
	for i := range t.Blocks {
		ranges := t.Blocks[i].Ranges
		self += int64(t.Blocks[i].Chunks) << t.BlockShift
		if len(ranges) == 0 || f(ranges) != never {
			any = true
		}
	}
	if !any {
		return nil, size
	}
	return c, size + self
}

func (s *splitter) partition(b blob.Interface) (*net.TCPAddr, error) {
	info, err := b.Stat()
	if err != nil {
		return nil, err
	}

	// just two fixed random values
	key0 := uint64(0x5d1ec810)
	key1 := uint64(0xfebed702)

	hashBlob := siphash.Hash(key0, key1, []byte(info.ETag))
	maxUint64 := ^uint64(0)
	endPoint := s.peers[hashBlob/(maxUint64/uint64(len(s.peers)))]
	return endPoint, nil
}

func (s *splitter) transport(nodeID string) plan.Transport {
	if nodeID == s.selfAddr {
		return &plan.LocalTransport{}
	}

	return &tnproto.Remote{
		Tenant: s.workerID,
		Net:    "tcp",
		Addr:   nodeID,
	}
}
