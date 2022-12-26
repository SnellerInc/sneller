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
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
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

	// MaxScan is the computed maximum bytes
	// scanned after sparse indexing has been
	// applied.
	MaxScan uint64
}

func (s *Splitter) Split(table expr.Node, handle plan.TableHandle) (plan.Subtables, error) {
	var blobs []blob.Interface
	fh, ok := handle.(*FilterHandle)
	if !ok {
		return nil, fmt.Errorf("cannot split table handle of type %T", handle)
	}
	size := s.SplitSize
	if s.SplitSize == 0 {
		size = DefaultSplitSize
	}
	flt, _ := fh.CompileFilter()
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
		stat, err := b.Stat()
		if err != nil {
			return nil, err
		}
		c, ok := b.(*blob.Compressed)
		if !ok {
			// we can only really do interesting
			// splitting stuff with blob.Compressed
			if err := insert(b); err != nil {
				return nil, err
			}
			s.MaxScan += uint64(stat.Size)
			continue
		}
		sub, err := c.Split(int(size))
		if err != nil {
			return nil, err
		}
		sub = stripsub(&c.Trailer, sub, flt)
		for i := range sub {
			s.MaxScan += uint64(sub[i].Decompressed())
			if err := insert(&sub[i]); err != nil {
				return nil, err
			}
		}
	}
	return &Subtables{
		splits:    compact(splits),
		table:     table,
		blobs:     blobs,
		fields:    fh.Fields,
		allFields: fh.AllFields,
		filter:    nil, // pushed down later
		fn:        blobsToHandle,
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

func stripsub(t *blockfmt.Trailer, lst []blob.CompressedPart, f *blockfmt.Filter) []blob.CompressedPart {
	if f == nil || f.Trivial() {
		return lst
	}
	out := lst[:0]
	for i := range lst {
		if f.Overlaps(&t.Sparse, lst[i].StartBlock, lst[i].EndBlock) {
			out = append(out, lst[i])
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

// encode as [tp, blobs]
func (s *split) encode(st *ion.Symtab, buf *ion.Buffer) error {
	buf.BeginList(-1)
	if err := plan.EncodeTransport(s.tp, st, buf); err != nil {
		return err
	}
	buf.BeginList(-1)
	for i := range s.blobs {
		buf.WriteInt(int64(s.blobs[i]))
	}
	buf.EndList()
	buf.EndList()
	return nil
}

func decodeSplit(st *ion.Symtab, body []byte) (split, error) {
	var s split
	if ion.TypeOf(body) != ion.ListType {
		return s, fmt.Errorf("expected a list; found ion type %s", ion.TypeOf(body))
	}
	body, _ = ion.Contents(body)
	if body == nil {
		return s, fmt.Errorf("invalid list encoding")
	}
	var err error
	s.tp, err = plan.DecodeTransport(st, body)
	if err != nil {
		return s, err
	}
	body = body[ion.SizeOf(body):]
	_, err = ion.UnpackList(body, func(body []byte) error {
		n, _, err := ion.ReadInt(body)
		if err != nil {
			return err
		}
		s.blobs = append(s.blobs, int(n))
		return nil
	})
	return s, err
}

// A tableHandleFn is used to produce a TableHandle
// from a list of blobs and a filter.
type tableHandleFn func(blobs []blob.Interface, h *plan.Hints) plan.TableHandle

// Subtables is the plan.Subtables implementation
// returned by (*splitter).Split.
type Subtables struct {
	splits []split
	table  expr.Node
	blobs  []blob.Interface

	// from plan.Hints:
	filter    expr.Node
	fields    []string
	allFields bool

	next *Subtables // set if combined

	// fn is called to produce the TableHandles
	// embedded in the subtables
	fn tableHandleFn
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
	name := fmt.Sprintf("part.%d", i)
	table := &expr.Table{
		Binding: expr.Bind(s.table, name),
	}
	blobs := make([]blob.Interface, len(sp.blobs))
	for i, bi := range sp.blobs {
		blobs[i] = s.blobs[bi]
	}
	hint := plan.Hints{
		Filter:    s.filter,
		Fields:    s.fields,
		AllFields: s.allFields,
	}
	*sub = plan.Subtable{
		Transport: sp.tp,
		Table:     table,
		Handle:    s.fn(blobs, &hint),
	}
}

func blobsToHandle(blobs []blob.Interface, hints *plan.Hints) plan.TableHandle {
	return &FilterHandle{
		Blobs:     &blob.List{Contents: blobs},
		Fields:    hints.Fields,
		AllFields: hints.AllFields,
		Expr:      hints.Filter,
	}
}

// Encode implements plan.Subtables.Encode.
func (s *Subtables) Encode(st *ion.Symtab, dst *ion.Buffer) error {
	// encode as [splits, table, blobs, filter, fields, next]
	dst.BeginList(-1)
	dst.BeginList(-1)
	for i := range s.splits {
		if err := s.splits[i].encode(st, dst); err != nil {
			return err
		}
	}
	dst.EndList()
	s.table.Encode(dst, st)
	lst := blob.List{Contents: s.blobs}
	lst.Encode(dst, st)
	if s.filter == nil {
		dst.WriteNull()
	} else {
		s.filter.Encode(dst, st)
	}
	// we write null for allFields and [] for zero fields
	if s.allFields {
		dst.WriteNull()
	} else {
		dst.BeginList(-1)
		for i := range s.fields {
			dst.WriteString(s.fields[i])
		}
		dst.EndList()
	}
	if s.next == nil {
		dst.WriteNull()
	} else if err := s.next.Encode(st, dst); err != nil {
		return err
	}
	dst.EndList()
	return nil
}

func DecodeSubtables(st *ion.Symtab, body []byte, fn tableHandleFn) (*Subtables, error) {
	if ion.TypeOf(body) != ion.ListType {
		return nil, fmt.Errorf("expected a list; found ion type %s", ion.TypeOf(body))
	}
	body, _ = ion.Contents(body)
	if body == nil {
		return nil, fmt.Errorf("invalid list encoding")
	}
	s := &Subtables{fn: fn}
	body, err := ion.UnpackList(body, func(body []byte) error {
		sp, err := decodeSplit(st, body)
		if err != nil {
			return err
		}
		s.splits = append(s.splits, sp)
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.table, body, err = expr.Decode(st, body)
	if err != nil {
		return nil, err
	}
	lst, err := blob.DecodeList(st, body)
	if err != nil {
		return nil, err
	}
	s.blobs = lst.Contents
	body = body[ion.SizeOf(body):]
	if ion.TypeOf(body) != ion.NullType {
		s.filter, body, err = expr.Decode(st, body)
		if err != nil {
			return nil, err
		}
	} else {
		body = body[ion.SizeOf(body):]
	}
	if ion.TypeOf(body) != ion.NullType {
		s.allFields = false
		body, err = ion.UnpackList(body, func(field []byte) error {
			var str string
			str, _, err = ion.ReadString(field)
			if err != nil {
				return err
			}
			s.fields = append(s.fields, str)
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		s.allFields = true
		body = body[ion.SizeOf(body):]
	}
	if ion.TypeOf(body) != ion.NullType {
		s.next, err = DecodeSubtables(st, body, fn)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
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
