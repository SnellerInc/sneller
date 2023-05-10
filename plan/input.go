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

package plan

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"

	"github.com/dchest/siphash"
	"golang.org/x/exp/slices"
)

// Input represents a collection of input objects.
// The zero value of [Input] represents an empty table.
type Input struct {
	// Descs is the list of referenced descriptors in the input.
	//
	// The query planner uses the contents of Descs[*].Trailer
	// for the purposes of query planning, but otherwise leaves
	// the semantics of Descs[*].{Path,ETag,...} up to the
	// [Runner] used to execute the query.
	Descs []blockfmt.Descriptor

	// NOTE for devs: we expect *roughly* one Block per 50-100MB
	// of decompressed input data, so a 1TB table would
	// have on the order of 10,000 Blocks.
	// When you are manipulating this data structure,
	// please be sure to use algorithms that run in linear
	// time with respect to the number of blocks!

	// Blocks indicates the list of blocks
	// within Descs that are actually referenced.
	// Each Block references
	//
	//   Input.Descs[Block.Index].Trailer.Blocks[Block.Offset]
	//
	Blocks []blockfmt.Block

	// Fields are the fields in the input that are
	// needed by the query. If nil, all fields are
	// assumed to be needed. To specify zero
	// fields, use a non-nil slice of length 0.
	Fields []string

	// cached result of call to Partitions,
	// since it is relatively expensive to compute
	groupcache struct {
		lock  sync.Mutex
		parts []string
		final *InputGroups
	}
}

type ipart struct {
	in     *Input
	values []ion.Datum
}

// InputGroups is a set of [Input]s partitioned
// along a particular axis. InputGroups can be queried
// efficiently for a particular group with [Get] or it
// can be used to iterate all the groups individually with [Each].
type InputGroups struct {
	fields []string
	groups map[string]*ipart
}

// Groups returns the number of unique groups in [i].
func (i *InputGroups) Groups() int { return len(i.groups) }

// Fields returns the list of fields over which the input has been grouped.
func (i *InputGroups) Fields() []string { return i.fields }

// Each calls [fn] on each unique group.
// The [parts] slice indicates the constants that are
// associated with [i.Fields] for each Input [i].
func (i *InputGroups) Each(fn func(parts []ion.Datum, i *Input)) {
	for _, v := range i.groups {
		fn(v.values, v.in)
	}
}

// Get returns the [Input] associated with the given partition.
// Get will return nil if there is no data associated with
// the given partition constraints.
func (i *InputGroups) Get(equal []ion.Datum) *Input {
	var st ion.Symtab
	var buf ion.Buffer
	for i := range equal {
		equal[i].Encode(&buf, &st)
	}
	c := i.groups[string(buf.Bytes())]
	if c != nil {
		return c.in
	}
	return nil
}

// Empty is equivalent to
//
//	len(i.Blocks) == 0
func (i *Input) Empty() bool { return len(i.Blocks) == 0 }

// Clone produces a deep copy of [i].
func (i *Input) Clone() Input {
	return Input{
		Descs:  slices.Clone(i.Descs),
		Blocks: slices.Clone(i.Blocks),
	}
}

// Filter returns a copy of [i] that contains only
// the blocks for which [e] may evaluate to TRUE.
func (i *Input) Filter(e expr.Node) *Input {
	var f blockfmt.Filter
	f.Compile(e)
	ret := &Input{
		Fields: i.Fields,
	}

	// sort blocks so we can compact descriptors during visiting
	slices.SortFunc(i.Blocks, func(a, b blockfmt.Block) bool {
		if a.Index == b.Index {
			return a.Offset < b.Offset
		}
		return a.Index < b.Index
	})

	// TODO: do this in a more efficient way
	keep := func(idx, start, end int) {
		for j := range i.Blocks {
			b := &i.Blocks[j]
			if b.Index > idx {
				return
			}
			if b.Index == idx && start <= b.Offset && b.Offset < end {
				ret.Blocks = append(ret.Blocks, blockfmt.Block{
					Index:  idx,
					Offset: b.Offset,
				})
			}
		}
	}

	// for each descriptor, copy out only the matching blocks,
	// taking care not to copy over descriptors with zero matching blocks
	for j := range i.Descs {
		f.Visit(&i.Descs[j].Trailer.Sparse, func(start, end int) {
			d := len(ret.Descs)
			if d > 0 && ret.Descs[d-1].ObjectInfo == i.Descs[j].ObjectInfo {
				d--
			} else if start < end {
				ret.Descs = append(ret.Descs, i.Descs[j])
			}
			keep(j, start, end)
		})
	}
	return ret
}

// Size returns the decompressed size of all
// the data referenced by [i].
func (i *Input) Size() int64 {
	s := int64(0)
	for _, blk := range i.Blocks {
		d := &i.Descs[blk.Index]
		s += int64(d.Trailer.Blocks[blk.Offset].Chunks) << d.Trailer.BlockShift
	}
	return s
}

// CanPartition indicates whether a call to Partition
// including the given part would be successful.
func (i *Input) CanPartition(part string) bool {
	for j := range i.Descs {
		if _, ok := i.Descs[j].Trailer.Sparse.Const(part); !ok {
			return false
		}
	}
	return true
}

// Partition clusters the blocks in [i] by the given partitions.
func (i *Input) Partition(parts []string) (*InputGroups, bool) {
	i.groupcache.lock.Lock()
	defer i.groupcache.lock.Unlock()
	if slices.Equal(i.groupcache.parts, parts) {
		return i.groupcache.final, true
	}
	val, ok := i.partition(parts)
	if ok {
		i.groupcache.parts = slices.Clone(parts)
		i.groupcache.final = val
		return val, true
	}
	return nil, false
}

func (i *Input) partition(parts []string) (*InputGroups, bool) {
	sets := make(map[string]*ipart)
	desc2part := make([]*ipart, len(i.Descs))
	descmap := make([]int, len(i.Descs))
	var raw ion.Bag

	// first, map descriptors to their partitions
	for j := range i.Descs {
		raw.Reset()
		for k := range parts {
			dat, ok := i.Descs[j].Trailer.Sparse.Const(parts[k])
			if !ok {
				return nil, false
			}
			raw.AddDatum(dat)
		}
		key := string(raw.Raw())
		c := sets[key]
		if c == nil {
			c = new(ipart)
			raw.Each(func(d ion.Datum) bool {
				d = d.Clone() // don't alias bag memory
				c.values = append(c.values, d)
				return true
			})
			c.in = &Input{}
			sets[key] = c
		}
		desc2part[j] = c
		// track new descriptor positions within their groups:
		descmap[j] = len(c.in.Descs)
		c.in.Descs = append(c.in.Descs, i.Descs[j])
	}

	// map blocks to their partitions based upon
	// the partitions to which the descriptors were assigned
	for j := range i.Blocks {
		c := desc2part[i.Blocks[j].Index]
		if c != nil {
			c.in.Blocks = append(c.in.Blocks, blockfmt.Block{
				Index:  descmap[i.Blocks[j].Index],
				Offset: i.Blocks[j].Offset, // unchanged
			})
		}
	}
	return &InputGroups{
		fields: parts,
		groups: sets,
	}, true
}

// HashSplit splits the input [i] into [n] groups deterministically
// based on the ETags within [i.Descs].
func (i *Input) HashSplit(n int) []*Input {
	const (
		k0    = 0x5d1ec810febed702
		k1    = 0x40fd7fee17262f71
		clamp = ^uint64(0)
	)

	ret := make([]*Input, n)

	// order blocks by descriptor so that they are easy to deduplicate
	// as we copy them into the appropriate output
	slices.SortFunc(i.Blocks, func(a, b blockfmt.Block) bool {
		if a.Index == b.Index {
			return a.Offset < b.Offset
		}
		return a.Index < b.Index
	})

	var tmp []byte
	for j := range i.Blocks {
		k := i.Blocks[j].Index
		tmp = append(tmp[:0], i.Descs[k].ETag...)
		tmp = binary.LittleEndian.AppendUint32(tmp, uint32(i.Blocks[j].Offset))
		h := siphash.Hash(k0, k1, tmp)
		n := int(h / (clamp / uint64(n)))
		if ret[n] == nil {
			ret[n] = &Input{}
		}
		// the descriptor index for the block will either
		// be the index of the new descriptor or the most-recently-inserted one,
		// since we sorted all the blocks by offset up front
		index := len(ret[n].Descs)
		if index > 0 && ret[n].Descs[index-1].ObjectInfo == i.Descs[k].ObjectInfo {
			index--
		} else {
			ret[n].Descs = append(ret[n].Descs, i.Descs[k])
		}
		ret[n].Blocks = append(ret[n].Blocks, blockfmt.Block{
			Index:  index,
			Offset: i.Blocks[j].Offset,
		})
	}
	return ret
}

// Append appends the contents of [other] to [i].
func (i *Input) Append(other *Input) {
	delta := len(i.Descs)
	i.Descs = append(i.Descs, other.Descs...)

	start := len(i.Blocks)
	i.Blocks = append(i.Blocks, other.Blocks...)
	tail := i.Blocks[start:]
	for j := range tail {
		tail[j].Index += delta
	}
}

func (i *Input) encode(dst *ion.Buffer, st *ion.Symtab) {
	// TODO: compress very large Input lists
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("descs"))
	blockfmt.WriteDescriptors(dst, st, i.Descs)
	dst.BeginField(st.Intern("blocks"))
	dst.BeginList(-1)
	indexsym := st.Intern("index")
	offsetsym := st.Intern("offset")
	for j := range i.Blocks {
		dst.BeginStruct(-1)
		dst.BeginField(indexsym)
		dst.WriteInt(int64(i.Blocks[j].Index))
		dst.BeginField(offsetsym)
		dst.WriteInt(int64(i.Blocks[j].Offset))
		dst.EndStruct()
	}
	dst.EndList()
	// NOTE: there is a meaningful difference
	// between nil and empty i.Fields...
	if i.Fields != nil {
		dst.BeginField(st.Intern("fields"))
		dst.BeginList(-1)
		for j := range i.Fields {
			dst.WriteString(i.Fields[j])
		}
		dst.EndList()
	}
	dst.EndStruct()
}

func (i *Input) decode(v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "descs":
			descs, err := blockfmt.ReadDescriptors(f.Datum)
			if err != nil {
				return err
			}
			i.Descs = descs
			return nil
		case "blocks":
			return f.UnpackList(func(item ion.Datum) error {
				var block blockfmt.Block
				err := item.UnpackStruct(func(f ion.Field) error {
					var n int64
					var err error
					switch f.Label {
					case "index":
						n, err = f.Int()
						block.Index = int(n)
					case "offset":
						n, err = f.Int()
						block.Offset = int(n)
					default:
						err = errUnexpectedField
					}
					return err
				})
				if err == nil {
					i.Blocks = append(i.Blocks, block)
				}
				return err
			})
		case "fields":
			if f.IsNull() {
				i.Fields = nil
				return nil
			}
			i.Fields = []string{}
			return f.UnpackList(func(v ion.Datum) error {
				s, err := v.String()
				if err != nil {
					return err
				}
				i.Fields = append(i.Fields, s)
				return nil
			})
		default:
			return errUnexpectedField
		}
	})
	if err != nil {
		return fmt.Errorf("plan.Decode: %w", err)
	}
	return err
}
