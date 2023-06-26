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

// A Descriptor describes a single input object.
type Descriptor struct {
	// Descriptor is the input object descriptor.
	//
	// The query planner uses the contents of Descriptor.Trailer
	// for the purposes of query planning, but otherwise leaves
	// the semantics of Descriptor.{Path,ETag,...} up to the
	// [Runner] used to execute the query.
	blockfmt.Descriptor

	// NOTE for devs: we expect *roughly* one
	// Block per 50-100MB of decompressed input
	// data, so a 1TB table would have on the
	// order of 10,000 Blocks. When you are
	// manipulating this data structure, please be
	// sure to use algorithms that run in linear
	// time with respect to the number of blocks!

	// TODO: we could use a more compact data
	// structure for representing this information

	// Blocks indicates the list of blocks within
	// the object that are actually referenced.
	Blocks []int
}

// Empty is equivalent to
//
//	len(d.Blocks) == 0
func (d *Descriptor) Empty() bool { return len(d.Blocks) == 0 }

// Input represents a collection of input objects.
// The zero value of [Input] represents an empty table.
type Input struct {
	// Descs is the list of referenced descriptors
	// in the input.
	Descs []Descriptor

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

// Groups returns the number of unique groups in [in].
func (in *InputGroups) Groups() int { return len(in.groups) }

// Fields returns the list of fields over which the input has been grouped.
func (in *InputGroups) Fields() []string { return in.fields }

// Each calls [fn] on each unique group.
// The [parts] slice indicates the constants that are
// associated with [i.Fields] for each Input [in].
func (in *InputGroups) Each(fn func(parts []ion.Datum, i *Input)) {
	for _, v := range in.groups {
		fn(v.values, v.in)
	}
}

// Get returns the [Input] associated with the given partition.
// Get will return nil if there is no data associated with
// the given partition constraints.
func (in *InputGroups) Get(equal []ion.Datum) *Input {
	var st ion.Symtab
	var buf ion.Buffer
	for i := range equal {
		equal[i].Encode(&buf, &st)
	}
	c := in.groups[string(buf.Bytes())]
	if c != nil {
		return c.in
	}
	return nil
}

// Blocks returns the number of blocks across
// all inputs.
func (in *Input) Blocks() int {
	n := 0
	for i := range in.Descs {
		n += len(in.Descs[i].Blocks)
	}
	return n
}

// Empty returns whether the inputs are all empty.
func (in *Input) Empty() bool {
	for i := range in.Descs {
		if !in.Descs[i].Empty() {
			return false
		}
	}
	return true
}

// Filter returns an equivalent of [in] which
// contains only the blocks for which [e] may
// evaluate to TRUE. This will return a distinct
// object if [e] would exclude any of the blocks
// referenced by [in], but may simply return [in]
// if the filtered result would be identical.
// This method will not mutate [in].
func (in *Input) Filter(e expr.Node) *Input {
	var f blockfmt.Filter
	f.Compile(e)
	if f.Trivial() {
		return in
	}
	// TODO: we can avoid making a copy of i if f
	// would not change the result which may
	// actually be the common case; for now just
	// make a copy because it's easier that way
	ret := &Input{
		Descs:  make([]Descriptor, 0, len(in.Descs)),
		Fields: in.Fields,
	}
	// for each input, copy out only the matching
	// blocks, taking care not to copy over
	// descriptors with zero matching blocks
	for i := range in.Descs {
		ret.Descs = in.Descs[i].appendFiltered(ret.Descs, &f)
	}
	return ret
}

// appendFiltered filters [d] using [f] and
// appends it to [to], returning the appended
// list. If [f] excludes [d] completely, [to] is
// returned unchanged.
func (d *Descriptor) appendFiltered(to []Descriptor, f *blockfmt.Filter) []Descriptor {
	var blocks []int
	f.Visit(&d.Trailer.Sparse, func(start, end int) {
		// TODO: do this in a more efficient way
		for i := range d.Blocks {
			if start <= d.Blocks[i] && d.Blocks[i] < end {
				blocks = append(blocks, d.Blocks[i])
			}
		}
	})
	if len(blocks) > 0 {
		to = append(to, Descriptor{
			Descriptor: d.Descriptor,
			Blocks:     blocks,
		})
	}
	return to
}

// CompressedSize returns the number of compressed
// bytes that comprise all of the input blocks.
func (in *Input) CompressedSize() (n int64) {
	for i := range in.Descs {
		n += in.Descs[i].CompressedSize()
	}
	return n
}

// CompressedSize returns the number of compressed
// bytes that comprise all of the input blocks.
func (d *Descriptor) CompressedSize() (n int64) {
	for _, i := range d.Blocks {
		n += d.Trailer.BlockSize(i)
	}
	return n
}

// Size returns the decompressed size of all
// the data referenced by [in].
func (in *Input) Size() (n int64) {
	for i := range in.Descs {
		n += in.Descs[i].Size()
	}
	return n
}

// Size returns the decompressed size of all
// the data referenced by [d].
func (d *Descriptor) Size() (n int64) {
	for _, i := range d.Blocks {
		n += int64(d.Trailer.Blocks[i].Chunks) << d.Trailer.BlockShift
	}
	return n
}

// CanPartition indicates whether a call to Partition
// including the given part would be successful.
func (in *Input) CanPartition(part string) bool {
	for i := range in.Descs {
		if _, ok := in.Descs[i].Trailer.Sparse.Const(part); !ok {
			return false
		}
	}
	return true
}

// Partition clusters the blocks in [in] by the given partitions.
func (in *Input) Partition(parts []string) (*InputGroups, bool) {
	in.groupcache.lock.Lock()
	defer in.groupcache.lock.Unlock()
	if slices.Equal(in.groupcache.parts, parts) {
		return in.groupcache.final, true
	}
	val, ok := in.partition(parts)
	if ok {
		in.groupcache.parts = slices.Clone(parts)
		in.groupcache.final = val
		return val, true
	}
	return nil, false
}

func (in *Input) partition(parts []string) (*InputGroups, bool) {
	sets := make(map[string]*ipart)
	desc2part := make([]*ipart, len(in.Descs))
	descmap := make([]int, len(in.Descs))
	var raw ion.Bag

	// first, map descriptors to their partitions
	for i := range in.Descs {
		raw.Reset()
		for k := range parts {
			dat, ok := in.Descs[i].Trailer.Sparse.Const(parts[k])
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
		desc2part[i] = c
		// track new descriptor positions within their groups:
		descmap[i] = len(c.in.Descs)
		c.in.Descs = append(c.in.Descs, in.Descs[i])
	}

	// map blocks to their partitions based upon
	// the partitions to which the descriptors were assigned
	for i := range in.Descs {
		c := desc2part[i]
		if c != nil {
			c.in.Descs[descmap[i]].Blocks = slices.Clone(in.Descs[i].Blocks)
		}
	}
	return &InputGroups{
		fields: parts,
		groups: sets,
	}, true
}

// HashSplit splits the input [in] into [n]
// groups deterministically based on the ETags
// within [in.Descs].
//
// The resulting slice may contain nil pointers
// if no blocks were assigned to that slot.
func (in *Input) HashSplit(n int) []*Input {
	const (
		k0    = 0x5d1ec810febed702
		k1    = 0x40fd7fee17262f71
		clamp = ^uint64(0)
	)

	ret := make([]*Input, n)

	var tmp []byte
	for i := range in.Descs {
		tmp = append(tmp[:0], in.Descs[i].ETag...)
		cut := len(tmp)
		for _, off := range in.Descs[i].Blocks {
			tmp = binary.LittleEndian.AppendUint32(tmp[:cut], uint32(off))
			h := siphash.Hash(k0, k1, tmp)
			n := int(h / (clamp / uint64(n)))
			if ret[n] == nil {
				ret[n] = &Input{
					Descs:  make([]Descriptor, len(in.Descs)),
					Fields: in.Fields,
				}
			}
			if ret[n].Descs[i].Empty() {
				ret[n].Descs[i].Descriptor = in.Descs[i].Descriptor
			}
			ret[n].Descs[i].Blocks = append(ret[n].Descs[i].Blocks, off)
		}
	}
	for i := range ret {
		if ret[i] == nil {
			continue
		}
		all := ret[i].Descs
		ret[i].Descs = ret[i].Descs[:0]
		for j := range all {
			if !all[j].Empty() {
				ret[i].Descs = append(ret[i].Descs, all[j])
			}
		}
	}
	return ret
}

// Append appends the contents of [other] to [in].
func (in *Input) Append(other *Input) {
	end := len(in.Descs)
	in.Descs = append(in.Descs, other.Descs...)
	for i := end; i < len(in.Descs); i++ {
		in.Descs[i].Blocks = slices.Clone(in.Descs[i].Blocks)
	}
}

func (in *Input) Encode(dst *ion.Buffer, st *ion.Symtab) {
	// TODO: compress very large Input lists
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("descs"))
	dst.BeginList(-1)
	for i := range in.Descs {
		in.Descs[i].Encode(dst, st)
	}
	dst.EndList()
	// NOTE: there is a meaningful difference
	// between nil and empty i.Fields...
	if in.Fields != nil {
		dst.BeginField(st.Intern("fields"))
		dst.BeginList(-1)
		for i := range in.Fields {
			dst.WriteString(in.Fields[i])
		}
		dst.EndList()
	}
	dst.EndStruct()
}

func (d *Descriptor) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("descriptor"))
	d.Descriptor.Encode(dst, st)
	dst.BeginField(st.Intern("blocks"))
	dst.BeginList(-1)
	for _, off := range d.Blocks {
		dst.WriteInt(int64(off))
	}
	dst.EndList()
	dst.EndStruct()
}

func (in *Input) decode(v ion.Datum) error {
	err := v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "descs":
			var td blockfmt.TrailerDecoder
			in.Descs = in.Descs[:0]
			return f.UnpackList(func(v ion.Datum) error {
				in.Descs = append(in.Descs, Descriptor{})
				return in.Descs[len(in.Descs)-1].Decode(&td, v)
			})
		case "fields":
			if f.IsNull() {
				in.Fields = nil
				return nil
			}
			in.Fields = []string{}
			return f.UnpackList(func(v ion.Datum) error {
				s, err := v.String()
				if err != nil {
					return err
				}
				in.Fields = append(in.Fields, s)
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

func (d *Descriptor) Decode(td *blockfmt.TrailerDecoder, v ion.Datum) error {
	return v.UnpackStruct(func(f ion.Field) error {
		switch f.Label {
		case "descriptor":
			return d.Descriptor.Decode(td, f.Datum, blockfmt.FlagSkipInputs)
		case "blocks":
			d.Blocks = d.Blocks[:0]
			return f.UnpackList(func(v ion.Datum) error {
				n, err := v.Int()
				if err != nil {
					return err
				}
				d.Blocks = append(d.Blocks, int(n))
				return nil
			})
		default:
			return errUnexpectedField
		}
	})
}
