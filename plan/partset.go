// Copyright (C) 2023 Sneller, Inc.
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
	"github.com/SnellerInc/sneller/ion"
)

type cluster[T any] struct {
	values []ion.Datum
	items  []T
}

// PartGroups contains a list of values that are
// grouped by sets of ion datum constants.
// PartGroups is constructed by the [Partition] function.
type PartGroups[T any] struct {
	fields []string
	groups map[string]*cluster[T]
}

// Groups returns the number of distinct groups in the set.
func (p *PartGroups[T]) Groups() int { return len(p.groups) }

// Fields returns the ordered list of named fields
// used to group the associated values.
func (p *PartGroups[T]) Fields() []string { return p.fields }

// Each iterates all the groups within p and calls fn for each group.
// Each parts[i] corresponds to fields[i] returned from [p.Fields].
func (p *PartGroups[T]) Each(fn func(parts []ion.Datum, group []T)) {
	for _, v := range p.groups {
		fn(v.values, v.items)
	}
}

// Get returns the values associated with the partition
// for which parts[i]==equal[i] for each part label
// given by p.Fields. The result of Get is unspecified
// if len(equal) is not equal to len(p.Fields())
func (p *PartGroups[T]) Get(equal []ion.Datum) []T {
	var st ion.Symtab
	var buf ion.Buffer
	for i := range equal {
		equal[i].Encode(&buf, &st)
	}
	c := p.groups[string(buf.Bytes())]
	if c != nil {
		return c.items
	}
	return nil
}

// Partition groups a list of items by associated constants.
//
// This function is often helpful for implementing PartitionHandle.SplitBy.
func Partition[T any](lst []T, parts []string, getconst func(T, string) (ion.Datum, bool)) (*PartGroups[T], bool) {
	sets := make(map[string]*cluster[T])
	var raw ion.Bag
	for i := range lst {
		raw.Reset()
		for j := range parts {
			dat, ok := getconst(lst[i], parts[j])
			if !ok {
				return nil, false
			}
			raw.AddDatum(dat)
		}
		key := string(raw.Raw())
		c := sets[key]
		if c == nil {
			c = new(cluster[T])
			raw.Each(func(d ion.Datum) bool {
				d = d.Clone() // don't alias bag memory
				c.values = append(c.values, d)
				return true
			})
			sets[key] = c
		}
		c.items = append(c.items, lst[i])
	}
	return &PartGroups[T]{
		fields: parts,
		groups: sets,
	}, true
}
