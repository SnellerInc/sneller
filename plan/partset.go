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
