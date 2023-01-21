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

package ion

import (
	"io"

	"golang.org/x/exp/slices"
)

// Bag is a (possibly empty) sequence of ion datums.
// A bag is stored efficiently in memory so as to reduce
// the CPU and memory footprint of the constituent data elements.
type Bag struct {
	st    Symtab
	data  []byte
	items int
}

func (b *Bag) Reset() {
	b.st.Reset()
	b.data = b.data[:0]
	b.items = 0
}

// Each iterates each item in the bag
// and calls fn() on it. If fn returns false,
// then Each returns early.
func (b *Bag) Each(fn func(d Datum) bool) {
	d := b.data
	for len(d) > 0 {
		size := SizeOf(d)
		if !fn(Datum{
			st:  b.st.interned,
			buf: d[:size],
		}) {
			return
		}
		d = d[size:]
	}
}

// Equals compares two bags and returns true
// if they are equivalent.
//
// Note that Equals is sensitive to the order of
// items in each Bag.
func (b *Bag) Equals(o *Bag) bool {
	if b.items != o.items {
		return false
	}
	srcdata := o.data
	eq := true
	b.Each(func(d Datum) bool {
		if len(srcdata) == 0 {
			eq = false
			return false
		}
		s := SizeOf(srcdata)
		rhs := Datum{st: o.st.interned, buf: srcdata[:s]}
		eq = d.Equal(rhs)
		srcdata = srcdata[s:]
		return eq
	})
	return eq && len(srcdata) == 0
}

// Clone creates a deep copy of the Bag.
func (b *Bag) Clone() Bag {
	ret := Bag{
		data:  slices.Clone(b.data),
		items: b.items,
	}
	b.st.CloneInto(&ret.st)
	return ret
}

// Transcoder returns a function that can be used to efficiently
// transcode datums from within the bag to a different symbol table.
// The returned function is only valid for use with Datum objects
// returned from b.Each.
func (b *Bag) Transcoder(st *Symtab) func(dst *Buffer, src Datum) {
	rs := &resymbolizer{
		srctab: &b.st,
		dsttab: st,
		expand: true, // un-intern strings
	}
	return func(dst *Buffer, src Datum) {
		rs.resym(dst, src.buf)
	}
}

// Len returns the number of items in the bag.
func (b *Bag) Len() int { return b.items }

// Size returns the *approximate* size of the bag in memory.
func (b *Bag) Size() int { return b.st.memsize + len(b.data) }

type bagWriter struct {
	srctab Symtab
	dst    *Bag
}

func (b *bagWriter) Write(p []byte) (int, error) {
	n := len(p)
	var err error
	if IsBVM(p) || TypeOf(p) == AnnotationType {
		p, err = b.srctab.Unmarshal(p)
		if err != nil {
			return 0, err
		}
	}
	err = b.dst.Add(&b.srctab, p)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Writer returns an io.Writer that can be used
// to write data directly into the bag.
func (b *Bag) Writer() io.Writer {
	return &bagWriter{dst: b}
}

// Add adds zero or more raw ion datums from a buffer
// and an associated symbol table.
func (b *Bag) Add(st *Symtab, raw []byte) error {
	if b.st.Contains(st) {
		b.data = append(b.data, raw...)
		return nil
	}
	var tmp Buffer
	tmp.Set(b.data)
	rs := resymbolizer{
		srctab: st,
		dsttab: &b.st,
	}
	for len(raw) > 0 {
		raw = rs.resym(&tmp, raw)
		b.items++
	}
	b.data = tmp.Bytes()
	return nil
}

// AddDatum adds a single datum to the bag.
func (b *Bag) AddDatum(d Datum) {
	if d.IsEmpty() {
		return
	}
	if b.st.contains(d.st) {
		b.data = append(b.data, d.buf...)
		b.items++
		return
	}
	var tmp Buffer
	tmp.Set(b.data)
	d.Encode(&tmp, &b.st)
	b.data = tmp.Bytes()
	b.items++
}

// Encode encodes the contents of the bag
// to dst using the symbol table st.
func (b *Bag) Encode(dst *Buffer, st *Symtab) {
	rs := resymbolizer{
		srctab: &b.st,
		dsttab: st,
	}
	d := b.data
	for len(d) > 0 {
		d = rs.resym(dst, d)
	}
}

// Append appends the contents of src to b.
func (b *Bag) Append(src *Bag) {
	if b.items == 0 {
		*b = src.Clone()
		return
	}
	rs := resymbolizer{
		srctab: &src.st,
		dsttab: &b.st,
	}
	var tmp Buffer
	tmp.Set(b.data)
	d := src.data
	for len(d) > 0 {
		d = rs.resym(&tmp, d)
	}
	b.data = tmp.Bytes()
	b.items += src.items
}
