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

// Package versify implements an ion "versifier:"
// code that performs procedural data generation
// based on example input.
//
// The Union value represents many ion values
// that have been superimposed in one structural
// position as part of training the output generation.
// Union.Add superimposes a new training value on
// the existing state, and Union.Generate draws from
// the existing distribution of superimposed values
// to generate a new value.
package versify

import (
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// Union represents the superposition
// of multiple values.
type Union interface {
	// Add returns a new Union that
	// superimposes the value on the receiver
	Add(value ion.Datum) Union

	// Generate generates a new value based
	// on the template implied by the superposition
	// of values.
	Generate(src *rand.Rand) ion.Datum

	// String describes the union
	String() string
}

func merge(self Union, kind ion.Type, hits int, value ion.Datum) Union {
	any := &Any{}
	any.typemap[kind] = self
	any.hits[kind] = hits
	if value == nil {
		any.typemap[missingType] = &None{hits: 1}
		any.hits[missingType] = 1
	} else {
		t := value.Type()
		if t == ion.UintType {
			t = ion.IntType
		}
		any.typemap[t] = Single(value)
		any.hits[t] = 1
	}
	any.total = hits + 1
	return any
}

// None is the missing value.
type None struct {
	hits int
}

// Generate implements Union.Generate
//
// Generate always returns nil
func (n *None) Generate(src *rand.Rand) ion.Datum {
	return nil
}

// Add implments Union.Add
func (n *None) Add(value ion.Datum) Union {
	if value == nil {
		n.hits++
		return n
	}
	return merge(n, missingType, n.hits, value)
}

func (n *None) String() string {
	return "MISSING"
}

// Null is the null value.
type Null struct {
	hits int
}

// Generate implements Union.Generate
func (n *Null) Generate(src *rand.Rand) ion.Datum {
	return ion.UntypedNull{}
}

// Add implements Union.Add
func (n *Null) Add(value ion.Datum) Union {
	if value == (ion.UntypedNull{}) {
		n.hits++
		return n
	}
	return merge(n, ion.NullType, n.hits, value)
}

func (n *Null) String() string {
	return "NULL"
}

// Bool is a Generator that generates
// only boolean values.
type Bool struct {
	truecount, falsecount int
}

// FromBool returns a Bool Generator that
// draws from a distribution of one value.
func FromBool(value ion.Datum) *Bool {
	b := &Bool{}
	if value == ion.Bool(true) {
		b.truecount++
	} else {
		b.falsecount++
	}
	return b
}

func (b *Bool) String() string {
	truepct := (100 * float64(b.truecount)) / float64(b.truecount+b.falsecount)
	return fmt.Sprintf("bool[%02f true]", truepct)
}

// Generate generates a random boolean value.
// The likelihood that the boolean value is true
// is determined by sampling from the distribution
// of input values passed to Add.
func (b *Bool) Generate(src *rand.Rand) ion.Datum {
	total := b.truecount + b.falsecount
	return ion.Bool((rand.Intn(total) >= b.falsecount) == (b.falsecount < b.truecount))
}

// Add implements Union.Add
func (b *Bool) Add(value ion.Datum) Union {
	if value.Type() == ion.BoolType {
		if bool(value.(ion.Bool)) {
			b.truecount++
		} else {
			b.falsecount++
		}
		return b
	}
	return merge(b, ion.BoolType, b.truecount+b.falsecount, value)
}

// Integer is a Generator that
// generates integer values.
//
// Integer generates values that are
// uniformly distributed along the range
// of input values.
type Integer struct {
	lo, hi int64 // versification range
	hits   int
}

func (i *Integer) String() string {
	return fmt.Sprintf("integer[%d, %d]", i.lo, i.hi)
}

// FromInt64 returns an Integer that
// only generates i.
func FromInt64(i int64) *Integer {
	return &Integer{
		hi:   i,
		lo:   i,
		hits: 1,
	}
}

// FromUint64 returns an Integer
// that only generates u.
func FromUint64(u uint64) *Integer {
	return &Integer{
		hi:   int64(u),
		lo:   int64(u),
		hits: 1,
	}
}

// Generate generates a new random integer.
//
// The output integer is drawn from the
// uniform distribution along the open interval
// from the lowest integer passed to Add up to
// the highest.
func (i *Integer) Generate(src *rand.Rand) ion.Datum {
	distance := i.hi + 1 - i.lo
	if distance < 0 {
		// range has overflowed; pick any integer
		return ion.Int(int64(src.Uint64()))
	}
	return ion.Int(i.lo + src.Int63n(i.hi+1-i.lo))
}

// Add implements Union.Add.
func (i *Integer) Add(value ion.Datum) Union {
	if vi, ok := value.(ion.Int); ok {
		vii := int64(vi)
		if vii > i.hi {
			i.hi = vii
		}
		if vii < i.lo {
			i.lo = vii
		}
		i.hits++
		return i
	}
	if vu, ok := value.(ion.Uint); ok {
		vuu := uint64(vu)
		if int64(vuu) > i.hi {
			i.hi = int64(vuu)
		}
		if i.lo >= 0 && vuu < uint64(i.lo) {
			i.lo = int64(vuu)
		}
		i.hits++
		return i
	}
	return merge(i, ion.IntType, i.hits, value)
}

// Float is a Generator that returns
// floating point values.
type Float struct {
	lo, hi float64
	hits   int
}

// FromFloat returns a Float generator
// that always returns f.
func FromFloat(f float64) *Float {
	return &Float{
		lo:   f,
		hi:   f,
		hits: 1,
	}
}

func (f *Float) String() string {
	return fmt.Sprintf("float[%g, %g]", f.lo, f.hi)
}

// Generate generates a new floating point value
// drawn uniformly from the open interval from
// the lowest value passed to Add up to the highest.
func (f *Float) Generate(src *rand.Rand) ion.Datum {
	val := (src.Float64() * (f.hi - f.lo)) + f.lo
	if float64(int64(val)) == val {
		return ion.Int(int64(val))
	}
	return ion.Float(val)
}

// Add implements Union.Add
func (f *Float) Add(value ion.Datum) Union {
	if value.Type() == ion.FloatType {
		fv := float64(value.(ion.Float))
		if fv < f.lo {
			f.lo = fv
		}
		if fv > f.hi {
			f.hi = fv
		}
		f.hits++
		return f
	}
	return merge(f, ion.FloatType, f.hits, value)
}

// Time is a Generator that generates
// timestamp datums.
type Time struct {
	earliest, latest date.Time
	hits             int
}

// FromTime returns a Time Generator
// that only returns t.
func FromTime(t date.Time) *Time {
	return &Time{
		hits:     1,
		earliest: t,
		latest:   t,
	}
}

func (t *Time) String() string {
	return fmt.Sprintf("time[%s, %s]", t.earliest, t.latest)
}

// Generate draws a new timestamp value
// that lives uniformly in the open time interval
// between the earliest and latest times passed
// to Add.
func (t *Time) Generate(src *rand.Rand) ion.Datum {
	start, end := t.earliest.UnixNano(), t.latest.UnixNano()
	distance := end + 1 - start
	var nano int64
	if distance < 0 {
		// range has overflowed; just produce any value
		nano = int64(src.Uint64())
	} else {
		nano = start + src.Int63n(distance)
	}
	secs, nsecs := nano/int64(time.Second), nano%int64(time.Second)
	return ion.Timestamp(date.Unix(secs, nsecs))
}

// Add implements Union.Add
func (t *Time) Add(value ion.Datum) Union {
	if vt, ok := value.(ion.Timestamp); ok {
		vt := date.Time(vt)
		if vt.Before(t.earliest) {
			t.earliest = vt
		}
		if vt.After(t.latest) {
			t.latest = vt
		}
		t.hits++
		return t
	}
	return merge(t, ion.TimestampType, t.hits, value)
}

// String is a Generator String value
// that returns a string value drawn
// from a set of input strings.
type String struct {
	set  []string
	seen map[string]int
	hits int
}

// FromString returns a Generator that
// only returns the string s.
func FromString(s string) *String {
	return &String{
		set:  []string{s},
		seen: map[string]int{s: 0},
		hits: 1,
	}
}

// Generate returns a string drawn pseudorandomly
// from the distribution of input strings.
// Currently, every input string is equiprobable
// as an output datum.
//
// NOTE: This should be fixed to generate strings
// based on the histogram of input values rather
// than just making each input string equiprobable!
func (s *String) Generate(src *rand.Rand) ion.Datum {
	i := src.Intn(len(s.set))
	str := s.set[i]
	return ion.String(str)
}

// Add implements Union.Add
func (s *String) Add(value ion.Datum) Union {
	if si, ok := value.(ion.String); ok {
		si := string(si)
		if _, ok := s.seen[si]; !ok {
			s.seen[si] = len(s.set)
			s.set = append(s.set, si)
		}
		s.hits++
		return s
	}
	return merge(s, ion.StringType, s.hits, value)
}

func (s *String) String() string {
	return fmt.Sprintf("string[%d unique]", len(s.set))
}

// Struct is Generator
// that represents a superposition of structures.
//
// The Struct generator generates a set
// of struture fields based on the input corpus
// of structure fields by computing the union
// of all the structure fields it sees.
type Struct struct {
	// currently, capture the set of fields
	// and the union of their values
	// and generate output structures that
	// reproduce the input set of fields
	// taking into account how often a field
	// is missing
	indices map[string]int
	fields  []string
	values  []Union
	touched []bool

	hits int
}

func (s *Struct) describe(w io.Writer) {
	io.WriteString(w, "{")
	for i := range s.fields {
		if i != 0 {
			fmt.Fprintf(w, ", ")
		}
		fmt.Fprintf(w, "%s: %s", s.fields[i], s.values[i])
	}
	io.WriteString(w, "}")
}

func (s *Struct) String() string {
	var out strings.Builder
	s.describe(&out)
	return out.String()
}

// Generate generates an ion.Struct by
// drawing from the set of observed fields
// in the test corpus.
//
// The likelihood that any particular field is
// present is equal to the likelihood that the
// field was present in the input, and similarly
// the type of the structure field is determined
// by drawing from the distribution of input
// field types weighted by their relative likelihoods.
func (s *Struct) Generate(src *rand.Rand) ion.Datum {
	var lst []ion.Field
	for i := range s.fields {
		f := s.fields[i]
		val := s.values[i].Generate(src)
		if val == nil {
			continue // MISSING
		}
		lst = append(lst, ion.Field{Label: f, Value: val})
	}
	return ion.NewStruct(nil, lst)
}

// Add implements Union.Add
func (s *Struct) Add(value ion.Datum) Union {
	st, ok := value.(*ion.Struct)
	if !ok {
		return merge(s, ion.StructType, s.hits, value)
	}
	s.hits++
	if cap(s.touched) < len(s.fields) {
		s.touched = make([]bool, len(s.fields))
	} else {
		for i := range s.touched[:len(s.fields)] {
			s.touched[i] = false
		}
	}
	s.touched = s.touched[:len(s.fields)]
	st.Each(func(f ion.Field) bool {
		j, ok := s.indices[f.Label]
		if !ok {
			// new field: add the new field label
			// and set the value to the union of MISSING or the actual field value
			s.fields = append(s.fields, f.Label)
			s.values = append(s.values,
				(&None{hits: s.hits - 1}).Add(f.Value))
			s.indices[f.Label] = len(s.fields) - 1
			return true
		}
		s.touched[j] = true
		s.values[j] = s.values[j].Add(f.Value)
		return true
	})
	for i := range s.touched {
		if s.touched[i] {
			continue
		}
		// add a MISSING entry
		s.values[i] = s.values[i].Add(nil)
	}
	return s
}

// FromStruct returns a Struct Generator
// that only returns s.
func FromStruct(s *ion.Struct) *Struct {
	out := &Struct{hits: 1}
	out.indices = make(map[string]int)
	s.Each(func(f ion.Field) bool {
		out.indices[f.Label] = len(out.fields)
		out.fields = append(out.fields, f.Label)
		out.values = append(out.values, Single(f.Value))
		return true
	})
	return out
}

// List is a Generator that returns list values.
type List struct {
	// currently, just capture
	// the min and max length and
	// generate lists that are uniformly
	// distributed in that range with
	// each value being drawn from the
	// union of values at each position
	values   []Union
	min, max int // min and max length
	hits     int
}

// Generate returns an ion.List that has a length
// drawn from the uniform distribution over the open
// interval of the lowest list length in the input
// corpus to the highest. The value at each position
// in the returned list is drawn from the distribution of
// input values at that position.
func (l *List) Generate(src *rand.Rand) ion.Datum {
	var lst []ion.Datum
	n := l.min + src.Intn(1+l.max-l.min)
	for i := range l.values[:n] {
		lst = append(lst, l.values[i].Generate(src))
	}
	return ion.NewList(nil, lst)
}

func (l *List) String() string {
	var out strings.Builder
	fmt.Fprintf(&out, "list(%d-%d)[", l.min, l.max)
	for i := range l.values {
		if i != 0 {
			fmt.Fprintf(&out, ", ")
		}
		io.WriteString(&out, l.values[i].String())
	}
	io.WriteString(&out, "]")
	return out.String()
}

// Add implements Union.Add
func (l *List) Add(value ion.Datum) Union {
	list, ok := value.(*ion.List)
	if !ok {
		return merge(l, ion.ListType, l.hits, value)
	}
	lst := list.Items(nil)
	l.hits++
	if len(lst) < l.min {
		l.min = len(lst)
	} else if len(lst) > l.max {
		l.max = len(lst)
	}
	for i := range l.values {
		if i >= len(lst) {
			break
		}
		l.values[i].Add(lst[i])
	}
	if len(lst) > len(l.values) {
		tail := lst[len(l.values):]
		for i := range tail {
			l.values = append(l.values, Single(tail[i]))
		}
	}
	return l
}

// FromList returns a List Generator
// that always returns lst.
func FromList(lst *ion.List) *List {
	out := &List{hits: 1}
	lst.Each(func(d ion.Datum) bool {
		out.min++
		out.max++
		out.values = append(out.values, Single(d))
		return true
	})
	return out
}

// Single constructs a Union from a single value.
// The returned Union is constrained to generating
// a value identical (or in some cases just very similar)
// to the input value.
func Single(value ion.Datum) Union {
	if value == nil {
		return &None{hits: 1}
	}
	switch value.Type() {
	case ion.NullType:
		return &Null{hits: 1}
	case ion.BoolType:
		return FromBool(value)
	case ion.IntType:
		return FromInt64(int64(value.(ion.Int)))
	case ion.UintType:
		return FromUint64(uint64(value.(ion.Uint)))
	case ion.FloatType:
		return FromFloat(float64(value.(ion.Float)))
	case ion.TimestampType:
		return FromTime(date.Time(value.(ion.Timestamp)))
	case ion.StringType:
		return FromString(string(value.(ion.String)))
	case ion.ListType:
		return FromList(value.(*ion.List))
	case ion.StructType:
		return FromStruct(value.(*ion.Struct))
	default:
		println("type:", fmt.Sprintf("%T", value))
		panic("type not supported for versification")
	}
}
