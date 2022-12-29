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

package expr

import (
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// FiniteSet represents the set of values a certain variable can have.
type FiniteSet struct {
	values []Constant
}

// CreateFiniteSet makes a new set from a set of ion.Datum values.
func CreateFiniteSet(consts []ion.Datum) (*FiniteSet, error) {
	if len(consts) == 0 {
		return nil, fmt.Errorf("cannot create an empty set")
	}

	fs := &FiniteSet{
		values: make([]Constant, 0, len(consts)),
	}

	for i := range consts {
		n, ok := AsConstant(consts[i])
		if !ok {
			return nil, fmt.Errorf("item #%d (%v) cannot be converted into a node", i, consts[i])
		}

		fs.values = append(fs.values, n)
	}

	return fs, nil
}

type FiniteSetRelation uint8

const (
	AlwaysFalse  FiniteSetRelation = iota // a relation is true for all values in a set
	AlwaysTrue                            // a relation is false for all values in a set
	Indefinitive                          // a relation is neither true or false
)

func (r FiniteSetRelation) String() string {
	switch r {
	case AlwaysTrue:
		return "<always true>"
	case AlwaysFalse:
		return "<always false>"
	case Indefinitive:
		return "<indefinitive>"
	}

	return "<unknown relation>"
}

// Singleton returns value if the set has exactly one element
func (f *FiniteSet) Singleton() Constant {
	if len(f.values) == 1 {
		return f.values[0]
	}

	return nil
}

// test compares all values from the set with the constant `c`.
//
// Relation is considered true if compare function return
// value `cmpresult1` or `cmpresult2` (both have to be one
// of: -1, +1, 0)
func (f *FiniteSet) test(c Constant, cmpresult1, cmpresult2 int) FiniteSetRelation {
	v := cmpconstants(f.values[0], c)
	if v == uncomparable {
		return Indefinitive
	}

	relation := (v == cmpresult1 || v == cmpresult2)
	for i := range f.values[1:] {
		v := cmpconstants(f.values[i], c)
		if v == uncomparable {
			return Indefinitive
		}

		p := (v == cmpresult1 || v == cmpresult2)
		if relation != p {
			return Indefinitive
		}
	}

	if relation {
		return AlwaysTrue
	}
	return AlwaysFalse
}

// uncomparable is returned by cmpconstants if values cannot be compared
const uncomparable = -2

// cmpconstants compares two constants and returns
// +1 if a > b, -1 if a < b, 0 if a == b or
// -2 if arguments cannot be compared
func cmpconstants(a, b Constant) int {
	switch x := a.(type) {
	case Float:
		switch y := b.(type) {
		case Float:
			return cmpfloat64(float64(x), float64(y))

		case Integer:
			return cmpfloat64(float64(x), float64(y))
		}

	case Integer:
		switch y := b.(type) {
		case Float:
			trunc := int64(float64(y))
			if float64(trunc) == float64(y) {
				return cmpint64(int64(x), trunc)
			} else {
				return cmpfloat64(float64(x), float64(y))
			}

		case Integer:
			return cmpint64(int64(x), int64(y))
		}

	case String:
		switch y := b.(type) {
		case String:
			return strings.Compare(string(x), string(y))
		}

	case *Timestamp:
		switch y := b.(type) {
		case *Timestamp:
			return cmptimestamp(x.Value, y.Value)
		}
	}

	return uncomparable
}

// cmpfloat64 compares two floats, returning -1, +1 or 0
func cmpfloat64(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return +1
	}
	return 0
}

// cmpfloat64 compares two ints, returning -1, +1 or 0
func cmpint64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return +1
	}
	return 0
}

// cmpfloat64 compares two timestamps, returning -1, +1 or 0
func cmptimestamp(a, b date.Time) int {
	if a.Before(b) {
		return -1
	}
	if b.Before(a) {
		return +1
	}
	return 0
}

// Compare returns relation between all values from the set with a constant.
func (f *FiniteSet) Compare(op CmpOp, c Constant) FiniteSetRelation {
	switch op {
	case Equals:
		return f.test(c, 0, 0)
	case NotEquals:
		return f.test(c, -1, +1)
	case Less:
		return f.test(c, -1, -1)
	case LessEquals:
		return f.test(c, -1, 0)
	case Greater:
		return f.test(c, +1, +1)
	case GreaterEquals:
		return f.test(c, +1, 0)
	}

	return Indefinitive
}

// In checks if all values from `f` are present in the `set` given as an argument.
func (f *FiniteSet) In(set []Constant) FiniteSetRelation {
	inset := func(c Constant) bool {
		for i := range set {
			if c.Equals(set[i]) {
				return true
			}
		}

		return false
	}

	in := inset(f.values[0])
	for _, v := range f.values[1:] {
		if in != inset(v) {
			return Indefinitive
		}
	}

	if in {
		return AlwaysTrue
	}
	return AlwaysFalse
}
