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

package versify

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/SnellerInc/sneller/ion"
)

const (
	missingType = ion.ReservedType
	ntypes      = ion.ReservedType + 1
)

// Any is a generator that generates
// ion datums of different types based
// on their relative frequency of occurrence
// in the input corpus.
//
// Note that a value being "missing" (returned as nil)
// is considered to be a distinct type in terms
// of drawing from the distribution of inputs.
type Any struct {
	typemap [ntypes]Union
	hits    [ntypes]int
	total   int
}

func (o *Any) Generate(src *rand.Rand) ion.Datum {
	n := rand.Intn(o.total)
	i := 0
	for n > o.hits[i] || o.typemap[i] == nil {
		n -= o.hits[i]
		i++
	}
	return o.typemap[i].Generate(src)
}

func (o *Any) Add(value ion.Datum) Union {
	var t ion.Type
	if value.IsEmpty() {
		t = missingType
	} else {
		t = value.Type()
		if t == ion.UintType {
			t = ion.IntType
		}
	}
	o.hits[t]++
	o.total++
	if o.typemap[t] == nil {
		o.typemap[t] = Single(value)
	} else {
		o.typemap[t] = o.typemap[t].Add(value)
	}
	return o
}

func (o *Any) String() string {
	var out strings.Builder
	out.WriteString("any{")
	first := true
	for i := ion.Type(0); i < ntypes; i++ {
		c := o.hits[i]
		if c == 0 {
			continue
		}
		if !first {
			out.WriteString(", ")
		}
		fmt.Fprintf(&out, "%.02f: %s", (100*float64(c))/float64(o.total), o.typemap[i])
		first = false
	}
	out.WriteString("}")
	return out.String()
}
