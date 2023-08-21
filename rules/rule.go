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

// Package rules defines a syntax for rule-based
// re-writing DSLs. The supported syntax is as follows:
//
//	comment = < go comment syntax >
//	string = < go double-quote syntax > | < go backtick syntax >
//	identifier = < go identifier >
//	rule = value {',' value} '->' term
//	term = (identifier ':' value) | identifier | value
//	value = list | string
//	list = '(' item {space+ item} ')'
//
// For example:
//
//	// here is a line comment
//	(foo "quoted-string" x:(baz `backtick-string`)) -> x
package rules

import (
	"io"
	"slices"
	"strconv"
	"strings"
	"text/scanner"
)

// Rule represents one rule. The rule package
// does not assign any semantic meaning to the
// structure of a Rule; it only defines the syntax
// for Rules.
type Rule struct {
	// From is the conjunction of
	// expressions to match against.
	From []Value
	// To is the value associated with
	// the right-hand-side of the rule.
	To Term
	// Location is the original textual position
	// at which the rule began
	Location scanner.Position
}

// String implements fmt.Stringer
func (r *Rule) String() string {
	var out strings.Builder
	for i := range r.From {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(r.From[i].String())
	}
	out.WriteString(" -> ")
	out.WriteString(r.To.String())
	return out.String()
}

// Equal returns true if two rules are equal, or false otherwise.
// Equal does not compare the Location field of terms
// (see also Term.Equal).
func (r *Rule) Equal(o *Rule) bool {
	return slices.EqualFunc(r.From, o.From, equal) && r.To.Equal(&o.To)
}

// WriteTo writes formatted rules to dst.
// Each rule is written on its own line.
func WriteTo(dst io.Writer, lst []Rule) (int64, error) {
	n := int64(0)
	for i := range lst {
		nn, err := io.WriteString(dst, lst[i].String())
		n += int64(nn)
		if err != nil {
			return n, err
		}
		nn, err = io.WriteString(dst, "\n")
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Term is a term in a rule. Every term in
// a rule is optionally named (see Name) so
// that it can be referred to in other terms.
type Term struct {
	// Name is the name (identifier) of this term.
	// If Value is non-nil, then Name
	// may be the empty string.
	Name string

	// Value is the value of the term.
	// Value may be nil if this term
	// is a bare identifier.
	Value Value

	// Input is scratch space that can
	// be used by external packages.
	Input string

	// Location is the location of the term
	// in the original text file. It can be
	// used to provide more helpful error messages.
	Location scanner.Position
}

// String implements fmt.Stringer
//
// String returns the canonical textual
// representation of the Term t.
func (t *Term) String() string {
	if t.Name == "" {
		if t.Value != nil {
			return t.Value.String()
		}
		return "_"
	}
	if t.Value == nil {
		return t.Name
	}
	return t.Name + ":" + t.Value.String()
}

// Value is one of List, String, Int, or Float
type Value interface {
	String() string
}

// List is a list of terms.
type List []Term

func (l List) String() string {
	var out strings.Builder
	out.WriteByte('(')
	for i := range l {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(l[i].String())
	}
	out.WriteByte(')')
	return out.String()
}

// String is a literal Go string
type String string

// String returns the literal representation
// of s, *not* s itself. Cast s to a string if
// you want to use the raw string value.
func (s String) String() string { return strconv.Quote(string(s)) }

// Int is a literal integer
type Int int64

// Float is a literal float
type Float float64

func (i Int) String() string { return strconv.FormatInt(int64(i), 10) }

func (f Float) String() string { return strconv.FormatFloat(float64(f), 'g', -1, 64) }

func equal(x, y Value) bool {
	if l, ok := x.(List); ok {
		if l2, ok := y.(List); ok {
			return slices.EqualFunc(l, l2, func(x, y Term) bool {
				return x.Equal(&y)
			})
		}
		return false
	}
	// should be String, Int, or Float,
	// so we can use == for equality
	return x == y
}

// Equal returns true if two terms are equivalent,
// or false otherwise. Equal does not compare the
// Location fields of each Term.
func (t *Term) Equal(o *Term) bool {
	if t.Name != o.Name {
		return false
	}
	return equal(t.Value, o.Value)
}
