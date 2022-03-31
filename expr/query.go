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
	"strings"
)

// CTE is one arm of a "common table expression"
// (i.e. WITH table AS (SELECT ...))
type CTE struct {
	Table string
	As    *Select
}

func (c *CTE) text(dst *strings.Builder, redact bool) {
	dst.WriteString(c.Table)
	dst.WriteString(" AS ")
	c.As.text(dst, redact)
}

// Equals returns true if
// c and other are equivalent CTE bindings,
// or false otherwise.
func (c *CTE) Equals(other *CTE) bool {
	return c.Table == other.Table && c.As.Equals(other.As)
}

// Query contains a complete query.
type Query struct {
	With []CTE

	// Body is the body of the query.
	// Body can be:
	//   - A SELECT expression
	// TODO:
	//   - A UNION expression
	//   - A UNION ALL expression
	Body Node
}

// Text returns the unredacted query text.
// See also: ToString.
//
// NOTE: we aren't implementing fmt.Stringer
// here so that queries aren't unintentionally
// printed in unredacted form.
func (q *Query) Text() string {
	var dst strings.Builder
	q.text(&dst, false)
	return dst.String()
}

func (q *Query) text(dst *strings.Builder, redact bool) {
	if len(q.With) > 0 {
		dst.WriteString("WITH ")
		for i := range q.With {
			if i != 0 {
				dst.WriteString(", ")
			}
			q.With[i].text(dst, redact)
		}
		dst.WriteByte(' ')
	}
	if s, ok := q.Body.(*Select); ok {
		// do not parenthesize final SELECT
		s.write(dst, redact)
	} else {
		q.Body.text(dst, redact)
	}
}

// Redacted returns the redacted query text.
// See also: ToRedacted
func (q *Query) Redacted() string {
	var dst strings.Builder
	q.text(&dst, true)
	return dst.String()
}

// Equals returns true if q and other
// are syntactically equivalent queries,
// or false otherwise.
func (q *Query) Equals(other *Query) bool {
	if len(q.With) != len(other.With) {
		return false
	}
	for i := range q.With {
		if !q.With[i].Equals(&other.With[i]) {
			return false
		}
	}
	return q.Body.Equals(other.Body)
}
