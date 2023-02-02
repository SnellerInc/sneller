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

	"github.com/SnellerInc/sneller/ion"
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
	Explain ExplainFormat

	With []CTE
	// Into, if non-nil, is the INTO
	// portion of Body when Body is
	// a SELECT-FROM-WHERE that includes
	// an INTO clause.
	Into Node
	// Body is the body of the query.
	// Body can be:
	//   - A SELECT expression
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
	switch q.Explain {
	case ExplainDefault:
		dst.WriteString("EXPLAIN ")
	case ExplainText:
		dst.WriteString("EXPLAIN AS text ")
	case ExplainList:
		dst.WriteString("EXPLAIN AS list ")
	case ExplainGraphviz:
		dst.WriteString("EXPLAIN AS graphviz ")
	}

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
		s.write(dst, redact, q.Into)
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

// Encode encodes a query as an Ion structure
func (q *Query) Encode(dst *ion.Buffer, st *ion.Symtab) {
	field := func(name string) {
		dst.BeginField(st.Intern(name))
	}

	dst.BeginStruct(-1)
	field("type")
	dst.WriteSymbol(st.Intern("query"))

	field("explain")
	dst.WriteInt(int64(q.Explain))

	if len(q.With) > 0 {
		field("with")
		dst.BeginList(-1)
		for i := range q.With {
			dst.WriteString(q.With[i].Table)
			q.With[i].As.Encode(dst, st)
		}
		dst.EndList()
	}
	if q.Into != nil {
		field("into")
		q.Into.Encode(dst, st)
	}
	field("body")
	q.Body.Encode(dst, st)
	dst.EndStruct()
}

// DecodeQuery decodes an Ion structure representing a query.
//
// Returns query, tail of unprocessed Ion and error.
func DecodeQuery(d ion.Datum) (*Query, error) {
	q, err := ion.UnpackTyped(d, func(typ string) (*Query, bool) {
		if typ == "query" {
			return new(Query), true
		}
		return nil, false
	})
	if err != nil {
		return nil, err
	}
	if q.Body == nil {
		return nil, fmt.Errorf(`DecodeQuery: missing "body" field`)
	}
	return q, nil
}

func (q *Query) SetField(f ion.Field) error {
	var err error
	switch f.Label {
	case "explain":
		var v int64
		v, err = f.Int()
		if err != nil {
			return err
		}
		q.Explain = ExplainFormat(v)
	case "with":
		hastable := false
		var table string
		err = f.UnpackList(func(d ion.Datum) error {
			if !hastable {
				var err error
				table, err = d.String()
				if err != nil {
					return err
				}
				hastable = true
				return nil
			}
			// hastable == true
			node, err := Decode(d)
			if err != nil {
				return err
			}
			sel, ok := node.(*Select)
			if !ok {
				return fmt.Errorf("expected Select node, got %T", node)
			}
			q.With = append(q.With, CTE{Table: table, As: sel})
			hastable = false
			return nil
		})
		if err != nil {
			return err
		}
	case "into":
		q.Into, err = Decode(f.Datum)
	case "body":
		q.Body, err = Decode(f.Datum)
	default:
		err = fmt.Errorf("DecodeQuery: unknown field %q", f.Label)
	}
	return err
}

// CheckHint checks consistency of the whole query using a hint
func (q *Query) CheckHint(h Hint) error {
	with := map[string]Node{}
	for i := range q.With {
		name := q.With[i].Table
		err := CheckHint(q.With[i].As, h)
		if err != nil {
			return err
		}

		_, exists := with[name]
		if exists {
			return fmt.Errorf("WITH query name %q specified more than once", name)
		}

		with[name] = q.With[i].As
	}

	// TODO: check references between CTEs and the main query
	return CheckHint(q.Body, h)
}

// Check checks consistency of the whole query
func (q *Query) Check() error {
	return q.CheckHint(NoHint)
}
