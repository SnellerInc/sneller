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
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// Unnest joins a row on a list-like field
// within that row and computes a projection
// plus an optional conditional clause
type Unnest struct {
	Nonterminal               // source op
	PivotField   *expr.Path   // pivot field (array) binding from outer
	InnerProject vm.Selection // projected fields from inside pivot
	OuterProject vm.Selection // projected fields
	InnerMatch   expr.Node    // WHERE that uses inner match fields
}

func (u *Unnest) rewrite(rw expr.Rewriter) {
	u.From.rewrite(rw)
	for i := range u.InnerProject {
		u.InnerProject[i].Expr = expr.Rewrite(rw, u.InnerProject[i].Expr)
	}
	for i := range u.OuterProject {
		u.OuterProject[i].Expr = expr.Rewrite(rw, u.OuterProject[i].Expr)
	}
	u.InnerMatch = expr.Rewrite(rw, u.InnerMatch)
}

func (u *Unnest) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("unnest", dst, st)

	dst.BeginField(st.Intern("pivot"))
	u.PivotField.Encode(dst, st)

	dst.BeginField(st.Intern("inner"))
	expr.EncodeBindings(u.InnerProject, dst, st)

	dst.BeginField(st.Intern("outer"))
	expr.EncodeBindings(u.OuterProject, dst, st)

	if u.InnerMatch != nil {
		dst.BeginField(st.Intern("match"))
		u.InnerMatch.Encode(dst, st)
	}
	dst.EndStruct()
	return nil
}

func decodeSel(dst *vm.Selection, st *ion.Symtab, src []byte) error {
	bind, err := expr.DecodeBindings(st, src)
	if err != nil {
		return err
	}
	*dst = bind
	return nil
}

func (u *Unnest) setfield(d Decoder, name string, st *ion.Symtab, body []byte) error {
	switch name {
	case "pivot":
		e, _, err := expr.Decode(st, body)
		if err != nil {
			return err
		}
		p, ok := e.(*expr.Path)
		if !ok {
			return fmt.Errorf("cannot use node of type %T as plan.Unnest.Pivot", e)
		}
		u.PivotField = p
	case "inner":
		return decodeSel(&u.InnerProject, st, body)
	case "outer":
		return decodeSel(&u.OuterProject, st, body)
	case "match":
		e, _, err := expr.Decode(st, body)
		if err != nil {
			return err
		}
		u.InnerMatch = e
	}
	return nil
}

func (u *Unnest) String() string {
	var out strings.Builder
	if len(u.OuterProject) != 0 {
		out.WriteString("PROJECT ")
		for i := range u.OuterProject {
			out.WriteString(expr.ToString(&u.OuterProject[i]))
			if i != len(u.OuterProject)-1 {
				out.WriteString(", ")
			}
		}
		out.WriteString(" + ")
	}
	out.WriteString("UNNEST ")
	out.WriteString(expr.ToString(u.PivotField))
	if len(u.InnerProject) != 0 {
		out.WriteString(" PROJECT ")
		for i := range u.InnerProject {
			out.WriteString(expr.ToString(&u.InnerProject[i]))
			if i != len(u.InnerProject)-1 {
				out.WriteString(", ")
			}
		}
	}
	if u.InnerMatch != nil {
		out.WriteString(" WHERE ")
		out.WriteString(expr.ToString(u.InnerMatch))
	}
	return out.String()
}

func (u *Unnest) wrap(dst vm.QuerySink, ep *execParams) (int, vm.QuerySink, error) {
	return u.From.wrap(vm.NewUnnest(
		dst,
		u.PivotField,
		u.OuterProject,
		u.InnerProject,
		u.InnerMatch,
	), ep)
}
