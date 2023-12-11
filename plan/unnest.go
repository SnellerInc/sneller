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
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// Unnest joins a row on a list-like field
// within that row and computes a projection
// plus an optional conditional clause
type Unnest struct {
	Nonterminal // source op
	Expr        expr.Node
	Result      string
}

func (u *Unnest) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("unnest", dst, st)
	dst.BeginField(st.Intern("expr"))
	ep.rewrite(u.Expr).Encode(dst, st)
	dst.BeginField(st.Intern("result"))
	dst.WriteString(u.Result)
	dst.EndStruct()
	return nil
}

func decodeSel(dst *vm.Selection, d ion.Datum) error {
	bind, err := expr.DecodeBindings(d)
	if err != nil {
		return err
	}
	*dst = bind
	return nil
}

func (u *Unnest) SetField(f ion.Field) error {
	switch f.Label {
	case "result":
		s, err := f.String()
		if err != nil {
			return err
		}
		u.Result = s
	case "expr":
		e, err := expr.Decode(f.Datum)
		if err != nil {
			return err
		}
		u.Expr = e
	default:
		return errUnexpectedField
	}
	return nil
}

func (u *Unnest) String() string {
	var out strings.Builder
	out.WriteString("UNNEST ")
	out.WriteString(expr.ToString(u.Expr))
	out.WriteString(" AS ")
	out.WriteString(u.Result)
	return out.String()
}

func (u *Unnest) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	op, err := vm.NewUnnest(dst, ep.rewrite(u.Expr), u.Result)
	if err != nil {
		return err
	}
	return u.From.exec(op, src, ep)
}
