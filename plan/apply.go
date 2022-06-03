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
	"strconv"
	"strings"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

// Apply generates new columns by
// applying one or more built-in functions
// to columns
type Apply struct {
	Nonterminal
	Funcs []expr.Binding
}

func (a *Apply) rewrite(rw expr.Rewriter) {
	a.From.rewrite(rw)
	for i := range a.Funcs {
		a.Funcs[i].Expr = expr.Rewrite(rw, a.Funcs[i].Expr)
	}
}

type visitfn func(e expr.Node) expr.Visitor

func (v visitfn) Visit(e expr.Node) expr.Visitor {
	return v(e)
}

// cannotCompile walks an expression tree
// and finds nodes that we know cannot be
// lowered into assembly
//
func cannotCompile(e expr.Node) bool {
	o := false
	var visit visitfn
	visit = func(e expr.Node) expr.Visitor {
		// can't lower || yet
		if b, ok := e.(*expr.Builtin); ok && b.Func == expr.Concat {
			o = true
			return nil
		}
		return visit
	}
	expr.Walk(visitfn(visit), e)
	return o
}

func (a *Apply) exec(dst vm.QuerySink, ep *ExecParams) error {
	app, err := vm.Apply(a.Funcs, dst)
	if err != nil {
		return err
	}
	return a.From.exec(app, ep)
}

func (a *Apply) String() string {
	var out strings.Builder
	out.WriteString("APPLY ")
	for i := range a.Funcs {
		out.WriteString(expr.ToString(&a.Funcs[i]))
		if i != len(a.Funcs)-1 {
			out.WriteString(", ")
		}
	}
	return out.String()
}

func (a *Apply) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("apply", dst, st)
	dst.BeginField(st.Intern("funcs"))
	expr.EncodeBindings(a.Funcs, dst, st)
	dst.EndStruct()
	return nil
}

func (a *Apply) setfield(d Decoder, name string, st *ion.Symtab, buf []byte) error {
	switch name {
	case "funcs":
		bind, err := expr.DecodeBindings(st, buf)
		if err != nil {
			return err
		}
		a.Funcs = bind
	}
	return nil
}

func gensym(n int) string {
	return "$_" + strconv.Itoa(n)
}

// lower expressions inside SELECT into
// their own intermediate step
//
// FIXME: PartiQL semantics dictate that
// each binding can reference any bindings
// on the left-hand-side, for example
//   x+y as z, z+1 as q
// in which case we either need to guarantee
// that Apply can handle references to bindings
// that it generates itself OR create another
// Apply step that uses the bindings from the
// previous step...
func (p *Project) lowerApplications(tmp int) Op {
	var app *Apply
	results := make([]string, len(p.Using))
	for i := range p.Using {
		e := p.Using[i].Expr
		results[i] = p.Using[i].Result()
		if results[i] == "" {
			results[i] = autoname(p.Using[i].Expr, i)
		}
		if !cannotCompile(e) {
			continue
		}
		if app == nil {
			// create application step
			// and re-write projection to
			// use this intermediate binding step
			app = &Apply{
				Nonterminal: p.Nonterminal,
			}
			p.From = app
		}
		tmpname := gensym(tmp)
		tmp++
		p.Using[i].Expr = &expr.Path{First: tmpname}
		app.Funcs = append(app.Funcs, expr.Bind(e, tmpname))
	}
	// if there is no projection happening
	// *except* for the projection implied by
	// the Apply node, then just do the Apply
	if app != nil && len(app.Funcs) == len(p.Using) {
		for i := range app.Funcs {
			app.Funcs[i].As(results[i])
		}
		return app
	}
	return p
}
