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

// Project is a plan Op that
// projects and re-names fields
type Project struct {
	Nonterminal
	Using []expr.Binding
}

func (p *Project) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	proj, err := vm.NewProjection(ep.rewriteBind(p.Using), dst)
	if err != nil {
		return err
	}
	return p.From.exec(proj, src, ep)
}

func (p *Project) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("project", dst, st)
	dst.BeginField(st.Intern("project"))
	encodeBindings(p.Using, dst, st, ep)
	dst.EndStruct()
	return nil
}

func (p *Project) SetField(f ion.Field) error {
	switch f.Label {
	case "project":
		bind, err := expr.DecodeBindings(f.Datum)
		if err != nil {
			return err
		}
		p.Using = bind
	default:
		return errUnexpectedField
	}

	return nil
}

func (p *Project) String() string {
	var out strings.Builder
	out.WriteString("PROJECT ")
	for i := range p.Using {
		out.WriteString(expr.ToString(&p.Using[i]))
		if i != len(p.Using)-1 {
			out.WriteString(", ")
		}
	}
	return out.String()
}
