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
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/vm"
)

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	e := mkexec(ep, t.Inputs)
	return e.do(dst, &t.Root)
}

func (e *executor) subexec(n *Node) (expr.Rewriter, error) {
	if len(n.Children) == 0 {
		return nil, nil
	}
	rp := make([]replacement, len(n.Children))
	var wg sync.WaitGroup
	wg.Add(len(n.Children))
	errors := make([]error, len(n.Children))
	for i := range n.Children {
		subex := e.clone()
		go func(i int) {
			defer wg.Done()
			errors[i] = subex.do(&rp[i], n.Children[i])
			e.ep.Stats.atomicAdd(&subex.ep.Stats)
		}(i)
	}
	wg.Wait()
	if err := appenderrs(nil, errors); err != nil {
		return nil, err
	}
	repl := &replacer{
		inputs: rp,
	}
	return repl, nil
}

type executor struct {
	ep     *ExecParams
	inputs []Input
}

func (e *executor) clone() *executor {
	return &executor{
		ep: &ExecParams{
			Output:   nil, // not for sub-queries to use
			Parallel: e.ep.Parallel,
			Context:  e.ep.Context,
			Rewriter: e.ep.Rewriter,
		},
		inputs: e.inputs,
	}
}

func mkexec(ep *ExecParams, inputs []Input) *executor {
	return &executor{
		ep:     ep,
		inputs: inputs,
	}
}

func (e *executor) do(dst vm.QuerySink, n *Node) error {
	rw, err := e.subexec(n)
	if err != nil {
		return err
	}
	if rw != nil {
		// this rewrite is scoped to just this node
		e.ep.AddRewrite(rw)
		defer e.ep.PopRewrite()
	}
	fn := n.Op.wrap(dst, e.ep)
	i := n.Input
	if i >= len(e.inputs) {
		return fmt.Errorf("input %d not in plan", i)
	}
	var h TableHandle
	if i >= 0 {
		h = e.inputs[i].Handle
	}
	return fn(h)
}
