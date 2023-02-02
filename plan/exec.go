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

	"github.com/SnellerInc/sneller/vm"
)

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	e := mkexec(ep, t.Inputs)
	return e.do(dst, &t.Root)
}

func (e *executor) subexec(n *Node, ep *ExecParams) error {
	if len(n.Children) == 0 {
		return nil
	}
	rp := make([]replacement, len(n.Children))
	var wg sync.WaitGroup
	wg.Add(len(n.Children))
	errors := make([]error, len(n.Children))
	for i := range n.Children {
		go func(i int) {
			defer wg.Done()
			errors[i] = e.do(&rp[i], n.Children[i])
		}(i)
	}
	wg.Wait()
	if err := appenderrs(nil, errors); err != nil {
		return err
	}
	repl := &replacer{
		inputs: rp,
	}
	ep.AddRewrite(repl)
	return nil
}

type executor struct {
	ep     *ExecParams
	inputs []Input
}

func mkexec(ep *ExecParams, inputs []Input) *executor {
	return &executor{
		ep:     ep,
		inputs: inputs,
	}
}

func (e *executor) do(dst vm.QuerySink, n *Node) error {
	if err := e.subexec(n, e.ep); err != nil {
		return err
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
