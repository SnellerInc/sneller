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
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/SnellerInc/sneller/vm"
)

// totalInputs returns the total number of
// inputs in the entire tree.
func (t *Tree) totalInputs() int {
	return len(t.Inputs) + t.Root.totalInputs()
}

func (n *Node) totalInputs() int {
	sum := len(n.Inputs)
	for i := range n.Children {
		sum += n.Children[i].totalInputs()
	}
	return sum
}

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	parallel := ep.Parallel
	if parallel <= 0 {
		parallel = runtime.NumCPU()
	}
	if n := t.totalInputs(); n <= 0 {
		parallel = 1
	} else if n < parallel {
		parallel = n
	}
	p := mkpool(parallel)
	defer close(p)
	e := mkexec(p, ep, t.Inputs)
	if err := e.add(dst, &t.Root); err != nil {
		return err
	}
	return e.run()
}

func (n *Node) subexec(p pool, ep *ExecParams) error {
	if len(n.Children) == 0 {
		return nil
	}
	e := mkexec(p, ep, n.Inputs)
	rp := make([]replacement, len(n.Children))
	var wg sync.WaitGroup
	wg.Add(len(n.Children))
	errors := make([]error, len(n.Children))
	for i := range n.Children {
		go func(i int) {
			defer wg.Done()
			errors[i] = e.add(&rp[i], n.Children[i])
		}(i)
	}
	wg.Wait()
	if err := appenderrs(nil, errors); err != nil {
		return err
	}
	if err := e.run(); err != nil {
		return err
	}
	repl := &replacer{
		inputs: rp,
	}
	n.Op.rewrite(repl)
	return repl.err
}

type task struct {
	input vm.Table
	sink  vm.QuerySink
}

type executor struct {
	pool   pool
	ep     *ExecParams
	inputs []Input
	subp   int
	tasks  []task
	extra  []io.Closer // sinks with no inputs
	lock   sync.Mutex
}

func mkexec(p pool, ep *ExecParams, inputs []Input) *executor {
	e := &executor{
		pool:   p,
		ep:     ep,
		inputs: inputs,
		subp:   1,
	}
	if len(inputs) > 0 {
		e.tasks = make([]task, len(inputs))
		e.subp = (ep.Parallel + len(inputs) - 1) / len(inputs)
		if e.subp <= 0 {
			e.subp = 1
		}
	}
	return e
}

func (e *executor) add(dst vm.QuerySink, n *Node) error {
	if err := n.subexec(e.pool, e.ep); err != nil {
		return err
	}
	in, sink, err := n.Op.wrap(dst, e.ep)
	if err != nil {
		return err
	}
	if sink == nil {
		return nil
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	if in == -1 {
		e.extra = append(e.extra, sink)
		return nil
	}
	if in < 0 || in >= len(e.tasks) {
		return fmt.Errorf("input %d not in plan", in)
	}
	if e.tasks[in].input == nil {
		handle := e.inputs[in].Handle
		if handle == nil {
			return fmt.Errorf("nil table handle")
		}
		tbl, err := handle.Open(e.ep.Context)
		if err != nil {
			return err
		}
		e.tasks[in].input = tbl
	}
	e.tasks[in].sink = appendSink(e.tasks[in].sink, sink)
	return nil
}

func (e *executor) run() error {
	var wg sync.WaitGroup
	wg.Add(len(e.tasks))
	errors := make([]error, len(e.tasks))
	for i := range e.tasks {
		if e.tasks[i].input == nil {
			wg.Done()
			continue
		}
		e.pool.do(i, func(i int) {
			defer wg.Done()
			t := &e.tasks[i]
			errors[i] = e.runtask(t)
			e.ep.Stats.observe(t.input)
		})
	}
	wg.Wait()
	err := appenderrs(nil, errors)
	for i := range e.extra {
		err = appenderr(err, e.extra[i].Close())
	}
	return err
}

func (e *executor) runtask(t *task) error {
	err := t.input.WriteChunks(t.sink, e.subp)
	err2 := t.sink.Close()
	if err == nil {
		err = err2
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return err
}

// pool is a work queue for a goroutine pool.
// Closing the pool cleans up the goroutines.
type pool chan struct {
	i int
	f func(int)
}

func mkpool(parallel int) pool {
	if parallel <= 0 {
		panic("mkpool: size out of range")
	}
	ch := make(pool, parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			for t := range ch {
				t.f(t.i)
			}
		}()
	}
	return ch
}

func (p pool) do(i int, f func(int)) {
	p <- struct {
		i int
		f func(int)
	}{i, f}
}
