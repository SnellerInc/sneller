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

package vm

import (
	"fmt"
	"io"
	"sort"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

type outfield struct {
	name string     // field name
	sym  ion.Symbol // raw symbol integer
	op   builtin    // operation that produces the output
}

type builtin interface {
	// exec should write the result
	// of the query to a.tmp, and
	// the result should begin with
	// a call to a.tmp.BeginField(*sym)
	// if sym is non-nil and exec
	// decides to write output
	//
	// exec may decide to write zero output
	// if the logical result of the operation
	// is the PartiQL MISSING value
	setarg(argnum, id int)
	exec(a *argstate, sym *ion.Symbol)

	// dup should return a new
	// builtin with the same arguments,
	// or the same builtin if the builtin
	// does not have mutable internal state
	dup() builtin
}

// intermediate ops that produce results
// for the final output ops
type localop struct {
	op     builtin
	output int
}

type argstate struct {
	st     *ion.Symtab
	tmp    ion.Buffer // scratch
	locals [][]byte   // pointers to raw data
}

type applicator struct {
	parent    *Applicator
	stsize    int
	args      argstate
	argtmp    []byte
	ssa2local []int          // ssa slot to args.locals[i]
	during    []localop      // intermediate (non-projected) outputs
	outmap    []outfield     // projected outputs
	dst       io.WriteCloser // destination of writes
	rc        rowConsumer    // non-nil when dst is a RowConsumer
	bc        bytecode
	prog      prog
	nlocals   int
}

type binaryBuiltin struct {
	left, right int
}

func (b *binaryBuiltin) setarg(n, id int) {
	switch n {
	case 0:
		b.left = id
	case 1:
		b.right = id
	}
}

func (b *binaryBuiltin) args(a *argstate) ([]byte, []byte) {
	return a.locals[b.left], a.locals[b.right]
}

type literalBuiltin struct {
	datum ion.Datum
}

func (l *literalBuiltin) setarg(n, id int) {
	panic("cannot setarg for literalBuiltin")
}

func (l *literalBuiltin) dup() builtin { return l }

func (l *literalBuiltin) exec(a *argstate, sym *ion.Symbol) {
	if sym != nil {
		a.tmp.BeginField(*sym)
	}
	l.datum.Encode(&a.tmp, a.st)
}

// builtinConcat is the '||' operator
type builtinConcat struct {
	binaryBuiltin
}

func (b *builtinConcat) exec(a *argstate, sym *ion.Symbol) {
	left, right := b.args(a)
	if left == nil || right == nil ||
		ion.TypeOf(left) != ion.StringType ||
		ion.TypeOf(right) != ion.StringType {
		return
	}
	lbody, _ := ion.Contents(left)
	rbody, _ := ion.Contents(right)
	if sym != nil {
		a.tmp.BeginField(*sym)
	}
	a.tmp.BeginString(len(lbody) + len(rbody))
	a.tmp.UnsafeAppend(lbody)
	a.tmp.UnsafeAppend(rbody)
}

// no internal state
func (b *builtinConcat) dup() builtin { return b }

func (a *applicator) bind(id int, val []byte) {
	a.args.locals[id] = val
}

func (a *applicator) run() error {
	if len(a.during) > 0 {
		// swap the output of args.tmp
		// to a temporary buffer that
		// we will throw away
		old := a.args.tmp.Bytes()
		a.args.tmp.Set(a.argtmp[:0])
		for i := range a.during {
			start := a.args.tmp.Size()
			a.during[i].op.exec(&a.args, nil)
			end := a.args.tmp.Size()
			outid := a.during[i].output
			if start == end {
				a.args.locals[outid] = nil
			} else {
				a.args.locals[outid] = a.args.tmp.Bytes()[start:end]
			}
		}
		// now swap back to the real output buffer
		a.args.tmp.Set(old)
	}
	// now actually write the output row
	start := a.args.tmp.Size()
	a.args.tmp.BeginStruct(-1)
	for i := range a.outmap {
		sym := a.outmap[i].sym
		op := a.outmap[i].op
		op.exec(&a.args, &sym)
	}
	a.args.tmp.EndStruct()
	size := a.args.tmp.Size() - start
	if size >= 1<<21 {
		return fmt.Errorf("applicator.run(): structure with size %d", size)
	}
	return nil
}

type builtinspec struct {
	argcount int
	cons     func() builtin
}

var builtintable = map[string]builtinspec{
	"CONCAT": builtinspec{argcount: 2, cons: func() builtin { return &builtinConcat{} }},
}

type compilewalk struct {
	curslot   int            // current slot index when walking
	ssa2local []int          // ssa slot to local slot
	global    map[string]int // bindings introduced here
	err       error

	locals []localop
	out    []outfield

	// SSA state
	p   prog
	mem []*value
}

// compile a FUNC(args...) expression
func (c *compilewalk) compileBuiltin(b *expr.Builtin) builtin {
	spec, ok := builtintable[b.Func.String()]
	if !ok || spec.argcount != len(b.Args) {
		c.err = fmt.Errorf("unrecognized builtin %q", b.Func)
		return nil
	}
	op := spec.cons()
	for i := range b.Args {
		op.setarg(i, c.compileLocal(b.Args[i]))
	}
	return op
}

func (c *compilewalk) compileSSA(n expr.Node) *value {
	// for now, just path expressions
	if _, ok := n.(*expr.Path); ok {
		v, err := compile(&c.p, n)
		if err != nil {
			c.err = err
			return nil
		}
		return v
	}
	return nil
}

func (c *compilewalk) compileLocal(n expr.Node) int {
	if c.err != nil {
		return -1
	}
	// if this expression can be compiled
	// and loaded via SSA, then do that:
	// assign and output ssa stack slot
	// for the output bytes and then
	// remember the mapping from that slot
	// to the local var slot
	if v := c.compileSSA(n); v != nil {
		vmem, err := c.p.Store(c.p.InitMem(), v, stackSlotFromIndex(regV, len(c.ssa2local)))
		if err != nil {
			goto nope
		}
		c.mem = append(c.mem, vmem)
		slot := c.curslot
		c.ssa2local = append(c.ssa2local, slot)
		c.curslot++
		return slot
	}
nope:
	if c.err != nil {
		return -1
	}

	// otherwise, compile this as a local
	// expression and assign it a slot
	op := c.compile(n)
	if op == nil {
		return -1
	}
	slot := c.curslot
	c.locals = append(c.locals, localop{
		op:     op,
		output: slot,
	})
	c.curslot++
	return slot
}

func (c *compilewalk) compileLiteral(d ion.Datum) builtin {
	return &literalBuiltin{datum: d}
}

func (c *compilewalk) compile(n expr.Node) builtin {
	if c.err != nil {
		return nil
	}
	if b, ok := n.(*expr.Builtin); ok {
		return c.compileBuiltin(b)
	}
	if s, ok := n.(expr.String); ok {
		return c.compileLiteral(ion.String(s))
	}

	c.err = fmt.Errorf("cannot compile node %q within builtin", n)
	// TODO: support arbitrary expr.Node;
	// at the very least let's get support
	// for immediates...
	return nil
}

// visit a set of bindings and populate
// c.mem, c.p, c.globals, c.out, c.locals
func (c *compilewalk) visit(bind []expr.Binding) error {
	c.p.Begin()
	c.global = make(map[string]int)
	for i := range bind {
		op := c.compile(bind[i].Expr)
		if c.err != nil {
			return c.err
		}
		if op == nil {
			return fmt.Errorf("cannot compile expr %q", bind[i].Expr)
		}
		c.out = append(c.out, outfield{
			name: bind[i].Result(),
			op:   op,
		})
		c.global[bind[i].Result()] = i
	}
	return nil
}

type Applicator struct {
	dst       QuerySink
	prog      prog
	ssa2local []int
	localops  []localop
	outputs   []outfield
	nlocals   int
}

// Apply applies expressions slowly
//
// FIXME: all of this can be deleted!
func Apply(bind []expr.Binding, dst QuerySink) (*Applicator, error) {
	var cc compilewalk
	err := cc.visit(bind)
	if err != nil {
		return nil, fmt.Errorf("vm.Apply: %w", err)
	}
	cc.p.Return(cc.p.MergeMem(cc.mem...))

	return &Applicator{
		dst:       dst,
		prog:      cc.p,
		ssa2local: cc.ssa2local,
		localops:  cc.locals,
		outputs:   cc.out,
		nlocals:   cc.curslot,
	}, nil
}

func (a *Applicator) Open() (io.WriteCloser, error) {
	dst, err := a.dst.Open()
	if err != nil {
		return nil, err
	}
	// copy all of the ops' internal state
	// in case there is any...
	locals := make([]localop, len(a.localops))
	for i := range locals {
		locals[i].output = a.localops[i].output
		locals[i].op = a.localops[i].op.dup()
	}
	outputs := make([]outfield, len(a.outputs))
	copy(outputs, a.outputs)
	for i := range outputs {
		outputs[i].op = a.outputs[i].op.dup()
	}
	rc, _ := dst.(rowConsumer)
	return splitter(&applicator{
		parent:    a,
		during:    locals,
		outmap:    outputs,
		dst:       dst,
		rc:        rc,
		ssa2local: a.ssa2local,
		nlocals:   a.nlocals,
	}), nil
}

func (a *Applicator) Close() error {
	return a.dst.Close()
}

func (a *applicator) symbolize(st *ion.Symtab) error {
	a.args.st = st
	for i := range a.outmap {
		a.outmap[i].sym = st.Intern(a.outmap[i].name)
	}
	a.args.tmp.Reset()
	st.Marshal(&a.args.tmp, true)
	a.stsize = a.args.tmp.Size()

	sort.Slice(a.outmap, func(i, j int) bool {
		return a.outmap[i].sym < a.outmap[j].sym
	})

	if a.args.locals == nil {
		a.args.locals = make([][]byte, a.nlocals)
	}

	// compile the SSA portion of the application
	err := recompile(st, &a.parent.prog, &a.prog, &a.bc)
	if err != nil {
		return err
	}
	if a.rc != nil {
		return a.rc.symbolize(st)
	}
	return nil
}

func (a *applicator) writeRows(delims []vmref) error {
	if a.bc.compiled == nil {
		panic("applicator.symbolize() not called")
	}
	if len(a.ssa2local) == 0 {
		panic("ssa2local==0")
	}

	fieldsView := a.bc.find(delims, len(a.ssa2local))

	// compute the size of the probe side of the projection
	blockID := 0
	for i := range delims {
		laneID := i & bcLaneCountMask

		for j := range a.ssa2local {
			mem := fieldsView[blockID+j].item(laneID).mem()
			if len(mem) != 0 {
				a.bind(a.ssa2local[j], mem)
			} else {
				a.bind(a.ssa2local[j], nil)
			}
		}
		a.run()

		if laneID == bcLaneCountMask {
			blockID += 1
		}
	}

	// FIXME: actually do padding, alignment, etc.
	_, err := a.dst.Write(a.args.tmp.Bytes())
	a.args.tmp.Set(a.args.tmp.Bytes()[:a.stsize])
	return err
}

func (a *applicator) Close() error {
	return a.dst.Close()
}
