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
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type Input struct {
	// TODO: we should encode the underlying table
	// expr only so different bindings don't cause
	// the same table to be scanned multiple times
	Table  *expr.Table
	Handle TableHandle
}

func (i *Input) encode(dst *ion.Buffer, st *ion.Symtab, rw TableRewrite) error {
	dst.BeginStruct(-1)
	tbl, handle := i.Table, i.Handle
	if rw != nil {
		tbl, handle = rw(tbl, handle)
	}
	dst.BeginField(st.Intern("table"))
	tbl.Encode(dst, st)
	if handle != nil {
		dst.BeginField(st.Intern("handle"))
		err := handle.Encode(dst, st)
		if err != nil {
			return err
		}
	}
	dst.EndStruct()
	return nil
}

// A Tree is the root an executable query plan
// tree as well as the inputs for the plan.
//
// A Tree can be constructed with New
// or NewSplit and it can be executed
// with Exec or Transport.Exec.
type Tree struct {
	// Inputs are the tables to use as inputs to
	// the root of the plan tree.
	Inputs []Input
	// Root is the root node of the plan tree.
	Root Node
}

// A Node is one node of a query plan tree and
// contains the operation sequence for one step
// of the plan, as well as links to subtrees
// this step of the plan depends on.
//
// Simple operations like filtering,
// aggregation, extended projection, etc.
// are grouped into sequences, and
// relational operations and sub-queries
// are handled by connecting their constituent
// subsequences together into a Node.
//
// (One motivating analogy might be that
// of basic blocks within a control flow graph,
// except that we restrict the vertices to
// form a tree rather than any directed graph.)
type Node struct {
	// OutputType is the type of
	// the output column(s) of the
	// sub-query produced by this tree.
	// Note that we cannot always infer
	// the output types of every query.
	// For example, 'SELECT * ...' on data
	// without a schema does not produce
	// a known ResultSet.
	OutputType ResultSet

	// Inputs are the tables to use as inputs to
	// the children of this plan tree node.
	Inputs []Input

	// Children is the list of sub-queries
	// that produce results that are prerequisites
	// to computing this query.
	Children []*Node

	// Op is the first element of a linked list
	// of query execution steps. The linked list
	// is encoded in reverse-execution-order, so
	// Op is the last step in execution order,
	// and the terminal element of the list
	// is the first in execution order.
	Op Op
}

func tabify(n int, dst *strings.Builder) {
	for n > 0 {
		dst.WriteByte('\t')
		n--
	}
}

func tabfprintf(dst *strings.Builder, indent int, f string, args ...interface{}) {
	tabify(indent, dst)
	fmt.Fprintf(dst, f, args...)
}

func tabline(dst *strings.Builder, indent int, line string) {
	tabify(indent, dst)
	dst.WriteString(line)
	dst.WriteByte('\n')
}

func printops(dst *strings.Builder, indent int, op Op) {
	if in := op.input(); in != nil {
		printops(dst, indent, in)
	}
	tabline(dst, indent, op.String())
}

func (t *Tree) describe(dst *strings.Builder) {
	for i := range t.Inputs {
		tbl := expr.ToString(t.Inputs[i].Table)
		fmt.Fprintf(dst, "WITH INPUT(%d) AS %s\n", i, tbl)
	}
	t.Root.describe(0, dst)
}

func (n *Node) describe(indent int, dst *strings.Builder) {
	for i := range n.Children {
		tabfprintf(dst, indent, "WITH REPLACEMENT(%d) AS (\n", i)
		for i := range n.Inputs {
			tbl := expr.ToString(n.Inputs[i].Table)
			tabfprintf(dst, indent+1, "WITH INPUT(%d) AS %s\n", i, tbl)
		}
		n.Children[i].describe(indent+1, dst)
		tabline(dst, indent, ")")
	}
	printops(dst, indent, n.Op)
}

// String implements fmt.Stringer
func (t *Tree) String() string {
	var out strings.Builder
	t.describe(&out)
	return out.String()
}

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	// TODO: we should prepare inputs for scanning
	// before passing them to (*Node).exec
	ep2 := execParams{
		ExecParams: ep,
		inputs:     t.Inputs,
	}
	return t.Root.exec(dst, &ep2)
}

func (n *Node) exec(dst vm.QuerySink, ep *execParams) error {
	if len(n.Children) == 0 {
		return n.Op.exec(dst, ep)
	}
	parallel := ep.Parallel
	var wg sync.WaitGroup
	wg.Add(len(n.Children))
	rp := make([]replacement, len(n.Children))
	errors := make([]error, len(n.Children))
	subp := (parallel + len(n.Children) - 1) / len(n.Children)
	if subp <= 0 {
		subp = 1
	}
	for i := range n.Children {
		go func(i int) {
			defer wg.Done()
			sub := &execParams{
				ExecParams: &ExecParams{
					Output:   nil,
					Parallel: subp,
					Rewrite:  ep.Rewrite,
					Context:  ep.Context,
				},
				inputs: n.Inputs,
			}
			errors[i] = n.Children[i].exec(&rp[i], sub)
			ep.Stats.atomicAdd(&sub.Stats)
		}(i)
	}
	wg.Wait()
	var outerr error
	for i := range errors {
		if errors[i] != nil {
			if outerr == nil {
				outerr = errors[i]
			} else {
				// wrap so that the first error
				// is the one that we see in errors.Unwrap();
				// not sure this is ideal, but there isn't
				// really a way to make a tree out of them
				outerr = fmt.Errorf("%w (and %s)", outerr, errors[i])
			}
		}
	}
	if outerr != nil {
		return outerr
	}
	repl := &replacer{
		inputs: rp,
	}
	n.Op.rewrite(repl)
	if repl.err != nil {
		return repl.err
	}
	return n.Op.exec(dst, ep)
}
