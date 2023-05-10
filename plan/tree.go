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
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/vm"
)

type Table struct {
	// TODO: we should encode the underlying table
	// expr only so different bindings don't cause
	// the same table to be scanned multiple times
	Table    *expr.Table
	Contents *Input
}

// A Tree is the root an executable query plan
// tree as well as the inputs for the plan.
//
// A Tree can be constructed with New
// or NewSplit and it can be executed
// with Exec or Transport.Exec.
type Tree struct {
	// Inputs is the global list of inputs for the tree.
	// Each [Node.Input] references an element of this array.
	//
	// (These are stored globally so that the same table
	// referenced multiple times does not consume extra space.)
	Inputs []*Input
	// Data is arbitrary data that can be included
	// along with the tree during serialization.
	Data ion.Datum
	// Root is the root node of the plan tree.
	Root Node

	Results     []expr.Binding
	ResultTypes []expr.TypeSet
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
	if from := op.input(); from != nil {
		printops(dst, indent, from)
	}
	if l, ok := op.(*Leaf); ok {
		tabline(dst, indent, l.describe())
		return
	}
	tabline(dst, indent, op.String())
}

func (t *Tree) describe(dst *strings.Builder) {
	t.Root.describe(0, dst)
}

// String implements fmt.Stringer
func (t *Tree) String() string {
	var out strings.Builder
	t.describe(&out)
	return out.String()
}

// MaxScanned returns the maximum number of scanned
// bytes for this query plan by traversing the plan tree
// and adding TableHandle.Size bytes for each table reference.
func (t *Tree) MaxScanned() int64 {
	ret := int64(0)
	var walk func(*Node)
	walk = func(n *Node) {
		i := n.Input
		if i >= 0 && i < len(t.Inputs) {
			ret += t.Inputs[i].Size()
		}
		for op := n.Op; op != nil; op = op.input() {
			if s, ok := op.(*Substitute); ok {
				for j := range s.Inner {
					walk(s.Inner[j])
				}
			}
		}
	}
	walk(&t.Root)
	return ret
}

// Substitute is an Op that substitutes the result
// of executing a list of Nodes into its input Op.
type Substitute struct {
	Nonterminal

	// Inner is the list of sub-queries that need
	// their results substituted into the input
	// of this Substitute Op. The order of Inner elements
	// is important, as each Inner node i is used to substitute
	// results into the *REPLACEMENT(i) expressions.
	Inner []*Node
}

func (s *Substitute) exec(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	rp := make([]replacement, len(s.Inner))
	var wg sync.WaitGroup
	wg.Add(len(s.Inner))
	errlist := make([]error, len(s.Inner))
	for i := range s.Inner {
		subex := ep.clone()
		go func(i int) {
			defer wg.Done()
			errlist[i] = s.Inner[i].exec(&rp[i], subex)
			ep.Stats.atomicAdd(&subex.Stats)
		}(i)
	}
	wg.Wait()
	if err := errors.Join(errlist...); err != nil {
		return err
	}
	ep.AddRewrite(&replacer{inputs: rp, simpl: expr.Simplifier(expr.NoHint)})
	defer ep.PopRewrite()
	return s.From.exec(dst, src, ep)
}

func (s *Substitute) encode(dst *ion.Buffer, st *ion.Symtab, ep *ExecParams) error {
	dst.BeginStruct(-1)
	settype("substitute", dst, st)
	dst.BeginField(st.Intern("inner"))
	dst.BeginList(-1)
	for i := range s.Inner {
		if err := s.Inner[i].encode(dst, st, ep); err != nil {
			return err
		}
	}
	dst.EndList()
	dst.EndStruct()
	return nil
}

func (s *Substitute) SetField(f ion.Field) error {
	switch f.Label {
	case "inner":
		return f.UnpackList(func(v ion.Datum) error {
			nn := &Node{}
			err := nn.decode(v)
			if err != nil {
				return err
			}
			s.Inner = append(s.Inner, nn)
			return nil
		})
	default:
		return errUnexpectedField
	}
}

// String implements fmt.Stringer
func (s *Substitute) String() string {
	var dst strings.Builder
	for i := range s.Inner {
		tabfprintf(&dst, 0, "WITH REPLACEMENT(%d) AS (\n", i)
		s.Inner[i].describe(1, &dst)
		tabline(&dst, 0, ")")
	}
	return dst.String()
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
	// Input is the original input associated with this Op.
	Input int
	// Op is the first element of a linked list
	// of query execution steps. The linked list
	// is encoded in reverse-execution-order, so
	// Op is the last step in execution order,
	// and the terminal element of the list
	// is the first in execution order.
	Op Op
}

func (n *Node) describe(indent int, dst *strings.Builder) {
	printops(dst, indent, n.Op)
}

// String implements fmt.Stringer
func (n *Node) String() string {
	var out strings.Builder
	n.describe(0, &out)
	return out.String()
}
