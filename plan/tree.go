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

	"github.com/SnellerInc/sneller/vm"
)

// Tree is an executable query plan
// represented as a tree of linear subsequences
// of operations.
// Simple operations like filtering,
// aggregation, extended projection, etc.
// are grouped into sequences, and
// relational operations and sub-queries
// are handled by connecting their constituent
// subsequences together into a Tree.
//
// (One motivating analogy might be that
// of basic blocks within a control flow graph,
// except that we restrict the vertices to
// form a tree rather than any directed graph.)
//
// A Tree can be constructed with New
// or NewSplit and it can be executed
// with Exec or Transport.Exec.
type Tree struct {
	// OutputType is the type of
	// the output column(s) of the
	// sub-query produced by this tree.
	// Note that we cannot always infer
	// the output types of every query.
	// For example, 'SELECT * ...' on data
	// without a schema does not produce
	// a known ResultSet.
	OutputType ResultSet

	// Children is the list of sub-queries
	// that produce results that are prerequisites
	// to computing this query.
	Children []*Tree

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

func (t *Tree) describe(indent int, dst *strings.Builder) {
	for i := range t.Children {
		tabfprintf(dst, indent, "WITH REPLACEMENT(%d) AS (", i)
		t.Children[i].describe(indent+1, dst)
		tabline(dst, indent, ")")
	}
	printops(dst, indent, t.Op)
}

// String implements fmt.Stringer
func (t *Tree) String() string {
	var out strings.Builder
	t.describe(0, &out)
	return out.String()
}

func (t *Tree) exec(dst vm.QuerySink, parallel int, stats *ExecStats, rw TableRewrite) error {
	if len(t.Children) == 0 {
		return t.Op.exec(dst, parallel, stats, rw)
	}
	var wg sync.WaitGroup
	wg.Add(len(t.Children))
	rp := make([]replacement, len(t.Children))
	errors := make([]error, len(t.Children))
	subp := (parallel + len(t.Children) - 1) / len(t.Children)
	if subp <= 0 {
		subp = 1
	}
	for i := range t.Children {
		go func(i int) {
			defer wg.Done()
			errors[i] = t.Children[i].exec(&rp[i], subp, stats, rw)
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
	t.Op.rewrite(repl)
	if repl.err != nil {
		return repl.err
	}
	return t.Op.exec(dst, parallel, stats, rw)
}
