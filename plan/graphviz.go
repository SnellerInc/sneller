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
	"io"
)

// Graphviz dumps the plan 't'
// to 'dst' as dot(1)-compatible text.
func Graphviz(t *Tree, dst io.Writer) error {
	_, err := io.WriteString(dst, "digraph plan {\n")
	if err != nil {
		return err
	}
	_, _, err = gv(&t.Root, dst, 0, 0)
	if err != nil {
		return err
	}
	_, err = io.WriteString(dst, "}\n")
	return err
}

func gv(n *Node, dst io.Writer, tid, oid int) (int, int, error) {
	_, err := fmt.Fprintf(dst, "subgraph cluster_%d {\n", tid)
	if err != nil {
		return tid, oid, err
	}
	var prev Op
	for o := n.Op; o != nil; o = o.input() {
		fmt.Fprintf(dst, "n%d [label=%q];\n", oid, o.String())
		if prev != nil {
			fmt.Fprintf(dst, "n%d -> n%d;\n", oid, oid-1)
		}
		oid++
		prev = o
	}
	var label string
	if tid == 0 {
		label = "root"
	} else {
		// subqueries are 0-indexed so that the
		// label matches the replacement id
		label = fmt.Sprintf("subquery %d", tid-1)
	}
	_, err = fmt.Fprintf(dst, "label=%q;\ncolor=lightgrey;\n}\n", label)
	if err != nil {
		return tid, oid, err
	}
	tid++
	self := oid - 1 // id of this Tree's terminal
	for i := range n.Children {
		start := oid // id of last op in child
		tid, oid, err = gv(n.Children[i], dst, tid, oid)
		if err != nil {
			return tid, oid, err
		}
		// draw edge from output of last op in child
		// to input of this Tree's terminal
		_, err = fmt.Fprintf(dst, "n%d -> n%d [label=\"REPLACEMENT(%d)\"];\n", start, self, i)
		if err != nil {
			return tid, oid, err
		}
	}
	return tid, oid, err
}
