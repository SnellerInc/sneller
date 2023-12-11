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
	var children []*Node
	for o := n.Op; o != nil; o = o.input() {
		fmt.Fprintf(dst, "n%d [label=%q];\n", oid, o.String())
		if prev != nil {
			fmt.Fprintf(dst, "n%d -> n%d;\n", oid, oid-1)
		}
		if s, ok := o.(*Substitute); ok {
			children = s.Inner
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
	for i := range children {
		start := oid // id of last op in child
		tid, oid, err = gv(children[i], dst, tid, oid)
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
