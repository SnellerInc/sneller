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

package regexp2

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/exp/slices"
)

type Graphviz struct {
	nodes vectorT[string]
	edges vectorT[string]
}

func newGraphiz() *Graphviz {
	return &Graphviz{
		nodes: newVector[string](),
		edges: newVector[string](),
	}
}

func nodeString(id nodeIDT) string {
	return fmt.Sprintf("%v", id)
}

func (dot *Graphviz) addNode(id string, start, accept, rlza bool) {
	if accept {
		if start {
			if rlza {
				dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=doubleoctagon]; #start; accept; RLZA\n", id))
			} else {
				dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=doubleoctagon]; #start; accept\n", id))
			}
		} else {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=doublecircle]; #accept\n", id))
		}
	} else {
		if start {
			if rlza {
				dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=octagon]; #start; RLZA\n", id))
			} else {
				dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=octagon]; #start\n", id))
			}
		} else {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=ellipse];\n", id))
		}
	}
}

func (dot *Graphviz) addNodeInt(id nodeIDT, start, accept, rlza bool) {
	dot.addNode(nodeString(id), start, accept, rlza)
}

func (dot *Graphviz) addEdge(from, to, label string) {
	dot.edges.pushBack(fmt.Sprintf("\ts%v -> s%v [label=\"%v\"];\n", from, to, label))
}

func (dot *Graphviz) addEdgeInt(from, to nodeIDT, label string) {
	dot.addEdge(nodeString(from), nodeString(to), label)
}

func (dot *Graphviz) DotContent(dst io.Writer, graphName, graphTitle string) error {
	_, err := fmt.Fprintf(dst, "digraph %v {\n\trankdir=LR;\n", graphName)
	if err != nil {
		return err
	}
	slices.Sort(dot.nodes)
	for _, s := range dot.nodes {
		_, err := fmt.Fprint(dst, s)
		if err != nil {
			return err
		}
	}
	slices.Sort(dot.edges)
	for _, s := range dot.edges {
		_, err := fmt.Fprint(dst, s)
		if err != nil {
			return err
		}
	}
	graphTitle = strings.Replace(graphTitle, `\`, `\\`, -1)
	_, err = fmt.Fprintf(dst, "\tlabelloc=\"t\";\n\tlabel=\"%v: %v\";\n}\n", graphName, graphTitle)
	return err
}

func (dot *Graphviz) WriteToFile(filename, graphName, graphTitle string) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	dot.DotContent(f, graphName, graphTitle)
}
