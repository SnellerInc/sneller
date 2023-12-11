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

package regexp2

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
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

func nodeString(id stateIDT) string {
	return fmt.Sprintf("%v", id)
}

func (dot *Graphviz) addNode(id string, start, accept, rlza bool) {
	colour := ""
	if rlza {
		colour = "; color=\"red\""
	}
	if accept {
		if start {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=doubleoctagon%v]; #start; acceptNodeID\n", id, colour))
		} else {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=doublecircle%v]; #acceptNodeID\n", id, colour))
		}
	} else {
		if start {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=octagon%v]; #start\n", id, colour))
		} else {
			dot.nodes.pushBack(fmt.Sprintf("\ts%v [shape=ellipse%v];\n", id, colour))
		}
	}
}

func (dot *Graphviz) addNodeInt(stateID stateIDT, start, accept, rlza bool) {
	dot.addNode(nodeString(stateID), start, accept, rlza)
}

func (dot *Graphviz) addEdge(from, to, label string) {
	dot.edges.pushBack(fmt.Sprintf("\ts%v -> s%v [label=\"%v\"];\n", from, to, label))
}

func (dot *Graphviz) addEdgeInt(from, to stateIDT, label string) {
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
	graphTitle = strings.ReplaceAll(graphTitle, `\`, `\\`)
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
