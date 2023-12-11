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
	"testing"
	"unicode/utf8"
)

func equalEdges(edges1, edges2 []edgeT) bool {
	for _, edge1 := range edges1 {
		present := false
		for _, edge2 := range edges2 {
			if (edge1.symbolRange == edge2.symbolRange) && (edge1.to == edge2.to) {
				present = true
				break
			}
		}
		if !present {
			return false
		}
	}
	return true
}

func edgesToString(edges []edgeT) string {
	result := ""
	for _, edge := range edges {
		result += fmt.Sprintf("%v->%v;", edge.symbolRange, edge.to)
	}
	return result
}

func TestMergeEdgeRanges(t *testing.T) {
	{
		node := new(DFA)
		node.addEdge(edgeT{newSymbolRange('a', 'a'), 0})
		node.addEdge(edgeT{newSymbolRange('b', 'b'), 0})
		newEdges := mergeEdgeRanges(node.edges, false)

		expected := newVector[edgeT]()
		expected.pushBack(edgeT{newSymbolRange('a', 'b'), 0})

		if !equalEdges(newEdges, expected) {
			t.Errorf("A: Observed %v expected %v\n", edgesToString(newEdges), edgesToString(expected))
		}
	}
	{
		node := new(DFA)
		node.addEdge(edgeT{newSymbolRange('a', 'c'), 0})
		node.addEdge(edgeT{newSymbolRange('d', 'e'), 1})
		newEdges := mergeEdgeRanges(node.edges, false)

		expected := newVector[edgeT]()
		expected.pushBack(edgeT{newSymbolRange('a', 'c'), 0})
		expected.pushBack(edgeT{newSymbolRange('d', 'e'), 1})

		if !equalEdges(newEdges, expected) {
			t.Errorf("B: Observed %v expected %v\n", edgesToString(newEdges), edgesToString(expected))
		}
	}
	{
		node := new(DFA)
		node.addEdge(edgeT{newSymbolRange('a', 'c'), 0})
		node.addEdge(edgeT{newSymbolRange('d', 'e'), 0})
		newEdges := mergeEdgeRanges(node.edges, false)

		expected := newVector[edgeT]()
		expected.pushBack(edgeT{newSymbolRange('a', 'e'), 0})

		if !equalEdges(newEdges, expected) {
			t.Errorf("C: Observed %v expected %v\n", edgesToString(newEdges), edgesToString(expected))
		}
	}
	{
		node := new(DFA)
		node.addEdge(edgeT{newSymbolRange('H', 'H'), 1})
		node.addEdge(edgeT{newSymbolRange('I', utf8.MaxRune), 1})
		node.addEdge(edgeT{newSymbolRange('D', 'G'), 1})
		newEdges := mergeEdgeRanges(node.edges, false)

		expected := newVector[edgeT]()
		expected.pushBack(edgeT{newSymbolRange('D', utf8.MaxRune), 1})

		if !equalEdges(newEdges, expected) {
			t.Errorf("D: Observed %v expected %v\n", edgesToString(newEdges), edgesToString(expected))
		}
	}
}
