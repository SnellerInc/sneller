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
