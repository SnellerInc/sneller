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
	"slices"
	"unicode"

	"golang.org/x/exp/maps"
)

type nfa struct {
	id     nodeIDT
	edges  []edgeT
	start  bool
	accept bool
}

func (n *nfa) addEdgeInternal(e edgeT) {
	n.edges = append(n.edges, e)
}

func (n *nfa) addEdgeRune(symbol rune, to nodeIDT, caseSensitive bool) {
	n.addEdgeInternal(edgeT{newSymbolRange(symbol, symbol), to})
	if !caseSensitive {
		for c := unicode.SimpleFold(symbol); c != symbol; c = unicode.SimpleFold(c) {
			n.addEdgeInternal(edgeT{newSymbolRange(c, c), to})
		}
	}
}

func (n *nfa) addEdge(symbolRange symbolRangeT, to nodeIDT) {
	n.addEdgeInternal(edgeT{symbolRange, to})
}

func (n *nfa) removeEdge(symbolRange symbolRangeT, to nodeIDT) {
	for index, edge := range n.edges {
		if (edge.to == to) && (edge.symbolRange == symbolRange) {
			n.edges = slices.Delete(n.edges, index, index+1)
			return
		}
	}
}

type NFAStore struct {
	nextID    nodeIDT
	startIDi  nodeIDT
	startRLZA bool // indicate that the start node has Remaining Length Zero Assertion (RLZ)
	data      map[nodeIDT]*nfa
	maxNodes  int
}

func newNFAStore(maxNodes int) NFAStore {
	return NFAStore{
		nextID:    0,
		startIDi:  notInitialized,
		startRLZA: false,
		data:      map[nodeIDT]*nfa{},
		maxNodes:  maxNodes,
	}
}

func (store *NFAStore) dot() *Graphviz {
	result := newGraphiz()
	for _, nodeID := range store.getIDs() {
		node, _ := store.get(nodeID)
		fromStr := fmt.Sprintf("%v", nodeID)
		result.addNode(fromStr, node.start, node.accept, false)
		for _, edge := range node.edges {
			result.addEdge(fromStr, fmt.Sprintf("%v", edge.to), edge.symbolRange.String())
		}
	}
	return result
}

func (store *NFAStore) newNode() (nodeIDT, error) {
	if len(store.data) >= store.maxNodes {
		return -1, fmt.Errorf("NFA exceeds max number of nodes %v::newNode", store.maxNodes)
	}
	nodeID := store.nextID
	store.nextID++
	node := new(nfa)
	node.id = nodeID
	node.accept = false
	store.data[nodeID] = node
	return nodeID, nil
}

func (store *NFAStore) get(nodeID nodeIDT) (*nfa, error) {
	if nfa, present := store.data[nodeID]; present {
		return nfa, nil
	}
	return nil, fmt.Errorf("NFAStore.get(%v): nfaId %v not present in map %v", nodeID, nodeID, store.data)
}

func (store *NFAStore) startID() (nodeIDT, error) {
	if store.startIDi == notInitialized {
		for nodeID, node := range store.data {
			if node.start {
				store.startIDi = nodeID
				return nodeID, nil
			}
		}
		return notInitialized, fmt.Errorf("NFAStore does not have a start node")
	}
	return store.startIDi, nil
}

// getIDs returns sorted slice of unique ids
func (store *NFAStore) getIDs() vectorT[nodeIDT] {
	ids := maps.Keys(store.data)
	slices.Sort(ids)
	return ids
}

// refactorEdges changes and adds edges such that nodes become choice free
func (store *NFAStore) refactorEdges() (err error) {

	// refactor any edges: replaces any-edges (meta edges) with regular edges with ranges
	for _, node := range store.data {
		for index1, anyEdge := range node.edges {
			sr := anyEdge.symbolRange
			if (sr == edgeAnyRange) || (sr == edgeAnyNotLfRange) {
				symbolRanges := anyEdge.symbolRanges()
				for index2, edge2 := range node.edges {
					if index1 != index2 { // remove range of regular edges from the range of the any-edge
						symbolRanges = symbolRangeSubtract(symbolRanges, edge2.symbolRanges())
					}
				}
				// replace any-edge with new edges in symbolRanges
				node.removeEdge(sr, anyEdge.to)            // remove the old any-edge
				for _, symbolRange := range symbolRanges { // add new regular edge
					node.addEdge(symbolRange, anyEdge.to)
				}
			}
		}
	}

	cg := newCharGroupsRange() // only place where symbol ranges are refactored
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.epsilon() && !edge.rlza() {
				cg.add(edge.symbolRange)
			}
		}
	}
	toRemove := newVector[edgeT]()
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.epsilon() && !edge.rlza() {
				if newSymbolRanges, present := cg.refactor(edge.symbolRange); present {
					toRemove.pushBack(edge)
					for _, newSymbolRange2 := range *newSymbolRanges {
						node.addEdge(newSymbolRange2, edge.to)
					}
				}
			}
		}
		for _, edge := range toRemove {
			node.removeEdge(edge.symbolRange, edge.to)
		}
		toRemove.clear()
	}
	return nil
}

// cleanupStaleEdges removes edges that point to removed nodes
func (store *NFAStore) cleanupStaleEdges() {
	nodeIDs := newSet[nodeIDT]()
	for _, nodeID := range maps.Keys(store.data) {
		nodeIDs.insert(nodeID)
	}
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !nodeIDs.contains(edge.to) {
				node.removeEdge(edge.symbolRange, edge.to)
			}
		}
	}
}

// pruneRLZ removes nodes that are 'after' the RLZA ('$')
// and are thus unreachable
func (store *NFAStore) pruneRLZ() error {

	// get the nodeIDs that are eligible for prune
	// 1. add all nodes that are reachable from a $-node.
	//    a $-node is a node that has an incoming $-edge.
	// 2. remove all nodes that are reachable from the
	//    start node, but stop traversing when a $-edge
	//    is encountered.
	// 3. remove all these eligible nodes from the NFA.

	eligible := newSet[nodeIDT]()
	done := newSet[nodeIDT]()

	var reachable1 func(nodeID nodeIDT) error
	reachable1 = func(nodeID nodeIDT) error {
		if done.contains(nodeID) {
			return nil
		}
		done.insert(nodeID)

		node, err := store.get(nodeID)
		if err != nil {
			return err
		}
		for _, edge := range node.edges {
			if edge.rlza() || edge.epsilon() {
				if err := reachable1(edge.to); err != nil {
					return err
				}
			} else {
				eligible.insert(edge.to)
			}
		}
		return nil
	}

	var reachable2 func(nodeID nodeIDT) error
	reachable2 = func(nodeID nodeIDT) error {
		if done.contains(nodeID) {
			return nil
		}
		done.insert(nodeID)

		eligible.erase(nodeID)
		node, err := store.get(nodeID)
		if err != nil {
			return err
		}
		for _, edge := range node.edges {
			if !edge.rlza() {
				if err := reachable2(edge.to); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// 1. add all nodes that are reachable from a $-node.
	for _, node := range store.data {
		for _, edge := range node.edges {
			if edge.rlza() {
				if err := reachable1(edge.to); err != nil {
					return err
				}
			}
		}
	}
	done.clear()

	// 2. remove all nodes that are reachable from the
	if err := reachable2(store.startIDi); err != nil {
		return err
	}

	// 3. remove all these eligible nodes from the NFA.
	changed := false
	for nodeID := range eligible {
		changed = true
		delete(store.data, nodeID)
	}
	if changed {
		store.cleanupStaleEdges()
	}
	return nil
}
