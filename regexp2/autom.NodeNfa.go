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
	"unicode"

	"golang.org/x/exp/slices"
)

type nfa struct {
	id     nodeIDT
	key    string
	edges  []edgeT
	start  bool
	accept bool
}

func (n *nfa) addEdgeInternal(e edgeT) {
	n.edges = append(n.edges, e)
}

func (n *nfa) addEdgeRune(symbol rune, to nodeIDT, caseSensitive bool) {
	n.addEdgeInternal(edgeT{newSymbolRange(symbol, symbol, false), to})
	if !caseSensitive {
		for c := unicode.SimpleFold(symbol); c != symbol; c = unicode.SimpleFold(c) {
			n.addEdgeInternal(edgeT{newSymbolRange(c, c, false), to})
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
	nextID     nodeIDT
	startIDi   nodeIDT
	startIdRLZ bool // indicate that the start node has Remaining Length Zero Assertion (RLZ)
	data       map[nodeIDT]*nfa
	maxNodes   int
}

func newNFAStore(maxNodes int) NFAStore {
	return NFAStore{
		nextID:     0,
		startIDi:   -1,
		startIdRLZ: false,
		data:       map[nodeIDT]*nfa{},
		maxNodes:   maxNodes,
	}
}

func (store *NFAStore) dot() *Graphviz {
	result := newGraphiz()
	ids, _ := store.getIDs()
	for _, nodeID := range ids {
		node, _ := store.get(nodeID)
		fromStr := fmt.Sprintf("%v", nodeID)
		result.addNode(fromStr, node.start, node.accept, store.startIdRLZ)
		for _, edge := range node.edges {
			result.addEdge(fromStr, fmt.Sprintf("%v", edge.to), edge.symbolRange.String())
		}
	}
	return result
}

func (store *NFAStore) newNode() (nodeIDT, error) {
	if int(store.nextID) >= store.maxNodes {
		return -1, fmt.Errorf("NFA exceeds max number of nodes %v", store.maxNodes)
	}
	nodeID := store.nextID
	store.nextID++
	node := new(nfa)
	node.id = nodeID
	node.accept = false
	store.data[nodeID] = node
	return nodeID, nil
}

func (store *NFAStore) get(nodeId nodeIDT) (*nfa, error) {
	if nfa, present := store.data[nodeId]; present {
		return nfa, nil
	}
	return nil, fmt.Errorf("NFAStore.get(%v): nfaId %v not present in map %v", nodeId, nodeId, store.data)
}

func (store *NFAStore) startID() (nodeIDT, error) {
	if store.startIDi == -1 {
		for nodeId, node := range store.data {
			if node.start {
				store.startIDi = nodeId
				return nodeId, nil
			}
		}
		return -1, fmt.Errorf("NFAStore does not have a start node")
	}
	return store.startIDi, nil
}

//getIDs returns vector of the ids; first element is the start node
func (store *NFAStore) getIDs() (vectorT[nodeIDT], error) {
	ids := make([]nodeIDT, len(store.data))
	if startId, err := store.startID(); err != nil {
		return nil, err
	} else {
		ids[0] = startId
		index := 1
		for nodeId := range store.data {
			if nodeId != startId {
				ids[index] = nodeId
				index++
			}
		}
		return ids, nil
	}
}

// numberOfStates return the number of nodes in this dfa store
func (store *NFAStore) numberOfStates() int {
	return len(store.data)
}

func (store *NFAStore) reachableNodesTraverse(nodeId nodeIDT, reachable *setT[nodeIDT]) error {
	if !reachable.contains(nodeId) {
		reachable.insert(nodeId)
		if node, err := store.get(nodeId); err != nil {
			return err
		} else {
			for _, edge := range node.edges {
				if err := store.reachableNodesTraverse(edge.to, reachable); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// reachableNodes return set of all nodes that are reachable from the start-state
func (store *NFAStore) reachableNodes() (*setT[nodeIDT], error) {
	if startId, err := store.startID(); err != nil {
		return nil, err
	} else {
		reachable := newSet[nodeIDT]()
		err = store.reachableNodesTraverse(startId, &reachable)
		return &reachable, err
	}
}

// removeNonReachableNodes removes states that are not reachable from the start-state
func (store *NFAStore) removeNonReachableNodes() (bool, error) {
	changed := false
	if reachableNodes, err := store.reachableNodes(); err != nil {
		return false, err
	} else {
		for nodeId := range store.data {
			if !reachableNodes.contains(nodeId) {
				delete(store.data, nodeId)
			}
		}
	}
	return changed, nil
}

func (store *NFAStore) moveRLZAUpstream(nodeID, nodeIDDest nodeIDT, rlza bool, done *setT[nodeIDT]) (err error) {
	// TODO consider using a backref to prevent iterating over all nodes just to find a backref
	if !done.contains(nodeID) {
		done.insert(nodeID)

		if node, _ := store.get(nodeID); node.start {
			// Give the start node the Remaining Length Zero Assertion. If there is a
			// non-empty edge reachable from the start state, then this automaton is not
			// supported. Eg for regex "a|$"
			for _, edge3 := range node.edges {
				if !edge3.epsilon() {
					return fmt.Errorf("remaining Length Zero Assertion $ for non empty regex is not supported")
				}
			}
			store.startIdRLZ = true
			node.addEdge(newSymbolRange(edgeEpsilonRune, edgeEpsilonRune, false), nodeIDDest)
		} else {
			for nodeID2, node2 := range store.data {
				for _, edge2 := range node2.edges {
					if edge2.to == nodeID {
						if edge2.epsilon() {
							if err = store.moveRLZAUpstream(nodeID2, nodeIDDest, rlza, done); err != nil {
								return err
							}
						} else {
							min, max, _ := edge2.symbolRange.split()
							node2.addEdge(newSymbolRange(min, max, rlza), nodeIDDest)
						}
					}
				}
			}
		}
	}
	return nil
}

func (store *NFAStore) removeNode(nodeID nodeIDT) {
	if startNode, _ := store.startID(); startNode != nodeID {
		//NOTE start node cannot be removed
		for _, node := range store.data {
			toRemove := newVector[edgeT]()
			for _, edge := range node.edges {
				if edge.to == nodeID {
					toRemove.pushBack(edge)
				}
			}
			for _, edge := range toRemove {
				node.removeEdge(edge.symbolRange, nodeID)
			}
		}
		delete(store.data, nodeID)
	}
}

func (store *NFAStore) reachableAcceptTraverse(nodeID nodeIDT, reachable *mapT[nodeIDT, bool]) bool {
	if reachable.containsKey(nodeID) {
		return reachable.at(nodeID)
	} else {
		reachable.insert(nodeID, true) // assume that it is reachable, this is to prevent eternal recursion
		if node, _ := store.get(nodeID); node.accept {
			reachable.insert(nodeID, true)
			return true
		} else {
			isReachable := false
			for _, edge := range node.edges {
				if store.reachableAcceptTraverse(edge.to, reachable) {
					reachable.insert(nodeID, true)
					isReachable = true
					//NOTE cannot break this loop since reachable needs to be filled for other edges
				}
			}
			if !isReachable {
				reachable.insert(nodeID, false)
			}
			return isReachable
		}
	}
}

func (store *NFAStore) reachableAccept() mapT[nodeIDT, bool] {
	reachable := newMap[nodeIDT, bool]()
	startID, _ := store.startID()
	store.reachableAcceptTraverse(startID, &reachable)
	return reachable
}

//refactorEdges changes and adds edges such that nodes become choice free
func (store *NFAStore) refactorEdges() (err error) {

	//refactor any edges: replaces any-edges (meta edges) with regular edges with ranges
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

	// move all rlza flags on epsilon edges to non-epsilon edges
	{
		done := newSet[nodeIDT]()

		changed := false
		for nodeId, node := range store.data {
			for _, edge := range node.edges {
				if edge.epsilon() {
					if _, _, rlza := edge.symbolRange.split(); rlza {
						if err := store.moveRLZAUpstream(nodeId, edge.to, rlza, &done); err != nil {
							return err
						}
						node.removeEdge(edge.symbolRange, edge.to) // remove the edge with the flag
						changed = true
					}
				}
			}
		}
		// optional: remove dead nodes, ie remove nodes that cannot reach an accept state
		if changed {
			reachable := store.reachableAccept()
			for nodeId := range store.data {
				if !reachable.containsKey(nodeId) || !reachable.at(nodeId) {
					store.removeNode(nodeId)
				}
			}
		}
	}

	cg := newCharGroupsRange() // only place where symbol ranges are refactored
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.epsilon() {
				cg.add(edge.symbolRange)
			}
		}
	}
	toRemove := newVector[edgeT]()
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.epsilon() {
				if newSymbolRanges, present := cg.refactor(edge.symbolRange); present {
					toRemove.pushBack(edge)
					for _, newSymbolRange := range *newSymbolRanges {
						node.addEdge(newSymbolRange, edge.to)
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
