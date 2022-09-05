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
	"math"
	"math/bits"
	"unicode"

	"golang.org/x/exp/slices"
)

func nBitsNeeded(i int) int {
	return bits.Len(uint(i))
}

type DFA struct {
	id     nodeIDT
	key    string
	edges  []edgeT
	start  bool
	accept bool

	symbolSet setT[symbolRangeT]

	// needed for closure
	items setT[nodeIDT]
	trans mapT[symbolRangeT, nodeIDT]
}

func (d *DFA) addEdge(e edgeT) {
	d.edges = slices.Insert(d.edges, 0, e)
}

func addWithMerge(edge1 edgeT, edges *[]edgeT) {
	min1, max1, rlza1 := edge1.symbolRange.split()
	for index2, edge2 := range *edges {
		if edge1.to == edge2.to {
			min2, max2, rlza2 := edge2.symbolRange.split()
			if ((min1 != min2) || (max1 != max2)) && (rlza1 == rlza2) {
				if (max1 + 1) == min2 {
					*edges = slices.Delete(*edges, index2, index2+1) // remove existing edge
					addWithMerge(edgeT{newSymbolRange(min1, max2, rlza1), edge1.to}, edges)
					return
				}
				if (max2 + 1) == min1 {
					*edges = slices.Delete(*edges, index2, index2+1) // remove existing edge
					addWithMerge(edgeT{newSymbolRange(min2, max1, rlza1), edge1.to}, edges)
					return
				}
			}
		}
	}
	*edges = slices.Insert(*edges, len(*edges), edge1)
}

func (d *DFA) mergeEdgeRanges() []edgeT {
	if len(d.edges) <= 1 {
		return d.edges // nothing to merge
	}
	newEdges := make([]edgeT, 0)
	for _, edge := range d.edges {
		addWithMerge(edge, &newEdges)
	}
	return newEdges
}

type DFAStore struct {
	nextID    nodeIDT
	startIDi  nodeIDT
	StartRLZA bool // indicate that the start node has Remaining Length Zero Assertion (RLZA)
	data      map[nodeIDT]*DFA
	maxNodes  int
}

func newDFAStore(maxNodes int) DFAStore {
	return DFAStore{
		nextID:    0,
		startIDi:  -1,
		StartRLZA: false,
		data:      map[nodeIDT]*DFA{},
		maxNodes:  maxNodes,
	}
}

func (store *DFAStore) Dot() *Graphviz {
	result := newGraphiz()
	ids, _ := store.getIDs()
	for _, nodeID := range ids {
		node, _ := store.get(nodeID)
		fromStr := fmt.Sprintf("%v", nodeID)
		result.addNode(fromStr, node.start, node.accept, store.StartRLZA)
		for _, edge := range node.edges {
			result.addEdge(fromStr, fmt.Sprintf("%v", edge.to), edge.symbolRange.String())
		}
	}
	return result
}

func (store *DFAStore) newNode() (nodeIDT, error) {
	if int(store.nextID) >= store.maxNodes {
		return -1, fmt.Errorf("DFA exceeds max number of nodes %v::newNode", store.maxNodes)
	}
	nodeID := store.nextID
	store.nextID++
	node := new(DFA)
	node.id = nodeID
	node.symbolSet = newSet[symbolRangeT]()
	node.items = newSet[nodeIDT]()
	node.trans = newMap[symbolRangeT, nodeIDT]()
	store.data[nodeID] = node
	return nodeID, nil
}

func (store *DFAStore) get(nodeID nodeIDT) (*DFA, error) {
	if dfa, present := store.data[nodeID]; present {
		return dfa, nil
	}
	return nil, fmt.Errorf("DFAStore.get(nodeIDT) c7de4d3a: nodeIDT %v not present in map %v", nodeID, store.data)
}

func (store *DFAStore) startID() (nodeIDT, error) {
	if store.startIDi == -1 {
		for nodeID, dfa := range store.data {
			if dfa.start {
				store.startIDi = nodeID
				return nodeID, nil
			}
		}
		return -1, fmt.Errorf("DFAStore does not have a start node")
	}
	return store.startIDi, nil
}

// getIDs returns vector of unique ids; first element is the start node
func (store *DFAStore) getIDs() (vectorT[nodeIDT], error) {
	ids := make([]nodeIDT, len(store.data))
	if startID, err := store.startID(); err != nil {
		return nil, fmt.Errorf("%v::getIDs", err)
	} else {
		ids[0] = startID
		index := 1
		for nodeID := range store.data {
			if nodeID != startID {
				ids[index] = nodeID
				index++
			}
		}
		return ids, nil
	}
}

// NumberOfNodes return the number of nodes in this automaton
func (store *DFAStore) NumberOfNodes() int {
	return len(store.data)
}

func (store *DFAStore) reachableNodesTraverse(nodeID nodeIDT, reachable *setT[nodeIDT]) {
	if !reachable.contains(nodeID) {
		reachable.insert(nodeID)
		node, _ := store.get(nodeID)
		for _, edge := range node.edges {
			store.reachableNodesTraverse(edge.to, reachable)
		}
	}
}

// reachableNodes return set of all nodes that are reachable from the start-state
func (store *DFAStore) reachableNodes() (*setT[nodeIDT], error) {
	startID, err := store.startID()
	if err != nil {
		return nil, fmt.Errorf("%v::reachableNodes", err)
	}
	reachable := newSet[nodeIDT]()
	store.reachableNodesTraverse(startID, &reachable)
	return &reachable, nil
}

// removeNonReachableNodes removes states that are not reachable from the start-state
func (store *DFAStore) removeNonReachableNodes() error {
	if reachableNodes, err := store.reachableNodes(); err != nil {
		return fmt.Errorf("%v::removeNonReachableNodes", err)
	} else {
		for nodeID := range store.data {
			if !reachableNodes.contains(nodeID) {
				delete(store.data, nodeID)
			}
		}
		return nil
	}
}

func (store *DFAStore) removeEdgesFromAcceptNodes() {
	if store.StartRLZA {
		//NOTE: if the start-node has the remaining length zero assertion, then do not remove edges.
		return
	}
	for _, node := range store.data {
		if node.accept && len(node.edges) > 0 {
			node.edges = node.edges[:0] // remove all edges
		}
	}
}

// mergeAcceptNodes merges all accept states into one single state (and thus reduces the number of states)
func (store *DFAStore) mergeAcceptNodes() {
	// find all accept nodes
	acceptNodeIDs := newVector[nodeIDT]()
	for id, node := range store.data {
		if node.accept {
			acceptNodeIDs.pushBack(id)
		}
	}
	// if there are more than 1 accept nodes, then merge them
	if acceptNodeIDs.size() > 1 {
		// get the smallest accept node id
		newAcceptID := nodeIDT(math.MaxInt32)
		for _, id := range acceptNodeIDs {
			if id < newAcceptID {
				newAcceptID = id
			}
		}
		// update the edges
		for _, node := range store.data {
			for index, edge := range node.edges {
				if acceptNodeIDs.contains(edge.to) {
					node.edges[index].to = newAcceptID
				}
			}
			for symbol, to := range node.trans {
				if acceptNodeIDs.contains(to) {
					node.trans.insert(symbol, newAcceptID)
				}
			}
		}
		// remove accept nodes that are not used anymore
		for _, id := range acceptNodeIDs {
			if id != newAcceptID {
				delete(store.data, id)
			}
		}
	}
}

// HasOnlyASCII return true if the dfa matches only ascii characters
func (store *DFAStore) HasOnlyASCII() bool {
	for _, node := range store.data {
		for _, edge := range node.edges {
			min, max, _ := edge.symbolRange.split()
			if (min > unicode.MaxASCII) || (max > unicode.MaxASCII) {
				return false
			}
		}
	}
	return true
}

// HasRLZA returns whether this automaton contains at least one edge with
// a Remaining Length Zero Assertion (RLZA) '$'
func (store *DFAStore) HasRLZA() bool {
	if store.StartRLZA {
		return true
	}
	for _, node := range store.data {
		for _, edge := range node.edges {
			if _, _, rlza := edge.symbolRange.split(); rlza {
				return true
			}
		}
	}
	return false
}

func (store *DFAStore) rebuildInternals() {
	for _, node := range store.data {
		node.symbolSet.clear()
		node.items.clear()
		node.trans.clear()
		for _, edge := range node.edges {
			node.symbolSet.insert(edge.symbolRange)
			node.items.insert(edge.to)
			node.trans.insert(edge.symbolRange, edge.to)
		}
	}
}

func (store *DFAStore) renumberNodes() error {
	names := newMap[nodeIDT, nodeIDT]()
	newID := nodeIDT(0)
	maxID := nodeIDT(0)
	if ids, err := store.getIDs(); err != nil {
		return fmt.Errorf("%v::renumberNodes", err)
	} else {
		for _, oldID := range ids { //NOTE: first element of getIDs() is the start state
			if oldID > maxID {
				maxID = oldID
			}
			names.insert(oldID, newID)
			newID++
		}
		if maxID >= newID {
			newData := newMap[nodeIDT, *DFA]()
			for oldID, node := range store.data {
				newID := names.at(oldID)
				node.id = newID
				newEdges := newVector[edgeT]()
				for _, edge := range node.edges {
					newEdges.pushBack(edgeT{edge.symbolRange, names.at(edge.to)})
				}
				node.edges = newEdges
				newData.insert(newID, node)
			}
			store.data = newData
		}
		return nil
	}
}

func (store *DFAStore) MergeEdgeRanges() {
	for _, node := range store.data {
		node.edges = node.mergeEdgeRanges()
	}
}
