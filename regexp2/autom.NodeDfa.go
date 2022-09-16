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
	"strconv"
	"unicode"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

const notInitialized = -1

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

// rlza return true when this DFA node contains a RLZ edge
func (d *DFA) rlza() bool {
	for _, edge := range d.edges {
		if edge.rlza() {
			return true
		}
	}
	return false
}

// mergeEdgeRanges merges all regular edges into a new smallest edge range; optionally, the RLZA edge is discarded
func mergeEdgeRanges(edges []edgeT, discardRLZ bool) []edgeT {
	var addWithMerge func(edge1 edgeT, edges *[]edgeT)
	addWithMerge = func(edge1 edgeT, edges *[]edgeT) {
		min1, max1 := edge1.symbolRange.split()
		for index2, edge2 := range *edges {
			if edge1.to == edge2.to {
				min2, max2 := edge2.symbolRange.split()
				if (min1 != min2) || (max1 != max2) {
					if (max1 + 1) == min2 {
						*edges = slices.Delete(*edges, index2, index2+1) // remove existing edge
						addWithMerge(edgeT{newSymbolRange(min1, max2), edge1.to}, edges)
						return
					}
					if (max2 + 1) == min1 {
						*edges = slices.Delete(*edges, index2, index2+1) // remove existing edge
						addWithMerge(edgeT{newSymbolRange(min2, max1), edge1.to}, edges)
						return
					}
				}
			}
		}
		*edges = slices.Insert(*edges, len(*edges), edge1)
	}

	newEdges := make([]edgeT, 0)
	for _, edge := range edges {
		if !(discardRLZ && edge.rlza()) {
			addWithMerge(edge, &newEdges)
		}
	}
	return newEdges
}

type DFAStore struct {
	nextID   nodeIDT
	startIDi nodeIDT
	data     map[nodeIDT]*DFA
	maxNodes int
}

func newDFAStore(maxNodes int) DFAStore {
	return DFAStore{
		nextID:   0,
		startIDi: notInitialized,
		data:     map[nodeIDT]*DFA{},
		maxNodes: maxNodes,
	}
}

func (store *DFAStore) Dot() *Graphviz {
	result := newGraphiz()
	for _, nodeID := range store.getIDs() {
		node, _ := store.get(nodeID)
		fromStr := strconv.Itoa(int(nodeID))
		result.addNode(fromStr, node.start, node.accept, node.rlza())
		for _, edge := range node.edges {
			result.addEdge(fromStr, strconv.Itoa(int(edge.to)), edge.symbolRange.String())
		}
	}
	return result
}

func (store *DFAStore) newNode() (nodeIDT, error) {
	nNodesBefore := store.NumberOfNodes()
	if nNodesBefore >= store.maxNodes {
		store.removeNonReachableNodes()
		nNodesAfter := store.NumberOfNodes()
		if nNodesAfter > (nNodesBefore - 10) {
			return -1, fmt.Errorf("DFA exceeds max number of nodes %v::newNode", store.maxNodes)
		}
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

// acceptNodeID returns the (sole) accepting nodeID
func (store *DFAStore) acceptNodeID() (bool, nodeIDT) {
	for nodeID, node := range store.data {
		if node.accept {
			return true, nodeID
		}
	}
	return false, notInitialized
}

// startID returns the (sole) start nodeID
func (store *DFAStore) startID() (nodeIDT, error) {
	if store.startIDi == notInitialized {
		for nodeID, dfa := range store.data {
			if dfa.start {
				store.startIDi = nodeID
				return nodeID, nil
			}
		}
		return notInitialized, fmt.Errorf("DFAStore does not have a start node")
	}
	return store.startIDi, nil
}

// getIDs returns sorted slice of unique ids
func (store *DFAStore) getIDs() []nodeIDT {
	ids := maps.Keys(store.data)
	slices.Sort(ids)
	return ids
}

// NumberOfNodes return the number of nodes in this automaton
func (store *DFAStore) NumberOfNodes() int {
	return len(store.data)
}

// removeNonReachableNodes removes states that are not reachable from the start-state
func (store *DFAStore) removeNonReachableNodes() error {

	// reachableNodes return set of all nodes that are reachable from the start-state
	reachableNodes := func() (*setT[nodeIDT], error) {

		var reachableNodesTraverse func(nodeID nodeIDT, reachable *setT[nodeIDT])
		reachableNodesTraverse = func(nodeID nodeIDT, reachable *setT[nodeIDT]) {
			if !reachable.contains(nodeID) {
				reachable.insert(nodeID)
				node, _ := store.get(nodeID)
				for _, edge := range node.edges {
					reachableNodesTraverse(edge.to, reachable)
				}
			}
		}

		startID, err := store.startID()
		if err != nil {
			return nil, fmt.Errorf("%v::reachableNodes", err)
		}
		reachable := newSet[nodeIDT]()
		reachableNodesTraverse(startID, &reachable)
		return &reachable, nil
	}

	if reachableNodes, err := reachableNodes(); err != nil {
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

// removeEdgesFromAcceptNodes removes edges from accepting nodes
func (store *DFAStore) removeEdgesFromAcceptNodes() {
	for _, node := range store.data {
		if node.accept {
			rlzaEdge := edgeT{}
			rlzaEdgePresent := false
			for _, edge := range node.edges {
				if edge.rlza() {
					rlzaEdge = edge
					rlzaEdgePresent = true
					break
				}
			}
			node.edges = node.edges[:0] // remove all edges; NOTE unreachable nodes may be created here
			if rlzaEdgePresent {
				node.addEdge(rlzaEdge) // restore RLZA edge
			}
		}
	}
}

// mergeAcceptNodes merges all acceptNodeID states into one single state (and thus reduces the number of states)
func (store *DFAStore) mergeAcceptNodes() {

	merge := func(vec vectorT[nodeIDT]) {
		nNodes := vec.size()
		if nNodes == 0 {
			return
		}
		if nNodes > 1 {
			// get the smallest acceptNodeID node id
			singleNodeID := nodeIDT(math.MaxInt32)
			for _, id := range vec {
				if id < singleNodeID {
					singleNodeID = id
				}
			}
			// update the edges
			for _, node := range store.data {
				for index, edge := range node.edges {
					if vec.contains(edge.to) {
						node.edges[index].to = singleNodeID
					}
				}
			}
			// remove nodes that are not used anymore
			for _, id := range vec {
				if id != singleNodeID {
					delete(store.data, id)
				}
			}
		}
	}

	// find all acceptNodeIDs
	acceptNodeIDs := newVector[nodeIDT]()
	for nodeID, node := range store.data {
		if node.accept {
			acceptNodeIDs.pushBack(nodeID)
		}
	}

	merge(acceptNodeIDs)

	store.removeNonReachableNodes()
	store.rebuildInternals()
}

// HasUnicodeEdge returns true if the store has an edge with a non-ASCII unicode
// symbol excluding a unicode wildcard edge.
func (store *DFAStore) HasUnicodeEdge() bool {
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.rlza() {
				min, max := edge.symbolRange.split()
				isWildcardEdge := (min <= unicode.MaxASCII) && (max == unicode.MaxRune)
				if (max > unicode.MaxASCII) && !isWildcardEdge {
					return true
				}
			}
		}
	}
	return false
}

// HasUnicodeWildcard returns true if the DFA has at least one wildcard edge for ALL non-ASCII values,
// and all other edges are ASCII observations (thus no regular non-ASCII edges)
func (store *DFAStore) HasUnicodeWildcard() (present bool, wildcardRange symbolRangeT) {
	hasAnyEdge := false
	for _, node := range store.data {
		for _, edge := range node.edges {
			if !edge.rlza() {
				min, max := edge.symbolRange.split()
				if (min <= unicode.MaxASCII) && (max == unicode.MaxRune) {
					hasAnyEdge = true
					wildcardRange = edge.symbolRange
				} else if (min > unicode.MaxASCII) || (max > unicode.MaxASCII) {
					return false, 0xFF
				}
			}
		}
	}
	return hasAnyEdge, wildcardRange
}

// HasRLZA returns whether this automaton contains at least one node with
// a Remaining Length Zero Assertion (RLZA) '$'
func (store *DFAStore) HasRLZA() bool {
	for _, node := range store.data {
		if node.rlza() {
			return true
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

func (store *DFAStore) MergeEdgeRanges(discardRLZ bool) {
	for _, node := range store.data {
		node.edges = mergeEdgeRanges(node.edges, discardRLZ)
	}
}
