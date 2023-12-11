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
	"math"
	"slices"
	"strconv"
	"unicode"

	"golang.org/x/exp/maps"
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
		if err := store.pruneUnreachable(); err != nil {
			return -1, err
		}
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
func (store *DFAStore) mergeAcceptNodes() error {

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

	if err := store.pruneUnreachable(); err != nil {
		return fmt.Errorf("%v::mergeAcceptNodes", err)
	}
	if err := store.mergeConsecutiveRLZ(); err != nil {
		return fmt.Errorf("%v::mergeAcceptNodes", err)
	}
	return nil
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

// mergeConsecutiveRLZ merges Consecutive nodes with only RLZ edges:
// "a -$> b; b -$> c" becomes "a -$> c"
func (store *DFAStore) mergeConsecutiveRLZ() error {

	nodeDone := newSet[nodeIDT]()
	nodeDone.insert(store.startIDi)

	changed := true
	for changed {
		changed = false
	label:
		for _, node1 := range store.data {
			if !node1.rlza() {
				continue
			}
			for edge1Idx, edge1 := range node1.edges {
				if !edge1.rlza() {
					continue
				}
				nodeID2 := edge1.to
				node2, err := store.get(nodeID2)
				if err != nil {
					return err
				}
				if node2.rlza() && (len(node2.edges) == 1) && !nodeDone.contains(nodeID2) {
					nodeDone.insert(nodeID2)
					node1.edges[edge1Idx].to = node2.edges[0].to // this may create unreachable nodes
					changed = true
					goto label // jump out of both for-loops
				}
			}
		}
	}
	return store.pruneUnreachable()
}

// pruneUnreachable removes nodes that are unreachable from the start node
func (store *DFAStore) pruneUnreachable() error {
	reachable := func() (setT[nodeIDT], error) {
		nodesTodo := newSet[nodeIDT]()
		nodesTodo.insert(store.startIDi)
		nodesReachable := newSet[nodeIDT]()
		for !nodesTodo.empty() {
			for nodeID1 := range nodesTodo {
				nodesTodo.erase(nodeID1)
				if nodesReachable.contains(nodeID1) {
					continue
				}
				nodesReachable.insert(nodeID1)
				node1, err := store.get(nodeID1)
				if err != nil {
					return nil, err
				}
				for _, edge1 := range node1.edges {
					nodesTodo.insert(edge1.to)
				}
			}
		}
		return nodesReachable, nil
	}
	{
		// find nodes that are reachable from the start node
		nodesReachable, err := reachable()
		if err != nil {
			return err
		}
		// remove all nodes that are unreachable
		for nodeID := range store.data {
			if !nodesReachable.contains(nodeID) {
				delete(store.data, nodeID)
			}
		}
	}
	return nil
}

// pruneNeverAccepting removes nodes that cannot reach an accepting node
func (store *DFAStore) pruneNeverAccepting() error {

	acceptReachable := newSet[nodeIDT]()
	{
		// create a reverse lookup table
		reverse := newMap[nodeIDT, *setT[nodeIDT]]()
		for nodeID1, node1 := range store.data {
			for _, edge := range node1.edges {
				nodeID2 := edge.to
				if reverse.containsKey(nodeID2) {
					reverse.at(nodeID2).insert(nodeID1)
				} else {
					s := newSet[nodeIDT]()
					s.insert(nodeID1)
					reverse.insert(nodeID2, &s)
				}
			}
		}

		// find out which nodes will reach an accepting node
		nodesTodo := newSet[nodeIDT]()
		if present, acceptID := store.acceptNodeID(); present {
			nodesTodo.insert(acceptID)
		}
		for !nodesTodo.empty() {
			for nodeID1 := range nodesTodo {
				nodesTodo.erase(nodeID1)
				if !acceptReachable.contains(nodeID1) {
					acceptReachable.insert(nodeID1)

					if reverse.containsKey(nodeID1) {
						for nodeID2 := range *reverse.at(nodeID1) {
							nodesTodo.insert(nodeID2)
						}
					}
				}
			}
		}
	}

	// remove all nodes that do not reach an accepting node,
	// but do not remove the start node
	startID, err := store.startID()
	if err != nil {
		return err
	}
	startNode, err := store.get(startID)
	if err != nil {
		return err
	}
	for nodeID := range store.data {
		if !acceptReachable.contains(nodeID) {
			if nodeID == startID {
				// we were about to remove the start node, don't do that
				// but do remove all outgoing edges from the start node
				startNode.edges = startNode.edges[:0]
			} else {
				delete(store.data, nodeID)
			}
		}
	}
	return nil
}

// IsTrivial return whether the DFA is a trivial automation; if
// the DFA is trivial, accept indicate whether the DFA always accepts
// or always rejects
func (store *DFAStore) IsTrivial() (trivial, accept bool) {
	// check if an accepting node exists, if there are none than the machine cannot accept
	present, acceptID := store.acceptNodeID()
	if !present {
		return true, false
	}
	// there are thus at least one node (the accept-node)
	startID, err := store.startID()
	if err != nil {
		// there was no start node
		return true, false
	}
	if startID == acceptID {
		// the start node is also accepting, this machine accepts anything
		return true, true
	}
	// the DFA is not trivial
	return false, false
}
