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
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type queueT []nodeIDT

func newQueue() queueT {
	var q queueT
	return q
}

// Empty test whether queue is empty
func (q *queueT) empty() bool {
	return len(*q) == 0
}

// Pop discard next element (if any)
func (q *queueT) pop() {
	if len(*q) > 0 {
		*q = (*q)[1:] // remove first element
	}
}

// Front access next element
func (q *queueT) front() nodeIDT {
	return (*q)[0] // return first element
}

// Push element to queue
func (q *queueT) push(e nodeIDT) {
	*q = append(*q, e) // append to end
}

type revEdgesT map[edgeT][]nodeIDT

type edgesT map[nodeIDT]map[nodeIDT]setT[symbolRangeT]

func joinSortSetInt(set *setT[nodeIDT]) string {
	var vec2 []nodeIDT = set.toVector()
	slices.Sort(vec2)
	return joinVector(vec2)
}

// joinVector is the inverse of splitVector
func joinVector(vec vectorT[nodeIDT]) string {
	return string(vec)
}

// splitVector is the inverse of joinVector
func splitVector(str string) vectorT[nodeIDT] {
	runes := []rune(str)
	var result vectorT[nodeIDT] = make([]nodeIDT, len(runes))
	for i, r := range runes {
		result[i] = nodeIDT(r)
	}
	return result
}

func getReverseEdges(startID nodeIDT, dfaStore *DFAStore) (setT[symbolRangeT], revEdgesT) {
	queue := newQueue()
	visited := newSet[nodeIDT]()
	symbolSet := newSet[symbolRangeT]()
	revEdges := revEdgesT{} // mapT id to the ids which connects to the id with an alphabet

	queue.push(startID)
	visited.insert(startID)
	for !queue.empty() {
		topID := queue.front()
		queue.pop()
		top, _ := dfaStore.get(topID)
		for symbolRange := range top.symbolSet {
			symbolSet.insert(symbolRange)
			nextID := top.trans.at(symbolRange)

			e := edgeT{symbolRange, nextID}
			revEdges[e] = append(revEdges[e], topID)

			if !visited.contains(nextID) {
				visited.insert(nextID)
				queue.push(nextID)
			}
		}
	}
	return symbolSet, revEdges
}

func hopcroft(symbolSet setT[symbolRangeT], revEdges revEdgesT, dfaStore *DFAStore) partitionsType {
	group1A := newVector[nodeIDT]()
	group2A := newVector[nodeIDT]()
	partitionMap := map[string]vectorT[nodeIDT]{}
	visited := newMap[string, uint32]()
	vec := make([]*string, 0)

	ids, _ := dfaStore.getIDs()

	for _, nodeID := range ids {
		node, _ := dfaStore.get(nodeID)
		if node.accept {
			group1A.pushBack(nodeID)
		} else {
			group2A.pushBack(nodeID)
		}
	}
	{
		key3str := joinVector(ids)
		partitionMap[key3str] = group1A
		vec = append(vec, &key3str)
		visited.insert(key3str, 0)

		if !group2A.empty() {
			key4str := joinVector(group2A)
			partitionMap[key4str] = group2A
			vec = append(vec, &key4str)
		}
	}
	front := 0
	var key1strB strings.Builder
	var key2strB strings.Builder

	for front < len(vec) {
		topStr := vec[front]
		front++

		if topStr != nil {
			top := splitVector(*topStr)

			for symbolRange := range symbolSet {
				revGroup := newBitSet()
				for _, topj := range top {
					e := edgeT{symbolRange, topj}
					if x2, ok2 := revEdges[e]; ok2 {
						for _, x3 := range x2 {
							revGroup.insert(int(x3))
						}
					}
				}
				for partKey, partMap := range partitionMap {
					key1strEmpty := true
					key2strEmpty := true

					for _, nodeID := range partMap {
						if revGroup.contains(int(nodeID)) {
							key1strEmpty = false
							break
						}
					}
					if !key1strEmpty {
						for _, nodeID := range partMap {
							if !revGroup.contains(int(nodeID)) {
								key2strEmpty = false
								break
							}
						}
					}
					if !key1strEmpty && !key2strEmpty {
						key1strB.Reset()
						key2strB.Reset()

						for _, nodeID := range partMap {
							if revGroup.contains(int(nodeID)) {
								key1strB.WriteRune(rune(nodeID))
							} else {
								key2strB.WriteRune(rune(nodeID))
							}
						}
						delete(partitionMap, partKey)
						key1str := key1strB.String()
						key2str := key2strB.String()
						partitionMap[key1str] = splitVector(key1str)
						partitionMap[key2str] = splitVector(key2str)

						if visited.containsKey(key1str) {
							vec[visited[key1str]] = nil
							xKey := uint32(len(vec))
							visited.insert(key1str, xKey)
							visited.insert(key2str, xKey+1)
							vec = append(vec, &key1str, &key2str)
						} else if len(key1str) <= len(key2str) {
							visited.insert(key1str, uint32(len(vec)))
							vec = append(vec, &key1str)
						} else {
							visited.insert(key2str, uint32(len(vec)))
							vec = append(vec, &key2str)
						}
					}
				}
			}
		}
	}
	return maps.Values(partitionMap)
}

type partitionsType []vectorT[nodeIDT]

func buildMinDfa(
	startID nodeIDT,
	partitions partitionsType,
	revEdges revEdgesT,
	dfaStoreOld *DFAStore,
	maxNodes int) (*DFAStore, error) {

	dfaStoreNew := newDFAStore(maxNodes)
	dfaStoreNew.StartRLZA = dfaStoreOld.StartRLZA

	nodes := newVector[nodeIDT]()
	group := newMap[nodeIDT, nodeIDT]()
	edges := edgesT{}

	for i := 0; i < len(partitions); i++ {
		if partitions[i].contains(startID) {
			if i > 0 {
				tmp := partitions[i]
				partitions[i] = partitions[0]
				partitions[0] = tmp
			}
			break
		}
	}
	for i := 0; i < len(partitions); i++ {
		part := partitions[i]

		nodeID, err := dfaStoreNew.newNode()
		if err != nil {
			return nil, fmt.Errorf("%v::buildMinDfa", err)
		}
		node, err := dfaStoreNew.get(nodeID)
		if err != nil {
			return nil, fmt.Errorf("%v::buildMinDfa", err)
		}
		node.id = nodeIDT(i + 1)
		node.key = joinVector(part)
		dfaOld, err := dfaStoreOld.get(part[0])
		if err != nil {
			return nil, fmt.Errorf("%v::buildMinDfa", err)
		}
		node.accept = dfaOld.accept
		node.start = dfaOld.start

		for _, q := range part {
			node.items.insert(q)
			group.insert(q, nodeIDT(i))
		}
		delete(edges, nodeIDT(i))
		nodes.pushBack(nodeID)
	}
	for e, fromSet := range revEdges {
		for _, from := range fromSet {
			gf := group.at(from)
			gt := group.at(e.to)
			if _, present1 := edges[gf]; !present1 {
				edges[gf] = map[nodeIDT]setT[symbolRangeT]{}
			}
			if _, present2 := edges[gf][gt]; !present2 {
				edges[gf][gt] = newSet[symbolRangeT]()
			}
			s := edges[gf][gt]
			s.insert(e.symbolRange)
		}
	}
	for from, toMap := range edges {
		for to, v := range toMap {
			dfaNew, err := dfaStoreNew.get(from)
			if err != nil {
				return nil, fmt.Errorf("%v::buildMinDfa", err)
			}
			for symbolRange := range v {
				dfaNew.addEdge(edgeT{symbolRange, nodes.at(int(to))})
			}
		}
	}
	for _, node := range dfaStoreNew.data {
		node.trans.clear()
		node.items.clear()
	}
	dfaStoreNew.startIDi = nodes.at(0)
	return &dfaStoreNew, nil
}

// minDfa Minimizes the provided DFA with Hopcroft's algorithm and returns a new (minimized) DFA
func minDfa(dfaStore *DFAStore, maxNodes int) (*DFAStore, error) {
	dfaStore.rebuildInternals()
	startNodeID, err := dfaStore.startID()
	if err != nil {
		return nil, fmt.Errorf("%v::minDfa", err)
	}
	symbolSet, revEdges := getReverseEdges(startNodeID, dfaStore)

	partitions := hopcroft(symbolSet, revEdges, dfaStore)
	return buildMinDfa(startNodeID, partitions, revEdges, dfaStore, maxNodes)
}
