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
	"sort"
	"strconv"
	"strings"

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
	length := set.size()
	vec2 := make([]nodeIDT, length)
	index := 0
	for nodeID := range *set {
		vec2[index] = nodeID
		index++
	}
	slices.Sort(vec2)

	var sb strings.Builder
	sb.WriteString(fmt.Sprint(vec2[0]))
	for i := 1; i < length; i++ {
		sb.WriteRune('.')
		sb.WriteString(fmt.Sprint(vec2[i]))
	}
	return sb.String()
}

func joinVector(vec *vectorT[nodeIDT]) string {
	arr := make([]string, vec.size())
	for index, nodeID := range *vec {
		arr[index] = fmt.Sprint(nodeID)
	}
	return strings.Join(arr[:], splitChar)
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
	visited := newMap[string, int]()
	vec := make([]string, 0)
	keySplitMap := map[string]vectorT[nodeIDT]{}

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
		key3str := joinVector(&ids)
		partitionMap[key3str] = group1A
		vec = append(vec, key3str)
		keySplitMap[key3str] = ids
		visited.insert(key3str, 0)

		if !group2A.empty() {
			key4str := joinVector(&group2A)
			partitionMap[key4str] = group2A
			vec = append(vec, key4str)
			keySplitMap[key4str] = group2A
		}
	}
	front := 0
	for front < len(vec) {
		top := vec[front]
		front++

		if top != "EMPTY" {
			top2 := keySplitMap[top]

			for symbolRange := range symbolSet {
				revGroup := newSet[nodeIDT]()
				for _, topj := range top2 {
					e := edgeT{symbolRange, topj}
					if x2, ok2 := revEdges[e]; ok2 {
						for _, x3 := range x2 {
							revGroup.insert(x3)
						}
					}
				}
				keys := make([]string, len(partitionMap))
				for key := range partitionMap {
					keys = append(keys, key)
				}
				//sort.Strings(keys[:]) // seems not necessary
				for _, keyX := range keys {
					group1B := newVector[nodeIDT]()
					group2B := newVector[nodeIDT]()

					for _, p := range partitionMap[keyX] {
						if revGroup.contains(p) {
							group1B.pushBack(p)
						} else {
							group2B.pushBack(p)
						}
					}
					if !group1B.empty() && !group2B.empty() {
						delete(partitionMap, keyX)
						key1str := joinVector(&group1B)
						key2str := joinVector(&group2B)
						partitionMap[key1str] = group1B
						partitionMap[key2str] = group2B

						if visited.containsKey(key1str) {
							vec[visited[key1str]] = "EMPTY"
							visited.insert(key1str, len(vec))
							vec = append(vec, key1str)
							keySplitMap[key1str] = group1B
							visited.insert(key2str, len(vec))
							vec = append(vec, key2str)
							keySplitMap[key2str] = group2B
						} else if group1B.size() <= group2B.size() {
							visited.insert(key1str, len(vec))
							vec = append(vec, key1str)
							keySplitMap[key1str] = group1B
						} else {
							visited.insert(key2str, len(vec))
							vec = append(vec, key2str)
							keySplitMap[key2str] = group2B
						}
					}
				}
			}
		}
	}
	partitions := make(partitionsType, len(partitionMap))
	{
		//TODO make efficient sort
		tmp := make([]string, len(partitionMap))
		index := 0
		for _, v := range partitionMap {
			tmp[index] = joinVector(&v)
			index++
		}
		sort.Strings(tmp[:])

		for i, v := range tmp {
			partitions[i] = make([]nodeIDT, 0)

			for _, s := range strings.Split(v, splitChar) {
				x, _ := strconv.ParseInt(s, 10, 64)
				partitions[i] = append(partitions[i], nodeIDT(x))
			}
		}
	}
	return partitions
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
			return nil, err
		}
		node, err := dfaStoreNew.get(nodeID)
		if err != nil {
			return nil, err
		}
		node.id = nodeIDT(i + 1)
		node.key = joinVector(&part)
		dfaOld, err := dfaStoreOld.get(part[0])
		if err != nil {
			return nil, err
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
				return nil, err
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

//minDfa Minimizes the provided DFA with Hopcroft's algorithm and returns a new (minimized) DFA
func minDfa(dfaStore *DFAStore, maxNodes int) (*DFAStore, error) {
	dfaStore.rebuildInternals()
	startNodeID, err := dfaStore.startID()
	if err != nil {
		return nil, err
	}
	symbolSet, revEdges := getReverseEdges(startNodeID, dfaStore)

	partitions := hopcroft(symbolSet, revEdges, dfaStore)
	return buildMinDfa(startNodeID, partitions, revEdges, dfaStore, maxNodes)
}
