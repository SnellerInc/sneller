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

import "fmt"

type stackT []nodeIDT

func newStack() stackT {
	var s stackT
	return s
}

// Empty Test whether stack is empty
func (s *stackT) empty() bool {
	return len(*s) == 0
}

// Pop discard next element (if any)
func (s *stackT) pop() {
	if len(*s) > 0 {
		*s = (*s)[:len(*s)-1] // remove next element
	}
}

// Top access next element
func (s *stackT) top() nodeIDT {
	return (*s)[len(*s)-1] // return next element
}

// Push element to stack
func (s *stackT) push(e nodeIDT) {
	*s = append(*s, e) // append to end
}

func getClosure(nodes *vectorT[nodeIDT], nfaStore *NFAStore, dfaStore *DFAStore) (nodeIDT, error) {
	//NOTE nodes is never empty, if it is, that an internal error
	stack := newStack()
	closure := newSet[nodeIDT]()
	symbolSet := newSet[symbolRangeT]()
	accept := false

	for _, nodeID := range *nodes {
		stack.push(nodeID)
		node, err := nfaStore.get(nodeID)
		if err != nil {
			return -1, fmt.Errorf("%v::getClosure", err)
		}
		if node.accept {
			accept = true
		}
		closure.insert(nodeID)
	}
	for !stack.empty() {
		top := stack.top()
		stack.pop()
		node, err := nfaStore.get(top)
		if err != nil {
			return -1, fmt.Errorf("%v::getClosure", err)
		}
		for _, edge := range node.edges {
			if edge.epsilon() {
				if !closure.contains(edge.to) {
					stack.push(edge.to)
					node, err := nfaStore.get(edge.to)
					if err != nil {
						return -1, fmt.Errorf("%v::getClosure", err)
					}
					if node.accept {
						accept = node.accept
					}
					closure.insert(edge.to)
				}
			} else {
				symbolSet.insert(edge.symbolRange)
			}
		}
	}
	resultID, err := dfaStore.newNode() //Note: only place where nodes are created in NFA->DFA
	if err != nil {
		return -1, fmt.Errorf("%v::getClosure", err)
	}
	result, err := dfaStore.get(resultID)
	if err != nil {
		return -1, fmt.Errorf("%v::getClosure", err)
	}
	result.key = joinSortSetInt(&closure)
	result.items = closure
	result.symbolSet = symbolSet
	result.accept = accept
	return resultID, nil
}

func getClosedMove(closureID nodeIDT, symbolRange symbolRangeT, nfaStore *NFAStore, dfaStore *DFAStore) (nodeIDT, error) {
	closure, err := dfaStore.get(closureID)
	if err != nil {
		return -1, fmt.Errorf("%v::getClosedMove", err)
	}
	nextNodes := newVector[nodeIDT]()
	for nodeID := range closure.items {
		node, err := nfaStore.get(nodeID)
		if err != nil {
			return -1, fmt.Errorf("%v::getClosedMove", err)
		}
		for _, edge := range node.edges {
			if symbolRange == edge.symbolRange {
				nextNodes.pushBack(edge.to)
			}
		}
	}
	return getClosure(&nextNodes, nfaStore, dfaStore)
}

// nfaToDfa converts the provided NFA into a DFA
func nfaToDfa(nfaStore *NFAStore, maxNodes int) (*DFAStore, error) {
	dfaStore := newDFAStore(maxNodes)

	v := newVector[nodeIDT]()
	startNode, err := nfaStore.startID()
	if err != nil {
		return nil, fmt.Errorf("%v::nfaToDfa", err)
	}
	v.pushBack(startNode)

	startID, err := getClosure(&v, nfaStore, &dfaStore)
	if err != nil {
		return nil, fmt.Errorf("%v::nfaToDfa", err)
	}
	dfaStore.startIDi = startID

	first, err := dfaStore.get(startID)
	if err != nil {
		return nil, fmt.Errorf("%v::nfaToDfa", err)
	}
	first.start = true

	states := newMap[string, nodeIDT]()
	states.insert(first.key, startID)

	queue := newQueue()
	queue.push(startID)

	for !queue.empty() {
		topID := queue.front()
		queue.pop()
		top, err := dfaStore.get(topID)
		if err != nil {
			return nil, fmt.Errorf("%v::nfaToDfa", err)
		}
		for symbolRange := range top.symbolSet {
			closureID, err := getClosedMove(topID, symbolRange, nfaStore, &dfaStore)
			if err != nil {
				return nil, fmt.Errorf("%v::nfaToDfa", err)
			}
			node, err := dfaStore.get(closureID)
			if err != nil {
				return nil, fmt.Errorf("%v::nfaToDfa", err)
			}
			key := node.key
			if !states.containsKey(key) {
				states.insert(key, closureID)
				queue.push(closureID)
			}
			top.trans.insert(symbolRange, states.at(key))
			top.addEdge(edgeT{symbolRange, states.at(key)})
		}
	}
	dfaStore.removeEdgesFromAcceptNodes()
	if err = dfaStore.removeNonReachableNodes(); err != nil {
		return nil, fmt.Errorf("%v::nfaToDfa", err)
	}
	dfaStore.mergeAcceptNodes()
	return &dfaStore, nil
}
