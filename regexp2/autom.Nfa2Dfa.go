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
	if err = dfaStore.pruneUnreachable(); err != nil {
		return nil, fmt.Errorf("%v::nfaToDfa", err)
	}
	dfaStore.mergeAcceptNodes()
	return &dfaStore, nil
}
