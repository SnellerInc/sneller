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
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"

	"golang.org/x/exp/slices"
)

// DsLarge is a data structure for the large DFA implementation
type DsLarge struct {
	data []uint32
}

func edgeSort(srn []edgeT) {
	slices.SortFunc(srn, func(edge1, edge2 edgeT) bool {
		return edge1.symbolRange < edge2.symbolRange
	})
}

func selectStates(store *DFAStore) ([]nodeIDT, mapT[nodeIDT, *DFA]) {
	result := newMap[nodeIDT, *DFA]()
	selected := newVector[nodeIDT]()
	notSelected := newVector[nodeIDT]()

	startID, _ := store.startID()
	startNode, _ := store.get(startID)
	if startNode.accept {
		// special situation: the start node is also accepting -> empty DFA
		return selected, result
	}
	if startNode.accept && startNode.rlza() && (len(startNode.edges) == 1) {
		return selected, result
	}

	selected.pushBack(startID)

	for _, nodeID := range store.getIDs() {
		if nodeID != startID {
			node, _ := store.get(nodeID)
			if node.accept {
				notSelected.pushBack(nodeID)
			} else if node.rlza() && (len(node.edges) == 1) {
				notSelected.pushBack(nodeID)
			} else {
				selected.pushBack(nodeID)
			}
		}
	}

	translation := newMap[nodeIDT, nodeIDT]()
	{
		newID := nodeIDT(0)
		for _, nodeID := range selected {
			translation.insert(nodeID, newID)
			newID++
		}
		for _, nodeID := range notSelected {
			translation.insert(nodeID, newID)
			newID++
		}
	}

	for oldID, newID := range translation {
		newEdges := make([]edgeT, 0)
		oldNode, _ := store.get(oldID)
		for _, edge := range oldNode.edges {
			newEdges = append(newEdges, edgeT{edge.symbolRange, translation.at(edge.to)})
		}
		result.insert(newID, &DFA{
			id:        newID,
			edges:     newEdges,
			start:     oldNode.start,
			accept:    oldNode.accept,
			key:       "",
			symbolSet: nil,
			items:     nil,
			trans:     nil,
		})
	}

	selected2 := make([]nodeIDT, 0)
	for _, nodeID := range selected {
		selected2 = append(selected2, translation.at(nodeID))
	}
	slices.Sort(selected2)
	return selected2, result
}

// NewDsLarge creates a data structure that is accepted by the large DFA
func NewDsLarge(store *DFAStore) (*DsLarge, error) {

	result := new(DsLarge)
	result.data = make([]uint32, 0)

	// special situation where there are no regular edges,
	// corresponding to regex "$", (|)$, etc; his gives DFA "-> s0 -$> s1"
	if store.NumberOfNodes() == 2 {
		startID, _ := store.startID()
		startNode, _ := store.get(startID)
		if (len(startNode.edges) == 1) && startNode.rlza() {
			result.addInt(0xFFFFFFFF) // add -1 to indicate that there is only 1 state and this state has RLZA
			return result, nil
		}
	}

	selectedStates, data := selectStates(store)
	nStates := len(selectedStates)
	result.addInt(nStates)

	for _, stateID := range selectedStates {
		if data.containsKey(stateID) {
			newNode := data.at(stateID)
			mergedEdges := mergeEdgeRanges(newNode.edges, true)

			// write symbol ranges to the data-structure
			nEdges := len(mergedEdges)
			if nEdges == 0 {
				return nil, fmt.Errorf("internal error: NewDsLarge retrieved an empty node which is invalid")
			}
			result.addInt((nEdges * 12) + 8) // add the total bytes of edges; used in 33839A60
			result.addInt(nEdges)            // add number of edges

			edgeSort(mergedEdges) //NOTE: sort is NOT necessary but makes debugging the data-structure easier
			for _, edge := range mergedEdges {
				min, max := edge.symbolRange.split()
				result.addUint32(runeToUtf8int(min)) // first int32 is UTF8 with min of range
				result.addUint32(runeToUtf8int(max)) // second int32 is UTF8 with max of range
				result.addUint32(nodeIDForDs(edge.to, data.at(edge.to)))
			}
		}
	}
	return result, nil
}

func (d *DsLarge) addUint32(nStates uint32) {
	d.data = append(d.data, nStates)
}

func (d *DsLarge) addInt(i int) {
	d.addUint32(uint32(i))
}

func nodeIDForDs(id nodeIDT, node *DFA) uint32 {
	result := uint32(id + 1) //NOTE plus 1 because node0 is reserved for fail node
	if node.accept {
		result |= 0x40000000 // set second-highest significant bit to indicate this state has the 'accept' property
	}
	if node.rlza() {
		result |= 0x80000000 // set the highest significant bit to indicate this state has the 'RLZ' property
	}
	return result
}

// runeToUtf8int transforms a rune to a UTF8 byte sequence; eg: for ſ (unicode 0x17F) yield 0000C5BF (UTF8)
func runeToUtf8int(r rune) uint32 {
	buf := make([]byte, 4)
	utf8.EncodeRune(buf, r)
	for i := 0; i < 4; i++ { // strip leading zero bytes
		if buf[3] == 0 {
			buf[3] = buf[2]
			buf[2] = buf[1]
			buf[1] = buf[0]
			buf[0] = 0
		}
	}
	return binary.BigEndian.Uint32(buf)
}

func utf8intToRune(v uint32) rune {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	r, _ := utf8.DecodeLastRune(buf)
	return r
}

// DumpDebug dumps this data structure with annotations to dst
func (d *DsLarge) DumpDebug(dst io.Writer) (err error) {
	nNodes := d.data[0]

	if nNodes == 0xFFFFFFFF { // special situation when there are no regular edges: DFA -> s0 -$> s1
		fmt.Fprintf(dst, "0xFFFFFFFF\t(special situation start node has RLZA; LARGE DFA DATA-STRUCTURE)\n")
		fmt.Fprintf(dst, "  (nNodes=1; nEdgesTotal=0)\n")
	}

	_, err = fmt.Fprintf(dst, "%08X\t(#nodes; LARGE DFA DATA-STRUCTURE)\n", d.data[0])
	if err != nil {
		return err
	}
	nEdgesTotal := 0
	index := 1
	for i := 0; i < int(nNodes); i++ {
		index++ // read away the number of bytes in edges
		nEdges := int(d.data[index])
		nEdgesTotal += nEdges
		_, err := fmt.Fprintf(dst, "  %08X\t(nodeIDT=%08X)\n", nEdges, i+1)
		if err != nil {
			return err
		}
		index++
		for j := 0; j < nEdges; j++ {
			min := d.data[index]
			max := d.data[index+1]

			rlzaStr := ""
			if ((d.data[index+2] >> 31) & 1) == 1 {
				rlzaStr = ", rlza "
			}
			acceptStr := ""
			if ((d.data[index+2] >> 30) & 1) == 1 {
				acceptStr = ", accept"
			}

			if min == max {
				_, err = fmt.Fprintf(dst, "    %08X %08X %08X\t(min=%##U%v%v)\n", min, max, d.data[index+2], utf8intToRune(min), rlzaStr, acceptStr)
			} else {
				_, err = fmt.Fprintf(dst, "    %08X %08X %08X\t(min=%##U; max=%##U%v%v)\n", min, max, d.data[index+2], utf8intToRune(min), utf8intToRune(max), rlzaStr, acceptStr)
			}
			if err != nil {
				return err
			}
			index += 3
		}
	}
	_, err = fmt.Fprintf(dst, "  (nNodes=%v; nEdgesTotal=%v)\n", nNodes, nEdgesTotal)
	return err
}

func (d *DsLarge) Data() []byte {
	//TODO currently inefficient copy, a pointer cast would be a lot faster
	result := make([]byte, len(d.data)*4)
	for i, v := range d.data {
		binary.LittleEndian.PutUint32(result[i*4:i*4+4], v)
	}
	return result
}
