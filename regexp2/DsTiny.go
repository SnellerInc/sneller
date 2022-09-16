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
	"math/bits"
	"unicode/utf8"
)

type DsTiny struct {
	Store        *DFAStore
	stateMap     mapT[nodeIDT, stateIDT]
	charGroupMap mapT[symbolRangeT, groupIDT]
}

func NewDsTiny(store *DFAStore) (*DsTiny, error) {
	result := new(DsTiny)
	result.Store = store
	result.stateMap = newMap[nodeIDT, stateIDT]()
	result.charGroupMap = newMap[symbolRangeT, groupIDT]()

	// create map that translates nodeIDs from the DFAStore to stateIDs from the DFA data-structure
	// NOTE stateID = 0 is the fail-state; stateID = 1 is the start-state
	ids := store.getIDs()
	stateID := stateIDT(1)
	for _, nodeID := range ids {
		result.stateMap.insert(nodeID, stateID)
		stateID++
	}

	// create character table data
	symbolRanges := newSet[symbolRangeT]()
	for _, node := range store.data {
		for _, edge := range node.edges {
			if edge.epsilon() {
				return nil, fmt.Errorf("NewDsTiny: illegal epsilon edge in DFA")
			}
			if !edge.rlza() {
				symbolRanges.insert(edge.symbolRange)
			}
		}
	}
	charGroupID := groupIDT(1) // NOTE charGroupID = 0 is reserved for non-matching characters
	for symbolRange := range symbolRanges {
		result.charGroupMap.insert(symbolRange, charGroupID)
		charGroupID++
	}
	return result, nil
}

func addCharGroup2Ds(r rune, charGroupID groupIDT, nBitsStates int, data *[]byte) {
	if r < utf8.RuneSelf { //rune is ASCII
		(*data)[r] = byte(charGroupID << nBitsStates)
	}
}

func addTrans2Ds(stateIDIn stateIDT, charGroupID groupIDT, stateIDOut stateIDT, nBitsStates, nBits int, data *[]byte) {
	if key := int(stateIDIn) | (int(charGroupID) << nBitsStates); key > 4*64 {
		panic(fmt.Sprintf("addTrans2Ds lookup key %v is too big", key))
	} else {
		(*data)[2*64+key] = byte(stateIDOut)
	}
}

func prettyBin(value int, pos int) string {
	v := fmt.Sprintf("%08b", value)
	pos = 8 - pos
	if pos < len(v) {
		return v[:pos] + "'" + v[pos:]
	}
	return v
}

func DumpDebug(writer io.Writer, data []byte, nBits, nNodes, nGroups int, regex string) {
	nBitsNodes := bits.Len(uint(nNodes))
	nBitsGroups := bits.Len(uint(nGroups))
	result := ""
	fmt.Fprintf(writer, "DfaT%v\n", nBits)
	result += fmt.Sprintf("regex: %v\n", regex)

	switch nBits {
	case 6:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)      |\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group     char    group      | key          next-node  | node      type\n")
		fmt.Fprintf(writer, "       Z21               Z22        |              Z23        |\n")
	case 7:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)                              |\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group      char   group      | key          next-node key           next-node  | node      type\n")
		fmt.Fprintf(writer, "       Z21               Z22        |              Z23                     Z24        |\n")
	case 8:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)                                                                              |\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group      char  group       | key          next-node  key          next-node  key          next-node  key          next-node  | node      type\n")
		fmt.Fprintf(writer, "       Z21               Z22        |              Z23                     Z24                     Z25                     Z26        |\n")
	}

	offset := 0
	switch nBits {
	case 6:
		offset = 3 * 64
	case 7:
		offset = 4 * 64
	case 8:
		offset = 6 * 64
	}
	acceptNodeID := int(binary.LittleEndian.Uint32(data[offset+6:]))
	rlzMask := binary.LittleEndian.Uint64(data[offset+10:])

	for i := 0; i < 64; i++ {
		// write character map
		s1 := ""
		if i > 32 {
			s1 = fmt.Sprintf("'%v' -> %v; ", string(rune(i)), prettyBin(int(data[i]), nBitsNodes))
		} else {
			s1 = fmt.Sprintf("%02Xh -> %v; ", i, prettyBin(int(data[i]), nBitsNodes))
		}
		j := i + 64
		if j < 127 {
			s1 += fmt.Sprintf("'%v' -> %v;", string(rune(j)), prettyBin(int(data[j]), nBitsNodes))
		} else {
			s1 += fmt.Sprintf("%02Xh -> %v;", j, prettyBin(int(data[j]), nBitsNodes))
		}

		// write translation map
		s3 := ""
		switch nBits {
		case 6:
			s3 = fmt.Sprintf("%v -> %v;",
				prettyBin(i, nBitsNodes), prettyBin(int(data[i+(2*64)]), nBitsNodes))
		case 7:
			s3 = fmt.Sprintf("%v -> %v; %v -> %v;",
				prettyBin(i+(0*64), nBitsNodes), prettyBin(int(data[i+(2*64)]), nBitsNodes),
				prettyBin(i+(1*64), nBitsNodes), prettyBin(int(data[i+(3*64)]), nBitsNodes))
		case 8:
			s3 = fmt.Sprintf("%v -> %v; %v -> %v; %v -> %v; %v -> %v;",
				prettyBin(i+(0*64), nBitsNodes), prettyBin(int(data[i+(2*64)]), nBitsNodes),
				prettyBin(i+(1*64), nBitsNodes), prettyBin(int(data[i+(3*64)]), nBitsNodes),
				prettyBin(i+(2*64), nBitsNodes), prettyBin(int(data[i+(4*64)]), nBitsNodes),
				prettyBin(i+(3*64), nBitsNodes), prettyBin(int(data[i+(5*64)]), nBitsNodes))
		}

		// write state information
		s4 := ""
		if i <= nNodes {
			s4 = prettyBin(i, nBitsNodes)
			if i == 0 {
				s4 += " (fail-state)"
			} else if i == 1 {
				s4 += " (start-state)"
			}
			if i == acceptNodeID {
				s4 += " (accept-state)"
			}
			if ((rlzMask >> i) & 1) == 1 {
				s4 += " (rlz-state)"
			}
		}
		fmt.Fprintf(writer, "%v | %v | %v\n", s1, s3, s4)
	}
}

func (d *DsTiny) NumberOfGroups() int {
	return d.charGroupMap.size()
}

func (d *DsTiny) DataWithGraphviz(writeDot bool, nBits int, hasWildcard bool, wildcardRange symbolRangeT) ([]byte, bool, *Graphviz) {
	if (nBits < 6) || (nBits > 9) { // nBits 6,7,8 supported
		return []byte{}, false, nil
	}

	nNodes := d.Store.NumberOfNodes()
	nCharGroups := d.NumberOfGroups()
	nBitsCharGroup := bits.Len(uint(nCharGroups))
	nBitsNodes := bits.Len(uint(nNodes))

	if (nBitsCharGroup + nBitsNodes) > nBits {
		return []byte{}, false, nil
	}

	var dot *Graphviz
	if writeDot {
		dot = newGraphiz()
		for nodeID, stateID := range d.stateMap {
			node, _ := d.Store.get(nodeID)
			dot.addNodeInt(stateID, node.start, node.accept, node.rlza())
		}
	}

	offset := 0
	var data []byte
	switch nBits {
	case 6:
		offset = 3 * 64
	case 7:
		offset = 4 * 64
	case 8:
		offset = 6 * 64
	}
	data = make([]byte, offset+18) // 18 = 2b for wildcardFlag, 4b for wildcardCharGroupID, 4b for acceptNodeID; 8b for rlzNodeIDs;

	// fill data structure with char transformations
	for symbolRange, charGroupID := range d.charGroupMap {
		min, max := symbolRange.split()
		for r := min; r <= max; r++ {
			addCharGroup2Ds(r, charGroupID, nBitsNodes, &data)
		}
	}
	// fill data structure with state transformations
	for nodeID, node := range d.Store.data {
		stateID := d.stateMap.at(nodeID)

		if node.accept { // optimization: a regular acceptNodeID-state will map into itself on all possible inputs
			addTrans2Ds(stateID, 0, stateID, nBitsNodes, nBits, &data)
			for _, charGroupID := range d.charGroupMap {
				addTrans2Ds(stateID, charGroupID, stateID, nBitsNodes, nBits, &data)
			}
		} else {
			charGroupsAlreadyDone := newVector[groupIDT]()
			for _, edge := range node.edges {
				if edge.rlza() {
					// only draw the edge in the .dot; no need to add it to the transitions since it handled differently
					if writeDot {
						if present, acceptNodeID := d.Store.acceptNodeID(); present {
							if d.stateMap.containsKey(acceptNodeID) {
								dot.addEdgeInt(stateID, d.stateMap.at(acceptNodeID), "<$>")
							}
						}
					}
				} else {
					toStateID := d.stateMap.at(edge.to)
					charGroupID := d.charGroupMap.at(edge.symbolRange)
					if writeDot {
						dot.addEdgeInt(stateID, toStateID, fmt.Sprintf("%v:%v", charGroupID, edge.symbolRange))
					}
					addTrans2Ds(stateID, charGroupID, toStateID, nBitsNodes, nBits, &data)
					charGroupsAlreadyDone.pushBack(charGroupID)
				}
			}
		}
	}

	wildcardCharGroupID := groupIDT(0)
	wildcardFlag := uint16(0)
	if hasWildcard {
		if d.charGroupMap.containsKey(wildcardRange) {
			wildcardCharGroupID = d.charGroupMap.at(wildcardRange) << nBitsNodes
			wildcardFlag = 0xFFFF
		}
	}

	acceptStateID := stateIDT(0xFF) // set it to something that is an invalid nodeID
	if present, acceptNodeID := d.Store.acceptNodeID(); present {
		acceptStateID = d.stateMap.at(acceptNodeID)
	}

	rlzMask := uint64(0)
	for nodeID, stateID := range d.stateMap {
		if stateID > 64 {
			// very unlikely but we found a RLZ stateID larger than 64, not supported.
			return []byte{}, false, nil
		}
		node, _ := d.Store.get(nodeID)
		if node.rlza() {
			rlzMask |= uint64(1) << stateID
		}
	}

	binary.LittleEndian.PutUint16(data[offset+0:], wildcardFlag)
	binary.LittleEndian.PutUint32(data[offset+2:], uint32(wildcardCharGroupID))
	binary.LittleEndian.PutUint32(data[offset+6:], uint32(acceptStateID))
	binary.LittleEndian.PutUint64(data[offset+10:], rlzMask)

	return data, true, dot
}

// Data creates the data-structure with the provided parameters
func (d *DsTiny) Data(nBits int, hasUnicodeWildcard bool, wildcardRange symbolRangeT) ([]byte, bool) {
	ds, valid, _ := d.DataWithGraphviz(false, nBits, hasUnicodeWildcard, wildcardRange)
	return ds, valid
}
