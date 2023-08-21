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
	"slices"
	"strings"
	"unicode/utf8"
)

type DsTiny struct {
	Store        *DFAStore
	stateMap     mapT[nodeIDT, stateIDT]
	charGroupMap mapT[symbolRangeT, groupIDT]
}

type symbolRangeIDT int

type tupleT struct {
	symbolRangeID symbolRangeIDT
	groupID       groupIDT
}

func (t tupleT) String() string {
	return fmt.Sprintf("(%v,%v)", t.symbolRangeID, t.groupID)
}

func compressGroups(groupData [][]symbolRangeIDT) (result []tupleT) {

	commonGroup := func(idx1, idx2 int, groupData [][]symbolRangeIDT, done setT[symbolRangeIDT]) (result []symbolRangeIDT) {

		contains := func(data []symbolRangeIDT, element symbolRangeIDT) bool {
			_, found := slices.BinarySearch(data, element)
			return found
		}

		// isSubset returns true if a is a subset of b
		isSubset := func(a, b []symbolRangeIDT) bool {
			for _, x := range a {
				if !contains(b, x) {
					return false
				}
			}
			return true
		}

		// intersect returns the intersection of the sorted slice A and sorted slice B
		intersect := func(a, b []symbolRangeIDT) (result []symbolRangeIDT) {
			i := 0
			j := 0
			for i < len(a) && j < len(b) {
				// If the elements are the same, add it to the intersection slice
				if a[i] == b[j] {
					result = append(result, a[i])
					i++
					j++
				} else if a[i] < b[j] {
					i++
				} else {
					j++
				}
			}
			return result
		}

		// subtract removes all elements in toRemove from data
		subtract := func(data []symbolRangeIDT, toRemove setT[symbolRangeIDT]) (result []symbolRangeIDT) {
			for _, e := range data {
				if !toRemove.contains(e) {
					result = append(result, e)
				}
			}
			return result
		}

		element := groupData[idx1][idx2]

		first := true
		for _, symbolRangeIDs2 := range groupData {
			if contains(symbolRangeIDs2, element) {
				if first {
					result = symbolRangeIDs2
					first = false
				} else {
					result = intersect(result, symbolRangeIDs2)
				}
			}
		}

		// remove all elements from result that are already done
		result = subtract(result, done)
		symbolRangeIDs := groupData[idx1]

		toRemove := newSet[symbolRangeIDT]()
		for i, e := range symbolRangeIDs {
			if i == idx2 {
				continue
			}
			for k, y := range groupData {
				if k == idx1 {
					continue
				}
				// all elements of y should be in result2, if not then element 2 should be removed from result2
				if slices.Contains(y, e) && !isSubset(result, y) {
					toRemove.insert(e)
				}
			}
		}
		return subtract(result, toRemove)
	}

	charGroupID := groupIDT(1) // NOTE charGroupID = 0 is reserved for non-matching characters
	done := newSet[symbolRangeIDT]()

	for idx1, symbolRangeIDs := range groupData {
		for idx2, symbolRangeID1 := range symbolRangeIDs {
			if !done.contains(symbolRangeID1) {
				newGroup := commonGroup(idx1, idx2, groupData, done)
				for _, symbolRangeID2 := range newGroup {
					done.insert(symbolRangeID2)
					result = append(result, tupleT{groupID: charGroupID, symbolRangeID: symbolRangeID2})
				}
				charGroupID++
			}
		}
	}
	return result
}

func createCharGroupMap(store *DFAStore) (charGroupMap mapT[symbolRangeT, groupIDT], err error) {
	// create dataMap with all edges of the DFA
	dataMap1 := newMap[string, *setT[symbolRangeT]]()

	for _, node := range store.data {
		for _, edge := range node.edges {
			if edge.epsilon() {
				return nil, fmt.Errorf("createCharGroupMap: illegal epsilon edge in DFA")
			}
			if !edge.rlza() {
				key := fmt.Sprintf("%v->%v", node.id, edge.to)
				if dataMap1.containsKey(key) {
					data := dataMap1.at(key)
					data.insert(edge.symbolRange)
					dataMap1.insert(key, data)
				} else {
					data := newSet[symbolRangeT]()
					data.insert(edge.symbolRange)
					dataMap1.insert(key, &data)
				}
			}
		}
	}

	// create translations from symbolRangeT to an int identifier
	translationA := newMap[symbolRangeIDT, symbolRangeT]()
	translationB := newMap[symbolRangeT, symbolRangeIDT]()
	symbolRangeID := symbolRangeIDT(0)

	for _, symbolRanges := range dataMap1 {
		for symbolRange := range *symbolRanges {
			if !translationB.containsKey(symbolRange) {
				translationB.insert(symbolRange, symbolRangeID)
				translationA.insert(symbolRangeID, symbolRange)
				symbolRangeID++
			}
		}
	}

	// create another dataMap with all edges of the DFA based on the int identifier
	dataMap2 := make([][]symbolRangeIDT, 0)
	for _, symbolRanges := range dataMap1 {
		data2 := make([]symbolRangeIDT, symbolRanges.len())
		idx := 0
		for symbolRange := range *symbolRanges {
			data2[idx] = translationB.at(symbolRange)
			idx++
		}
		slices.Sort(data2)
		dataMap2 = append(dataMap2, data2)
	}

	charGroupMap = newMap[symbolRangeT, groupIDT]()
	for _, tuple := range compressGroups(dataMap2) {
		charGroupMap.insert(translationA.at(tuple.symbolRangeID), tuple.groupID)
	}
	return charGroupMap, nil
}

func NewDsTiny(store *DFAStore) (*DsTiny, error) {
	result := new(DsTiny)
	result.Store = store
	result.stateMap = newMap[nodeIDT, stateIDT]()

	// create map that translates nodeIDs from the DFAStore to stateIDs from the DFA data-structure
	// NOTE stateID = 0 is the fail-state; stateID = 1 is the start-state
	ids := store.getIDs()
	stateID := stateIDT(1)
	for _, nodeID := range ids {
		result.stateMap.insert(nodeID, stateID)
		stateID++
	}
	var err error
	result.charGroupMap, err = createCharGroupMap(store)
	return result, err
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
	groupIDs := newSet[groupIDT]()
	for _, v := range d.charGroupMap {
		groupIDs.insert(v)
	}
	return groupIDs.len()
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
	var groupAlreadyDone setT[string]
	if writeDot {
		groupAlreadyDone = newSet[string]()
	}

	for nodeID, node := range d.Store.data {
		stateID := d.stateMap.at(nodeID)

		if node.accept { // optimization: a regular acceptNodeID-state will map into itself on all possible inputs
			addTrans2Ds(stateID, 0, stateID, nBitsNodes, nBits, &data)
			for _, charGroupID := range d.charGroupMap {
				addTrans2Ds(stateID, charGroupID, stateID, nBitsNodes, nBits, &data)
			}
		} else {
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
						// retrieve all edges from the same group, and make a nice label for graphviz
						key := fmt.Sprintf("%v->%v(%v)", stateID, toStateID, charGroupID)
						if !groupAlreadyDone.contains(key) {
							groupAlreadyDone.insert(key)
							symbolRanges := make([]string, 0)
							for k, v := range d.charGroupMap {
								if v == charGroupID {
									symbolRanges = append(symbolRanges, k.String())
								}
							}
							slices.Sort(symbolRanges)
							label := strings.Join(symbolRanges, ",")
							dot.addEdgeInt(stateID, toStateID, fmt.Sprintf("%v:%v", charGroupID, label))
						}
					}
					addTrans2Ds(stateID, charGroupID, toStateID, nBitsNodes, nBits, &data)
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
