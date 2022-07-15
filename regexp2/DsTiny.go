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
)

const drawFailState = false

type DsTiny struct {
	Store        *DFAStore
	stateMap     mapT[nodeIDT, nodeIDT]
	charGroupMap mapT[symbolRangeT, int]
}

func NewDsTiny(store *DFAStore) (*DsTiny, error) {
	result := new(DsTiny)
	result.Store = store
	result.stateMap = newMap[nodeIDT, nodeIDT]()
	result.charGroupMap = newMap[symbolRangeT, int]()

	// create character table data and transformation table data
	// NOTE stateID = 0 is reserved for fail states; stateID = 1 is the start state
	if ids, err := store.getIDs(); err != nil { // NOTE the first element in ids is the start node
		return nil, err
	} else {
		stateID := nodeIDT(1)
		for _, dfaID := range ids {
			result.stateMap.insert(dfaID, stateID)
			stateID++
		}
	}
	{
		symbolRanges := newSet[symbolRangeT]()
		for _, node := range store.data {
			for _, edge := range node.edges {
				if edge.epsilon() {
					return nil, fmt.Errorf("illegal epsilon edge in DFA")
				} else {
					symbolRanges.insert(edge.symbolRange.clearRLZA())
				}
			}
		}
		charGroupID := 1 // NOTE charGroupID = 0 is reserved for non-matching characters
		for charGroup := range symbolRanges {
			result.charGroupMap.insert(charGroup, charGroupID)
			charGroupID++
		}
	}
	return result, nil
}

func addCharGroup2Ds(r rune, charGroupID, nBitsStates int, data *[]byte) {
	if r < utf8.RuneSelf { //rune is ASCII
		(*data)[r] = byte(charGroupID << nBitsStates)
	}
}

func addTrans2Ds(stateIDIn nodeIDT, charGroupID int, rlza bool, stateIDOut nodeIDT, nBitsStates, nBits int, data *[]byte) {
	if key := int(stateIDIn) | (charGroupID << nBitsStates); key > 4*64 {
		panic(fmt.Sprintf("addTrans2Ds lookup key %v is too big", key))
	} else {
		if rlza {
			switch nBits {
			case 6:
				(*data)[2*64+key+32] = byte(stateIDOut)
			case 7:
				(*data)[2*64+key+64] = byte(stateIDOut)
			case 8:
				(*data)[2*64+key+128] = byte(stateIDOut)
			}
		} else {
			(*data)[2*64+key] = byte(stateIDOut)
		}
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

func DumpDebug(writer io.Writer, data []byte, nBits, nNodes, nGroups int, rlza, startNodeRLZA bool, regex string) {

	nBitsNodes := nBitsNeeded(nNodes)
	nBitsGroups := nBitsNeeded(nGroups)
	result := ""
	if rlza {
		fmt.Fprintf(writer, "DfaT%v (RLZA-CAPABLE=true bit %v in key is RLZA)\n", nBits, nBits)
	} else {
		fmt.Fprintf(writer, "DfaT%v (RLZA-CAPABLE=false)\n", nBits)
	}
	result += fmt.Sprintf("regex: %v\n", regex)

	switch nBits {
	case 6:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)      | accept map\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group     char   group       | key         next-node   | node        accept\n")
		fmt.Fprintf(writer, "       Z21              Z22         |             Z23         |             Z25\n")
	case 7:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)                              | accept map\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group      char   group      | key          next-node key           next-node  | node         accept\n")
		fmt.Fprintf(writer, "       Z21               Z22        |              Z23                     Z24        |              Z25\n")
	case 8:
		fmt.Fprintf(writer, "char-group map (%v bit)              | update map (%v bit)                                                                              | accept map\n", nBitsGroups, nBitsNodes)
		fmt.Fprintf(writer, "char   group      char  group       | key          next-node  key          next-node  key          next-node  key          next-node  | node         accept\n")
		fmt.Fprintf(writer, "       Z21               Z22        |              Z23                     Z24                     Z25                     Z26        |              Z27\n")
	}

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

		// write accept map
		s4 := ""
		if i <= nNodes {
			offsetByte := 0
			switch nBits {
			case 6:
				offsetByte = (i >> 3) + (3 * 64)
			case 7:
				offsetByte = (i >> 3) + (4 * 64)
			case 8:
				offsetByte = (i >> 3) + (6 * 64)
			}
			offsetBit := i & 0b111
			bit := (data[offsetByte] >> offsetBit) & 1
			s4 = fmt.Sprintf("%v -> %b", prettyBin(i, nBitsNodes), bit)
			if i == 0 {
				s4 += " (fail-state)"
			} else if i == 1 {
				s4 += " (start-state)"
				if startNodeRLZA {
					s4 += " (rlza)"
				}
			}
			if (bit == 1) && (i > 0) {
				s4 += " (accept-state)"
			}
		}
		fmt.Fprintf(writer, "%v | %v | %v\n", s1, s3, s4)
	}
}

func (d *DsTiny) NumberOfGroups() int {
	return d.charGroupMap.size()
}

func (d *DsTiny) DataWithGraphviz(writeDot bool, nBits int, rlza bool) ([]byte, bool, *Graphviz) {

	if (nBits < 6) || (nBits > 9) { // nBits 6,7,8,9 (9 only with rlza) supported
		return make([]byte, 0), false, nil
	}
	if (nBits == 8) && rlza { // when we need to encode 8 bits we do not have space for a rlza bit
		return make([]byte, 0), false, nil
	}

	if d.Store.StartRLZA {
		//NOTE: although it should be possible to handle RLZA in tiny implementations, there are issues
		//(that need fixes) thus (for the time being) handle these regexes with large implementations
		return make([]byte, 0), false, nil
	}
	nNodes := d.Store.NumberOfNodes()
	if nNodes > 64 {
		return make([]byte, 0), false, nil
	}

	nCharGroups := d.NumberOfGroups()
	nBitsCharGroup := nBitsNeeded(nCharGroups)
	nBitsNodes := nBitsNeeded(nNodes)

	//log.Printf("08d5b39d: nBitsCharGroup=%v (nCharGroups=%v); nBitsNodes=%v; (nNodes=%v)", nBitsCharGroup, nCharGroups, nBitsNodes, nNodes)

	if rlza {
		if (nBitsCharGroup + nBitsNodes + 1) > nBits { // plus 1 for the RLZA bit part of state
			return make([]byte, 0), false, nil
		}
	} else {
		if (nBitsCharGroup + nBitsNodes) > nBits {
			return make([]byte, 0), false, nil
		}
	}

	var dot *Graphviz
	if writeDot {
		startID, _ := d.Store.startID()
		dot = newGraphiz()
		for dfaID, nodeID := range d.stateMap {
			node, _ := d.Store.get(dfaID)
			rlza := false
			if d.Store.StartRLZA && (dfaID == startID) {
				rlza = true
			}
			dot.addNodeInt(nodeID, node.start, node.accept, rlza)
		}
	}

	var data []byte
	switch nBits {
	case 6:
		data = make([]byte, (3*64)+8)
	case 7:
		data = make([]byte, (4*64)+8)
	case 8:
		data = make([]byte, (6*64)+8)
	}

	// fill data structure with char transformations
	for symbolRange, charGroupID := range d.charGroupMap {
		min, max, _ := symbolRange.split()
		for r := min; r <= max; r++ {
			addCharGroup2Ds(r, charGroupID, nBitsNodes, &data)
		}
	}
	// fill data structure with state transformations
	for nodeID, node := range d.Store.data {
		fromID := d.stateMap.at(nodeID)

		if node.accept { // optimization: an accept-state will map into itself on all possible inputs
			if writeDot {
				dot.addEdgeInt(fromID, fromID, fmt.Sprintf("%v:%v", 0, ""))
			}
			for symbolRange, charGroupID := range d.charGroupMap {
				if writeDot {
					dot.addEdgeInt(fromID, fromID, fmt.Sprintf("%v:%v", charGroupID, symbolRange))
				}
				addTrans2Ds(fromID, charGroupID, false, fromID, nBitsNodes, nBits, &data)
				if rlza {
					addTrans2Ds(fromID, charGroupID, true, fromID, nBitsNodes, nBits, &data)
				}
			}
		} else {
			charGroupsAlreadyDone := newVector[int]()
			for _, edge := range node.edges {
				toID := d.stateMap.at(edge.to)
				charGroupID := d.charGroupMap.at(edge.symbolRange.clearRLZA())
				if writeDot {
					dot.addEdgeInt(fromID, toID, fmt.Sprintf("%v:%v", charGroupID, edge.symbolRange))
				}
				_, _, rlza := edge.symbolRange.split()
				addTrans2Ds(fromID, charGroupID, rlza, toID, nBitsNodes, nBits, &data)
				charGroupsAlreadyDone.pushBack(charGroupID)
			}
			if writeDot && drawFailState {
				for symbolRange, charGroupID := range d.charGroupMap {
					if !charGroupsAlreadyDone.contains(charGroupID) {
						dot.addEdgeInt(fromID, 0, fmt.Sprintf("%v:%v", charGroupID, symbolRange))
					}
					// NOTE following line is not necessary; it is implicit
					//addTrans2Ds(fromID, charGroupID, 0, nBitsStates, &data)
				}
				dot.addEdgeInt(fromID, 0, fmt.Sprintf("%v:%v", 0, ""))
				// NOTE following line is not necessary; it is implicit
				//addTrans2Ds(fromID, 0, 0, nBitsStates, &data)
			}
		}
	}
	// fill data structure with accept states info
	acceptMask := uint64(0)
	for nodeID, stateID := range d.stateMap {
		if node, err := d.Store.get(nodeID); err == nil {
			if node.accept {
				acceptMask |= uint64(1) << stateID
			}
		}
	}
	if rlza && d.Store.StartRLZA { // use the first node, fail-state for RLZA
		acceptMask |= 1
	}

	switch nBits {
	case 6:
		binary.LittleEndian.PutUint64(data[3*64:], acceptMask)
	case 7:
		binary.LittleEndian.PutUint64(data[4*64:], acceptMask)
	case 8:
		binary.LittleEndian.PutUint64(data[6*64:], acceptMask)
	case 9:
		binary.LittleEndian.PutUint64(data[6*64:], acceptMask)
	}
	return data, true, dot
}

func (d *DsTiny) Data(nBits int, lrza bool) ([]byte, bool) {
	ds, valid, _ := d.DataWithGraphviz(false, nBits, lrza)
	return ds, valid
}
