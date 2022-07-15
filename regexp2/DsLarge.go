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
	data        []uint32
	rlzaCapable bool
}

func edgeSort(srn []edgeT) {
	slices.SortFunc(srn, func(edge1, edge2 edgeT) bool {
		return edge1.symbolRange < edge2.symbolRange
	})
}

//NewDsLarge creates a data structure that is accepted by the large DFA
func NewDsLarge(store *DFAStore, rlzaCapable bool) (*DsLarge, error) {
	result := new(DsLarge)
	result.rlzaCapable = rlzaCapable
	result.data = make([]uint32, 0)
	result.addInt(store.NumberOfNodes())

	ids, _ := store.getIDs()
	slices.Sort(ids) // NOTE sorting necessary to get node0 (start node) first

	for _, nodeID := range ids {
		node, err := store.get(nodeID)
		if err != nil {
			return nil, err
		}
		// get the edge ranges, and merge if possible
		newEdges := node.mergeEdgeRanges()
		// write symbol ranges to the data-structure
		nTransitions := len(newEdges)
		result.addInt(nTransitions)

		edgeSort(newEdges) //NOTE: sort is NOT necessary but makes debugging the data-structure easier
		for _, edge := range newEdges {
			min, max, rlza := edge.symbolRange.split()
			if !rlzaCapable && rlza {
				return nil, fmt.Errorf("rlza not supported (with rlzaCapable set false)")
			}
			result.addUint32(runeToUtf8int(min)) // first int32 is UTF8 with min of range
			result.addUint32(runeToUtf8int(max)) // second int32 is UTF8 with max of range

			node, err := nodeIDForDs(edge.to, store, rlza)
			if err != nil {
				return nil, err
			}
			result.addUint32(node)
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

func nodeIDForDs(id nodeIDT, store *DFAStore, rlza bool) (uint32, error) {
	node, err := store.get(id)
	if err != nil {
		return 0, err
	}
	result := uint32(id + 1) //NOTE plus 1 because node0 is reserved for fail node
	if node.accept {
		result |= 0x40000000 //0b0100 set bit to indicate this node is an accepting node
	}
	if rlza {
		result |= 0x80000000
	}
	return result, nil
}

//runeToUtf8int transforms a rune to a UTF8 byte sequence; eg: for ſ (unicode 0x17F) yield 0000C5BF (UTF8)
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
	_, err = fmt.Fprintf(dst, "%08X\t(#nodes; RLZA-CAPABLE=%v; LARGE DFA DATA-STRUCTURE)\n", nNodes, d.rlzaCapable)
	if err != nil {
		return err
	}
	nEdgesTotal := 0
	index := 1
	for i := 0; i < int(nNodes); i++ {
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
