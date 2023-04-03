// Copyright (C) 2023 Sneller, Inc.
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

package iguana

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	maxUint24  = (1 << 24) - 1
	maxUint16  = (1 << 16) - 1
	maxVarUint = 254*254*254 - 1
)

const (
	varUIntThreshold1B = 254
	varUIntThreshold3B = 254 * 254
	varUIntThreshold4B = 254 * 254 * 254
)

const (
	costToken    = 1
	costOffs16   = 2
	costOffs24   = 3
	costInfinite = math.MaxInt32
)

const (
	cmdCopyRaw        byte = 0x00
	cmdDecodeIguana   byte = 0x01
	cmdDecodeANS      byte = 0x02
	lastCommandMarker byte = 0x80
	cmdMask                = ^lastCommandMarker
)

const (
	minOffset = 64
	minLength = 64
)

type encodingStation struct {
	dst               []byte
	ctrl              []byte
	lastCommandOffset int
}

func Compress(src []byte, dst []byte, ansRejectionThreshold float32) ([]byte, error) {
	return CompressComposite(dst, []EncodingRequest{{Src: src, Mode: EncodingIguanaANS, ANSRejectionThreshold: ansRejectionThreshold}})
}

func CompressComposite(dst []byte, reqs []EncodingRequest) ([]byte, error) {
	es := encodingStation{dst: dst, lastCommandOffset: -1}
	return es.encode(reqs)
}

type iguanaEncodingContext struct {
	src         []byte
	tokens      []byte
	offsets16   []byte
	offsets24   []byte
	varLitLen   []byte
	varMatchLen []byte
	literals    []byte
	// The compressor's state
	pendingLiterals   []byte
	currentOffset     uint32
	lastEncodedOffset uint32
}

func (es *encodingStation) encode(reqs []EncodingRequest) ([]byte, error) {
	{ // Compute totalInputSize
		totalInputSize := uint64(0)
		for _, req := range reqs {
			totalInputSize += uint64(len(req.Src))
		}
		es.appendControlVarUint(totalInputSize)
	}

	for _, req := range reqs {
		srcLen := len(req.Src)
		if srcLen < 0 {
			return nil, fmt.Errorf("invalid input size %d", srcLen)
		} else if srcLen == 0 {
			continue
		}
		switch req.Mode {

		case EncodingRaw:
			if err := es.encodeRaw(req.Src); err != nil {
				return nil, err
			}

		case EncodingANS:
			if err := es.encodeANS(req.Src, req.ANSRejectionThreshold); err != nil {
				return nil, err
			}

		case EncodingIguana:
			if err := es.encodeIguana(req.Src, 0.0); err != nil {
				return nil, err
			}

		case EncodingIguanaANS:
			if err := es.encodeIguana(req.Src, req.ANSRejectionThreshold); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unrecognized mode %02x", req.Mode)
		}
	}

	// Append the control bytes in reverse order

	for i := len(es.ctrl) - 1; i >= 0; i-- {
		es.dst = append(es.dst, es.ctrl[i])
	}
	return es.dst, nil
}

func (es *encodingStation) encodeRaw(src []byte) error {
	srcLen := uint64(len(src))
	es.appendControlCommand(cmdCopyRaw)
	es.appendControlVarUint(srcLen)
	es.dst = append(es.dst, src...)
	return nil
}

func (es *encodingStation) encodeANS(src []byte, threshold float32) error {
	ans, err := AnsEncode(src)
	if err != nil {
		return err
	}
	srcLen := uint64(len(src))
	lenANS := uint64(len(ans))
	if ratio := float64(lenANS) / float64(srcLen); ratio >= float64(threshold) {
		es.appendControlCommand(cmdCopyRaw)
		es.appendControlVarUint(srcLen)
		es.dst = append(es.dst, src...)
	} else {
		es.appendControlCommand(cmdDecodeANS)
		es.appendControlVarUint(srcLen)
		es.appendControlVarUint(lenANS)
		es.dst = append(es.dst, ans...)
	}
	return nil
}

func (es *encodingStation) encodeIguana(src []byte, threshold float32) error {
	if len(src) < minLength {
		return es.encodeRaw(src)
	}
	ec := iguanaEncodingContext{src: src}
	ec.encodeIguanaHashChains()

	hdr := byte(0)
	ustreams := [streamCount][]byte{ec.tokens, ec.offsets16, ec.offsets24, ec.varLitLen, ec.varMatchLen, ec.literals}
	cstreams := [streamCount][]byte{}

	if threshold > 0.0 {
		for i := 0; i < int(streamCount); i++ {
			// ANS-compress the stream
			cs, err := AnsEncode(ustreams[i])
			if err != nil {
				return err
			}
			csLen := len(cs)
			if ratio := float64(csLen) / float64(len(ustreams[i])); ratio < float64(threshold) {
				hdr |= (1 << i)
				cstreams[i] = cs
			}
		}
	}

	es.appendControlCommand(cmdDecodeIguana)
	es.appendControlByte(hdr)

	// Append the uncompressed streams' lengths
	for i := 0; i < int(streamCount); i++ {
		es.appendControlVarUint(uint64(len(ustreams[i])))
	}

	// Append streams' data and compressed lengths
	for i := 0; i < int(streamCount); i++ {
		if hdr&(1<<i) == 0 {
			es.dst = append(es.dst, ustreams[i]...)
		} else {
			es.appendControlVarUint(uint64(len(cstreams[i])))
			es.dst = append(es.dst, cstreams[i]...)
		}
	}
	return nil
}

func (es *encodingStation) appendControlByte(v byte) {
	es.ctrl = append(es.ctrl, v)
}

func (es *encodingStation) appendControlCommand(v byte) {
	if es.lastCommandOffset >= 0 {
		es.ctrl[es.lastCommandOffset] &= cmdMask
	}
	es.lastCommandOffset = len(es.ctrl)
	es.appendControlByte(v | lastCommandMarker)
}

func (es *encodingStation) appendControlVarUint(v uint64) {
	cnt := (bits.Len64(v) / 7) + 1
	for i := cnt - 1; i >= 0; i-- {
		x := byte(v>>(i*7)) & 0x7f
		if i == 0 {
			x |= 0x80
		}
		es.ctrl = append(es.ctrl, x)
	}
}

func appendVarUint(s []byte, v uint32) []byte {
	if v < varUIntThreshold1B {
		return append(s, byte(v))
	} else if v < varUIntThreshold3B {
		x := byte(v % 254)
		y := byte(v / 254)
		return append(s, byte(254), x, y)
	} else if v < varUIntThreshold4B {
		x := byte(v % 254)
		t := v / 254
		y := byte(t % 254)
		z := byte(t / 254)
		return append(s, byte(255), x, y, z)
	} else {
		// Should never happen by design
		panic(fmt.Sprintf("%d is out of the VarUint range", v))
	}
}

func appendUint24(s []byte, v uint32) []byte {
	if v <= maxUint24 {
		return append(s, byte(v), byte(v>>8), byte(v>>16))
	}
	// Should never happen by design
	panic(fmt.Sprintf("%d is out of the uint24 range", v))
}

func appendUint16(s []byte, v uint32) []byte {
	if v <= maxUint16 {
		return append(s, byte(v), byte(v>>8))
	}
	// Should never happen by design
	panic(fmt.Sprintf("%d is out of the uint16 range", v))
}

type matchDescriptor struct {
	start  uint32
	length uint32
	cost   int32
}

func hashMatch(c0, c1 byte) uint {
	return uint(c1)<<8 | uint(c0)
}

func (ec *iguanaEncodingContext) encodeIguanaHashChains() {
	src := ec.src
	srcLen := len(src)
	if srcLen < minOffset {
		panic("satisfying this constraint should have been ensured by the caller")
	}
	last := srcLen - minOffset
	data := src[:last]

	table := [256 * 256][]uint32{}
	ec.lastEncodedOffset = 0

	for ec.currentOffset = 0; ec.currentOffset < uint32(last); {
		c0 := src[ec.currentOffset]
		c1 := src[ec.currentOffset+1]
		// Find the lowest cost match in table[]
		if match := pickBestMatch(ec, data, table[hashMatch(c0, c1)]); match.cost >= 0 {
			ec.pendingLiterals = append(ec.pendingLiterals, c0)
			if ec.currentOffset > 0 {
				h := hashMatch(src[ec.currentOffset-1], c0)
				table[h] = append(table[h], ec.currentOffset-1)
			}
			ec.currentOffset++
		} else {
			for i := uint32(0); i < match.length; i++ {
				h := hashMatch(src[ec.currentOffset+i-1], src[ec.currentOffset+i])
				table[h] = append(table[h], ec.currentOffset+i-1)
			}
			ec.emit(&match)
			ec.currentOffset += match.length
		}
	}

	// Flush the pendingLiterals buffer and append the non-compressed part of the input
	ec.literals = append(append(ec.literals, ec.pendingLiterals...), src[last:]...)
	ec.pendingLiterals = ec.pendingLiterals[:0]
}

func (ec *iguanaEncodingContext) costVarUInt(v uint32) int32 {
	// The cost of a VarUInt is the number of bytes its encoding requires
	if v < varUIntThreshold1B {
		return 1
	} else if v < varUIntThreshold3B {
		return 3
	} else if v < varUIntThreshold4B {
		return 4
	} else {
		// Should never happen by design
		panic(fmt.Sprintf("%d is out of the VarUint range", v))
	}
}

func (ec *iguanaEncodingContext) cost(offs uint32, length uint32) int32 {
	// The cost is the sum of all emission costs (what we pay) minus the length of the covered match (what we get).
	if offs < minOffset {
		return costInfinite // The offset must be at least the size of a vector register used for match copying
	}
	c := costToken - int32(length)
	litLen := len(ec.pendingLiterals)

	if offs == ec.lastEncodedOffset || offs <= maxUint16 {
		if offs != ec.lastEncodedOffset {
			c += costOffs16
		}
		if litLen >= maxShortLitLen {
			c += ec.costVarUInt(uint32(litLen) - maxShortLitLen)
		}
		if length >= maxShortMatchLen {
			c += ec.costVarUInt(length - maxShortMatchLen)
		}
		return c
	} else {
		if length <= maxShortMatchLen {
			return costInfinite // Lenths < 16 an offs >= 2^16 are not encodable with the current token constellation...
		}
		if litLen > 0 { // Offsets >= 2^16 cannot carry literals, so a flush must be done first
			c += costToken
			if litLen >= maxShortLitLen {
				c += ec.costVarUInt(uint32(litLen) - maxShortLitLen)
			}
		}
		c += costOffs24
		if length >= (lastLongOffset + mmLongOffsets) {
			c += ec.costVarUInt(length - (lastLongOffset + mmLongOffsets))
		}
		return c
	}
}

func (ec *iguanaEncodingContext) emit(m *matchDescriptor) {
	// Flush the pending literals, if any
	litLen := len(ec.pendingLiterals)
	if litLen > 0 {
		ec.literals = append(ec.literals, ec.pendingLiterals...)
		ec.pendingLiterals = ec.pendingLiterals[:0]
	}

	matchLen := m.length
	offs := ec.currentOffset - m.start

	if offs == ec.lastEncodedOffset || offs <= maxUint16 {
		token := byte(0x80)
		if offs != ec.lastEncodedOffset {
			token = 0x00
			ec.offsets16 = appendUint16(ec.offsets16, offs)
		}

		if litLen < maxShortLitLen {
			token |= byte(litLen)
		} else {
			token |= byte(maxShortLitLen)
			ec.varLitLen = appendVarUint(ec.varLitLen, uint32(litLen)-maxShortLitLen)
		}

		if matchLen < maxShortMatchLen {
			token |= byte(matchLen << literalLenBits)
		} else {
			token |= byte(maxShortMatchLen << literalLenBits)
			ec.varMatchLen = appendVarUint(ec.varMatchLen, matchLen-maxShortMatchLen)
		}

		ec.tokens = append(ec.tokens, token)

	} else {
		if litLen > 0 {
			// The tokens for offsets >= 2^16 cannot carry literals, so a flush must be done first
			token := byte(0x80) // No match part, hence retain the offset

			if litLen < maxShortLitLen {
				token |= byte(litLen)
			} else {
				token |= byte(maxShortLitLen)
				ec.varLitLen = appendVarUint(ec.varLitLen, uint32(litLen)-maxShortLitLen)
			}
			ec.tokens = append(ec.tokens, token)
		}

		if matchLen <= maxShortMatchLen {
			// Should not happen by design, as the cost estimator assigns an infinite cost to this case.
			panic("Lenths < 16 an offs >= 2^16 are not encodable with the current token constellation")
		}

		ec.offsets24 = appendUint24(ec.offsets24, offs)
		token := byte(0x00)

		if matchLen < (lastLongOffset + mmLongOffsets) {
			token = byte(matchLen - mmLongOffsets)
		} else {
			token = 0x1f
			ec.varMatchLen = appendVarUint(ec.varMatchLen, matchLen-(lastLongOffset+mmLongOffsets))
		}

		ec.tokens = append(ec.tokens, token)
	}

	ec.lastEncodedOffset = offs
}

var pickBestMatch func(ec *iguanaEncodingContext, src []byte, candidates []uint32) matchDescriptor = pickBestMatchReference

func init() {
	if minLength < minOffset {
		panic("minLength must not be less than minOffs")
	}
}

func pickBestMatchReference(ec *iguanaEncodingContext, src []byte, candidates []uint32) matchDescriptor {
	match := matchDescriptor{cost: costInfinite} // Use the worst possible match as the first candidate
	n := uint32(len(src))
	cLen := len(candidates)

	for i := cLen - 1; i >= 0; i-- {
		start := candidates[i]
		if start >= ec.currentOffset {
			continue
		}
		offs := ec.currentOffset - start
		if offs < minOffset {
			continue
		}

		length := uint32(0)
		for ec.currentOffset+length < n && src[start+length] == src[ec.currentOffset+length] {
			length++
		}
		cost := ec.cost(offs, length)
		if cost < match.cost {
			// A cheaper match has been found, so it becomes the current optimum.
			match.start = start
			match.length = length
			match.cost = cost
		}
	}

	return match
}
