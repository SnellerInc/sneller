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

package iguana

import (
	"encoding/binary"
	"fmt"
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
	cmdCopyRaw         byte = 0x00
	cmdDecodeIguana    byte = 0x01
	cmdDecodeANS32     byte = 0x02
	cmdDecodeANS1      byte = 0x03
	cmdDecodeANSNibble byte = 0x04
	lastCommandMarker  byte = 0x80
	cmdMask                 = ^lastCommandMarker
)

const (
	iguanaChunkSize = 32
	minOffset       = iguanaChunkSize
	minLength       = iguanaChunkSize
)

type encodingStation struct {
	dst                []byte
	ctrl               []byte
	lastCommandOffset  int
	ans32              ANS32Encoder
	ans1               ANS1Encoder
	ansnib             ANSNibbleEncoder
	ctx                encodingContext
	ustreams, cstreams [streamCount][]byte
}

type Encoder struct {
	es encodingStation
}

func (e *Encoder) Compress(src []byte, dst []byte, entropyRejectionThreshold float32) ([]byte, error) {
	return e.CompressComposite(dst, []EncodingRequest{{Src: src, EncMode: EncodingIguana, EntMode: EntropyANS32, EntropyRejectionThreshold: entropyRejectionThreshold, EnableSecondaryResolver: false}})
}

func (e *Encoder) CompressComposite(dst []byte, reqs []EncodingRequest) ([]byte, error) {
	e.es.ctx.reset()
	e.es.dst = dst
	e.es.lastCommandOffset = -1
	e.es.ctrl = e.es.ctrl[:0]
	return e.es.encode(reqs)
}

type encodingContext struct {
	src         []byte
	tokens      []byte
	offsets16   []byte
	offsets24   []byte
	varLitLen   []byte
	varMatchLen []byte
	literals    []byte

	// The compressor's state
	currentOffset     uint32
	lastEncodedOffset uint32

	table matchtable
}

const (
	chainbits = 17 // selected empirically; roughly equiv. to 18, 19
	hashbytes = 5  // selected empirically; better than 4, 6, 7, 8
	histsize  = 4  //
)

type hashEntry struct {
	history [histsize]int32
}

// matchtable is a table of match chains
// that is optimized for fast lookups
type matchtable struct {
	entries [1 << chainbits]hashEntry
}

func (m *matchtable) reset() {
	for i := range m.entries {
		m.entries[i] = hashEntry{}
	}
}

func (m *matchtable) hash(seq []byte) uint32 {
	u := binary.LittleEndian.Uint64(seq)
	// multiply the low-order bits by a large prime
	// depending on the number of bytes we want to consider:
	switch hashbytes {
	case 4:
		u = (u << 32) * 2654435761
	case 5:
		u = (u << 24) * 889523592379
	case 6:
		u = (u << 16) * 227718039650203
	case 7:
		u = (u << 8) * 58295818150454627
	case 8:
		u = u * 0xcf1bbcdcb7a56463
	}
	return uint32(u >> (64 - chainbits))
}

func (m *matchtable) insert(src []byte, pos int32) {
	ent := &m.entries[m.hash(src[pos:])]
	switch histsize {
	case 4:
		ent.history[0], ent.history[1], ent.history[2], ent.history[3] = pos, ent.history[0], ent.history[1], ent.history[2]
	case 3:
		ent.history[0], ent.history[1], ent.history[2] = pos, ent.history[0], ent.history[1]
	case 2:
		ent.history[0], ent.history[1] = pos, ent.history[0]
	case 1:
		ent.history[0] = pos
	default:
		copy(ent.history[1:], ent.history[:])
		ent.history[0] = pos
	}
}

// produce a legal match position, target position, and length
// or (0, 0, 0) if no such match can be created
//
// caller should guarantee [from] < [to] and [minto] <= [to]
//
// match guarantees [matchpos] < [targetpos] and [targetpos] >= [minto]
func match(src []byte, minto, from, to int32) (targetpos, matchpos, matchlen int32) {
	targetpos = to
	matchpos = from
	matchlen = lcp(src, matchpos, targetpos)
	for matchpos > 0 && src[matchpos-1] == src[targetpos-1] && targetpos > minto {
		matchpos--
		targetpos--
		matchlen++
	}
	if matchpos >= targetpos {
		panic("uh oh")
	}
	if !isLegal(targetpos-matchpos, matchlen) {
		return 0, 0, 0
	}
	return targetpos, matchpos, matchlen
}

// return (target, position, length) of the best saved match for src[pos:]
func (m *matchtable) bestMatch(src []byte, litmin, pos int32) (int32, int32, int32) {
	ent := &m.entries[m.hash(src[pos:])]
	t, p, mlen := match(src, litmin, ent.history[0], pos)
	for i := 1; i < histsize; i++ {
		cpos := ent.history[i]
		if cpos == 0 {
			break // empty entry
		}
		altt, altp, altlen := match(src, litmin, ent.history[i], pos)
		if altlen > mlen {
			t, p, mlen = altt, altp, altlen
		}
	}
	return t, p, mlen
}

// longest-common-prefix of src[lo:] and src[:hi] where lo < hi
func lcp(src []byte, lo, hi int32) int32 {
	matched := int32(0)
	// fast path:
	for len(src)-int(hi+matched) >= 8 {
		// bounds-check hint:
		_ = src[lo+matched : lo+matched+8]
		_ = src[hi+matched : hi+matched+8]

		lobits := binary.LittleEndian.Uint64(src[lo+matched:])
		hibits := binary.LittleEndian.Uint64(src[hi+matched:])
		delta := lobits ^ hibits
		if delta == 0 {
			matched += 8
			continue
		}
		return matched + int32(bits.TrailingZeros64(delta)/8)
	}
	for len(src)-int(hi+matched) > 0 && src[lo+matched] == src[hi+matched] {
		matched++
	}
	return matched
}

func (ec *encodingContext) reset() {
	ec.src = ec.src[:0]
	ec.tokens = ec.tokens[:0]
	ec.offsets16 = ec.offsets16[:0]
	ec.offsets24 = ec.offsets24[:0]
	ec.varLitLen = ec.varLitLen[:0]
	ec.varMatchLen = ec.varMatchLen[:0]
	ec.literals = ec.literals[:0]
	ec.currentOffset = 0
	ec.lastEncodedOffset = 0
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
		switch req.EncMode {

		case EncodingRaw:
			switch req.EntMode {
			case EntropyNone:
				if err := es.encodeRaw(req.Src); err != nil {
					return nil, err
				}
			case EntropyANS32:
				if err := es.encodeANS32(&req); err != nil {
					return nil, err
				}
			case EntropyANS1:
				if err := es.encodeANS1(&req); err != nil {
					return nil, err
				}
			case EntropyANSNibble:
				if err := es.encodeANSNibble(&req); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unrecognized entropy mode %02x", req.EntMode)
			}

		case EncodingIguana:
			if err := es.encodeIguana(&req); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unrecognized encoding mode %02x", req.EncMode)
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

func (es *encodingStation) encodeANS32(req *EncodingRequest) error {
	ans, err := es.ans32.Encode(req.Src)
	if err != nil {
		return err
	}
	srcLen := uint64(len(req.Src))
	lenANS := uint64(len(ans))
	if ratio := float64(lenANS) / float64(srcLen); ratio >= float64(req.EntropyRejectionThreshold) {
		es.appendControlCommand(cmdCopyRaw)
		es.appendControlVarUint(srcLen)
		es.dst = append(es.dst, req.Src...)
	} else {
		es.appendControlCommand(cmdDecodeANS32)
		es.appendControlVarUint(srcLen)
		es.appendControlVarUint(lenANS)
		es.dst = append(es.dst, ans...)
	}
	return nil
}

func (es *encodingStation) encodeANS1(req *EncodingRequest) error {
	ans, err := es.ans1.Encode(req.Src)
	if err != nil {
		return err
	}
	srcLen := uint64(len(req.Src))
	lenANS := uint64(len(ans))
	if ratio := float64(lenANS) / float64(srcLen); ratio >= float64(req.EntropyRejectionThreshold) {
		es.appendControlCommand(cmdCopyRaw)
		es.appendControlVarUint(srcLen)
		es.dst = append(es.dst, req.Src...)
	} else {
		es.appendControlCommand(cmdDecodeANS1)
		es.appendControlVarUint(srcLen)
		es.appendControlVarUint(lenANS)
		es.dst = append(es.dst, ans...)
	}
	return nil
}

func (es *encodingStation) encodeANSNibble(req *EncodingRequest) error {
	ans, err := es.ansnib.Encode(req.Src)
	if err != nil {
		return err
	}
	srcLen := uint64(len(req.Src))
	lenANS := uint64(len(ans))
	if ratio := float64(lenANS) / float64(srcLen); ratio >= float64(req.EntropyRejectionThreshold) {
		es.appendControlCommand(cmdCopyRaw)
		es.appendControlVarUint(srcLen)
		es.dst = append(es.dst, req.Src...)
	} else {
		es.appendControlCommand(cmdDecodeANSNibble)
		es.appendControlVarUint(srcLen)
		es.appendControlVarUint(lenANS)
		es.dst = append(es.dst, ans...)
	}
	return nil
}

func (es *encodingStation) encodeIguana(req *EncodingRequest) error {
	if len(req.Src) < (minLength + hashbytes) {
		// we need enough bytes to actually produce a hash-chain match
		// within the preceding minLength bytes
		return es.encodeRaw(req.Src)
	}
	ec := &es.ctx
	ec.src = req.Src
	ec.compressSrc()

	hdr := uint64(0)
	es.ustreams = [streamCount][]byte{ec.tokens, ec.offsets16, ec.offsets24, ec.varLitLen, ec.varMatchLen, ec.literals}
	totalsize := 0
	for i := range es.cstreams {
		es.cstreams[i] = es.cstreams[i][:0]
	}
	for i := range es.ustreams {
		totalsize += len(es.ustreams[i])
	}

	if req.EntropyRejectionThreshold > 0.0 {
		for i := 0; i < int(streamCount); i++ {
			switch req.EntMode {
			case EntropyANS32:
				// ANS32-compress the stream
				cs, err := es.ans32.Encode(es.ustreams[i])
				if err != nil {
					return err
				}
				csLen := len(cs)
				if ratio := float64(csLen) / float64(len(es.ustreams[i])); ratio < float64(req.EntropyRejectionThreshold) {
					hdr |= (uint64(EntropyANS32) << (i * 4))
					totalsize -= len(es.ustreams[i])
					totalsize += len(cs)
					es.cstreams[i] = append(es.cstreams[i][:0], cs...)
					break
				}
				if !req.EnableSecondaryResolver {
					break
				}
				fallthrough // Resort to the scalar ANS version

			case EntropyANS1:
				// ANS1-compress the stream
				cs, err := es.ans1.Encode(es.ustreams[i])
				if err != nil {
					return err
				}
				csLen := len(cs)
				if ratio := float64(csLen) / float64(len(es.ustreams[i])); ratio < float64(req.EntropyRejectionThreshold) {
					hdr |= (uint64(EntropyANS1) << (i * 4))
					totalsize -= len(es.ustreams[i])
					totalsize += len(cs)
					es.cstreams[i] = append(es.cstreams[i][:0], cs...)
					break
				}
				if !req.EnableSecondaryResolver {
					break
				}
				fallthrough // Last ditch attempt

			case EntropyANSNibble:
				// ANSNibble-compress the stream
				cs, err := es.ansnib.Encode(es.ustreams[i])
				if err != nil {
					return err
				}
				csLen := len(cs)
				if ratio := float64(csLen) / float64(len(es.ustreams[i])); ratio < float64(req.EntropyRejectionThreshold) {
					hdr |= (uint64(EntropyANSNibble) << (i * 4))
					totalsize -= len(es.ustreams[i])
					totalsize += len(cs)
					es.cstreams[i] = append(es.cstreams[i][:0], cs...)
				}

			case EntropyNone:
				// Doing nothing is equivalent to a failed entropy compression

			default:
				panic("unrecognized entropy encoding")
			}
		}
	}
	// if we didn't compress anything, emit a copy command
	// (assume each stream consumes at least 1 varint byte
	// and the header includes 1 extra control byte)
	if totalsize+int(streamCount)+1 >= len(req.Src) {
		return es.encodeRaw(req.Src)
	}

	es.appendControlCommand(cmdDecodeIguana)
	es.appendControlVarUint(hdr)

	// Append the uncompressed streams' lengths
	for i := 0; i < int(streamCount); i++ {
		es.appendControlVarUint(uint64(len(es.ustreams[i])))
	}

	// Append streams' data and compressed lengths
	for i := 0; i < int(streamCount); i++ {
		if entropyMode := EntropyMode((hdr >> (i * 4)) & 0x0f); entropyMode == EntropyNone {
			es.dst = append(es.dst, es.ustreams[i]...)
		} else {
			es.appendControlVarUint(uint64(len(es.cstreams[i])))
			es.dst = append(es.dst, es.cstreams[i]...)
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

func (ec *encodingContext) bestMatchAt(src []byte, litpos, pos int32) (targetpos, matchpos, matchlen int32) {
	// first try last encoded offset
	targetpos = pos
	if p := pos - int32(ec.lastEncodedOffset); p < pos {
		matchpos = p
		matchlen = lcp(src, p, pos)
	}
	// then, try the hash table (must beat lastEncodedOffset by 2 bytes or more)
	if tp, mp, mlen := ec.table.bestMatch(src, litpos, pos); mlen-matchlen > 1 {
		targetpos = tp
		matchpos = mp
		matchlen = mlen
	}

	// if the end of the match is within one register distance
	// of the end of the buffer, then we need to make sure
	// this copy can be performed safely:
	if targetpos+matchlen > int32(len(src))-minOffset {
		if targetpos-matchpos >= minOffset {
			// common case: fast arithmetic
			const lomask = minOffset - 1
			if targetpos+(matchlen+lomask)&^lomask > int32(len(src)) {
				matchlen &^= lomask
			}
		} else {
			// rare case: long overlapping match
			movsize := targetpos - matchpos
			tailpos := matchlen - (matchlen % movsize)
			end := int32(len(src))
			if targetpos+tailpos+minOffset > end {
				safedist := (end - minOffset) - targetpos
				matchlen = (safedist / movsize) * movsize
			}
		}
	}

	// don't ask the decoder to implicitly write past
	// the end of the target buffer; it must be safe
	// for the final write of this match to be 32 bytes
	return targetpos, matchpos, matchlen
}

func (ec *encodingContext) compressSrc() {
	const skipStep = 2
	src := ec.src
	if len(src) < minOffset {
		panic("satisfying this constraint should have been ensured by the caller")
	}
	ec.table.reset()
	last := int32(len(src) - minOffset) // last allowed match position
	ec.lastEncodedOffset = 0
	pos := int32(5) // current match search position
	litpos := int32(0)
	ec.table.insert(src, 0)

	// search for matches up to *and including*
	// the last allowed match position
	for pos <= last {
		targetpos, matchpos, matchlen := ec.bestMatchAt(src, litpos, pos)
		// see if the very next byte would produce a longer match;
		// if so, then we should use that instead rather than breaking
		// up a large potential match
		if pos < last {
			tp1, mp1, mlen1 := ec.bestMatchAt(src, litpos, pos+1)
			// turns out that comparing raw match lengths
			// performs *better* in practice than the pure "cost"
			if mlen1 > matchlen {
				targetpos, matchpos, matchlen = tp1, mp1, mlen1
			}
		}
		// validity assertions:
		if targetpos+matchlen > int32(len(src)) {
			panic("pos + matchlen overflows src")
		}
		if litpos > targetpos {
			panic("litpos > pos?")
		}
		if matchlen >= 4 {
			ec.emit(src[litpos:targetpos], uint32(targetpos-matchpos), uint32(matchlen))
			// add new possible matches to the hash table,
			// but only those than have not yet been inserted:
			for i := int32(targetpos); i < (targetpos+matchlen) && i < last; i += skipStep {
				ec.table.insert(src, i)
			}
			pos = targetpos + matchlen // position is advanced equal to the match length
			litpos = pos               // start of current literal is replaced
		} else {
			ec.table.insert(src, pos)
			pos += skipStep
		}
	}
	// flush remaining literals
	ec.literals = append(ec.literals, src[litpos:]...)
}

// isLegal returns the legality of an offset+length pair
func isLegal(offs, length int32) bool {
	return offs <= maxUint16 || length > maxShortMatchLen
}

// emit lit+match+offset triple
func (ec *encodingContext) emit(lit []byte, offs, matchLen uint32) {
	ec.literals = append(ec.literals, lit...)
	litLen := len(lit)
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
			panic("Lengths < 16 and offs > 2^16 are not encodable with the current token constellation")
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

func init() {
	if minLength < minOffset {
		panic("minLength must not be less than minOffs")
	}
}
