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
	"fmt"
	"math"
	"math/bits"

	"github.com/SnellerInc/sneller/ints"

	"golang.org/x/exp/slices"
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

	// turn this on to get a much more expensive
	// (but optimal) matcher
	forceOptimalMatching = !false
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
	pendingLiterals   []byte
	currentOffset     uint32
	lastEncodedOffset uint32

	table matchtable
}

const (
	hunksize  = 16 // selected to be friendly to assembly
	chainbits = 19 // selected empirically; seems to work better than 16
	hashbytes = 3  // bytes necessary to hash data
)

type hunk struct {
	offsets [hunksize]uint32
	next    uint32
	valid   int8
}

func (h *hunk) entries() []uint32 {
	return h.offsets[:h.valid]
}

// matchtable is a table of match chains
// that is optimized for fast lookups
type matchtable struct {
	chains [1 << chainbits]uint32
	hunks  []hunk
}

func (m *matchtable) pos(c0, c1, c2 byte) uint {
	return (uint(c0) << (chainbits - 8)) |
		(uint(c1) << (chainbits - 8 - 8)) |
		(uint(c2) & ((1 << (chainbits - 8 - 8)) - 1))
}

func (m *matchtable) reset(wantcap int) {
	m.hunks = slices.Grow(m.hunks[:0], (wantcap*2)/hunksize)
	for i := range m.chains {
		m.chains[i] = 0
	}
}

func (m *matchtable) next(h *hunk) *hunk {
	if h.next == 0 {
		return nil
	}
	return &m.hunks[^h.next]
}

func (m *matchtable) chain(seq []byte) *hunk {
	e := m.chains[m.pos(seq[0], seq[1], seq[2])]
	if e == 0 {
		return nil
	}
	return &m.hunks[^e]
}

func (m *matchtable) insert(seq []byte, pos uint32) {
	j := m.pos(seq[0], seq[1], seq[2])
	e := m.chains[j]
	if e == 0 || m.hunks[^e].valid == hunksize {
		// grow
		idx := len(m.hunks)
		m.hunks = append(m.hunks, hunk{})
		h := &m.hunks[idx]
		h.offsets[0] = pos
		h.valid = 1
		h.next = e
		m.chains[j] = ^uint32(idx)
		return
	}
	h := &m.hunks[^e]
	h.offsets[h.valid] = pos
	h.valid++
}

func (ec *encodingContext) reset() {
	ec.src = ec.src[:0]
	ec.tokens = ec.tokens[:0]
	ec.offsets16 = ec.offsets16[:0]
	ec.offsets24 = ec.offsets24[:0]
	ec.varLitLen = ec.varLitLen[:0]
	ec.varMatchLen = ec.varMatchLen[:0]
	ec.literals = ec.literals[:0]
	ec.pendingLiterals = ec.pendingLiterals[:0]
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
	ec.encodeIguanaHashChains()

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

type matchDescriptor struct {
	start  uint32
	length uint32
	cost   int32
}

func clamp(buf []byte, maxlen int) []byte {
	if len(buf) > maxlen {
		return buf[:maxlen]
	}
	return buf
}

func (ec *encodingContext) encodeIguanaHashChains() {
	src := ec.src
	srcLen := len(src)
	if srcLen < minOffset {
		panic("satisfying this constraint should have been ensured by the caller")
	}
	last := srcLen - minOffset // last allowed match position

	ec.table.reset(len(src))
	ec.lastEncodedOffset = 0

	// encode the very first possible match position
	prefix := clamp(src, minOffset)
	ec.currentOffset = uint32(len(prefix))
	ec.pendingLiterals = append(ec.pendingLiterals, prefix...)
	ec.table.insert(prefix, 0)

	// search for matches up to *and including*
	// the last allowed match position
	for ec.currentOffset <= uint32(last) {
		seq := src[ec.currentOffset:]
		curmatch := matchDescriptor{cost: costInfinite}
		for h := ec.table.chain(seq); h != nil; h = ec.table.next(h) {
			maybe := pickBestMatch(ec, src, h.entries())
			if maybe.cost < curmatch.cost {
				curmatch = maybe
			}
		}
		if curmatch.cost >= 0 {
			ec.pendingLiterals = append(ec.pendingLiterals, seq[0])
			if ec.currentOffset > minOffset {
				start := ec.currentOffset - minOffset + 1
				ec.table.insert(src[start:], start)
			}
			ec.currentOffset++
		} else {
			if ec.currentOffset+ints.AlignUp32(curmatch.length, minLength) > uint32(len(src)) {
				// make sure we don't implicitly ask the decoder
				// to write past the end of the target buffer
				curmatch.length = ints.AlignDown32(curmatch.length, minLength)
			}
			clamp := curmatch.length
			if clamp > minOffset && !forceOptimalMatching {
				// if we get a match longer than minOffset,
				// then by definition we are inserting a repetition
				// (i.e. we are extending the current position forward),
				// which means inserting the repetition into the match table
				// duplicates a substring that's already in the table
				//
				// it is much *faster* to avoid inserting the duplicate
				// substring into the table, but it also decreases the compression
				// ratio slightly because we end up encoding larger (longer)
				// match-offset+length descriptors than we would otherwise
				clamp = minOffset
			}
			for i := uint32(0); i < clamp; i++ {
				start := ec.currentOffset - minOffset + i + 1
				ec.table.insert(src[start:], start)
			}
			ec.emit(&curmatch)
			ec.currentOffset += curmatch.length
		}
	}

	// Flush the pendingLiterals buffer and append the non-compressed part of the input
	ec.literals = append(append(ec.literals, ec.pendingLiterals...), src[ec.currentOffset:]...)
	ec.pendingLiterals = ec.pendingLiterals[:0]
}

func (ec *encodingContext) costVarUInt(v uint32) int32 {
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

func (ec *encodingContext) cost(offs uint32, length uint32) int32 {
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

func (ec *encodingContext) emit(m *matchDescriptor) {
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

var pickBestMatch func(ec *encodingContext, src []byte, candidates []uint32) matchDescriptor = pickBestMatchReference

func init() {
	if minLength < minOffset {
		panic("minLength must not be less than minOffs")
	}
}

func pickBestMatchReference(ec *encodingContext, src []byte, candidates []uint32) matchDescriptor {
	match := matchDescriptor{cost: costInfinite} // Use the worst possible match as the first candidate
	n := uint32(len(src))
	cLen := len(candidates)

	for i := cLen - 1; i >= 0; i-- {
		start := candidates[i]
		if start >= ec.currentOffset {
			panic("should never happen")
		}
		offs := ec.currentOffset - start
		if offs < minOffset {
			panic("should never happen")
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
