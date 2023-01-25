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

package ion

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"golang.org/x/exp/slices"

	"github.com/SnellerInc/sneller/date"
)

var (
	ErrTooLarge = errors.New("ion: object size exceeds max size")
)

func err2big(max int) error {
	return fmt.Errorf("%w: %d", ErrTooLarge, max)
}

// Chunker is a wrapper for a Buffer
// and Symtab that allows objects to
// be written to an output stream on
// aligned boundaries with padding.
//
// In order to use a Chunker, populate
// its Align and W fields with the
// target alignment and destination
// of the ion output.
// Then, write objects to Buffer
// (optionally updating Symbols)
// and call Commit after each object
// has been written. After the complete
// object stream has been written,
// call Flush to flush any remaining
// buffer contents.
type Chunker struct {
	// Buffer is the current
	// buffered data.
	Buffer
	// Symbols is the current symbol table.
	Symbols  Symtab
	symEpoch int // incremented when Symbols resets

	// Align is the alignment of output data.
	// (Align should not be modified once the
	// data has begun being committed to the Chunker.)
	Align int
	// RangeAlign is the alignment
	// at which ranges are flushed to W.
	RangeAlign int
	// W is the output io.Writer.
	// All writes to W will have length equal to Align.
	//
	// If W implements Flusher, then Flush is called
	// immediately after ranges are written, which
	// should occur at most once every RangeAlign bytes.
	W io.Writer
	// Ranges stores field ranges for the current
	// chunk.
	Ranges Ranges

	writesyms Symtab // symbol table for Write()

	rowcount int // row count associated with Ranges

	// WalkTimeRanges is the list of time ranges
	// that is automatically scanned during
	// Chunker.Write.
	WalkTimeRanges [][]string
	// symbolized WalkTimeRanges
	rangeSyms [][]Symbol

	tmpbuf  Buffer // scratch buffer
	lastoff int    // last committed object offset
	lastst  int    // last symbol table size
	written int

	lastcomp   int // last compressed offset
	compressed bool

	// marshalled and flushed maximum symbol IDs, respectively:
	tmpID, flushID int

	// compression is disabled
	noCompress bool
}

// Set sets the buffer used by c to b and resets c to
// its initial state. This should only be used between
// benchmark runs to avoid allocation overhead.
func (c *Chunker) Set(b []byte) {
	c.Buffer.Set(b)
	c.Ranges.reset()
}

// Reset resets c to its initial state. This should
// only be used between benchmark runs to avoid
// allocation overhead.
func (c *Chunker) Reset() {
	c.Buffer.Reset()
	c.Ranges.reset()
}

// Flusher is an interface optionally
// implemented by io.Writers that would
// like to be notified when ranges have been flushed.
//
// See ion.Chunker.W.
type Flusher interface {
	Flush() error
}

// prepend a prefix pre to body,
// attempting to avoid re-allocating
// body if there is enough capacity
// to simply shuffle the bytes around
func prepend(body, pre []byte) []byte {
	presize := len(pre)
	if cap(body)-len(body) < presize {
		ret := make([]byte, len(body)+presize)
		copy(ret, pre)
		copy(ret[presize:], body)
		return ret
	}
	body = body[:len(body)+presize]
	copy(body[presize:], body)
	copy(body, pre)
	return body
}

func (c *Chunker) adjustSyms() bool {
	max := c.Symbols.MaxID()
	// ordering should be
	//   c.flushID <= c.tmpID <= max
	if max < c.tmpID || c.tmpID < c.flushID {
		println("flush", c.flushID, "tmp", c.tmpID, "max", max)
		panic("bad symbol ID bookkeeping")
	}
	if max == c.tmpID {
		// currently-marshalled symtab is up-to-date
		return true
	}

	// re-align the state buffer so that
	// it is prefixed by the symbol table again;
	// our assumption is that the symbol table changes
	// infrequently once we have "warmed up"
	c.tmpbuf.Reset()
	if c.flushID == 0 {
		// haven't flushed a symbol table; need the whole thing
		c.Symbols.Marshal(&c.tmpbuf, true)
	} else {
		c.Symbols.MarshalPart(&c.tmpbuf, Symbol(c.flushID))
	}
	prefix := c.tmpbuf.Bytes()
	data := c.Buffer.Bytes()[c.lastst:]
	if len(prefix)+len(data) > c.Align {
		return false
	}

	// adjust the offset of the previously-committed
	// object based on the new symbol table size
	size := len(prefix)
	adj := size - c.lastst
	c.lastst = size
	c.lastoff += adj
	c.lastcomp += adj

	c.Buffer.Set(prepend(data, prefix))
	c.tmpID = max
	return true
}

func pad(buf []byte, size int) []byte {
	if cap(buf) < size {
		ret := make([]byte, size)
		n := copy(ret, buf)
		noppad(ret[n:])
		return ret
	}
	n := len(buf)
	buf = buf[:size]
	noppad(buf[n:])
	return buf
}

func memzero(dst []byte) {
	for i := range dst {
		dst[i] = 0
	}
}

type minMaxSetter interface {
	SetMinMax(path []string, min, max Datum)
}

func (c *Chunker) flushRanges() error {
	// ensure we write out a fresh
	// symbol table after each Flush()
	c.tmpID = 0
	c.flushID = 0
	c.rangeSyms = c.rangeSyms[:0]

	// TODO: make this configurable?
	// Just 33% of rows containing a value
	// seems like a pretty conservative lower-bound.
	minRange := c.rowcount / 3

	if mm, ok := c.W.(minMaxSetter); ok {
		for _, p := range c.Ranges.paths {
			r := c.Ranges.m[p]
			if r.count() < minRange {
				// don't include this range if
				// it was too sparse to be interesting
				continue
			}
			if min, max, ok := r.ranges(); ok {
				path := p.resolve(&c.Symbols)
				mm.SetMinMax(path, min, max)
			}
		}
	}
	if f, ok := c.W.(Flusher); ok {
		err := f.Flush()
		if err != nil {
			return err
		}
	}
	c.Ranges.flush()
	c.rowcount = 0
	c.written = 0
	return nil
}

func (c *Chunker) compressOrFlush() error {
	if !c.noCompress && !c.compressed {
		c.compressed = c.compress()
		// if we are fully serialized, no need to flush
		if c.Buffer.Size() <= c.Align && c.compressed {
			// commit the current row
			c.lastoff = c.Buffer.Size()
			return nil
		}
	}
	return c.forceFlush(false)
}

// forceFlush flushes the current state
// up to the last commited object,
// then resets the output buffer with
// the latest symbol table and possibly
// an uncommitted object (if there was one),
// and resets the range list.
func (c *Chunker) forceFlush(final bool) error {
	var tail []byte
	cur := c.Buffer.Bytes()
	// if this is a symbol table with
	// no trailing data, we are done
	if len(cur) == c.lastst {
		return nil
	}
	if c.Buffer.Size() > c.Align || c.tmpID < c.Symbols.MaxID() {
		if final {
			panic("final && too much data")
		}
		// create a new copy of the tail so
		// that we can clobber it with nop padding;
		// this should only be 1 object worth of data
		c.tmpbuf.Reset()
		c.tmpbuf.UnsafeAppend(cur[c.lastoff:])
		// as an extra measure of paranoia, make sure
		// that the trailing bytes are zeroed before
		// we insert the nop pad
		memzero(cur[c.lastoff:])
		cur = cur[:c.lastoff]
		tail = c.tmpbuf.Bytes()
	}
	cur = pad(cur, c.Align)
	_, err := c.W.Write(cur)
	if err != nil {
		return err
	}
	// record which symbols we have flushed
	c.flushID = c.tmpID
	c.written += len(cur)
	if c.written >= c.RangeAlign || final {
		err = c.flushRanges()
		if err != nil {
			return err
		}
	}
	// reset the buffer so that it
	// just contains the new symbol table
	// plus the reserved
	c.Buffer.Set(cur[:0])
	if tail != nil {
		if final {
			panic("final && tail != nil")
		}
		// if we just flushed ranges OR the symbol table
		// is getting to be too large, then resymbolize
		// using just the trailing row
		if c.flushID == 0 || c.Symbols.memsize >= (c.Align/2) {
			// if we are going to need a full symbol table
			// in the next block, resymbolize so that we
			// don't carry over old symbols
			resymbolize(&c.Buffer, &c.Ranges, &c.Symbols, tail)
			c.symEpoch++
			c.tmpID = 0
			c.flushID = 0
		} else {
			c.Buffer.UnsafeAppend(tail)
		}
	}
	c.tmpbuf.Reset()
	// save the offset of either zero or one objects
	c.lastoff = c.Buffer.Size()
	c.lastst = 0
	c.lastcomp = 0
	if final {
		return nil
	}
	// at this point we have either
	// zero or one object in the buffer,
	// plus maybe the symbol table, so if this
	// doesn't fit within one output flush
	// then we are properly hosed
	if !c.adjustSyms() {
		return fmt.Errorf("1 object (+ symbol table) is %d bytes; above block size %d", c.Buffer.Size(), c.Align)
	}
	return nil
}

func noppad(buf []byte) {
	for len(buf) > 0 {
		wrote, padded := NopPadding(buf, len(buf))
		buf = buf[(wrote + padded):]
	}
}

// CheckSize checks that the current number
// of un-commited bytes will fit within c.Align.
// If the object would not fit, it returns an error immediately.
func (c *Chunker) CheckSize() error {
	if c.Buffer.Size()-c.lastoff < c.Align {
		return nil
	}
	return err2big(c.Align)
}

// Commit commits an object to the state buffer,
// taking care to flush it if we would
// exceed the block alignment.
//
// Note that Commit will refuse to commit
// objects that do not fit in the target
// output alignment.
//
// Commit should be called after each complete
// object has been written to a chunker.
func (c *Chunker) Commit() error {
	if len(c.Buffer.segs) != 0 {
		panic("ion.Chunker.Commit inside object")
	}
	cur := c.Buffer.Bytes()
	lastsize := len(cur) - c.lastoff
	if lastsize > c.Align {
		return err2big(c.Align)
	}
	c.compressed = false
	if len(cur) <= c.Align && c.adjustSyms() {
		c.lastoff = c.Buffer.Size()
		if c.lastoff > c.Align {
			panic("bad bookkeeping")
		}
	} else if err := c.compressOrFlush(); err != nil {
		return err
	}
	c.rowcount++
	c.Ranges.commit()
	return nil
}

// Flush flushes the output of the chunker,
// regardless of whether or not the current
// buffer is approaching the target alignment.
// (The output to c.W will still be padded to
// the appropriate alignment.)
//
// Flush must always be preceded by a
// call to Commit unless zero objects
// have been written to c.Buffer.
func (c *Chunker) Flush() error {
	if c.Buffer.Size() > c.Align {
		return fmt.Errorf("Chunker.Flush not preceded by a call to Chunker.Commit")
	}
	if c.lastoff == 0 {
		if c.written != 0 {
			// make sure c.W.Flush() gets called
			// if we have written any data since
			// the last call to Flush
			return c.flushRanges()
		}
		return nil
	}
	if !c.noCompress && !c.compressed {
		c.compressed = c.compress()
	}
	return c.forceFlush(true)
}

// ReadFrom reads ion from r and re-encodes it
// into the chunker by reading objects one-at-a-time.
// If cons is provided, these fields will be
// added to each structure.
//
// BUGS: ReadFrom only indexes data from the top-level
// of each structure.
func (c *Chunker) ReadFrom(r io.Reader, cons []Field) (int64, error) {
	b := bufio.NewReader(r)

	var typ Type
	var size int
	var err error
	var n int64
	var buf []byte
	var st Symtab
	for {
		typ, size, err = Peek(b)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return n, err
		}
		// discard nop pad
		if typ == NullType {
			b.Discard(size)
			n += int64(size)
			continue
		}
		if size >= c.Align {
			return n, err2big(c.Align)
		}
		var this []byte
		peeked := false
		if size < b.Size() {
			peeked = true
			this, err = b.Peek(size)
			if err != nil {
				return n, err
			}
		} else {
			if cap(buf) >= size {
				this = buf[:size]
			} else {
				buf = make([]byte, size)
				this = buf
			}
			_, err = io.ReadFull(b, buf)
			if err != nil {
				return n, err
			}
		}
		dat, _, err := ReadDatum(&st, this)
		if err != nil {
			return n, err
		}
		if !dat.Empty() {
			if s, ok := dat.Struct(); ok {
				dat = s.mergeFields(&st, cons).Datum()
			} else if len(cons) > 0 {
				return n, fmt.Errorf("row constants disallowed; not a struct (%s)", dat.Type())
			}
			dat.Encode(&c.Buffer, &c.Symbols)
			noteTimeFields(dat, c)
			err = c.Commit()
			if err != nil {
				return n, err
			}
		}
		n += int64(size)
		if peeked {
			b.Discard(size)
		}
	}
	return n, c.Flush()
}

func noteTimeFields(d Datum, c *Chunker) {
	s, ok := d.Struct()
	if !ok {
		return
	}
	err := s.Each(func(f Field) bool {
		ts, ok := f.Timestamp()
		if !ok {
			return true
		}
		sym, ok := c.Symbols.Symbolize(f.Label)
		if !ok {
			return true
		}
		var buf Symbuf
		buf.Prepare(1)
		buf.Push(sym)
		c.Ranges.AddTime(buf, date.Time(ts))
		return true
	})
	if err != nil {
		panic(err)
	}
}

// Write writes a block of ion data from a stream.
// If write does not begin with a BVM and/or symbol table,
// then previous calls to Write must have already set the symbol table.
// (The output stream of Chunker is compatible with Write.)
func (c *Chunker) Write(block []byte) (int, error) {
	// don't bother apply compression here;
	// we expect the input is reasonably well-compressed already
	c.noCompress = true
	defer func() {
		c.noCompress = false
	}()
	var err error
	start := len(block)
	r := resymbolizer{
		srctab: &c.writesyms,
		dsttab: &c.Symbols,
	}
	for len(block) > 0 {
		if IsBVM(block) {
			// we only need to reset on a BVM;
			// a symbol table append doesn't require us
			// to reset the symbol cache because all of
			// the existing old->new mappings are valid
			r.reset()
			block, err = c.writesyms.Unmarshal(block)
		} else if TypeOf(block) == AnnotationType {
			block, err = c.writesyms.Unmarshal(block)
		}
		if err != nil {
			return start - len(block), err
		}
		size := SizeOf(block)
		if size <= 0 || size > len(block) {
			return start - len(block), fmt.Errorf("size %d?", size)
		}
		dat := block[:size]
		block = block[size:]
		if TypeOf(dat) != StructType {
			continue // ignore nop pads
		}
		pos := c.Buffer.Size()
		id := c.Symbols.MaxID()
		r.resym(&c.Buffer, dat)
		if id != c.Symbols.MaxID() {
			c.rangeSyms = c.rangeSyms[:0] // force recomputation of range symbols
		}
		c.walkTimeRanges(c.Buffer.Bytes()[pos:])
		epoch := c.symEpoch
		err = c.Commit()
		if err != nil {
			return start - len(block), err
		}
		if c.symEpoch != epoch {
			r.reset() // symbol table was flushed
			c.rangeSyms = c.rangeSyms[:0]
		}
	}
	return start, nil
}

func pathLess(left, right []Symbol) bool {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	for i := range left[:n] {
		if left[i] < right[i] {
			return true
		}
		if left[i] > right[i] {
			return false
		}
	}
	return len(left) < len(right)
}

const badSymbol = Symbol(0xffffffff)

func resize[T any](x []T, size int) []T {
	if cap(x) >= size {
		return x[:size]
	}
	return make([]T, size)
}

func (c *Chunker) walkTimeRanges(rec []byte) {
	if len(c.WalkTimeRanges) == 0 {
		return
	}
	// rebuild rangeSyms
	if len(c.rangeSyms) == 0 {
		nranges := len(c.WalkTimeRanges)
		c.rangeSyms = resize(c.rangeSyms, nranges)
		for i := range c.WalkTimeRanges {
			path := c.WalkTimeRanges[i]
			sl := c.rangeSyms[i][:0]
			for j := range path {
				// we must use Symbolize instead of Intern
				// to ensure that this process doesn't add
				// new entries to the symbol table
				sym, ok := c.Symbols.Symbolize(path[j])
				if !ok {
					sym = badSymbol
				}
				sl = append(sl, sym)
			}
			c.rangeSyms[i] = sl
		}
		// produce ranges to search in symbol order
		slices.SortFunc(c.rangeSyms, pathLess)
	}
	body, _ := Contents(rec)
	for i := range c.rangeSyms {
		if len(body) == 0 {
			return
		}
		lst := c.rangeSyms[i]
		first := lst[0]
		if first == badSymbol {
			break
		}
		rest := lst[1:]
		var val []byte
		body, val = seek(first, body)
		if val == nil {
			continue
		}
		for j := range rest {
			// traverse into sub-structure
			if TypeOf(val) != StructType || rest[j] == badSymbol {
				val = nil
				break
			}
			val, _ = Contents(val)
			if len(val) == 0 {
				break
			}
			_, val = seek(rest[j], val)
			if len(val) == 0 {
				break
			}
		}
		if len(val) > 0 && TypeOf(val) == TimestampType {
			c.addTime(lst, val)
		}
	}
}

// seek through a record body and produce
// the offset in the struct where the field begins,
// plus the value at that offset
func seek(search Symbol, body []byte) ([]byte, []byte) {
	var err error
	var field Symbol
	var rest []byte
	prev := Symbol(0)
	for len(body) > 0 {
		field, rest, err = ReadLabel(body)
		if err != nil {
			return nil, nil
		}
		if field < prev {
			panic("symbols out-of-order")
		}
		size := SizeOf(rest)
		if size <= 0 || size > len(rest) {
			return nil, nil
		}
		if field == search {
			return body, rest[:size]
		}
		if field > search {
			return body, nil
		}
		prev = field
		body = rest[size:]
	}
	return body, nil
}

func (c *Chunker) addTime(lst []Symbol, val []byte) {
	tm, _, err := ReadTime(val)
	if err != nil {
		return
	}
	var sb Symbuf
	sb.Prepare(len(lst))
	for i := range lst {
		if lst[i] == Symbol(0xffffffff) {
			panic("bad AddTime call")
		}
		sb.Push(lst[i])
	}
	c.Ranges.AddTime(sb, tm)
}
