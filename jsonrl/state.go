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

// Package jsonrl implements a Ragel-generated
// JSON parser that converts JSON data into ion data (see Convert).
package jsonrl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/slices"
)

//go:generate gofmt -w .

type fieldoff struct {
	sym ion.Symbol
	off int32
}

type fieldlist struct {
	fields []fieldoff
	rotidx int
}

func (f *fieldlist) init() {
	f.fields = f.fields[:0]
	f.rotidx = -1
}

func (f *fieldlist) start(sym ion.Symbol, off int32) {
	f.rotidx = -1
	f.fields = append(f.fields, fieldoff{
		sym: sym,
		off: off,
	})
	if len(f.fields) == 1 || sym >= f.fields[len(f.fields)-2].sym {
		return
	}
	// walk the fields in reverse
	// to find the offset at which we
	// would like to insert this field
	// once we have its bits
	j := len(f.fields) - 2
	for ; j > 0 && f.fields[j-1].sym > sym; j-- {
	}
	f.rotidx = j
}

func (s *state) shift(f *fieldlist) {
	// need to rotate the last field
	// to the position occupied by f.fields[f.rotidx]
	buf := s.out.Bytes()
	start := f.fields[len(f.fields)-1].off
	s.tmp = append(s.tmp[:0], buf[start:]...)

	// copy the existing fields forward,
	// leaving space for the field
	width := int32(len(buf)) - start
	mov := f.fields[f.rotidx].off
	copy(buf[mov+width:], buf[mov:])
	// copy the saved field into the old space
	copy(buf[mov:], s.tmp)

	// adjust f.fields so that they are sorted
	sym := f.fields[len(f.fields)-1].sym
	copy(f.fields[f.rotidx+1:], f.fields[f.rotidx:])
	f.fields[f.rotidx].sym = sym
	for i := f.rotidx + 1; i < len(f.fields); i++ {
		f.fields[i].off += width
	}
}

const (
	flagInRecord = 1 << iota
	flagInList
	flagField
)

type hints int

const (
	hintDefault hints = 1 << iota

	hintString
	hintNumber
	hintInt
	hintBool
	hintDateTime
	hintUnixSeconds
	hintUnixMilliSeconds
	hintUnixMicroSeconds
	hintUnixNanoSeconds

	hintIgnore
	hintNoIndex
)

// Hint represents a structure containing type-hints and/or other flags to be used
// by the json parser. See ParseHint for further information.
type Hint struct {
	parent              *Hint
	hints               hints
	isRecursiveWildcard bool
	fields              map[string]*Hint
	wildcard            *Hint
}

// ParseHint parses a json byte array into a Hint structure which can
// later be used to pass type-hints and/or other flags to the json parser.
//
// The input must contain a valid json object with the individual rules:
//
//	  {
//		   "path.to.value.a": "hint",
//		   "path.to.value.b": ["hint_a", "hint_b"]
//	  }
//
// The precedence of overlapping rules is determined by the order in which the rules
// are written.
//
// The '?'/'[?]' wildcard can be used to match all keys of the current level.
//
// The '*'/'[*]' wildcard can be used to match all keys of the current level and all following
// levels. Must be the last segment in the path.
//
// Supported actions:
//   - `ignore` -> do not parse this property
//   - `no_index` -> do not add this property to the sparse index
//
// Supported hints:
//   - string
//   - number -> either float or int
//   - int
//   - bool
//   - datetime -> RFC3339Nano
//   - unix_seconds
func ParseHint(rules []byte) (*Hint, error) {
	var obj map[string]interface{}
	err := json.Unmarshal(rules, &obj)
	if err != nil {
		return nil, err
	}

	// We need the keys in their original order which is not guaranteed if we would
	// use `map[string]interface{}`
	keys, err := objectKeys(rules)
	if err != nil {
		return nil, err
	}

	root := makeHintNode(nil, hintDefault, false)

	for _, path := range keys {
		value := obj[path]
		hints, err := hintsFromJSON(value)
		if err != nil {
			return nil, err
		}
		if err = root.encodeRuleString(path, hints); err != nil {
			return nil, err
		}
	}

	return root, nil
}

func objectKeys(b []byte) ([]string, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	if t != json.Delim('{') {
		return nil, errors.New("expected start of object")
	}
	var keys []string
	for {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		if t == json.Delim('}') {
			return keys, nil
		}
		keys = append(keys, t.(string))
		if err := skipValue(d); err != nil {
			return nil, err
		}
	}
}

func skipValue(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	switch t {
	case json.Delim('['), json.Delim('{'):
		for {
			if err := skipValue(d); err != nil {
				if err == errErrDelim {
					break
				}
				return err
			}
		}
	case json.Delim(']'), json.Delim('}'):
		return errErrDelim
	}
	return nil
}

var errErrDelim = errors.New("invalid end of array or object")

func hintsFromJSON(value interface{}) (hints, error) {
	s, ok := value.(string)
	if ok {
		return hintFromString(s)
	}
	a, ok := value.([]string)

	if ok {
		result := hintDefault
		for _, v := range a {
			h, err := hintFromString(v)
			if err != nil {
				return hintDefault, err
			}
			result |= h
		}
		return result, nil
	}

	return hintDefault, errors.New("unsupported hint type; expected 'string' or '[]string'")
}

func hintFromString(value string) (hints, error) {
	switch value {
	case "default":
		return hintDefault, nil
	case "string":
		return hintString, nil
	case "number":
		return hintNumber, nil
	case "int":
		return hintInt, nil
	case "bool":
		return hintBool, nil
	case "datetime":
		return hintDateTime, nil
	case "unix_seconds":
		return hintUnixSeconds, nil
	case "unix_milli_seconds":
		return hintUnixMilliSeconds, nil
	case "unix_micro_seconds":
		return hintUnixMicroSeconds, nil
	case "unix_nano_seconds":
		return hintUnixNanoSeconds, nil
	case "ignore":
		return hintIgnore, nil
	case "no_index":
		return hintNoIndex, nil
	}

	return hintDefault, fmt.Errorf("unsupported hint '%s'", value)
}

func makeHintNode(parent *Hint, hints hints, isRecursiveWildcard bool) *Hint {
	return &Hint{
		parent:              parent,
		hints:               hints,
		isRecursiveWildcard: isRecursiveWildcard,
		fields:              map[string]*Hint{},
	}
}

func (n *Hint) hasWildcard() bool {
	return n.wildcard != nil
}

func (n *Hint) getNext(label []byte) *Hint {
	next, ok := n.fields[string(label)]
	if ok {
		return next
	}
	if n.wildcard != nil {
		return n.wildcard
	}
	return n
}

func (n *Hint) encodeRuleString(path string, hints hints) error {
	segments := strings.Split(path, ".")
	return n.encodeRule(segments, hints)
}

func (n *Hint) encodeRule(path []string, hints hints) error {
	segment := path[0]

	if segment == "" {
		return errors.New("empty path")
	}

	if segment == "[*]" {
		segment = "*"
	}
	if segment == "[?]" {
		segment = "?"
	}
	if strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") {
		return errors.New("array brackets must enclose a wildcard index ([*] or [?])")
	}

	isFinalSegment := len(path) == 1
	isWildcard := segment == "?" || segment == "*"
	isRecursiveWildcard := segment == "*"

	if isRecursiveWildcard && !isFinalSegment {
		return errors.New("recursive wildcard (*) is only valid at the end of a path")
	}
	if isWildcard && !isRecursiveWildcard && hints&hintIgnore != 0 {
		return errors.New("the 'ignore' hint is only valid for explicit fields or the recursive wildcard (*)")
	}

	if n.hasWildcard() && !isWildcard {
		// We are trying to encode an explicit field, but a wildcard is already present
		// => new rule would never match
		return nil
	}

	if !isRecursiveWildcard && n.hints != hintDefault {
		return nil
	}

	nextHints := hintDefault
	if isFinalSegment {
		nextHints = hints
	}
	next := n.getOrCreate(segment, nextHints, isWildcard, isRecursiveWildcard)

	if isFinalSegment && !isRecursiveWildcard && hints&hintIgnore != 0 {
		// Implicitly add a recursive wildcard to as well ignore nested elements, if the
		// explicit field is a struct or an array
		next.wildcard = next.getOrCreate("*", hintIgnore, true, true)
	}

	if !isFinalSegment {
		// Recursively encode the next segment
		err := next.encodeRule(path[1:], hints)
		if err != nil {
			return err
		}

		if !isWildcard {
			return nil
		}

		// We are encoding a wildcard (?) segment which is not the final segment
		// => all existing nodes on the same level must encode the subsequent segments
		for _, v := range n.fields {
			err = v.encodeRule(path[1:], hints)
			if err != nil {
				return err
			}
		}

		return nil
	}

	if !isWildcard {
		return nil
	}

	// We are encoding a wildcard (? or *) segment which is the final segment
	// => update the current wildcard node
	if next.hints == hintDefault {
		next.hints = hints
	}

	if !isRecursiveWildcard {
		return nil
	}

	// We are encoding a recursive wildcard (*) segment
	// => all existing nodes on the same level must encode the wildcard recursively
	for _, v := range n.fields {
		err := v.encodeRule(path, hints)
		if err != nil {
			return err
		}
	}
	if !n.isRecursiveWildcard {
		err := n.wildcard.encodeRule(path, hints)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *Hint) getOrCreate(label string, hints hints, isWildcard bool, isRecursiveWildcard bool) *Hint {
	if isWildcard {
		if n.wildcard == nil {
			n.wildcard = makeHintNode(n, hints, isRecursiveWildcard)
		}
		return n.wildcard
	}

	next, ok := n.fields[label]
	if ok {
		return next
	}

	if n.wildcard != nil {
		return n.wildcard
	}

	next = makeHintNode(n, hints, false)
	n.fields[label] = next

	return next
}

type hintState struct {
	root    *Hint
	hints   hints
	current *Hint
	next    *Hint
	level   int
}

func makeHintState(root *Hint) hintState {
	level := -1
	if root == nil {
		level = 1
	}
	return hintState{
		root:    root,
		hints:   hintDefault,
		current: root,
		level:   level,
	}
}

func (s *hintState) nextIsRecursive() bool {
	return s.next == nil || s.next.isRecursiveWildcard
}

// / enter should be invoked before a sub-structure (either record or array) is entered
func (s *hintState) enter() {
	if s.nextIsRecursive() || s.level > 0 {
		s.level++
		return
	}

	s.hints = hintDefault
	if s.next != nil {
		s.current = s.next
	}
}

// / leave should be invoked before a sub-structure (either record or array) is left
func (s *hintState) leave() {
	if s.level > 0 {
		s.level--
		if s.level == 0 {
			s.hints = hintDefault
		}
		return
	}

	if s.current.parent != nil {
		s.current = s.current.parent
	} else {
		s.level = -1
	}
}

// / field should be invoked before a field label is parsed
func (s *hintState) field(label []byte) {
	if s.level > 0 {
		return
	}

	next := s.current.getNext(label)
	if next == s.current && !s.current.isRecursiveWildcard {
		s.hints = hintDefault
	} else {
		s.hints = next.hints
	}

	s.next = next
}

// / afterListEntered should be invoked after a list has been entered
func (s *hintState) afterListEntered() {
	// Invoking `field` with an empty `label` sets `next` to `current` and updates the
	// currently effective hints
	s.field([]byte{})
}

type state struct {
	out *ion.Chunker

	// stack of structure state;
	// gets pushed on beginRecord()
	// and popped on endRecord()
	stack []fieldlist

	// various state bits;
	// pushed and popped when compound
	// (struct, list) objects are begun/ended
	flags    uint
	oldflags []uint

	// cache is a simple symbol LUT
	// that uses a super fast hash function
	// for common (ascii, small) strings
	//
	// TODO: would this work better in practice
	// with two caches? we could use first- and last-character
	// based hashes...
	cache []ion.Symbol

	// temporary buffer for formatting
	// un-escaped strings
	tmp []byte

	pathbuf ion.Symbuf // scratch buffer for path

	hints hintState

	constResolved bool
}

func newState(dst *ion.Chunker) *state {
	return &state{
		out: dst,
	}
}

func (s *state) UseHints(hints *Hint) {
	s.hints = makeHintState(hints)
}

func (s *state) shouldIgnore() bool {
	return s.hints.hints&hintIgnore != 0
}

func (s *state) shouldNotIndex() bool {
	return s.hints.hints&hintNoIndex != 0
}

func (s *state) coerceString() bool {
	return s.hints.hints&hintString != 0
}

func (s *state) coerceNumber() bool {
	return s.hints.hints&hintNumber != 0
}

func (s *state) coerceInt() bool {
	return s.hints.hints&hintInt != 0
}

func (s *state) coerceDateTime() bool {
	return s.hints.hints&hintDateTime != 0
}

func (s *state) coerceUnixSeconds() bool {
	return s.hints.hints&hintUnixSeconds != 0
}

func (s *state) coerceUnixMilliSeconds() bool {
	return s.hints.hints&hintUnixMilliSeconds != 0
}

func (s *state) coerceUnixMicroSeconds() bool {
	return s.hints.hints&hintUnixMicroSeconds != 0
}

func (s *state) coerceUnixNanoSeconds() bool {
	return s.hints.hints&hintUnixNanoSeconds != 0
}

func (s *state) Commit() error {
	if len(s.stack) != 0 {
		return fmt.Errorf("state.Commit inside object?")
	}
	return s.out.Commit()
}

// adjust the parser state after each
// object is inserted
func (s *state) after() {
	if s.flags&flagField == 0 {
		return
	}
	s.flags &^= flagField
	if s.stack[len(s.stack)-1].rotidx >= 0 {
		// slow-path: shift object bytes around
		// so that symbols remain sorted
		s.shift(&s.stack[len(s.stack)-1])
	}
}

// addTimeRange adds a time to the range for the path
// to the current field.
func (s *state) addTimeRange(t date.Time) {
	if s.shouldNotIndex() || len(s.stack) >= MaxIndexingDepth {
		return
	}
	if s.flags&(flagField|flagInList) != flagField {
		return
	}
	for i := 1; i < len(s.oldflags); i++ {
		if s.oldflags[i]&(flagField|flagInList) != flagField {
			return
		}
	}
	s.pathbuf.Prepare(len(s.stack))
	for i := range s.stack {
		fl := &s.stack[i]
		sym := fl.fields[len(fl.fields)-1].sym
		s.pathbuf.Push(sym)
	}
	s.out.Ranges.AddTime(s.pathbuf, t)
}

func (s *state) parseInt(i int64) {
	if s.shouldIgnore() {
		return
	}

	if s.coerceString() {
		v := strconv.Itoa(int(i))
		s.out.WriteString(v)
	} else if s.coerceUnixSeconds() {
		t := date.Unix(i, 0)
		s.addTimeRange(t)
		s.out.WriteTime(t)
	} else if s.coerceUnixMilliSeconds() {
		t := date.FromTime(time.UnixMilli(i))
		s.addTimeRange(t)
		s.out.WriteTime(t)
	} else if s.coerceUnixMicroSeconds() {
		t := date.UnixMicro(i)
		s.addTimeRange(t)
		s.out.WriteTime(t)
	} else if s.coerceUnixNanoSeconds() {
		t := date.Unix(i/1e9, i%1e9)
		s.addTimeRange(t)
		s.out.WriteTime(t)
	} else {
		s.out.WriteInt(i)
	}

	s.after()
}

// unescaped processes strings that include
// backslash escape sequences
func (s *state) unescaped(buf []byte) []byte {
	tmp := s.tmp[:0]

	for i := 0; i < len(buf); i++ {
		c := buf[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(buf[i:])
			if r == utf8.RuneError {
				tmp = utf8.AppendRune(tmp, r)
				if size == 1 {
					size = bits.LeadingZeros32(uint32(^buf[i])) - 24
				}
			} else {
				tmp = append(tmp, buf[i:i+size]...)
			}
			i += size - 1
			continue
		} else if c != '\\' {
			tmp = append(tmp, c)
			continue
		}
		i++
		c = buf[i]
		// from lex.rl:
		// escape_sequence = (("\\" [tvfnrab\\\"/]) | ("\\u" xdigit{4}))
		switch c {
		case '\\':
			tmp = append(tmp, '\\')
		case 't':
			tmp = append(tmp, '\t')
		case 'n':
			tmp = append(tmp, '\n')
		case 'r':
			tmp = append(tmp, '\r')
		case 'v':
			tmp = append(tmp, '\v')
		case 'f':
			tmp = append(tmp, '\f')
		case 'a':
			tmp = append(tmp, '\a')
		case 'b':
			tmp = append(tmp, '\b')
		case '"':
			tmp = append(tmp, '"')
		case '/':
			tmp = append(tmp, '/')
		case 'u':
			r := rune(0)
			i++
			for j := i; j < i+4; j++ {
				add := rune(buf[j])
				if add >= '0' && add <= '9' {
					add -= '0'
				} else if add >= 'A' && add <= 'F' {
					add -= 'A'
					add += 10
				} else if add >= 'a' && add <= 'f' {
					add -= 'a'
					add += 10
				}
				r = (r * 16) + add
			}
			i += 3
			if !utf8.ValidRune(r) {
				r = utf8.RuneError
			}
			tmp = utf8.AppendRune(tmp, r)
		default:
			fmt.Printf("char: %c\n", c)
			fmt.Printf("escape sequence %s\n", buf[i-1:i+1])
			os.WriteFile("bad-string", buf, 0666)
			panic("unexpected escape sequence")
		}
	}
	return tmp
}

func (s *state) parseFloat(f float64) {
	if s.shouldIgnore() {
		return
	}

	if s.coerceString() {
		v := strconv.FormatFloat(f, 'f', -1, 32)
		s.out.WriteString(v)
	} else {
		// emit the core-normalized representation of f
		if i := int64(f); float64(i) == f {
			s.out.WriteInt(i)
		} else {
			s.out.WriteFloat64(f)
		}
	}

	s.after()
}

func (s *state) pushRecord() {
	// push a new stack state
	if len(s.stack) == cap(s.stack) {
		s.stack = append(s.stack, fieldlist{})
	} else {
		s.stack = s.stack[:len(s.stack)+1]
	}
	s.stack[len(s.stack)-1].init()
}

func (s *state) popRecord() {
	s.stack = s.stack[:len(s.stack)-1]
}

func (s *state) pushFlags(u uint) {
	s.oldflags = append(s.oldflags, s.flags)
	s.flags = u
}

func (s *state) popFlags() {
	s.flags = s.oldflags[len(s.oldflags)-1]
	s.oldflags = s.oldflags[:len(s.oldflags)-1]
}

func (s *state) beginRecord() {
	ignore := s.shouldIgnore()
	s.hints.enter()
	if ignore {
		return
	}

	s.pushRecord()
	s.pushFlags(flagInRecord)
	s.out.BeginStruct(-1)
}

func (s *state) endRecord() {
	ignore := s.shouldIgnore()
	if s.hints.level == 0 && s.hints.next != nil && s.hints.next.isRecursiveWildcard && s.hints.hints&hintIgnore != 0 && s.hints.current.hints&hintIgnore == 0 {
		ignore = false
	}
	s.hints.leave()
	if ignore {
		return
	}

	// pop the previous field symbol
	s.popRecord()
	s.popFlags()
	s.out.EndStruct()
	s.after()
}

const maxDumb = 1 << 9

func dumb(buf []byte) (int, bool) {
	if len(buf) >= 16 || len(buf) == 0 {
		return 0, false
	}
	if buf[0] >= 'a' && buf[0] <= 'z' {
		return (int(buf[0]-'a') << 4) | len(buf), true
	}
	if buf[0] >= 'A' && buf[0] <= 'Z' {
		return (int(buf[0]-'A') << 4) | len(buf), true
	}
	return 0, false
}

func (s *state) emitConst(lst []ion.Field) {
	if !s.constResolved {
		for i := range lst {
			lst[i].Sym = s.out.Symbols.Intern(lst[i].Label)
		}
		// most of the time we should produce sorted results;
		// just in case we don't:
		slices.SortFunc(lst, func(x, y ion.Field) bool {
			return x.Sym < y.Sym
		})
		s.constResolved = true
	}
	for i := range lst {
		sym := lst[i].Sym
		s.stack[len(s.stack)-1].start(sym, int32(s.out.Size()))
		s.out.BeginField(sym)
		lst[i].Value.Encode(&s.out.Buffer, &s.out.Symbols)
		s.after()
	}
}

func (s *state) beginField(label []byte, esc bool) {
	if esc {
		label = s.unescaped(label)
	}

	s.hints.field(label)
	if s.shouldIgnore() {
		return
	}

	if s.cache == nil {
		s.cache = make([]ion.Symbol, maxDumb)
	}

	// try to see if we can do a fast lookup of this symbol
	var sym ion.Symbol
	if idx, ok := dumb(label); ok {
		sym = s.cache[idx]
		// FYI: relying on the compiler to
		// optimize this comparison so that
		// we don't actually allocate a string
		if s.out.Symbols.Get(sym) != string(label) {
			sym = s.out.Symbols.InternBytes(label)
			s.cache[idx] = sym
		}
	} else {
		sym = s.out.Symbols.InternBytes(label)
	}
	s.stack[len(s.stack)-1].start(sym, int32(s.out.Size()))
	s.flags |= flagField
	s.out.BeginField(sym)
}

func (s *state) beginList() {
	ignore := s.shouldIgnore()
	s.hints.enter()
	s.hints.afterListEntered()
	if ignore {
		return
	}

	s.pushFlags(flagInList)
	s.out.BeginList(-1)
}

func (s *state) endList() {
	ignore := s.shouldIgnore()
	s.hints.leave()
	if ignore {
		return
	}

	s.out.EndList()
	s.popFlags()
	s.after()
}

func (s *state) parseBool(b bool) {
	if s.shouldIgnore() {
		return
	}

	if s.coerceString() {
		if b {
			s.out.WriteString("true")
		} else {
			s.out.WriteString("false")
		}
	} else if s.coerceInt() {
		if b {
			s.out.WriteInt(1)
		} else {
			s.out.WriteInt(0)
		}
	} else {
		s.out.WriteBool(b)
	}

	s.after()
}

func (s *state) parseNull() {
	if s.shouldIgnore() {
		return
	}

	s.out.WriteNull()
	s.after()
}

func (s *state) parseString(seg []byte, esc bool) {
	if s.shouldIgnore() {
		return
	}

	if esc {
		seg = s.unescaped(seg)
	}

	emitDefault := true

	if s.coerceNumber() {
		if f, err := strconv.ParseFloat(string(seg), 64); err == nil {
			emitDefault = false
			// emit the core-normalized representation of f
			if i := int64(f); float64(i) == f {
				s.out.WriteInt(i)
			} else {
				s.out.WriteFloat64(f)
			}
		}
	} else if s.coerceInt() {
		if i, err := strconv.Atoi(string(seg)); err == nil {
			emitDefault = false
			s.out.WriteInt(int64(i))
		}
	} else if s.coerceDateTime() {
		if t, ok := date.Parse(seg); ok {
			emitDefault = false
			s.addTimeRange(t)
			s.out.WriteTime(t)
		}
	}

	if emitDefault {
		if t, ok := date.Parse(seg); ok {
			s.addTimeRange(t)
			s.out.WriteTime(t)
		} else if sym, ok := s.out.Symbols.SymbolizeBytes(seg); ok {
			s.out.WriteSymbol(sym)
		} else {
			s.out.BeginString(len(seg))
			s.out.UnsafeAppend(seg)
		}
	}

	s.after()
}
