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

// Package jsonrl implements a Ragel-accelerated
// JSON parser that can be used to translate
// JSON data into ion data.
package jsonrl

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

//go:generate ragel -L -Z -G2 lex.rl

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

func (s *State) shift(f *fieldlist) {
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

type TransitionType byte

const (
	transBeginRecord TransitionType = iota
	transEndRecord
	transBeginList
	transEndList
	transBeginField
	transParseInt
	transParseFloat
	transParseBool
	transParseNull
	transParseString
	transMaxValue
)

type SchemaStateToken byte

const (
	stateEntry SchemaStateToken = iota
	stateExpectFieldOrEnd
	stateExpectValueOrEnd
	stateExpectRecord
	stateExpectList
	stateExpectValue // arbitrary value expected -> emit default
	stateExpectString
	stateExpectNumber
	stateExpectInt
	stateExpectBool
	stateExpectDateTime
	stateExpectUnixSeconds
)

type SchemaState struct {
	token       SchemaStateToken
	transitions []*SchemaState
	fields      map[string]*SchemaState
}

// MakeSchema creates a schema structure from the given json-schema input
func MakeSchema(schema []byte) (*SchemaState, error) {

	var result map[string]interface{}
	err := json.Unmarshal(schema, &result)
	if err != nil {
		return nil, err
	}

	entry := makeNode(stateEntry)
	rec := entry.addTransition(transBeginRecord, stateExpectFieldOrEnd)

	for k, v := range result {
		if err = makeSchemaRecursive(k, v, rec); err != nil {
			return nil, err
		}
	}

	return entry, nil
}

func makeSchemaRecursive(k string, v interface{}, current *SchemaState) error {

	s, ok := v.(string)
	if ok {
		token := stateExpectString

		// TODO: Add more conversions
		switch s {
		case "number":
			token = stateExpectNumber
		case "int":
			token = stateExpectInt
		case "bool":
			token = stateExpectBool
		case "datetime":
			token = stateExpectDateTime
		case "unix_seconds":
			token = stateExpectUnixSeconds
		case "string":
			// nothing to do here
		default:
			return errors.New("type not implemented")
		}

		current.addField(k, token)
	}

	o, ok := v.(map[string]interface{})
	if ok {
		cur := current.addField(k, stateExpectRecord)
		rec := cur.addTransition(transBeginRecord, stateExpectFieldOrEnd)
		for k2, v2 := range o {
			if err := makeSchemaRecursive(k2, v2, rec); err != nil {
				return err
			}
		}
	}

	// TODO: Add support for lists

	//a, ok := v.([]interface{})
	//if ok {
	//	cur := current.addField(k, stateExpectList)
	//	rec := cur.addTransition(transBeginList, stateExpectValueOrEnd)
	//	for _, v2 := range a {
	//		makeSchemaRecursive("", v2, rec)
	//	}
	//}

	return nil
}

func makeNode(state SchemaStateToken) *SchemaState {

	if state == stateExpectFieldOrEnd {
		return &SchemaState{stateExpectFieldOrEnd, make([]*SchemaState, transMaxValue), make(map[string]*SchemaState)}
	}

	node := SchemaState{state, make([]*SchemaState, transMaxValue), nil}

	// entry [*] -> entry
	if state == stateEntry {
		for i := range node.transitions {
			node.transitions[i] = &node
		}
	}

	return &node
}

func (n *SchemaState) addTransition(trans TransitionType, nextState SchemaStateToken) *SchemaState {

	node := makeNode(nextState)

	if n.token == stateExpectList {
		// * [*] -> self
		for i := range node.transitions {
			node.transitions[i] = node
		}
	}

	n.transitions[trans] = node

	// expect_field_or_end [end_record] -> parent_token
	if nextState == stateExpectFieldOrEnd {
		node.transitions[transEndRecord] = n
	}

	// expect_value_or_end [end_list] -> parent_token
	if nextState == stateExpectValueOrEnd {
		node.transitions[transEndList] = n
	}

	// expect_record|expect_list [*] -> parent_token
	if nextState == stateExpectRecord || nextState == stateExpectList {
		for i := range node.transitions {
			node.transitions[i] = n
		}
	}

	return node
}

func (n *SchemaState) addField(label string, nextState SchemaStateToken) *SchemaState {

	node := makeNode(nextState)

	// * [*] -> parent_token
	for i := range node.transitions {
		node.transitions[i] = n
	}

	n.fields[label] = node

	return node
}

type State struct {
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

	schemaState *SchemaState
	schemaStack []*SchemaState
	ignore      bool
	ignoreLevel int
	pathbuf     ion.Symbuf // scratch buffer for path
}

func NewState(dst *ion.Chunker) *State {
	return &State{out: dst}
}

// rewind the state when attempting to
// parse objects incrementally; this needs
// to reset all of the internal state that
// gets set up during object parsing
func (s *State) rewind(snapshot *ion.Snapshot) {
	s.out.Load(snapshot)
	s.stack = s.stack[:0]
	s.flags = 0
	s.oldflags = s.oldflags[:0]
	if len(s.schemaStack) != 0 {
		s.schemaState = s.schemaStack[0]
	}
	s.schemaStack = s.schemaStack[:0]
	s.ignore = false
	s.ignoreLevel = 0
}

func (s *State) Commit() error {
	if len(s.stack) != 0 {
		return fmt.Errorf("State.Commit inside object?")
	}
	return s.out.Commit()
}

// adjust the parser state after each
// object is inserted
func (s *State) after() {
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
func (s *State) addTimeRange(t date.Time) {
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

func (s *State) parseInt(i int64) {
	token, skip := s.doTransition(transParseInt)
	if skip {
		return
	}

	switch token {
	case stateExpectString:
		v := strconv.Itoa(int(i))
		s.out.WriteString(v)
	case stateExpectUnixSeconds:
		t := date.Unix(i, 0)
		s.addTimeRange(t)
		s.out.WriteTime(t)
	default:
		s.out.WriteInt(i)
	}

	s.after()
}

var ErrBadUTF8 = errors.New("bad utf8 sequence")

func appendRune(dst []byte, r rune) []byte {
	// FIXME: this is slow and gross
	tmp := append(dst, 0, 0, 0, 0)
	l := utf8.EncodeRune(tmp[len(dst):], r)
	return tmp[:len(dst)+l]
}

// unescaped processes strings that include
// backslash escape sequences
func (s *State) unescaped(buf []byte) []byte {
	tmp := s.tmp[:0]

	for i := 0; i < len(buf); i++ {
		c := buf[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(buf[i:])
			if r == utf8.RuneError {
				tmp = appendRune(tmp, r)
				if size == 1 {
					size = bits.LeadingZeros(uint(^buf[0])) - 24
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
			tmp = appendRune(tmp, r)
		default:
			fmt.Printf("char: %c\n", c)
			fmt.Printf("escape sequence %s\n", buf[i-1:i+1])
			os.WriteFile("bad-string", buf, 0666)
			panic("unexpected escape sequence")
		}
	}
	return tmp
}

func (s *State) parseFloat(f float64) {
	token, skip := s.doTransition(transParseFloat)
	if skip {
		return
	}

	switch token {
	case stateExpectString:
		v := strconv.FormatFloat(f, 'f', -1, 32)
		s.out.WriteString(v)
	default:
		// emit the core-normalized representation of f
		if i := int64(f); float64(i) == f {
			s.out.WriteInt(i)
		} else {
			s.out.WriteFloat64(f)
		}
	}

	s.after()
}

func (s *State) pushRecord() {
	// push a new stack state
	if len(s.stack) == cap(s.stack) {
		s.stack = append(s.stack, fieldlist{})
	} else {
		s.stack = s.stack[:len(s.stack)+1]
	}
	s.stack[len(s.stack)-1].init()
}

func (s *State) popRecord() {
	s.stack = s.stack[:len(s.stack)-1]
}

func (s *State) pushFlags(u uint) {
	s.oldflags = append(s.oldflags, s.flags)
	s.flags = u
}

func (s *State) popFlags() {
	s.flags = s.oldflags[len(s.oldflags)-1]
	s.oldflags = s.oldflags[:len(s.oldflags)-1]
}

func (s *State) beginRecord() {
	if _, skip := s.doTransition(transBeginRecord); skip {
		return
	}

	s.pushRecord()
	s.pushFlags(flagInRecord)
	s.out.BeginStruct(-1)
}

func (s *State) endRecord() {
	if _, skip := s.doTransition(transEndRecord); skip {
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

func (s *State) beginField(label []byte, esc bool) {
	if s.ignore {
		return
	}

	if esc {
		label = s.unescaped(label)
	}

	if skip := s.doFieldTransition(label); skip {
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

func (s *State) beginList() {
	if _, skip := s.doTransition(transBeginList); skip {
		return
	}

	s.pushFlags(flagInList)
	s.out.BeginList(-1)
}

func (s *State) endList() {
	if _, skip := s.doTransition(transEndList); skip {
		return
	}

	s.out.EndList()
	s.popFlags()
	s.after()
}

func (s *State) parseBool(b bool) {
	token, skip := s.doTransition(transParseBool)
	if skip {
		return
	}

	switch token {
	case stateExpectString:
		if b {
			s.out.WriteString("true")
		} else {
			s.out.WriteString("false")
		}
	case stateExpectInt:
		if b {
			s.out.WriteInt(1)
		} else {
			s.out.WriteInt(0)
		}
	default:
		s.out.WriteBool(b)
	}

	s.after()
}

func (s *State) parseNull() {
	if _, skip := s.doTransition(transParseNull); skip {
		return
	}

	s.out.WriteNull()
	s.after()
}

func (s *State) parseString(seg []byte, esc bool) {
	token, skip := s.doTransition(transParseString)
	if skip {
		return
	}

	if esc {
		seg = s.unescaped(seg)
	}

	emitDefault := true

	switch token {
	case stateExpectNumber:
		if f, err := strconv.ParseFloat(string(seg), 64); err == nil {
			emitDefault = false
			// emit the core-normalized representation of f
			if i := int64(f); float64(i) == f {
				s.out.WriteInt(i)
			} else {
				s.out.WriteFloat64(f)
			}
		}
	case stateExpectInt:
		if i, err := strconv.Atoi(string(seg)); err == nil {
			emitDefault = false
			s.out.WriteInt(int64(i))
		}
	case stateExpectDateTime:
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
		} else {
			s.out.BeginString(len(seg))
			s.out.UnsafeAppend(seg)
		}
	}

	s.after()
}

func (s *State) doTransition(trans TransitionType) (SchemaStateToken, bool) {

	if s.schemaState == nil {
		// No schema provided -> take everything
		return stateExpectValue, false
	}

	prev := s.schemaState
	var next *SchemaState

	ignore := s.ignore

	if trans == transBeginRecord || trans == transBeginList {
		if s.ignore {
			s.ignoreLevel++
		} else {
			// PUSH
			a := prev.transitions[transEndRecord]
			s.schemaStack = append(s.schemaStack, a)
			next = prev.transitions[trans]
		}
	} else if trans == transEndRecord || trans == transEndList {
		if s.ignore {
			s.ignoreLevel--
		} else {
			// POP
			s.schemaState = s.schemaStack[len(s.schemaStack)-1]
			s.schemaStack = s.schemaStack[:len(s.schemaStack)-1]
			next = s.schemaState
		}
	} else {
		if prev != nil {
			next = prev.transitions[trans]
		}
	}

	if s.ignore && s.ignoreLevel == 0 {
		s.ignore = false
	}

	if !ignore {
		s.schemaState = next
	}

	return prev.token, ignore
}

func (s *State) doFieldTransition(label []byte) (skip bool) {

	if s.schemaState == nil {
		// No schema provided -> take everything
		return false
	}

	next, ok := s.schemaState.fields[string(label)]
	if !ok {
		s.ignore = true
		return true
	}

	s.schemaState = next

	return false
}
