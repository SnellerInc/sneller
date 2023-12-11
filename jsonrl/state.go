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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

//go:generate gofmt -w .

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

var (
	hintStrings = map[hints]string{
		hintDefault:          "default",
		hintString:           "string",
		hintNumber:           "number",
		hintInt:              "int",
		hintBool:             "bool",
		hintDateTime:         "datetime",
		hintUnixSeconds:      "unix_seconds",
		hintUnixMilliSeconds: "unix_milli_seconds",
		hintUnixMicroSeconds: "unix_micro_seconds",
		hintUnixNanoSeconds:  "unix_nano_seconds",
		hintIgnore:           "ignore",
		hintNoIndex:          "no_index",
	}
	hintValues = reverseMap(hintStrings)
)

func reverseMap[K, V comparable](m map[K]V) map[V]K {
	n := make(map[V]K, len(m))
	for k, v := range m {
		n[v] = k
	}
	return n
}

func (h hints) String() string {
	if h == hintDefault {
		return hintStrings[hintDefault]
	}

	var result []string

	for k, v := range hintStrings {
		if k == hintDefault {
			continue
		}
		if h&k != 0 {
			result = append(result, v)
		}
	}

	if len(result) == 0 {
		return "invalid"
	}

	sort.Strings(result)

	return strings.Join(result[:], ", ")
}

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
// The input must contain a valid JSON array with the individual rules:
//
//	[
//	  { "path": "path.to.value.a", "hints": "hint" },
//	  { "path": "path.to.value.b", "hints": ["hint_a", "hint_b"] }
//	]
//
// A JSON object may be used as an alternative (not recommended):
//
//	{
//	  "path.to.value.a": "hint",
//	  "path.to.value.b": ["hint_a", "hint_b"]
//	}
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
func ParseHint(rules []byte) (hint *Hint, err error) {
	hint = &Hint{}
	err = json.Unmarshal(rules, hint)
	if err != nil {
		return nil, err
	}
	return
}

func (n *Hint) String() string {
	return n.printTree("root", 0)
}

func (n *Hint) UnmarshalJSON(data []byte) error {
	*n = *makeHintNode(nil, hintDefault, false)

	d := json.NewDecoder(bytes.NewReader(data))
	t, err := d.Token()
	if err != nil {
		return err
	}

	switch t {
	case json.Delim('{'):
		return n.decodeRulesFromObject(d)
	case json.Delim('['):
		return n.decodeRulesFromArray(d)
	}

	return errors.New("unsupported type; expected 'object' or 'array'")
}

func (n *Hint) decodeRulesFromObject(d *json.Decoder) error {
	for {
		t, err := d.Token()
		if err != nil {
			return err
		}
		if t == json.Delim('}') {
			// End of main json object -> done
			return nil
		}

		path := t.(string)
		hints, err := decodeHints(d)
		if err != nil {
			return err
		}

		if err = n.encodeRuleString(path, hints); err != nil {
			return err
		}
	}
}

func (n *Hint) decodeRulesFromArray(d *json.Decoder) error {
	for {
		t, err := d.Token()
		if err != nil {
			return err
		}
		if t == json.Delim(']') {
			// End of main json array -> done
			return nil
		}

		if t == json.Delim('{') {
			path, hints, err := decodeRuleObject(d)
			if err != nil {
				return err
			}
			if err = n.encodeRuleString(path, hints); err != nil {
				return err
			}
			continue
		}

		return errors.New("unsupported type; expected 'object'")
	}
}

func decodeRuleObject(d *json.Decoder) (path string, hints hints, err error) {
	for {
		t, err := d.Token()
		if err != nil {
			return "", 0, err
		}
		if t == json.Delim('}') {
			// End of rule json object -> done
			break
		}

		label := strings.ToLower(t.(string))
		switch label {
		case "path":
			t, err = d.Token()
			if err != nil {
				return "", 0, err
			}
			value, ok := t.(string)
			if !ok {
				return "", 0, errors.New("unsupported type; expected 'string'")
			}
			path = value
		case "hints":
			value, err := decodeHints(d)
			if err != nil {
				return "", 0, err
			}
			hints = value
		default:
			// Ignore all extra fields..
			if err = skipValue(d); err != nil {
				return "", 0, err
			}
		}
	}
	return
}

func decodeHints(d *json.Decoder) (hints, error) {
	t, err := d.Token()
	if err != nil {
		return 0, err
	}

	value, ok := t.(string)
	if ok {
		return hintFromString(value)
	}

	if t != json.Delim('[') {
		return 0, errors.New("unsupported type; expected 'string' or '[]string'")
	}

	result := hints(0)
	for {
		t, err := d.Token()
		if err != nil {
			return 0, err
		}
		if t == json.Delim(']') {
			return result, nil
		}
		value, ok := t.(string)
		if !ok {
			return 0, errors.New("unsupported type; expected 'string'")
		}

		hint, err := hintFromString(value)
		if err != nil {
			return 0, err
		}

		result |= hint
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

func hintFromString(value string) (hints, error) {
	for k, v := range hintValues {
		if strings.EqualFold(value, k) {
			return v, nil
		}
	}

	return 0, fmt.Errorf("unsupported hint '%s'", value)
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

func (n *Hint) printTree(name string, level int) string {
	sp1 := ""
	sp2 := ""
	if level != 0 {
		sp1 = " :" + strings.Repeat("  :", level-1)
		sp2 = "."
	}

	hints := ""
	if n.hints != hintDefault {
		hints = fmt.Sprintf(" = [%s]", n.hints.String())
	}
	result := fmt.Sprintf("%s%s%s%s\n", sp1, sp2, name, hints)

	childCount := len(n.fields)
	if n.wildcard != nil {
		childCount++
	}
	children := make([]struct {
		key   string
		value string
	}, childCount)

	i := 0
	for k, v := range n.fields {
		children[i].key = k
		children[i].value = v.printTree(k, level+1)
		i++
	}
	slices.SortFunc(children, func(a, b struct {
		key   string
		value string
	}) int {
		return strings.Compare(a.key, b.key)
	})
	for i := range children {
		result += children[i].value
	}

	if n.wildcard != nil {
		wildcard := "wc_?"
		if n.isRecursiveWildcard {
			wildcard = "wc_*"
		}
		result += n.wildcard.printTree(wildcard, level+1)
	}

	return result
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

// enter should be invoked before a sub-structure (either record or array) is entered
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

// leave should be invoked before a sub-structure (either record or array) is left
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

// field should be invoked before a field label is parsed
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

// afterListEntered should be invoked after a list has been entered
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
	stack []ion.Symbol

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

func (s *state) coerceI64() bool {
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
		s.pathbuf.Push(s.stack[i])
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
	s.tmp = tmp[:0] // avoid realloc
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
	s.stack = append(s.stack, ^ion.Symbol(0))
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
		slices.SortFunc(lst, func(x, y ion.Field) int {
			return int(x.Sym) - int(y.Sym)
		})
		s.constResolved = true
	}
	for i := range lst {
		lst[i].Encode(&s.out.Buffer, &s.out.Symbols)
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
	s.stack[len(s.stack)-1] = sym
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
	} else if s.coerceI64() {
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
	} else if s.coerceI64() {
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
			s.out.WriteStringBytes(seg)
		}
	}

	s.after()
}
