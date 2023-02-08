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

// Package xsv implements parsing/converting CSV (RFC 4180) and
// TSV (tab separated values) files to binary ION format.
package xsv

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"golang.org/x/exp/slices"
)

var (
	ErrNoHints = errors.New("hints are mandatory")
)

// RowChopper implements fetching
// records row-by-row and chopping
// the records into individual fields
// until the reader is exhausted
type RowChopper interface {
	// GetNext return the next record and
	// splits fields in individual columns
	GetNext(r io.Reader) ([]string, error)
}

// Convert reads all records from the
// reader using the specified chopper/hints
// to determine the individual fields and
// writes it to the ION chunker
func Convert(r io.Reader, dst *ion.Chunker, ch RowChopper, hint *Hint, cons []ion.Field) error {
	// cannot convert without hints
	if hint == nil || len(hint.Fields) == 0 {
		return ErrNoHints
	}

	// make sure constant field IDs are interned
	prev := ion.Symbol(0)
	for i := range cons {
		cons[i].Sym = dst.Symbols.Intern(cons[i].Label)
		if cons[i].Sym < prev {
			return fmt.Errorf("xsv: internal error: constant interned symbols out-of-order")
		}
		prev = cons[i].Sym
	}

	// Add all symbols to the symbol table
	// (if we don't have them already)
	// allocate a symbol for each field and
	// prepare the field symbufs
	symbufs := make([]ion.Symbuf, len(hint.Fields))
	for i, f := range hint.Fields {
		symbufs[i] = ion.Symbuf{}
		symbufs[i].Prepare(len(f.fieldParts))
		for j, fp := range f.fieldParts {
			sym := dst.Symbols.Intern(fp.name)
			f.fieldParts[j].sym = sym
			symbufs[i].Push(sym)
		}
	}

	fm := newFieldMapFromHint(hint, symbufs)

	eof := false
	recordNr := 0
	for {
		fields, err := ch.GetNext(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			eof = true
		}

		// terminate the previous record
		if recordNr > 0 {
			if err := fm.writeMap(dst); err != nil {
				return err
			}
			dst.EndStruct()
			if err := dst.Commit(); err != nil {
				return err
			}
		}
		if eof {
			return nil
		}

		dst.BeginStruct(-1)
		for i := range cons {
			cons[i].Encode(&dst.Buffer, &dst.Symbols)
		}
		recordNr++

		for fieldNr := range fields {
			if fieldNr >= len(hint.Fields) {
				continue
			}
			field := hint.Fields[fieldNr]
			missing := hint.MissingValues
			if field.MissingValues != nil {
				missing = field.MissingValues
			}
			if field.Type == TypeIgnore {
				continue
			}
			text := fields[fieldNr]
			if slices.Contains(missing, text) {
				continue
			}
			if text == "" {
				text = field.Default
			}

			if text == "" && !field.AllowEmpty {
				continue
			}
			if field.isRootField() {
				// root-fields can be written immediately
				writeField(field.fieldParts[0].sym, &field, dst, text, symbufs[fieldNr])
			} else {
				// nested items are add to the map and written out later
				fm.addToMap(&field, text)
			}
		}
	}
}

// newFieldMapFromHint prepares a pre-build map
// that is reused for each row. This reduces the
// amount of allocations significantly and also
// builds a sorted list of keys. The field order
// shouldn't matter, but it needs to be sorted
// for consistent output for testing.
func newFieldMapFromHint(hint *Hint, symbufs []ion.Symbuf) *subfieldNode {
	fm := &subfieldNode{fields: make(map[ion.Symbol]any)}
	for i := range hint.Fields {
		f := hint.Fields[i]
		if !f.isRootField() {
			m := fm
			lf := subfieldLeaf{
				field:  &f,
				symbuf: symbufs[i],
			}
			for j := 0; j < len(f.fieldParts)-1; j++ {
				sub, ok := m.fields[f.fieldParts[j].sym].(*subfieldNode)
				if !ok {
					sub = &subfieldNode{fields: make(map[ion.Symbol]any)}
					m.fields[f.fieldParts[j].sym] = sub
				}
				m = sub
			}

			m.fields[f.fieldParts[len(f.fieldParts)-1].sym] = &lf
		}
	}
	fm.sortKeys()
	return fm
}

// subfieldNode represents the internal tree
// keeping track of the subfield values.
//
// the tree is re-used for each row to reduce
// allocations.
type subfieldNode struct {
	// a subfield should either be another
	// subfieldNode or a subfieldLef
	fields map[ion.Symbol]any
	// Keep track of a sorted list of keys
	// to ensure consistent results
	sortedKeys []ion.Symbol
}

// subfieldLeaf represents the subfield that
// actually holds the value.
type subfieldLeaf struct {
	field  *FieldHint
	inUse  bool
	text   string
	symbuf ion.Symbuf
}

func (fm *subfieldNode) sortKeys() {
	for k, v := range fm.fields {
		fm.sortedKeys = append(fm.sortedKeys, k)
		if subMap, ok := v.(*subfieldNode); ok {
			subMap.sortKeys()
		}
	}
	sort.Slice(fm.sortedKeys, func(a, b int) bool {
		return fm.sortedKeys[a] < fm.sortedKeys[b]
	})

}

func (fm *subfieldNode) addToMap(f *FieldHint, text string) {
	for i := 0; i < len(f.fieldParts)-1; i++ {
		fm = fm.fields[f.fieldParts[i].sym].(*subfieldNode)
	}
	leaf := fm.fields[f.fieldParts[len(f.fieldParts)-1].sym].(*subfieldLeaf)
	leaf.text = text
	leaf.inUse = true
}

func (fm *subfieldNode) writeMap(dst *ion.Chunker) error {
	for _, k := range fm.sortedKeys {
		v := fm.fields[k]
		switch vv := v.(type) {
		case *subfieldNode:
			dst.BeginField(k)
			dst.BeginStruct(-1)
			if err := vv.writeMap(dst); err != nil {
				return err
			}
			dst.EndStruct()

		case *subfieldLeaf:
			if vv.inUse {
				writeField(k, vv.field, dst, vv.text, vv.symbuf)
				vv.inUse = false
			}
		}
	}
	return nil
}

func writeField(sym ion.Symbol, field *FieldHint, dst *ion.Chunker, text string, symbufs ion.Symbuf) {
	dst.BeginField(sym)
	field.convertAndWrite(text, dst, field.NoIndex, symbufs)
}

func stringToION(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) {
	d.WriteString(text)
}

func floatToION(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	d.WriteFloat64(f)
}

func intToION(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) {
	i, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	d.WriteInt(i)
}

func customBoolToION(text string, d *ion.Chunker, trueValues []string, falseValues []string) {
	if slices.Contains(trueValues, text) {
		d.WriteBool(true)
		return
	}
	if slices.Contains(falseValues, text) {
		d.WriteBool(false)
		return
	}
	d.WriteString(text)
}

func boolToION(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) {
	b, err := strconv.ParseBool(text)
	if err != nil {
		d.WriteString(text)
		return
	}
	d.WriteBool(b)
}

func dateToION(text string, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	t, ok := date.Parse([]byte(text))
	if !ok {
		d.WriteString(text)
		return
	}
	timeToION(t, d, noIndex, symbuf)
}

func epochSecToION(text string, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	e, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	t := date.Unix(e, 0)
	timeToION(t, d, noIndex, symbuf)
}

func epochMSecToION(text string, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	e, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	t := date.UnixMicro(e)
	timeToION(t, d, noIndex, symbuf)
}

func epochUSecToION(text string, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	e, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	t := date.Unix(e/1e6, 1000*(e%1e6))
	timeToION(t, d, noIndex, symbuf)
}

func epochNSecToION(text string, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	e, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		d.WriteString(text)
		return
	}
	t := date.Unix(e/1e9, e%1e9)
	timeToION(t, d, noIndex, symbuf)
}

func timeToION(t date.Time, d *ion.Chunker, noIndex bool, symbuf ion.Symbuf) {
	d.WriteTime(t)
	if !noIndex {
		d.Ranges.AddTime(symbuf, t)
	}
}
