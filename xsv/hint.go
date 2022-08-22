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

package xsv

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/ion"
	"golang.org/x/exp/slices"
)

const (
	TypeIgnore   = "ignore"
	TypeString   = "string" // default
	TypeNumber   = "number" // also floating point
	TypeInt      = "int"    // integer only
	TypeBool     = "bool"
	TypeDateTime = "datetime"
)

const (
	FormatDateTime             = "datetime" // default
	FormatDateTimeUnixSec      = "unix_seconds"
	FormatDateTimeUnixMilliSec = "unix_milli_seconds"
	FormatDateTimeUnixMicroSec = "unix_micro_seconds"
	FormatDateTimeUnixNanoSec  = "unix_nano_seconds"
)

var (
	ErrIngestEmptyOnlyValidForStrings = errors.New("only strings can be empty")
	ErrFormatOnlyValidForDateTime     = errors.New("format only valid for datetime type")
	ErrBoolValuesOnlyValidForBool     = errors.New("custom true/false values only valid for bool type")
	ErrRequireBothTrueAndFalseValues  = errors.New("require both true and false values")
	ErrTrueAndFalseValuesOverlap      = errors.New("true and values values overlap")
)

// Hint specifies the options and
// mandatory fields for parsing
// CSV/TSV files.
type Hint struct {
	// SkipRecords allows skipping the first
	// N records (useful when headers are used)
	SkipRecords int `json:"skipRecords"`
	// Separator allows specifying a custom
	// separator (only applicable for CSV)
	Separator rune `json:"separator"`
	// Fields specifies the hint for each field
	Fields []FieldHint `json:"fields"`
}

// FieldHint defines if and how a
// field should be imported
type FieldHint struct {
	// Field-name (use dots to make it a subfield)
	Name string `json:"name,omitempty"`
	// Type of field (or ignore)
	Type string `json:"type,omitempty"`
	// Default value if the column is an empty string
	Default string `json:"default,omitempty"`
	// Ingestion format (i.e. different data formats)
	Format string `json:"format,omitempty"`
	// Allow empty values (only valid for strings) to
	// be ingested. If flag is set to false, then the
	// field won't be written for the record instead.
	AllowEmpty bool `json:"allowEmpty,omitempty"`
	// Don't use sparse-indexing for this value.
	// (only valid for date-time type)
	NoIndex bool `json:"noIndex,omitempty"`
	// Optional list of values that represent TRUE
	// (only valid for bool type)
	TrueValues []string `json:"trueValues,omitempty"`
	// Optional list of values that represent FALSE
	// (only valid for bool type)
	FalseValues []string `json:"falseValues,omitempty"`

	// internals
	fieldParts      []fieldPart
	convertAndWrite func(string, *ion.Chunker, bool, ion.Symbuf) error
}

type fieldPart struct {
	name string
	sym  ion.Symbol
}

func (fh *FieldHint) UnmarshalJSON(data []byte) error {
	// base JSON unmarshalling
	type _fieldHint FieldHint
	if err := json.Unmarshal(data, (*_fieldHint)(fh)); err != nil {
		return err
	}

	// set type to "ignore" if no name is set
	if fh.Name == "" || fh.Type == TypeIgnore {
		fh.Name = ""
		fh.Type = TypeIgnore
		return nil
	}

	// split the field-name into separate parts
	parts := strings.Split(fh.Name, ".")
	fh.fieldParts = make([]fieldPart, len(parts))
	for i, v := range parts {
		// the symbol will be added later
		fh.fieldParts[i] = fieldPart{name: v}
	}

	// determine type
	t := fh.Type
	if t == "" {
		t = TypeString
	}

	if t != TypeDateTime && fh.Format != "" {
		return ErrFormatOnlyValidForDateTime
	}
	if fh.Type != TypeString && fh.AllowEmpty {
		return ErrIngestEmptyOnlyValidForStrings
	}
	if t != TypeBool && (fh.TrueValues != nil || fh.FalseValues != nil) {
		return ErrBoolValuesOnlyValidForBool
	}

	switch t {
	case TypeString:
		fh.convertAndWrite = stringToION
	case TypeNumber:
		fh.convertAndWrite = floatToION
	case TypeInt:
		fh.convertAndWrite = intToION
	case TypeBool:
		if fh.TrueValues != nil || fh.FalseValues != nil {
			if len(fh.TrueValues) == 0 || len(fh.FalseValues) == 0 {
				return ErrRequireBothTrueAndFalseValues
			}
			// make sure there is no overlap
			for _, tv := range fh.TrueValues {
				if slices.Contains(fh.FalseValues, tv) {
					return ErrTrueAndFalseValuesOverlap
				}
			}
			fh.convertAndWrite = func(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) error {
				return customBoolToION(text, d, fh.TrueValues, fh.FalseValues)
			}
		} else {
			fh.convertAndWrite = boolToION
		}
	case TypeDateTime:
		f := FormatDateTime
		if fh.Format != "" {
			f = fh.Format
		}
		switch f {
		case FormatDateTime:
			fh.convertAndWrite = dateToION
		case FormatDateTimeUnixSec:
			fh.convertAndWrite = epochSecToION
		case FormatDateTimeUnixMilliSec:
			fh.convertAndWrite = epochMSecToION
		case FormatDateTimeUnixMicroSec:
			fh.convertAndWrite = epochUSecToION
		case FormatDateTimeUnixNanoSec:
			fh.convertAndWrite = epochNSecToION
		default:
			return fmt.Errorf("invalid date format %q", f)
		}
	}

	return nil
}

func (fh *FieldHint) isRootField() bool {
	return len(fh.fieldParts) == 1
}

// ParseHint parses a json byte array into a Hint structure which can
// later be used to pass type-hints and/or other flags to the TSV parser.
//
// The input must contain a valid JSON object, like:
//
//	{
//	  "fields": [
//	    {"name":"field", "type": "<type>"},
//	    {"name":"field.a", "type": "<type>", "default:" "empty"},
//	    {"name":"field.b", "type": "datetime", "format": "epoch", "noIndex": true},
//	    {"name":"anotherField", "type": "bool", "trueValues": ["Y"], "falseValues": ["N"]},
//	    ...
//	  ]
//	}
//
// With TSV each line represents a single record. The tab character is
// used to split the line into multiple fields. The 'fields' part in the
// hints is an order list that specify the name and type of each field.
//
// Each field will be given the specified 'name'. If no 'type' is specified
// then 'string' is assumed. When there are more fields in the data, then
// in the 'fields', then these are skipped.
//
// If a field doesn't need to be ingested, then you can insert an empty
// record (or set the 'type' to "ignore" explicitly).
//
// When there is no text between both tabs, the structure won't contain the
// field, unless a 'default' is specified (can be an empty string). Note that
// the default value should match the type.
//
// Note that the 'name' can contain multiple levels, so nested objects can
// be created. This can be useful to group information in the ingested data.
//
// Some values may be included in the sparse index. Set the 'noIndex' to `true`
// to prevent this behavior for the field.
//
// Supported types:
//   - string -> set 'allowEmpty' if you want empty strings to be ingested
//   - number -> either float or int
//   - int
//   - bool -> can support custom trueValues/falseValues
//   - datetime -> formats: text (default), epoch, epoch_ms, epoch_us, epoch_ns
func ParseHint(hint []byte) (*Hint, error) {
	var h Hint
	err := json.Unmarshal(hint, &h)
	if err != nil {
		return nil, err
	}
	return &h, nil
}
