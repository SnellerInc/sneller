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

package xsv

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/SnellerInc/sneller/ion"
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
	SkipRecords int `json:"skip_records,omitempty"`
	// Separator allows specifying a custom
	// separator (only applicable for CSV)
	Separator Delim `json:"separator,omitempty"`
	// MissingValues is an optional list of
	// strings which represent missing values.
	// Entries in Fields may override this on a
	// per-field basis.
	MissingValues []string `json:"missing_values,omitempty"`
	// Fields specifies the hint for each field
	Fields []FieldHint `json:"fields"`
}

// Delim is a rune that unmarshals from a
// string.
type Delim rune

// UnmarshalJSON implements json.Unmarshaler.
func (d *Delim) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil || s == "" {
		return err
	}
	if len(s) > 1 {
		return fmt.Errorf("xsv: delimiter must be at most one byte long")
	}
	*d = Delim(s[0])
	return nil
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
	AllowEmpty bool `json:"allow_empty,omitempty"`
	// Don't use sparse-indexing for this value.
	// (only valid for date-time type)
	NoIndex bool `json:"no_index,omitempty"`
	// Optional list of values that represent TRUE
	// (only valid for bool type)
	TrueValues []string `json:"true_values,omitempty"`
	// Optional list of values that represent FALSE
	// (only valid for bool type)
	FalseValues []string `json:"false_values,omitempty"`
	// Optional list of values that represent a
	// missing value
	MissingValues []string `json:"missing_values,omitempty"`

	// internals
	fieldParts      []fieldPart
	convertAndWrite func(string, *ion.Chunker, bool, ion.Symbuf)
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
			fh.convertAndWrite = func(text string, d *ion.Chunker, _ bool, _ ion.Symbuf) {
				customBoolToION(text, d, fh.TrueValues, fh.FalseValues)
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
	default:
		return fmt.Errorf("xsv: no converter for type %q", t)
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
//	    {"name":"field.b", "type": "datetime", "format": "epoch", "no_index": true},
//	    {"name":"anotherField", "type": "bool", "true_values": ["Y"], "false_values": ["N"]},
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
// Some values may be included in the sparse index. Set the 'no_index' field
// to `true` to prevent this behavior for the field.
//
// Supported types:
//   - string -> set 'allow_empty' if you want empty strings to be ingested
//   - number -> either float or int
//   - int
//   - bool -> can support custom true/false values
//   - datetime -> formats: text (default), epoch, epoch_ms, epoch_us, epoch_ns
func ParseHint(hint []byte) (*Hint, error) {
	var h Hint
	err := json.Unmarshal(hint, &h)
	if err != nil {
		return nil, err
	}
	return &h, nil
}
