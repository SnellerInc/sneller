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

package elastic_proxy

import (
	"math/bits"
	"strings"
)

// ElasticMapping represents type mappings used by ElasticSearch
type ElasticMapping struct {
	Properties Properties `json:"properties"`
}

type Properties map[string]MappingValue

type MappingValue struct {
	Type       string     `json:"type"`
	Indexed    bool       `json:"indexed,omitempty"`
	Properties Properties `json:"properties,omitempty"`
}

const (
	nullField       = "null"
	boolField       = "bool"
	intField        = "int"
	floatField      = "float"
	decimalField    = "decimal"
	timestampField  = "timestamp"
	stringField     = "string"
	listField       = "list"
	structField     = "struct"
	sexpField       = "sexp"
	clobField       = "clob"
	blobField       = "blob"
	annotationField = "annotation"
	listItems       = "$items"
)

type snellerType uint16

const (
	nullType snellerType = 1 << iota
	boolType
	intType
	floatType
	decimalType
	timestampType
	stringType
	listType
	structType
	sexpType
	clobType
	blobType
	annotationType
)

// DataShapeToElasticMapping translates raw 'fields' output from
// SNELLER_DATASHAPE into Elastic's Mapping structure.
func DataShapeToElasticMapping(fields map[string]any) *ElasticMapping {
	m := &ElasticMapping{Properties: make(map[string]MappingValue)}

	for field, val := range fields {
		if strings.Contains(field, listItems) {
			// "$items" is a union of values from list, not a real field
			continue
		}

		details, ok := val.(map[string]any)
		if !ok {
			// wrong input structure, but don't panic
			continue
		}

		typ := parseSnellerType(details)
		elasticType := obtainElasticType(typ)
		if elasticType == "" {
			// just fallback to safe default
			elasticType = defaultElasticType
		}

		m.Properties[field] = MappingValue{Type: elasticType}
	}

	rebuildObjectsHierarchy(&m.Properties)

	return m
}

var typeLookup = map[string]snellerType{
	nullField:       nullType,
	boolField:       boolType,
	intField:        intType,
	floatField:      floatType,
	decimalField:    decimalType,
	timestampField:  timestampType,
	stringField:     stringType,
	listField:       listType,
	structField:     structType,
	sexpField:       sexpType,
	clobField:       clobType,
	blobField:       blobType,
	annotationField: annotationType,
}

// parseSnellerType converts histogram details for given details
// into a set of Sneller engine types.
func parseSnellerType(details map[string]any) snellerType {
	var res snellerType
	for field, val := range details {
		typ, ok := typeLookup[field]
		if !ok {
			continue
		}

		if count, ok := val.(int); ok && count > 0 {
			res |= typ
		}
	}

	return res
}

const (
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/boolean.html
	elasticTypeBool = "boolean"

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html
	elasticTypeInt     = "long"
	elasticTypeFloat64 = "double"

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
	elasticTypeTimestamp = "date"

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
	// Note: this might be also text, it depends on context
	elasticTypeString = "keyword"

	elasticTypeList   = "list"
	elasticTypeStruct = "object"
)

const defaultElasticType = elasticTypeString

// obtainElasticType translates sneller type into ElasticSearch name.
// When translation is not possible, returns an empty string
//
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
func obtainElasticType(typ snellerType) string {
	// reset null, as this type is meaningless
	typ = typ &^ nullType

	if typ == 0 {
		return ""
	}

	if bits.OnesCount16(uint16(typ)) == 1 {
		switch typ {
		case boolType:
			return elasticTypeBool
		case intType:
			return elasticTypeInt
		case floatType:
			return elasticTypeFloat64
		case timestampType:
			return elasticTypeTimestamp
		case stringType:
			return elasticTypeString
		case structType:
			return elasticTypeStruct
		case listType:
			return elasticTypeList
		}

		return ""
	}

	if typ == (intType | floatType) {
		// mixed numeric type
		return elasticTypeFloat64
	}

	return ""
}

// SNELLER_DATASHAPE returns flattened list of all paths,
// this procedure rebuilds full hierarchy based on paths
func rebuildObjectsHierarchy(p *Properties) {
	var objects []string
	for path, val := range *p {
		if val.Type == elasticTypeStruct && val.Properties == nil {
			if !strings.ContainsRune(path, '.') {
				// a top-level child
				objects = append(objects, path)
			}
		}
	}

	for _, path := range objects {
		prefix := path + "."
		mv := (*p)[path]
		mv.Properties = make(Properties)
		for key := range *p {
			newpath, ok := strings.CutPrefix(key, prefix)
			if ok {
				mv.Properties[newpath] = (*p)[key]
				delete(*p, key)
			}
		}

		rebuildObjectsHierarchy(&mv.Properties)
		(*p)[path] = mv
	}
}
