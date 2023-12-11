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

package elastic_proxy

import (
	"strings"
	"testing"
)

var defaultQueryContext = QueryContext{
	TypeMapping: map[string]TypeMapping{
		"field.*": {
			Type: "text",
			Fields: map[string]string{
				"keyword": "keyword",
				"raw":     "keyword-ignore-case",
			},
		},
	},
}

func TestParseSubfieldNameExplicit(t *testing.T) {
	fn := ParseExprFieldNameParts(&defaultQueryContext, []string{"field", "test", "raw"})
	if strings.Join(fn.Fields, ".") != "field.test" {
		t.Fatal("invalid field-name")
	}
	if fn.SubField != "raw" {
		t.Fatal("invalid subfield-name")
	}
}

func TestParseSubfieldNameKeyword(t *testing.T) {
	fn := ParseExprFieldNameParts(&defaultQueryContext, []string{"fieldOther", "test", "keyword"})
	if strings.Join(fn.Fields, ".") != "fieldOther.test" {
		t.Fatal("invalid field-name")
	}
	if fn.SubField != "keyword" {
		t.Fatal("invalid subfield-name")
	}
}
