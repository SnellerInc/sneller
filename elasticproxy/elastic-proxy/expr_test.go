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
