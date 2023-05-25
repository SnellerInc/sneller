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
	"fmt"
	"strings"
	"testing"
)

func TestQSFieldExpression(t *testing.T) {
	data := []struct {
		title     string
		field     string
		value     string
		valueType valueType
		operator  string
		expected  string
	}{
		// plain comparison
		{
			title:     "default",
			field:     "field.test",
			value:     "Value",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `("$source"."field"."test" ~ '(^|[ \t])(?i)Value([ \t]|$)')`,
		},
		{
			title:     "keyword",
			field:     "field.test.keyword",
			value:     "Value",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `("$source"."field"."test" = 'Value')`,
		},
		{
			title:     "raw",
			field:     "field.test.raw",
			value:     "Value",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `(LOWER("$source"."field"."test") = 'value')`,
		},

		// plain comparison with wildcard
		{
			title:     "default-wildcard",
			field:     "field.test",
			value:     "Val?e",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `("$source"."field"."test" ~ '(^|[ \t])(?i)Val.e([ \t]|$)')`,
		},
		{
			title:     "keyword-wildcard",
			field:     "field.test.keyword",
			value:     "Val?e",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `("$source"."field"."test" SIMILAR TO 'Val_e')`,
		},
		{
			title:     "raw-wildcard",
			field:     "field.test.raw",
			value:     "Val?e",
			valueType: valueTypeText,
			operator:  "=",
			expected:  `(LOWER("$source"."field"."test") SIMILAR TO 'val_e')`,
		},

		// regex comparison
		{
			title:     "default-re",
			field:     "field.test",
			value:     "value",
			valueType: valueTypeRegex,
			operator:  "=",
			expected:  `(LOWER("$source"."field"."test") ~ '(^|[ \t])value([ \t]|$)')`,
		},
		{
			title:     "keyword-re",
			field:     "field.test.keyword",
			value:     "Value",
			valueType: valueTypeRegex,
			operator:  "=",
			expected:  `("$source"."field"."test" ~ '^Value$')`,
		},
		{
			title:     "raw-re",
			field:     "field.test.raw",
			value:     "Value",
			valueType: valueTypeRegex,
			operator:  "=",
			expected:  `("$source"."field"."test" ~ '^(?i)Value$')`,
		},
	}
	for n, i := range data {
		title := fmt.Sprintf("%02d-%s", n, i.title)
		t.Run(title, func(t *testing.T) {
			qse := qsFieldExpression{
				FieldName: qsFieldName{fields: strings.Split(i.field, ".")},
				Value:     i.value,
				Type:      i.valueType,
				Operator:  i.operator,
			}
			e, err := qse.Expression(&defaultQueryContext, qse.FieldName)
			if err != nil {
				t.Fatal(err)
			}
			text := PrintExprPretty(e)
			if text != i.expected {
				t.Fatalf("got: %s\nexp: %s", text, i.expected)
			}
		})
	}
}
