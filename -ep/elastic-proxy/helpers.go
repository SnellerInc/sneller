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
	"sort"
	"strings"

	"golang.org/x/exp/maps"
)

var groupByLimit = 10000

func effectiveSize(size *int) int {
	if size != nil {
		return *size
	}
	return 10
}

func keyToString(key any) string {
	switch k := key.(type) {
	case string:
		return k
	case *string:
		return *k
	case bool:
		if k {
			return "true"
		} else {
			return "false"
		}
	}
	return fmt.Sprintf("%v", key)
}

func sortedKeys[M ~map[string]V, V any](m M) []string {
	keys := maps.Keys(m)
	sort.Strings(keys)
	return keys
}

func keyAsString(va ...any) string {
	var sb strings.Builder
	for i, v := range va {
		if i > 0 {
			sb.WriteRune('|')
		}
		sb.WriteString(keyToString(v))
	}
	return sb.String()
}

func fieldEquals(field string, value JSONLiteral, qc *QueryContext) expression {
	if boolValue, ok := value.Value.(bool); ok {
		// use different syntax for boolean comparison
		if boolValue {
			return ParseExprFieldName(qc, field)
		} else {
			return &exprOperator1{
				Context:  qc,
				Operator: "NOT",
				Expr1:    ParseExprFieldName(qc, field),
			}
		}
	} else {
		return &exprOperator2{
			Context:  qc,
			Operator: "=",
			Expr1:    ParseExprFieldName(qc, field),
			Expr2:    &exprJSONLiteral{Context: qc, Value: value},
		}
	}

}
