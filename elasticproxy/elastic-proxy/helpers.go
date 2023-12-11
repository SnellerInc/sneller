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
