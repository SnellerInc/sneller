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
	"errors"
	"fmt"
)

// see https://github.com/SnellerInc/elastic-proxy/issues/25
type aggsTopHits struct {
	Sort   []SortField `json:"sort"`
	Source struct {
		Includes []string `json:"includes"`
	} `json:"_source"`
	Size *int `json:"size"`
}

func (f *aggsTopHits) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	effectiveSize := 3
	if f.Size != nil {
		effectiveSize = *f.Size
	}
	if effectiveSize == 0 {
		return nil, nil
	}

	var projection []projectAliasExpr

	// include the group-by keys to allow moving the hits to the proper groups later
	projection = append(projection, c.groupExprs...)

	// include the fields (or all data if no fields are specified)
	includes := f.Source.Includes
	if len(includes) > 0 {
		for _, incl := range includes {
			projection = append(projection, projectAliasExpr{
				Context:    c.context,
				expression: ParseExprFieldName(c.context, incl),
			})
		}
	} else {
		projection = append(projection, projectAliasExpr{
			Context:    c.context,
			expression: &exprFieldName{Context: c.context},
		})
	}

	rowNumExpr := exprOperatorOver{
		Context:  c.context,
		Function: exprFunction{Name: "ROW_NUMBER"},
	}
	for _, groupExpr := range c.groupExprs {
		rowNumExpr.PartitionBy = append(rowNumExpr.PartitionBy, groupExpr.expression)
	}
	for _, sortField := range f.Sort {
		rowNumExpr.OrderBy = append(rowNumExpr.OrderBy, orderByExpr{
			Context:    c.context,
			expression: ParseExprFieldName(c.context, sortField.Field),
			Order:      sortField.Order,
		})
	}

	rowNumConditionExpr := exprOperator2{
		Operator: "<",
		Expr1:    &rowNumExpr,
		Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{effectiveSize}},
	}

	projectExpr := projectAliasExpr{
		Alias: fmt.Sprintf("%s:%s%%hits", BucketPrefix, c.bucket),
		expression: &exprSelect{
			Context:    c.context,
			Projection: projection,
			From:       c.context.Sources,
			Where:      andExpressions([]expression{c.query, &rowNumConditionExpr}),
		},
	}

	return []projectAliasExpr{projectExpr}, nil
}

func (f *aggsTopHits) process(c *aggsProcessContext) (interface{}, error) {
	return nil, errors.New("'top_hits' not supported")
}
