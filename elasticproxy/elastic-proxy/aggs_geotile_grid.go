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

import "fmt"

type aggsGeotileGrid struct {
	Field     string     `json:"field"`
	Precision *int       `json:"precision"`
	Size      *int       `json:"size"`
	Bounds    *geoBounds `json:"bounds"`
}

func (f *aggsGeotileGrid) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	effectivePrecision := 7
	if f.Precision != nil {
		if *f.Precision < 0 || *f.Precision > 29 {
			return nil, fmt.Errorf("invalid precision %d", *f.Precision)
		}
		effectivePrecision = *f.Precision
	}
	precisionLiteral, _ := NewJSONLiteral(effectivePrecision)
	lat := ParseExprFieldName(c.context, f.Field+LatExt)
	long := ParseExprFieldName(c.context, f.Field+LonExt)
	keyExpr := &exprFunction{
		Context: c.context,
		Name:    "GEO_TILE_ES",
		Exprs: []expression{
			lat, long,
			&exprJSONLiteral{Context: c.context, Value: precisionLiteral},
		},
	}
	c.addGroupExpr(keyExpr)

	countStarExpr := c.makeCountStar()
	c.addProjection(DocCount, countStarExpr)
	c.addOrdering(orderByExpr{
		Context:    c.context,
		expression: countStarExpr,
		Order:      "DESC",
	})

	if c.nestingLevel == 1 {
		c.setSize(f.Size)
	} else {
		c.setSize(&groupByLimit)
	}

	if f.Bounds != nil {
		top, _ := NewJSONLiteral(f.Bounds.TopLeft.Lat)
		left, _ := NewJSONLiteral(f.Bounds.TopLeft.Lon)
		bottom, _ := NewJSONLiteral(f.Bounds.BottomRight.Lat)
		right, _ := NewJSONLiteral(f.Bounds.BottomRight.Lon)
		c.andQuery(andExpressions([]expression{
			&exprOperator2{Context: c.context, Operator: "<=", Expr1: lat, Expr2: &exprJSONLiteral{Context: c.context, Value: top}},
			&exprOperator2{Context: c.context, Operator: ">=", Expr1: long, Expr2: &exprJSONLiteral{Context: c.context, Value: left}},
			&exprOperator2{Context: c.context, Operator: ">=", Expr1: lat, Expr2: &exprJSONLiteral{Context: c.context, Value: bottom}},
			&exprOperator2{Context: c.context, Operator: "<=", Expr1: long, Expr2: &exprJSONLiteral{Context: c.context, Value: right}},
		}))
	}

	return c.transform()
}

func (f *aggsGeotileGrid) process(c *aggsProcessContext) (any, error) {
	result := bucketMultiResult{}
	var totalCount int64

	groups := c.groups()
	if groups != nil {
		size := effectiveSize(f.Size)
		groupCount := len(groups.OrderedGroups)
		if groupCount > size {
			groupCount = size
		}
		result.Buckets = make([]bucketSingleResultWithKey, 0, groupCount)
		for n := 0; n < groupCount; n++ {
			group := groups.OrderedGroups[n]

			if len(group.KeyValues) != 1 {
				return nil, fmt.Errorf("key-value count is %d, which is invalid for a geotile-grid aggregation", len(group.KeyValues))
			}

			docCount, err := group.docCount()
			if err != nil {
				return nil, err
			}

			c.docCount = docCount
			bucketResult, err := c.subResult(group)
			if err != nil {
				return nil, err
			}

			result.Buckets = append(result.Buckets, bucketSingleResultWithKey{
				bucketSingleResult: bucketSingleResult{
					SubAggregations: bucketResult,
					DocCount:        docCount,
				},
				Key:      group.KeyValues[0],
				KeyField: f.Field,
				Context:  c.context,
			})
			totalCount += docCount
		}
	} else {
		result.Buckets = []bucketSingleResultWithKey{}
	}

	// SumOtherDocCount should be the number of documents that
	// do match the bounding, but were not included due to the LIMIT
	// We cannot calculate this, unless we run an additional query
	return &result, nil
}
