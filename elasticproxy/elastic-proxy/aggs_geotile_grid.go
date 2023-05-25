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
