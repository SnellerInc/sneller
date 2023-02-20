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

type aggsMultiTerms struct {
	Terms []aggMultiTermField `json:"terms"`
	Order *order              `json:"order"`
	Size  *int                `json:"size"`
}

type aggMultiTermField struct {
	Field        string  `json:"field"`
	MissingValue *string `json:"missing"` // TODO
}

func (f *aggsMultiTerms) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	var keyExprs []*exprFieldName
	for _, mt := range f.Terms {
		keyExprs = append(keyExprs, ParseExprFieldName(c.context, mt.Field))
	}

	for _, keyExpr := range keyExprs {
		c.addGroupExpr(keyExpr)
	}

	countStarExpr := c.makeCountStar()
	c.addProjection(DocCount, countStarExpr)

	if f.Order != nil {
		for k, v := range *f.Order {
			switch k {
			case "_count":
				// order by count
				c.addOrdering(orderByExpr{
					Context:    c.context,
					expression: countStarExpr,
					Order:      v,
				})
			case "_key":
				// order by key
				for _, keyExpr := range keyExprs {
					c.addOrdering(orderByExpr{
						Context:    c.context,
						expression: keyExpr,
						Order:      v,
					})
				}
			}
		}
	} else {
		// no order set, then we'll order by doc-count
		c.addOrdering(orderByExpr{
			Context:    c.context,
			expression: countStarExpr,
			Order:      "DESC",
		})
	}

	if c.nestingLevel == 1 {
		c.setSize(f.Size)
	} else {
		c.setSize(&groupByLimit)
	}

	return c.transform()
}

func (f *aggsMultiTerms) process(c *aggsProcessContext) (any, error) {
	result := bucketMultiResult{}
	var totalCount int64

	bucketDocCount := c.docCount
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
				KeyField: f.Terms[0].Field, // TODO: Check if this is the proper field
				Context:  c.context,
			})
			totalCount += docCount
		}
	} else {
		result.Buckets = []bucketSingleResultWithKey{}
	}

	if !c.context.IgnoreSumOtherDocCount && bucketDocCount > 0 {
		otherDocCount := bucketDocCount - totalCount
		result.SumOtherDocCount = &otherDocCount
	}
	return &result, nil
}
