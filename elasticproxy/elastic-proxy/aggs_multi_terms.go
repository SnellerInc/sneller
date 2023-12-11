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
