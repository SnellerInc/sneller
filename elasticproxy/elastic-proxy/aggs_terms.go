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
)

type aggsTerms struct {
	Field                 string  `json:"field"`
	Order                 *order  `json:"order"`
	Size                  *int    `json:"size"`
	ShowTermDocCountError bool    `json:"show_term_doc_count_error"`
	MissingValue          *string `json:"missing"` // TODO
}

func (f *aggsTerms) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	keyExpr := ParseExprFieldName(c.context, f.Field)
	c.addGroupExpr(keyExpr)

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
				c.addOrdering(orderByExpr{
					Context:    c.context,
					expression: keyExpr,
					Order:      v,
				})
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

	c.setSize(f.Size)

	return c.transform()
}

func (f *aggsTerms) process(c *aggsProcessContext) (any, error) {
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

			if len(group.KeyValues) != 1 {
				return nil, fmt.Errorf("key-value count is %d, which is invalid for a terms aggregation", len(group.KeyValues))
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

	if !c.context.IgnoreSumOtherDocCount && bucketDocCount > 0 {
		otherDocCount := bucketDocCount - totalCount
		result.SumOtherDocCount = &otherDocCount
	}

	docCountErrorUpperBound := int64(0)
	result.DocCountErrorUpperBound = &docCountErrorUpperBound
	return &result, nil
}
