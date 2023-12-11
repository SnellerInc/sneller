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
	"encoding/json"
	"fmt"
)

type aggsFilters struct {
	Filters   map[string]Query `json:"filters"`
	anonymous bool
}

func (f *aggsFilters) UnmarshalJSON(data []byte) error {
	if data[0] == '[' {
		var anonymousQueries []Query
		if err := json.Unmarshal(data, &anonymousQueries); err != nil {
			return err
		}
		f.Filters = make(map[string]Query, len(anonymousQueries))
		for i, q := range anonymousQueries {
			f.Filters[fmt.Sprintf("%%%05d", i)] = q
		}
		f.anonymous = true
	} else {
		// TODO: The map makes the result order unpredictable
		type _aggsFilters aggsFilters
		if err := json.Unmarshal(data, (*_aggsFilters)(f)); err != nil {
			return err
		}
	}
	return nil
}

func (f *aggsFilters) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	var queries []projectAliasExpr

	for _, filterBucket := range sortedKeys(f.Filters) {
		f := f.Filters[filterBucket]

		fExpr, err := f.Expression(c.context)
		if err != nil {
			if err != nil {
				return nil, fmt.Errorf("cannot parse query %q in 'filters' aggregation %q: %w", filterBucket, c.bucket, err)
			}
		}

		subContext := c.clone().setBucket(filterBucket).andQuery(fExpr).addDocCount(true)
		filtersQueries, err := subContext.transform()
		if err != nil {
			return nil, err
		}
		queries = append(queries, filtersQueries...)
	}

	return queries, nil
}

func (f *aggsFilters) process(c *aggsProcessContext) (any, error) {
	result := bucketMappedResult{
		Buckets: make(map[string]*bucketSingleResult),
	}
	groups := c.groups()
	if groups != nil {
		groupCount := len(groups.OrderedGroups)
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

			key := keyAsString(group.KeyValues[0])
			result.Buckets[key] = &bucketSingleResult{
				SubAggregations: bucketResult,
				DocCount:        docCount,
			}
		}
	}

	return &result, nil
}
