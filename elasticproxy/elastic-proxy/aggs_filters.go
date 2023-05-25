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
