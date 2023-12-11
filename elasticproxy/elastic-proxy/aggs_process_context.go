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
	"strings"
)

type aggsProcessContext struct {
	context  *QueryContext
	bucket   string
	agg      aggregation
	data     any
	docCount int64
}

func (c *aggsProcessContext) groups() *groupResultMap {
	if g, ok := c.data.(*groupResultMap); ok {
		return g
	}
	return nil
}

func (c *aggsProcessContext) result() (any, error) {
	return c.data, nil
}

func (c *aggsProcessContext) subResult(group *groupResults) (map[string]any, error) {
	var err error

	subAggs := make(map[string]any, len(c.agg.SubAggregations))

	// process metric and bucket aggregations
	for subAggName, subAgg := range c.agg.SubAggregations {
		sub := aggsProcessContext{
			context:  c.context,
			bucket:   subAggName,
			agg:      subAgg,
			docCount: c.docCount,
		}

		switch a := subAgg.Aggregation.(type) {
		case metricAggregation:
			if group != nil {
				sub.data = group.Results[subAggName]
			}
			subAggs[subAggName], err = a.process(&sub)
			if err != nil {
				return nil, err
			}
		case bucketAggregation:
			if group != nil {
				sub.data = group.Nested[subAggName]
			}
			subAggs[subAggName], err = a.process(&sub)
			if err != nil {
				return nil, err
			}

			if group != nil {
				// extract inlined $doc_count for given sub-aggregate
				prefix := subAggName + ":" + DocCount
				for key, value := range group.Results {
					if !strings.HasPrefix(key, prefix) {
						continue
					}
					agg, ok := subAggs[subAggName].(*bucketSingleResult)
					if !ok {
						continue
					}

					val, ok2 := value.(int)
					if ok2 {
						agg.DocCount = int64(val)
					}
				}
			}
		}

		if subAgg.Meta != nil {
			subAggs["meta"] = subAgg.Meta
		}
	}

	return subAggs, nil
}
