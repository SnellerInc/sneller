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
