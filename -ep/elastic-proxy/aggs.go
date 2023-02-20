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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type aggregation struct {
	Aggregation     any
	SubAggregations map[string]aggregation
	Meta            map[string]any
}

var aggregationTypeMapping map[string]reflect.Type

func init() {
	aggregationTypeMapping = map[string]reflect.Type{
		// Metric aggregations
		"min":          reflect.TypeOf(&aggsMin{}),
		"avg":          reflect.TypeOf(&aggsAvg{}),
		"max":          reflect.TypeOf(&aggsMax{}),
		"sum":          reflect.TypeOf(&aggsSum{}),
		"cardinality":  reflect.TypeOf(&aggsCardinality{}),
		"value_count":  reflect.TypeOf(&aggsValueCount{}),
		"geo_centroid": reflect.TypeOf(&aggsGeoCentroid{}),
		"top_hits":     reflect.TypeOf(&aggsTopHits{}), // implemented as a bucket aggregation

		// Bucket aggregations
		"date_histogram": reflect.TypeOf(&aggsDateHistogram{}),
		"filter":         reflect.TypeOf(&aggsFilter{}),
		"filters":        reflect.TypeOf(&aggsFilters{}),
		"histogram":      reflect.TypeOf(&aggsHistogram{}),
		"terms":          reflect.TypeOf(&aggsTerms{}),
		"multi_terms":    reflect.TypeOf(&aggsMultiTerms{}),
		"geotile_grid":   reflect.TypeOf(&aggsGeotileGrid{}),

		// Pipeline aggregations
		"bucket_script": reflect.TypeOf(&aggsBucketScript{}),
		"bucket_sort":   reflect.TypeOf(&aggsBucketSort{}),
	}
}

func (a *aggregation) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('{') {
		return errors.New("expected '{'")
	}
	var aggs []string
	for dec.More() {
		t, err = dec.Token()
		if err != nil {
			return err
		}
		if field, ok := t.(string); ok {
			switch field {
			case "aggs":
				if err := dec.Decode(&a.SubAggregations); err != nil {
					return err
				}
			case "meta":
				if err = dec.Decode(&a.Meta); err != nil {
					return err
				}
			default:
				if t, ok := aggregationTypeMapping[field]; ok {
					a.Aggregation = reflect.New(t.Elem()).Interface()
					if err = dec.Decode(a.Aggregation); err != nil {
						return err
					}
					aggs = append(aggs, field)
				} else {
					return fmt.Errorf("invalid field %q", field)
				}
			}
		} else {
			return errors.New("expected a field")
		}
	}

	if len(aggs) > 1 {
		return fmt.Errorf("multiple aggregations are not allowed: %s", strings.Join(aggs, ","))
	}

	return nil
}

type metricAggregation interface {
	transform(subBucket string, c *aggsGenerateContext) error
	process(c *aggsProcessContext) (any, error)
}

type bucketAggregation interface {
	transform(c *aggsGenerateContext) ([]projectAliasExpr, error)
	process(c *aggsProcessContext) (any, error)
}

type pipelineAggregation interface {
	process(aggName string, data any) error
}

type fieldMetricAgg struct {
	Field string `json:"field"`
}
