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
	"errors"
	"strconv"
)

// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html
type aggsHistogram struct {
	Field          string   `json:"field"`
	Interval       float64  `json:"interval"`
	Keyed          bool     `json:"keyed"`         // TODO
	MissingValue   *float64 `json:"missing"`       // TODO
	Offset         float64  `json:"offset"`        // TODO
	MinDocCount    *int64   `json:"min_doc_count"` // TODO
	ExtendedBounds *struct {
		Min float64 `json:"min"`
		Max float64 `json:"max"`
	} `json:"extended_bounds"` // TODO
	HardBounds *struct {
		Min *float64 `json:"min"`
		Max *float64 `json:"max"`
	} `json:"hard_bounds"`
	Order map[string]order `json:"order"` // TODO
}

func (f *aggsHistogram) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	// Make sure the values are set
	if f.Interval <= 0 {
		return nil, errors.New("invalid interval")
	}
	if f.Offset < 0 || f.Offset >= f.Interval {
		return nil, errors.New("invalid offset")
	}
	if f.Keyed {
		return nil, errors.New("keyed histogram aggregation is not supported yet")
	}

	// We'll assume that 1000 buckets are sufficient
	buckets := float64(1000)

	// TODO: Check if the WIDTH_BUCKET represents the actual histogram
	e := &exprOperator2{
		Context:  c.context,
		Operator: "-",
		Expr1: &exprOperator2{
			Context:  c.context,
			Operator: "*",
			Expr1:    &exprJSONLiteral{Value: JSONLiteral{Value: f.Interval}},
			Expr2: &exprFunction{
				Context: c.context,
				Name:    "WIDTH_BUCKET",
				Exprs: []expression{
					&exprOperator2{
						Context:  c.context,
						Operator: "+",
						Expr1:    ParseExprFieldName(c.context, f.Field),
						Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{Value: f.Interval / 2}},
					},
					&exprJSONLiteral{Context: c.context, Value: JSONLiteral{Value: f.Offset}},
					&exprJSONLiteral{Context: c.context, Value: JSONLiteral{Value: f.Offset + buckets*f.Interval}},
					&exprJSONLiteral{Context: c.context, Value: JSONLiteral{Value: buckets}},
				},
			},
		},
		Expr2: &exprJSONLiteral{Value: JSONLiteral{Value: f.Interval}},
	}

	subContext := c.addGroupExpr(e).addOrdering(orderByExpr{
		Context:    c.context,
		expression: e,
		Order:      "ASC",
	}).addDocCount(false)

	if f.HardBounds != nil && f.HardBounds.Min != nil {
		c.andQuery(&exprOperator2{
			Context:  c.context,
			Operator: ">=",
			Expr1:    e,
			Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{*f.HardBounds.Min}},
		})
	}
	if f.HardBounds != nil && f.HardBounds.Max != nil {
		c.andQuery(&exprOperator2{
			Context:  c.context,
			Operator: "<=",
			Expr1:    e,
			Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{*f.HardBounds.Max}},
		})
	}

	return subContext.transform()
}

func (f *aggsHistogram) process(c *aggsProcessContext) (any, error) {
	result := bucketMultiResult{}
	var totalCount int64

	groups := c.groups()
	if groups != nil {
		result.Buckets = make([]bucketSingleResultWithKey, 0, len(groups.OrderedGroups))
		for _, group := range groups.OrderedGroups {

			key, err := NewElasticFloat(group.KeyValues[0])
			if err != nil {
				return nil, err
			}

			if f.HardBounds != nil && ((f.HardBounds.Min != nil && float64(key) < *f.HardBounds.Min) || (f.HardBounds.Max != nil && float64(key) > *f.HardBounds.Max)) {
				continue
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

			// TODO: We should emit an hash-bucket (instead of an array)
			//       when 'keyed' was set.
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

	return &result, nil
}

type elasticFloat float64

func NewElasticFloat(v any) (elasticFloat, error) {
	switch v := v.(type) {
	case int:
		return elasticFloat(float64(v)), nil
	case float64:
		return elasticFloat(v), nil
	}
	return elasticFloat(0), errors.New("unsupported type")
}

func (e *elasticFloat) String() string {
	if *e == elasticFloat(int64(*e)) {
		return strconv.FormatInt(int64(*e), 10) + ".0"
	}
	return strconv.FormatFloat(float64(*e), 'g', -1, 64)
}

func (e *elasticFloat) MarshalJSON() ([]byte, error) {
	return []byte(e.String()), nil
}
