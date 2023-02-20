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
	"sort"
	"time"
)

type aggsDateHistogram struct {
	Field            string            `json:"field"`
	Interval         *fixedInterval    `json:"interval"`
	FixedInterval    *fixedInterval    `json:"fixed_interval"`
	CalendarInterval *calendarInterval `json:"calendar_interval"`
	ExtendedBounds   *struct {
		Min int64 `json:"min"`
		Max int64 `json:"max"`
	} `json:"extended_bounds"` // TODO
	HardBounds *struct {
		Min *int64 `json:"min"`
		Max *int64 `json:"max"`
	} `json:"hard_bounds"`
	Keyed        bool             `json:"keyed"` // TODO
	Format       string           `json:"format"`
	TimeZone     string           `json:"time_zone"` // TODO
	MissingValue *string          `json:"missing"`   // TODO
	Offset       timeOffset       `json:"offset"`    // TODO
	Order        map[string]order `json:"order"`     // TODO
}

func (f *aggsDateHistogram) UnmarshalJSON(data []byte) error {
	type _aggsDateHistogram aggsDateHistogram
	if err := json.Unmarshal(data, (*_aggsDateHistogram)(f)); err != nil {
		return err
	}
	if f.FixedInterval == nil && f.Interval != nil {
		f.FixedInterval = f.Interval
	}
	return nil
}

func (f *aggsDateHistogram) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	var e expression

	if f.FixedInterval != nil {
		seconds, err := f.FixedInterval.Seconds()
		if err != nil {
			return nil, err
		}
		e = &exprFunction{
			Context: c.context,
			Name:    "TIME_BUCKET",
			Exprs: []expression{
				ParseExprFieldName(c.context, f.Field),
				&exprJSONLiteral{Context: c.context, Value: JSONLiteral{Value: seconds}},
			},
		}
	} else if f.CalendarInterval != nil {
		interval := string(*f.CalendarInterval)
		var intervalArg string
		switch interval {
		case "us":
			intervalArg = "MICROSECOND"
		case "ms":
			intervalArg = "MILLISECOND"
		case "s":
			intervalArg = "SECOND"
		case "m":
			intervalArg = "MINUTE"
		case "h":
			intervalArg = "HOUR"
		case "d":
			intervalArg = "DAY"
		case "w":
			intervalArg = "WEEK(SUNDAY)" // TODO: take locale into account
		case "M":
			intervalArg = "MONTH"
		case "q":
			intervalArg = "QUARTER"
		case "y":
			intervalArg = "YEAR"
		default:
			return nil, fmt.Errorf("unsupported interval %q", interval)
		}
		e = &exprFunction{
			Context: c.context,
			Name:    "DATE_TRUNC",
			Exprs: []expression{
				&exprText{Context: c.context, Value: intervalArg},
				ParseExprFieldName(c.context, f.Field),
			},
		}
	} else {
		return nil, fmt.Errorf("required either calendar or fixed interval")
	}

	if f.HardBounds != nil && f.HardBounds.Min != nil {
		// TODO: Check if times can be specified in different formats then only Epoch-ms
		minDate := time.UnixMilli(*f.HardBounds.Min).Format(time.RFC3339)
		c.andQuery(&exprOperator2{
			Context:  c.context,
			Operator: ">=",
			Expr1:    e,
			Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{minDate}},
		})
	}
	if f.HardBounds != nil && f.HardBounds.Max != nil {
		maxDate := time.UnixMilli(*f.HardBounds.Max).Format(time.RFC3339)
		c.andQuery(&exprOperator2{
			Context:  c.context,
			Operator: "<=",
			Expr1:    e,
			Expr2:    &exprJSONLiteral{Context: c.context, Value: JSONLiteral{maxDate}},
		})
	}

	subContext := c.addGroupExpr(e).addOrdering(orderByExpr{
		Context:    c.context,
		expression: e,
		Order:      "ASC",
	}).addDocCount(false)

	// try pull "filter" aggregates into the main query
	names := sortedKeys(c.currentAggregations)
	processed := make(map[string]aggregation)
	defer func() {
		// restore any aggregate moved to `processed`
		for name, agg := range processed {
			c.currentAggregations[name] = agg
		}
	}()

	for _, name := range names {
		agg := c.currentAggregations[name]
		filter, ok := agg.Aggregation.(*aggsFilter)
		if !ok {
			continue
		}

		where, err := filter.Query.Expression(c.context)
		if err != nil {
			return nil, err
		}

		expr := &exprFunction{
			Context: c.context,
			Name:    "COUNT",
			Exprs:   []expression{&exprFieldName{Context: c.context}},
			Filter:  where,
		}

		c.addProjection(fmt.Sprintf("%s:$doc_count", name), expr)

		// temporarily remove processed aggregates,
		// so the subsequent transform won't emit
		// them again
		processed[name] = agg
		delete(c.currentAggregations, name)
	}

	ret, err := subContext.transform()

	return ret, err
}

func (f *aggsDateHistogram) process(c *aggsProcessContext) (any, error) {
	result := bucketMultiResult{}
	var totalCount int64

	groups := c.groups()
	if groups != nil {
		result.Buckets = make([]bucketSingleResultWithKey, 0, len(groups.OrderedGroups))
		for _, group := range groups.OrderedGroups {
			var msSinceEpoch int64
			if f.FixedInterval != nil {
				// TIME_BUCKET always returns in seconds since epoch
				msSinceEpoch = int64(group.KeyValues[0].(int)) * 1000
			} else {
				// DATE_PART always return actual timestamp
				var ok bool
				t, ok := group.KeyValues[0].(time.Time)
				if !ok {
					return nil, fmt.Errorf("unexpected return-type from DATE_PART")
				}
				msSinceEpoch = t.UnixMilli()
			}

			if f.HardBounds != nil {
				// TODO: Check if times can be specified in different formats then only Epoch-ms
				if msSinceEpoch < *f.HardBounds.Min || msSinceEpoch > *f.HardBounds.Min {
					continue
				}
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
				Key:       msSinceEpoch,
				KeyFormat: f.Format,
				KeyField:  f.Field,
				Context:   c.context,
			})
			totalCount += docCount
		}
	} else {
		result.Buckets = []bucketSingleResultWithKey{}
	}

	// Add the missing groups
	if f.ExtendedBounds != nil {
		var step time.Duration
		if f.FixedInterval != nil {
			secs, err := f.FixedInterval.Seconds()
			if err != nil {
				return nil, err
			}
			step = time.Duration(secs) * time.Second
		} else if f.CalendarInterval != nil {
			interval := string(*f.CalendarInterval)
			switch interval {
			case "us":
				step = time.Microsecond
			case "ms":
				step = time.Millisecond
			case "s":
				step = time.Second
			case "m":
				step = time.Minute
			case "h":
				step = time.Hour
			case "d":
				step = 24 * time.Hour
			case "w", "M", "q", "y":
				return nil, fmt.Errorf("unsupported interval %q", interval)
			}
		}

		// Round the bound to the nearest interval
		buckets := result.Buckets[:]
		stepSize := int64(step / time.Millisecond)
		for interval := f.ExtendedBounds.Min - (f.ExtendedBounds.Min % stepSize); interval <= f.ExtendedBounds.Max; interval += stepSize {
			found := false
			for _, v := range buckets {
				key := v.Key.(int64)
				if key == interval {
					found = true
					break
				}
			}
			if found {
				continue
			}

			c.docCount = 0
			bucketResult, err := c.subResult(nil)
			if err != nil {
				return nil, err
			}

			result := bucketSingleResultWithKey{
				bucketSingleResult: bucketSingleResult{
					DocCount:        0,
					SubAggregations: bucketResult,
				},
				Key:       interval,
				KeyFormat: f.Format,
				KeyField:  f.Field,
				Context:   c.context,
			}

			buckets = append(buckets, result)
		}

		sort.Slice(buckets, func(i, j int) bool {
			a := buckets[i].Key.(int64)
			b := buckets[j].Key.(int64)
			return a < b
		})
		result.Buckets = buckets
	}

	return &result, nil
}
