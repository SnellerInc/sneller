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
	"sort"
	"strings"
)

type aggsBucketSort struct {
	Sort      []SortField `json:"sort"`
	From      int         `json:"from"` // TODO
	Size      int         `json:"size"`
	GapPolicy string      `json:"gap_policy"` // TODO
}

func (f *aggsBucketSort) process(aggName string, data any) error {
	bmr, ok := data.(*bucketMultiResult)
	if !ok {
		return errors.New("bucket sort can only be applied to a set of buckets")
	}

	sort.Slice(bmr.Buckets, func(i, j int) bool {
		a := bmr.Buckets[i]
		b := bmr.Buckets[j]
		for _, sortItem := range f.Sort {
			aField := a.SubAggregations[sortItem.Field]
			bField := b.SubAggregations[sortItem.Field]
			result := compareValues(aField, bField)
			if result < 0 {
				return sortItem.Order == OrderAscending
			} else if result > 0 {
				return sortItem.Order == OrderDescending
			}
		}
		return false
	})

	if f.Size > 0 && f.Size < len(bmr.Buckets) {
		bmr.Buckets = append([]bucketSingleResultWithKey{}, bmr.Buckets[:f.Size]...)
	}

	return nil
}

func compareValues(a, b any) int {
	switch aa := a.(type) {
	case *metricResult:
		bb, ok := b.(*metricResult)
		if !ok {
			return -1
		}
		return compareValues(aa.Value, bb.Value)
	case int:
		bb, ok := b.(int)
		if !ok {
			return -1
		}
		return aa - bb

	case int64:
		bb, ok := b.(int64)
		if !ok {
			return -1
		}
		return int(aa - bb)

	case float64:
		bb, ok := b.(float64)
		if !ok {
			return -1
		}
		return int(aa - bb)

	case bool:
		bb, ok := b.(bool)
		if !ok {
			return -1
		}
		if aa && !bb {
			return -1
		} else if !aa && bb {
			return 1
		}
		return 0

	case string:
		bb, ok := b.(string)
		if !ok {
			return -1
		}
		return strings.Compare(aa, bb)
	}
	return 0
}
