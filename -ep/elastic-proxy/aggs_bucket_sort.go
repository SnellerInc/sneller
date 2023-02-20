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
