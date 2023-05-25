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
	"fmt"
	"strings"
)

type aggsBucketScript struct {
	BucketsPath map[string]string `json:"buckets_path"`
	Script      string            `json:"script"`
}

func (f *aggsBucketScript) process(aggName string, data any) error {
	bmr, ok := data.(*bucketMultiResult)
	if !ok {
		return errors.New("bucket sort can only be applied to a set of buckets")
	}

	// "parse" script
	parts := strings.Split(f.Script, " ")
	if len(parts) != 3 {
		return fmt.Errorf("unsupported script %q", f.Script)
	}
	left := parts[0]
	op := parts[1]
	right := parts[2]
	if !strings.HasPrefix(left, "params.") || !strings.HasPrefix(right, "params.") || op != "+" {
		return fmt.Errorf("unsupported script %q", f.Script)
	}

	leftParam := left[7:]
	rightParam := right[7:]
	leftAggField, ok := f.BucketsPath[leftParam]
	if !ok {
		return fmt.Errorf("can't find param %q in script %q", leftParam, f.Script)
	}
	rightAggField, ok := f.BucketsPath[rightParam]
	if !ok {
		return fmt.Errorf("can't find param %q in script %q", rightParam, f.Script)
	}

	// TODO: Convert into an actual script execution
	for _, bucket := range bmr.Buckets {
		leftValue := 0
		if leftAgg, ok := bucket.SubAggregations[leftAggField].(*metricResult); ok {
			leftValue, _ = leftAgg.Value.(int)
		}
		rightValue := 0
		if rightAgg, ok := bucket.SubAggregations[rightAggField].(*metricResult); ok {
			rightValue, _ = rightAgg.Value.(int)
		}
		bucket.SubAggregations[aggName] = &metricResult{Value: leftValue + rightValue}
	}

	return nil
}
