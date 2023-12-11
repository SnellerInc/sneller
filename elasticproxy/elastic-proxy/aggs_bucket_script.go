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
