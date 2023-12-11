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
	"encoding/json"
	"fmt"
)

type timeOffset struct {
	Interval string
	Factor   int
}

func (fi *timeOffset) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	if len(text) == 0 {
		return invalidTimeOffsetError(text)
	}

	index := 0
	multiply := 1
	switch text[0] {
	case '+':
		multiply = 1
		index++
	case '-':
		multiply = -1
		index++
	default:
	}

	factor := 0
	for index < len(text) {
		ch := text[index]
		if ch >= '0' && ch <= '9' {
			factor = (factor * 10) + int(ch-'0')
			index++
		} else {
			break
		}
	}
	if factor == 0 {
		return invalidTimeOffsetError(text)
	}
	fi.Factor = factor * multiply

	interval := text[index:]
	switch interval {
	case "nanos":
		fi.Interval = "ns"
	case "micros":
		fi.Interval = "us"
	case "ms", "m", "h", "d":
		fi.Interval = interval
	default:
		return invalidTimeOffsetError(text)
	}

	return nil
}

func invalidTimeOffsetError(text string) error {
	return fmt.Errorf("invalid time-offset %q", text)
}
