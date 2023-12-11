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

type fixedInterval struct {
	Interval string
	Factor   int
}

func (fi *fixedInterval) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	fi.Factor = 0
	index := 0
	for index < len(text) {
		ch := text[index]
		if ch >= '0' && ch <= '9' {
			fi.Factor = (fi.Factor * 10) + int(ch-'0')
			index++
		} else {
			break
		}
	}
	if fi.Factor == 0 {
		return invalidCalendarIntervalError(text)
	}

	interval := text[index:]
	switch interval {
	case "ms", "s", "m", "h", "d":
		fi.Interval = interval
	default:
		return invalidCalendarIntervalError(text)
	}

	return nil
}

func invalidCalendarIntervalError(text string) error {
	return fmt.Errorf("invalid time-offset %q", text)
}

func (fi *fixedInterval) Seconds() (int, error) {
	switch fi.Interval {
	case "ms":
		if fi.Factor%1000 != 0 {
			return 0, fmt.Errorf("can't convert %dms to second-value", fi.Factor)
		}
		return fi.Factor / 1000, nil
	case "s":
		return fi.Factor, nil
	case "m":
		return fi.Factor * 60, nil
	case "h":
		return fi.Factor * 60 * 60, nil
	case "d":
		return fi.Factor * 60 * 60 * 24, nil
	default:
		return 0, fmt.Errorf("invalid interval %q", fi.Interval)
	}
}
