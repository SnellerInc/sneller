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

type calendarInterval string

func (ci *calendarInterval) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	switch text {
	case "1m", "minute":
		*ci = "m"
	case "1h", "hour":
		*ci = "h"
	case "1d", "day":
		*ci = "d"
	case "1w", "week":
		*ci = "w"
	case "1M", "month":
		*ci = "M"
	case "1q", "quarter":
		*ci = "q"
	case "1y", "year":
		*ci = "y"
	default:
		return fmt.Errorf("invalid calendar interval %q", text)
	}
	return nil
}
