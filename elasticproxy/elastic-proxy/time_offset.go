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
