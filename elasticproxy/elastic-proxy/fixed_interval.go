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
