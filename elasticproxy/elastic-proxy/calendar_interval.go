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
