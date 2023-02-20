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

const (
	OrderAscending  = "ASC"
	OrderDescending = "DESC"
)

type order map[string]Ordering

type Ordering string

func (o *Ordering) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	switch text {
	case "asc":
		*o = OrderAscending
	case "desc":
		*o = OrderDescending
	default:
		return fmt.Errorf("unknown order %q", text)
	}
	return nil
}
