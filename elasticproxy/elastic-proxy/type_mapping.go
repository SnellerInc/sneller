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

import "encoding/json"

type TypeMapping struct {
	Type   string            `json:"type"`
	Fields map[string]string `json:"fields,omitempty"`
}

func (tm *TypeMapping) UnmarshalJSON(data []byte) error {
	type _typeMapping TypeMapping
	if err := json.Unmarshal(data, (*_typeMapping)(tm)); err != nil {
		var typeName string
		if err := json.Unmarshal(data, &typeName); err != nil {
			return err
		}
		tm.Type = typeName
		tm.Fields = make(map[string]string, 0)
	}
	return nil
}
