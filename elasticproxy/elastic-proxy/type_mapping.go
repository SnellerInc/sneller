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
