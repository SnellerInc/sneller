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

import "testing"

func TestFormatMatcher(t *testing.T) {
	mapping := map[string]TypeMapping{
		"client_timestamp": TypeMapping{Type: "unix_nano_seconds"},
		"server_timestamp": TypeMapping{Type: "unix_nano_seconds"},
		"u_*":              TypeMapping{Type: "string"},
		"u_date_*":         TypeMapping{Type: "datetime"},
	}
	tests := map[string]string{
		"client_timestamp": "unix_nano_seconds",
		"server_timestamp": "unix_nano_seconds",
		"u_test_field":     "string",
		"u_date_test":      "datetime",
		"invalid":          "",
	}
	for k, v := range tests {
		t.Run(k, func(t *testing.T) {
			result, _ := format(k, mapping)
			if result != v {
				t.Fatalf("expected %q, got %q", v, result)
			}
		})
	}
}
