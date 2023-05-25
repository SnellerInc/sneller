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
