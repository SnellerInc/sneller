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
	"testing"
)

func TestGeoPointMarshal(t *testing.T) {
	type item struct {
		Point geoPoint
		Text  string
	}
	items := []item{
		{geoPoint{Lon: 4.9, Lat: 52.4}, `"POINT (4.9 52.4)"`},
	}
	for i, gp := range items {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			jsonData, err := json.Marshal(&gp.Point)
			if err != nil {
				t.Fatalf("cannot marshal geo-point: %v", err)
			}
			if string(jsonData) != gp.Text {
				t.Fatalf("got: %q, expected: %q", string(jsonData), gp.Text)
			}
			var p geoPoint
			if err = json.Unmarshal(jsonData, &p); err != nil {
				t.Fatalf("cannot unmarshal geo-point %q: %v", string(jsonData), err)
			}
			if p.Lat != gp.Point.Lat {
				t.Errorf("got lat: %g, expected lat: %g", p.Lat, gp.Point.Lat)
			}
			if p.Lon != gp.Point.Lon {
				t.Errorf("got long: %g, expected long: %g", p.Lat, gp.Point.Lat)
			}
		})
	}
}
