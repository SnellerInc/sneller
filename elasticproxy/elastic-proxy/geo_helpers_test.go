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
