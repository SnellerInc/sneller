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
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

const (
	LatExt = ".lat"
	LonExt = ".lon"
)

type geoPoint struct {
	Lon, Lat float64
}

func (p *geoPoint) UnmarshalJSON(data []byte) error {
	var text string
	err := json.Unmarshal(data, &text)
	if err != nil {
		var floats []float64
		err = json.Unmarshal(data, &floats)
		if err != nil {
			return err
		}
		if len(floats) != 2 {
			return errors.New("geo-point needs 2 values")
		}
		p.Lon = floats[0]
		p.Lat = floats[1]
		return nil
	}

	re := regexp.MustCompile(`^POINT\s*\((?P<long>[0-9]*(\.[0-9]+)?)\s+(?P<lat>[0-9]*(\.[0-9]+)?)\)$`)
	matches := re.FindStringSubmatch(text)
	var long, lat string
	for i, name := range re.SubexpNames() {
		switch name {
		case "long":
			long = matches[i]
		case "lat":
			lat = matches[i]
		}
	}
	p.Lon, err = strconv.ParseFloat(long, 64)
	if err != nil {
		return err
	}
	p.Lat, err = strconv.ParseFloat(lat, 64)
	if err != nil {
		return err
	}
	return nil
}

func (p *geoPoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("POINT (%g %g)", p.Lon, p.Lat))
}

type geoBounds struct {
	TopLeft     geoPoint `json:"top_left"`
	BottomRight geoPoint `json:"bottom_right"`
}
