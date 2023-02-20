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
