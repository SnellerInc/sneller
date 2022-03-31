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

package aws

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Metadata fetches EC2 instance metadata
// from the given path and returns an
// io.ReadCloser containing the body
// of the metadata.
//
// See also MetadataJSON and MetadataString.
func Metadata(path string) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("fetching metadata api token: %s", res.Status)
	}
	token, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	req, err = http.NewRequest(http.MethodGet, "http://169.254.169.254/latest/meta-data/"+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Aws-Ec2-Metadata-Token", string(token))
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, fmt.Errorf("aws.Metadata: %s", res.Status)
	}
	return res.Body, nil
}

// MetadataJSON decodes the metadata from 'path'
// into the json object 'into'
func MetadataJSON(path string, into interface{}) error {
	r, err := Metadata(path)
	if err != nil {
		return err
	}
	defer r.Close()
	return json.NewDecoder(r).Decode(into)
}

// MetadataString fetches the metdata
// from the provided path and returns it
// as a string.
func MetadataString(path string) (string, error) {
	r, err := Metadata(path)
	if err != nil {
		return "", err
	}
	defer r.Close()
	buf, err := io.ReadAll(r)
	return string(buf), err
}

// when we are running on EC2,
// we can guess the region thusly:
func ec2Region() (string, error) {
	str, err := MetadataString("placement/availability-zone")
	if err != nil {
		return "", err
	}
	if len(str) == 0 || str[len(str)-1] < 'a' || str[len(str)-1] > 'z' {
		return "", fmt.Errorf("unexpected AZ string %q", str)
	}
	return str[:len(str)-1], nil
}
