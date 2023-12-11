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
