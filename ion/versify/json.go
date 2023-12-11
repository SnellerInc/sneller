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

package versify

import (
	"encoding/json"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

// FromJSON computes a Union from all the objects
// that it is able to read from d.
func FromJSON(d *json.Decoder) (Union, *ion.Symtab, error) {
	var st ion.Symtab
	dat, err := ion.FromJSON(&st, d)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	u := Single(dat)
	for {
		dat, err = ion.FromJSON(&st, d)
		if err == io.EOF {
			break
		}
		u = u.Add(dat)
	}
	return u, &st, nil
}
