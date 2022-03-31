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
