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

package main

import (
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/SnellerInc/sneller/auth"
)

func (s *server) prepareAuth(spec string) {
	if strings.HasPrefix(spec, "http://") ||
		strings.HasPrefix(spec, "https://") {
		s.auth = &auth.S3Bearer{
			URI: spec,
		}
		return
	}

	// for clarity, allow file:// in the spec
	spec = strings.TrimPrefix(spec, "file://")
	f, err := os.Open(spec)
	if err != nil {
		s.logger.Fatal(err)
	}
	var static fileCreds
	err = json.NewDecoder(f).Decode(&f)
	if err != nil {
		s.logger.Fatal(err)
	}
	if static.Allowed != nil {
		allowed := make(map[string]struct{}, len(static.Allowed))
		notAllowed := errors.New("token not allowed")
		static.CheckToken = func(tok string) error {
			_, ok := allowed[tok]
			if !ok {
				return notAllowed
			}
			return nil
		}
	}
	s.auth = &static
}

type fileCreds struct {
	auth.S3Static
	Allowed []string `json:"allowed_tokens"`
}
