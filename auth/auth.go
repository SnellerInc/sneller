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

// Package auth describes some
// implementations of Provider
// that can be used in snellerd.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/SnellerInc/sneller/db"
)

// Provider is the interface through which
// HTTP Bearer tokens are turned into db.Tenant objects.
// The purpose of Provider is to hide the details
// mapping tokens to users and users to db.FS implementations.
//
// See, for example, S3Bearer for a Provider that
// uses a remote HTTP(s) endpoint to turn tokens into
// S3 credentials for implementing a Tenant.
type Provider interface {
	Authorize(ctx context.Context, token string) (db.Tenant, error)
}

// Parse will create a provider based on the
// given specification.
//
// It uses an authorization endpoint when a
// http(s):// prefix is detected and otherwise
// the specification is interpreted as a file name.
func Parse(spec string) (Provider, error) {
	if spec == "" {
		return NewEnvProvider()
	}
	if strings.HasPrefix(spec, "http://") || strings.HasPrefix(spec, "https://") {
		return FromEndPoint(spec)
	}
	return FromFile(spec)
}

// FromEndPoint creates an authorization provider that uses
// and endpoint to validate and return the proper credentials.
// See also S3Bearer.
func FromEndPoint(uri string) (Provider, error) {
	return &S3Bearer{
		URI: uri,
	}, nil
}

// FromFile creates an authorization provider that reads
// the credential information from the given file-name.
// See alse S3Static.
func FromFile(fileName string) (Provider, error) {
	// for clarity, allow file:// in the spec
	fileName = strings.TrimPrefix(fileName, "file://")
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	var static fileCreds
	err = json.NewDecoder(f).Decode(&static)
	if err != nil {
		return nil, err
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
	return &static, nil
}

type fileCreds struct {
	S3Static
	Allowed []string `json:"allowed_tokens"`
}
