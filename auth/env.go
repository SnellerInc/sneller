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

package auth

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func NewEnvProvider() (Provider, error) {
	tenantID := os.Getenv("SNELLER_TENANT_ID")
	if tenantID == "" {
		tenantID = "default"
	}

	bucket := os.Getenv("SNELLER_BUCKET")
	if bucket == "" {
		return nil, errors.New("missing SNELLER_BUCKET variable")
	}

	indexKeyText := os.Getenv("SNELLER_INDEX_KEY")
	if indexKeyText == "" {
		return nil, errors.New("missing SNELLER_INDEX_KEY variable")
	}
	indexKeyBytes, err := base64.StdEncoding.DecodeString(indexKeyText)
	if err != nil {
		return nil, fmt.Errorf("invalid SNELLER_INDEX_KEY variable: %v", err)
	}
	if len(indexKeyBytes) != blockfmt.KeyLength {
		return nil, fmt.Errorf("invalid SNELLER_INDEX_KEY variable: decoded length should be %d bytes", blockfmt.KeyLength)
	}

	snellerToken := os.Getenv("SNELLER_TOKEN")
	if snellerToken == "" {
		return nil, errors.New("missing SNELLER_TOKEN variable")
	}

	id, secret, region, sessionToken, err := aws.AmbientCreds()
	if err != nil {
		return nil, err
	}

	return &S3Static{
		CheckToken: func(t string) error {
			if t != snellerToken {
				return errors.New("incorrect token")
			}
			return nil
		},
		S3BearerIdentity: S3BearerIdentity{
			ID:       tenantID,
			Region:   region,
			IndexKey: indexKeyBytes,
			Bucket:   bucket,
			Credentials: S3BearerCredentials{
				AccessKeyID:     id,
				SecretAccessKey: secret,
				SessionToken:    sessionToken,
				Source:          bucket,
				BaseURI:         aws.S3EndPoint(region),
			},
		},
	}, nil
}
