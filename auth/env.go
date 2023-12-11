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
	id, secret, region, sessionToken, err := aws.AmbientCreds()
	if err != nil {
		return nil, err
	}

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
