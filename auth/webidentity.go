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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// NewWebIdentityProvider returns a provider that allows
// fetching AWS credentials using a web-identity token.
// It returns a `nil` provider, when one of the required
// environment variables isn't set.
func NewWebIdentityProvider() (Provider, error) {
	region := os.Getenv("AWS_REGION")
	roleARN := os.Getenv("AWS_ROLE_ARN")
	webIdentityTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if region == "" || roleARN == "" || webIdentityTokenFile == "" {
		return nil, nil
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

	return &webIdentityProvider{
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
				CanExpire: true,
			},
		},
		S3Endpoint: aws.S3EndPoint(region),
	}, nil
}

type webIdentityProvider struct {
	S3BearerIdentity

	CheckToken func(t string) error

	S3Endpoint string
	lock       sync.Mutex
}

func (p *webIdentityProvider) Authorize(ctx context.Context, token string) (db.Tenant, error) {
	if p.CheckToken != nil {
		err := p.CheckToken(token)
		if err != nil {
			return nil, err
		}
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if p.Expired() {
		id, secret, _, token, expiration, err := aws.WebIdentityCreds(nil)
		if err != nil {
			return nil, fmt.Errorf("can't exchange web-identity token to AWS credentials: %w", err)
		}

		p.Credentials = S3BearerCredentials{
			BaseURI:         p.S3Endpoint,
			Source:          p.Bucket,
			AccessKeyID:     id,
			SecretAccessKey: secret,
			SessionToken:    token,
			Expires:         expiration,
			CanExpire:       true,
		}
	}

	return p.Tenant(ctx)
}
