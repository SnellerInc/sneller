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
