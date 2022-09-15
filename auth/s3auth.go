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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

var _ Provider = &S3Bearer{}

// S3Bearer is a tenant authorization strategy
// that produces a db.Tenant from a remote HTTP(s) endpoint
// by passing it an opaque token. The remote HTTP(s) endpoint
// is expected to return a JSON object describing the S3
// bucket and access credentials necessary for the tenant to operate.
// See also S3BearerIdentity.
type S3Bearer struct {
	URI    string
	Client *http.Client
}

// S3BearerIdentity describes the JSON object that
// should be returned from the HTTP server implementing
// the S3Bearer API.
type S3BearerIdentity struct {
	ID       string `json:"TenantID"`
	Region   string `json:"Region"`
	IndexKey []byte `json:"IndexKey"`
	Bucket   string `json:"SnellerBucket"`
	// Credentials is a JSON-compatible
	// representation of the AWS SDK "Credentials" structure
	Credentials S3BearerCredentials `json:"Credentials"`
}

type S3BearerCredentials struct {
	BaseURI         string    `json:"BaseURI,omitempty"`
	AccessKeyID     string    `json:"AccessKeyID"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	SessionToken    string    `json:"SessionToken,omitempty"`
	Source          string    `json:"Source,omitempty"`
	Expires         time.Time `json:"Expires,omitempty"`
	CanExpire       bool      `json:"CanExpire"`
}

// Expired indicates whether or not the
// credentials in the identity have expired.
func (s *S3BearerIdentity) Expired() bool {
	return s.Credentials.CanExpire && s.Credentials.Expires.Before(time.Now())
}

// Tenant converts the S3BearerIdentity
// into a db.Tenant. Tenant will perform some
// validation of the fields in s to confirm
// that it describes a valid configuration.
func (s *S3BearerIdentity) Tenant() (db.Tenant, error) {
	u, err := url.Parse(s.Bucket)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("bad scheme %q in S3BearerIdentity.Bucket", u.Scheme)
	}
	if !s3.ValidBucket(u.Host) {
		return nil, fmt.Errorf("bucket %q is invalid", s.Bucket)
	}
	k := new(blockfmt.Key)
	if copy(k[:], s.IndexKey) != len(k[:]) {
		return nil, fmt.Errorf("invalid len(IndexKey)=%d", len(s.IndexKey))
	}
	c := &s.Credentials
	if s.Expired() {
		return nil, fmt.Errorf("credentials already expired at %s", c.Expires)
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" || s.Region == "" {
		return nil, fmt.Errorf("S3BearerIdentity missing proper credentials")
	}
	root := &db.S3FS{}
	root.Client = &s3.DefaultClient
	root.Bucket = u.Host
	root.Key = aws.DeriveKey(c.BaseURI, c.AccessKeyID, c.SecretAccessKey, s.Region, "s3")
	root.Key.Token = c.SessionToken
	return S3Tenant(s.ID, root, k), nil
}

func (s *S3Bearer) client() *http.Client {
	if s.Client == nil {
		return http.DefaultClient
	}
	return s.Client
}

// Authorize implements Provider.Authorize
//
// The provided token is forwarded verbatim to
// s.URI. The response is expected to be a JSON
// object matching structure of S3BearerIdentity.
func (s *S3Bearer) Authorize(ctx context.Context, token string) (db.Tenant, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URI, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := s.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// don't read an arbitrarily large response
		text := make([]byte, 1024)
		n, _ := io.ReadFull(res.Body, text)
		return nil, fmt.Errorf("S3Bearer: code %d (%q)", res.StatusCode, text[:n])
	}
	var identity S3BearerIdentity
	err = json.NewDecoder(res.Body).Decode(&identity)
	if err != nil {
		return nil, err
	}
	return identity.Tenant()
}

// s3Tenant implements db.Tenant
type s3Tenant struct {
	db.S3Resolver
	id   string
	root *db.S3FS
	ikey *blockfmt.Key
}

func S3Tenant(id string, root *db.S3FS, key *blockfmt.Key) db.Tenant {
	t := &s3Tenant{
		id:   id,
		root: root,
		ikey: key,
	}
	t.Client = root.Client
	t.DeriveKey = func(string) (*aws.SigningKey, error) {
		return t.root.Key, nil
	}
	return t
}

func (s *s3Tenant) ID() string                { return s.id }
func (s *s3Tenant) Key() *blockfmt.Key        { return s.ikey }
func (s *s3Tenant) Root() (db.InputFS, error) { return s.root, nil }

// S3Static is a Provider that is backed
// by a single static S3 identity.
type S3Static struct {
	// CheckToken is used to validate
	// tokens in Authorize.
	// If CheckToken is nil, then all
	// tokens are accepted.
	CheckToken func(token string) error
	// S3BearerIdentity is the embedded
	// static identity that is used to
	// implement the db.Tenant returned
	// from Authorize.
	S3BearerIdentity
}

// Authorize implements Provider.Authorize
func (f *S3Static) Authorize(ctx context.Context, token string) (db.Tenant, error) {
	if f.CheckToken != nil {
		err := f.CheckToken(token)
		if err != nil {
			return nil, err
		}
	}
	return f.Tenant()
}
