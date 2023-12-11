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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
	IndexKey []byte `json:"IndexKey,omitempty"`
	Bucket   string `json:"SnellerBucket"`
	// Credentials is a JSON-compatible
	// representation of the AWS SDK "Credentials" structure
	Credentials S3BearerCredentials `json:"Credentials"`
	// MaxScanBytes is the maximum number of bytes
	// allowed to be scanned on any query.
	MaxScanBytes uint64 `json:"MaxScanBytes"`
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
func (s *S3BearerIdentity) Tenant(ctx context.Context) (db.Tenant, error) {
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
	root.Ctx = ctx
	root.Client = &s3.DefaultClient
	root.Bucket = u.Host
	root.Key = aws.DeriveKey(c.BaseURI, c.AccessKeyID, c.SecretAccessKey, s.Region, "s3")
	root.Key.Token = c.SessionToken
	cfg := &db.TenantConfig{
		MaxScanBytes: s.MaxScanBytes,
	}
	return S3Tenant(ctx, s.ID, root, k, cfg), nil
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
	return identity.Tenant(ctx)
}

// s3Tenant implements db.Tenant
type s3Tenant struct {
	db.S3Resolver
	id   string
	root *db.S3FS
	ikey *blockfmt.Key
	cfg  *db.TenantConfig
}

// S3TenantFromEnv constructs an s3 tenant from the environment.
func S3TenantFromEnv(ctx context.Context, bucket string) (db.Tenant, error) {
	key, err := aws.AmbientKey("s3", s3.DeriveForBucket(bucket))
	if err != nil {
		return nil, err
	}
	root := &db.S3FS{}
	root.Key = key
	root.Bucket = bucket
	root.Ctx = ctx
	var indexkey *blockfmt.Key
	if key := os.Getenv("SNELLER_INDEX_KEY"); key != "" {
		keybytes, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, err
		}
		if len(keybytes) != blockfmt.KeyLength {
			return nil, fmt.Errorf("unexpected SNELLER_INDEX_KEY length %d", len(keybytes))
		}
		indexkey = new(blockfmt.Key)
		copy(indexkey[:], keybytes)
	}
	return S3Tenant(ctx, "", root, indexkey, nil), nil
}

func S3Tenant(ctx context.Context, id string, root *db.S3FS, key *blockfmt.Key, cfg *db.TenantConfig) db.Tenant {
	t := &s3Tenant{
		S3Resolver: db.S3Resolver{
			Ctx: ctx,
		},
		id:   id,
		root: root,
		ikey: key,
		cfg:  cfg,
	}
	t.Client = root.Client
	var bkc bucketKeyCache
	t.DeriveKey = func(bucket string) (*aws.SigningKey, error) {
		return bkc.BucketKey(bucket, t.root.Key)
	}
	return t
}

func (s *s3Tenant) ID() string                { return s.id }
func (s *s3Tenant) Key() *blockfmt.Key        { return s.ikey }
func (s *s3Tenant) Root() (db.InputFS, error) { return s.root, nil }
func (s *s3Tenant) Config() *db.TenantConfig  { return s.cfg }

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
	return f.Tenant(ctx)
}
