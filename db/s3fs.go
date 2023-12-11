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

package db

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// S3FS is an FS implementation
// that is backed by an S3 bucket.
type S3FS struct {
	blockfmt.S3FS
}

// URL implements db.URL
func (s *S3FS) URL(name, etag string) (string, error) {
	return s3.URL(s.Key, s.Bucket, name)
}

// Encode implements plan.UploadFS
func (s *S3FS) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("key"))
	s.Key.Encode(st, dst)
	dst.BeginField(st.Intern("bucket"))
	dst.WriteString(s.Bucket)
	dst.EndStruct()
	return nil
}

// DecodeS3FS decodes the output of (*S3FS).Encode.
func DecodeS3FS(d ion.Datum) (*S3FS, error) {
	s := &S3FS{}
	err := d.UnpackStruct(func(f ion.Field) error {
		var err error
		switch f.Label {
		case "key":
			s.Key, err = aws.DecodeKey(f.Datum)
		case "bucket":
			s.Bucket, err = f.String()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if s.Key == nil {
		return nil, fmt.Errorf("missing key")
	}
	if s.Bucket == "" {
		return nil, fmt.Errorf("missing bucket")
	}
	s.Ctx = context.Background()
	return s, nil
}

// S3Resolver is a resolver that expects only s3:// schemes.
type S3Resolver struct {
	// DeriveKey is the callback used to
	// derive a key for a particular bucket.
	DeriveKey func(bucket string) (*aws.SigningKey, error)
	// Client, if non-nil, sets the default
	// client used by returned s3.BucketFS objects.
	Client *http.Client
	Ctx    context.Context
}

// Split implements Resolver.Split
func (s *S3Resolver) Split(pattern string) (InputFS, string, error) {
	trimmedPattern, ok := strings.CutPrefix(pattern, "s3://")
	if !ok {
		return nil, "", badPattern(pattern)
	}
	i := strings.IndexByte(trimmedPattern, '/')
	if i == len(pattern)-1 || i <= 0 {
		return nil, "", badPattern(pattern)
	}
	bucket := trimmedPattern[:i]
	rest := trimmedPattern[i+1:]
	if !s3.ValidBucket(bucket) {
		return nil, "", badPattern(pattern)
	}
	key, err := s.DeriveKey(bucket)
	if err != nil {
		return nil, "", err
	}
	return &S3FS{
		S3FS: blockfmt.S3FS{
			BucketFS: s3.BucketFS{
				Key:    key,
				Bucket: bucket,
				Client: s.Client,
				// We'd like reopen(), etc. to be
				// able to logically open many files
				// without performing thousands of
				// simultaneous GET requests
				DelayGet: true,
				Ctx:      s.Ctx,
			},
		},
	}, rest, nil
}
