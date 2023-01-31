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

package db

import (
	"context"
	"fmt"
	"io/fs"
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
func (s *S3FS) URL(name string, info fs.FileInfo, etag string) (string, error) {
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
	if !strings.HasPrefix(pattern, "s3://") {
		return nil, "", badPattern(pattern)
	}
	trimmedPattern := strings.TrimPrefix(pattern, "s3://")
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
