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
	"context"
	"strings"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/vm"
)

func s3split(name string) (bucket, object string) {
	name = strings.TrimPrefix(name, "s3://")
	splitidx := strings.IndexByte(name, '/')
	if splitidx == -1 {
		return name, ""
	}
	return name[:splitidx], name[splitidx+1:]
}

func s3key(bucket string) *aws.SigningKey {
	sk, err := aws.AmbientKey("s3", s3.DeriveForBucket(bucket))
	if err != nil {
		exit(err)
	}
	return sk
}

func s3object(name string) (vm.Table, error) {
	bucket, object := s3split(name)
	key := s3key(bucket)
	obj, err := s3.Stat(key, bucket, object)
	if err != nil {
		return nil, err
	}
	return srcTable(obj, obj.Size, nil)
}

// s3-backed NDJSON table
func s3nd(name string) (vm.Table, error) {
	bucket, object := s3split(name)
	key := s3key(bucket)
	obj, err := s3.Stat(key, bucket, object)
	if err != nil {
		return nil, err
	}
	return &jstable{in: obj, size: obj.Size}, nil
}

func s3fs(name string) *db.S3FS {
	bucket, object := s3split(name)
	if object != "" {
		exitf("S3 object key prefixes are not supported")
	}
	key := s3key(bucket)
	root := &db.S3FS{}
	root.Client = &s3.DefaultClient
	root.Bucket = bucket
	root.Key = key
	root.Ctx = context.Background()
	return root
}
