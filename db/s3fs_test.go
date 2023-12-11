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
	"testing"

	"github.com/SnellerInc/sneller/aws"
)

func TestSplit(t *testing.T) {
	s := S3Resolver{
		DeriveKey: func(_ string) (*aws.SigningKey, error) {
			return nil, nil
		},
	}
	fs, rest, err := s.Split("s3://bucket.name/object/key")
	if err != nil {
		t.Fail()
	}
	s3fs, ok := fs.(*S3FS)
	if !ok || s3fs.Bucket != "bucket.name" || rest != "object/key" {
		t.Fail()
	}
}
