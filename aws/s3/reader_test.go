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

package s3

import "testing"

func TestValidBuckets(t *testing.T) {
	bucketNames := []string{
		// from AWS docs
		"docexamplebucket1",
		"log-delivery-march-2020",
		"my-hosted-content",

		// from AWS docs (valid, but not recommended)
		"docexamplewebsite.com",
		"www.docexamplewebsite.com",
		"my.example.s3.bucket",

		// additional valid bucket names
		"default",
		"abc",
		"123456789",
		"this.is.a.long.bucket-name",
		"123456789a123456789b123456789c123456789d123456789e123456789f123",
	}
	for _, bucketName := range bucketNames {
		t.Run(bucketName, func(t *testing.T) {
			if !ValidBucket(bucketName) {
				t.Fail()
			}
		})
	}
}

func TestInvalidBuckets(t *testing.T) {
	bucketNames := []string{
		// from AWS docs (invalid)
		"doc_example_bucket",  // contains underscores
		"DocExampleBucket",    // contains uppercase letters
		"doc-example-bucket-", // ends with a hyphen

		// additional invalid bucket names
		"-startwithhyphen",       // starts with a hyphen
		".startwithdot",          // starts with a dot
		"double..dot",            // two consecutive dots
		"xn---invalid-prefix",    // invalid prefix
		"invalid-suffix-s3alias", // invalid suffix
		"a",                      // too short (at least 3 chars)
		"ab",                     // too short (at least 2 chars)
		"123456789a123456789b123456789c123456789d123456789e123456789F1234", // too long (<=63 chars)
		// TODO: IP check is not implemented and is treated as a valid bucket-name
		//"192.168.5.4",		  // IP address
	}
	for _, bucketName := range bucketNames {
		t.Run(bucketName, func(t *testing.T) {
			if ValidBucket(bucketName) {
				t.Fail()
			}
		})
	}
}
